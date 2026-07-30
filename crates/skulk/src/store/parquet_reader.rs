//! Minimal active-set Parquet reader for wide rows.

use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use crate::store::buffer::{
    COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND, INGEST_SEQ_COLUMN, INGEST_SEQ_COLUMN_KIND,
    TAG_COLUMN_KIND, TIME_COLUMN, TIME_COLUMN_KIND,
};
use crate::store::manifest::{ActiveFile, ManifestState};
use crate::store::seq::{IngestSeq, SequencedRow};
use arrow_array::RecordBatch;
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Per-batch and aggregate row allocation limits for minimal reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetReaderConfig {
    batch_rows: usize,
    max_total_rows: usize,
}

impl ParquetReaderConfig {
    /// Creates non-zero read allocation limits.
    pub fn new(batch_rows: usize, max_total_rows: usize) -> Result<Self> {
        if batch_rows == 0 || max_total_rows == 0 {
            return Err(TsmError::InvalidInput(
                "Parquet reader row limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            batch_rows,
            max_total_rows,
        })
    }
}

impl Default for ParquetReaderConfig {
    fn default() -> Self {
        Self {
            batch_rows: 8_192,
            max_total_rows: 1_000_000,
        }
    }
}

/// Reads active Parquet files back into typed sequenced wide rows.
pub struct ParquetReader {
    config: ParquetReaderConfig,
}

impl ParquetReader {
    /// Creates a minimal reader with explicit allocation limits.
    pub const fn new(config: ParquetReaderConfig) -> Self {
        Self { config }
    }

    /// Reads only active files belonging to one measurement.
    pub fn read_measurement(
        &self,
        manifest: &ManifestState,
        segments_dir: impl AsRef<Path>,
        measurement: &str,
    ) -> Result<Vec<SequencedRow>> {
        let files = manifest
            .active_files()
            .values()
            .filter(|file| file.measurement() == measurement)
            .cloned()
            .collect::<Vec<_>>();
        self.read_active_files(&files, segments_dir.as_ref(), measurement)
    }

    pub(crate) fn read_active_files(
        &self,
        files: &[ActiveFile],
        segments_dir: &Path,
        measurement: &str,
    ) -> Result<Vec<SequencedRow>> {
        if files.iter().any(|file| file.measurement() != measurement) {
            return Err(TsmError::InvalidInput(
                "selected Parquet file belongs to another measurement".into(),
            ));
        }
        let expected_rows = files.iter().try_fold(0_usize, |total, file| {
            let rows = usize::try_from(file.row_count())
                .map_err(|_| TsmError::ResourceLimit("manifest row count exceeds usize".into()))?;
            total
                .checked_add(rows)
                .ok_or_else(|| TsmError::ResourceLimit("read row count overflow".into()))
        })?;
        if expected_rows > self.config.max_total_rows {
            return Err(TsmError::ResourceLimit(format!(
                "active measurement has {expected_rows} rows, read limit is {}",
                self.config.max_total_rows
            )));
        }

        let mut rows = Vec::with_capacity(expected_rows);
        for active_file in files {
            let before = rows.len();
            let mut series_cache = DecodedSeriesCache::default();
            let file = File::open(segments_dir.join(active_file.name()))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(parquet_error)?
                .with_batch_size(self.config.batch_rows)
                .build()
                .map_err(parquet_error)?;
            for batch in reader {
                decode_batch_with_series_cache(
                    measurement,
                    &batch.map_err(arrow_error)?,
                    &mut series_cache,
                    &mut rows,
                )?;
                if rows.len() > self.config.max_total_rows {
                    return Err(TsmError::ResourceLimit(
                        "decoded rows exceed reader limit".into(),
                    ));
                }
            }
            let decoded = rows.len() - before;
            if decoded as u64 != active_file.row_count() {
                return Err(TsmError::Corruption(format!(
                    "Parquet '{}' row count {decoded} differs from manifest {}",
                    active_file.name(),
                    active_file.row_count()
                )));
            }
            let decoded_rows = &rows[before..];
            let min_timestamp = decoded_rows
                .iter()
                .map(|row| row.row().timestamp())
                .min()
                .ok_or_else(|| {
                    TsmError::Corruption(format!(
                        "active Parquet '{}' contains no rows",
                        active_file.name()
                    ))
                })?;
            let max_timestamp = decoded_rows
                .iter()
                .map(|row| row.row().timestamp())
                .max()
                .ok_or_else(|| {
                    TsmError::Corruption(format!(
                        "active Parquet '{}' contains no rows",
                        active_file.name()
                    ))
                })?;
            if min_timestamp != active_file.min_timestamp()
                || max_timestamp != active_file.max_timestamp()
            {
                return Err(TsmError::Corruption(format!(
                    "Parquet '{}' timestamp range {min_timestamp}..={max_timestamp} differs from manifest {}..={}",
                    active_file.name(),
                    active_file.min_timestamp(),
                    active_file.max_timestamp()
                )));
            }
        }
        rows.sort_by_key(SequencedRow::ingest_seq);
        Ok(rows)
    }
}

#[derive(Default)]
pub(crate) struct DecodedSeriesCache {
    last: Option<Arc<SeriesKey>>,
}

pub(crate) struct DecodedFloatSample {
    pub(crate) series: Arc<SeriesKey>,
    pub(crate) timestamp: i64,
    pub(crate) ingest_seq: IngestSeq,
    pub(crate) value: Option<f64>,
}

pub(crate) fn decode_float_batch_with_series_cache(
    measurement: &str,
    field_name: &str,
    batch: &RecordBatch,
    series_cache: &mut DecodedSeriesCache,
    output: &mut Vec<DecodedFloatSample>,
) -> Result<bool> {
    let schema = batch.schema();
    let time_index = schema.index_of(TIME_COLUMN).map_err(arrow_error)?;
    let sequence_index = schema.index_of(INGEST_SEQ_COLUMN).map_err(arrow_error)?;
    validate_system_kind(schema.field(time_index), TIME_COLUMN_KIND)?;
    validate_system_kind(schema.field(sequence_index), INGEST_SEQ_COLUMN_KIND)?;
    let field_index = schema.index_of(field_name).ok();
    if let Some(field_index) = field_index {
        let field = schema.field(field_index);
        if field
            .metadata()
            .get(COLUMN_KIND_METADATA_KEY)
            .map(String::as_str)
            != Some(FIELD_COLUMN_KIND)
            || field.data_type() != &DataType::Float64
        {
            return Ok(false);
        }
    }

    let times = batch
        .column(time_index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| TsmError::Corruption("Parquet time column is not timestamp(ns)".into()))?;
    let sequences = batch
        .column(sequence_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| TsmError::Corruption("Parquet ingest sequence is not u64".into()))?;
    let values = field_index
        .map(|field_index| {
            batch
                .column(field_index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| TsmError::Corruption("Parquet float field is not float64".into()))
        })
        .transpose()?;
    let tags = tag_columns(schema.fields(), time_index, sequence_index)?;

    for row_index in 0..batch.num_rows() {
        if times.is_null(row_index) || sequences.is_null(row_index) {
            return Err(TsmError::Corruption(
                "Parquet system columns contain null".into(),
            ));
        }
        output.push(DecodedFloatSample {
            series: decoded_series(measurement, batch, &tags, row_index, series_cache)?,
            timestamp: times.value(row_index),
            ingest_seq: IngestSeq::new(sequences.value(row_index)),
            value: values
                .filter(|values| !values.is_null(row_index))
                .map(|values| values.value(row_index)),
        });
    }
    Ok(true)
}

pub(crate) fn decode_batch_with_series_cache(
    measurement: &str,
    batch: &RecordBatch,
    series_cache: &mut DecodedSeriesCache,
    output: &mut Vec<SequencedRow>,
) -> Result<()> {
    let schema = batch.schema();
    let time_index = schema.index_of(TIME_COLUMN).map_err(arrow_error)?;
    let sequence_index = schema.index_of(INGEST_SEQ_COLUMN).map_err(arrow_error)?;
    validate_system_kind(schema.field(time_index), TIME_COLUMN_KIND)?;
    validate_system_kind(schema.field(sequence_index), INGEST_SEQ_COLUMN_KIND)?;
    let times = batch
        .column(time_index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| TsmError::Corruption("Parquet time column is not timestamp(ns)".into()))?;
    let sequences = batch
        .column(sequence_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| TsmError::Corruption("Parquet ingest sequence is not u64".into()))?;

    let tags = tag_columns(schema.fields(), time_index, sequence_index)?;
    let mut fields = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if index == time_index || index == sequence_index {
            continue;
        }
        match field
            .metadata()
            .get(COLUMN_KIND_METADATA_KEY)
            .map(String::as_str)
        {
            Some(TAG_COLUMN_KIND) if field.data_type() == &DataType::Utf8 => {}
            Some(FIELD_COLUMN_KIND) => fields.push((index, field.name().clone())),
            _ => {
                return Err(TsmError::Corruption(format!(
                    "Parquet column '{}' has invalid Skulk metadata",
                    field.name()
                )));
            }
        }
    }

    for row_index in 0..batch.num_rows() {
        if times.is_null(row_index) || sequences.is_null(row_index) {
            return Err(TsmError::Corruption(
                "Parquet system columns contain null".into(),
            ));
        }
        let series = decoded_series(measurement, batch, &tags, row_index, series_cache)?;
        let series_id = series.id();
        let mut row_fields = Fields::new();
        for (index, name) in &fields {
            if let Some(value) = decode_field(batch, *index, row_index)? {
                row_fields.insert(name.clone(), value);
            }
        }
        output.push(SequencedRow::new(
            IngestSeq::new(sequences.value(row_index)),
            WideRow::with_shared_series(series, series_id, times.value(row_index), row_fields),
        ));
    }
    Ok(())
}

fn tag_columns(
    fields: &arrow_schema::Fields,
    time_index: usize,
    sequence_index: usize,
) -> Result<Vec<(usize, String)>> {
    let mut tags = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        if index == time_index || index == sequence_index {
            continue;
        }
        match field
            .metadata()
            .get(COLUMN_KIND_METADATA_KEY)
            .map(String::as_str)
        {
            Some(TAG_COLUMN_KIND) if field.data_type() == &DataType::Utf8 => {
                tags.push((index, field.name().clone()));
            }
            Some(FIELD_COLUMN_KIND) => {}
            _ => {
                return Err(TsmError::Corruption(format!(
                    "Parquet column '{}' has invalid Skulk metadata",
                    field.name()
                )));
            }
        }
    }
    Ok(tags)
}

fn decoded_series(
    measurement: &str,
    batch: &RecordBatch,
    tags: &[(usize, String)],
    row_index: usize,
    cache: &mut DecodedSeriesCache,
) -> Result<Arc<SeriesKey>> {
    if let Some(series) = &cache.last {
        if series.measurement() == measurement
            && series_matches_row(series, batch, tags, row_index)?
        {
            return Ok(Arc::clone(series));
        }
    }

    let mut row_tags = Tags::new();
    for (index, name) in tags {
        let values = batch
            .column(*index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TsmError::Corruption("tag column is not Utf8".into()))?;
        if !values.is_null(row_index) {
            row_tags.insert(name.clone(), values.value(row_index).to_owned());
        }
    }
    let series = Arc::new(SeriesKey::new(measurement, row_tags));
    cache.last = Some(Arc::clone(&series));
    Ok(series)
}

fn series_matches_row(
    series: &SeriesKey,
    batch: &RecordBatch,
    tags: &[(usize, String)],
    row_index: usize,
) -> Result<bool> {
    if series.tags().len() > tags.len() {
        return Ok(false);
    }
    for (index, name) in tags {
        let values = batch
            .column(*index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TsmError::Corruption("tag column is not Utf8".into()))?;
        let actual = (!values.is_null(row_index)).then(|| values.value(row_index));
        if series.tags().get(name).map(String::as_str) != actual {
            return Ok(false);
        }
    }
    Ok(true)
}

fn decode_field(batch: &RecordBatch, column: usize, row: usize) -> Result<Option<FieldValue>> {
    let array = batch.column(column);
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Float64 => FieldValue::Float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(type_corruption)?
                .value(row),
        ),
        DataType::Int64 => FieldValue::Integer(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(type_corruption)?
                .value(row),
        ),
        DataType::UInt64 => FieldValue::Unsigned(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(type_corruption)?
                .value(row),
        ),
        DataType::Boolean => FieldValue::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(type_corruption)?
                .value(row),
        ),
        DataType::Utf8 => FieldValue::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(type_corruption)?
                .value(row)
                .to_owned(),
        ),
        data_type => {
            return Err(TsmError::InvalidFormat(format!(
                "unsupported Skulk field Arrow type {data_type}"
            )));
        }
    };
    Ok(Some(value))
}

fn validate_system_kind(field: &Field, expected: &str) -> Result<()> {
    if field
        .metadata()
        .get(COLUMN_KIND_METADATA_KEY)
        .map(String::as_str)
        != Some(expected)
    {
        return Err(TsmError::Corruption(format!(
            "system column '{}' has invalid metadata",
            field.name()
        )));
    }
    Ok(())
}

fn type_corruption() -> TsmError {
    TsmError::Corruption("Arrow array does not match declared field type".into())
}

fn arrow_error(error: ArrowError) -> TsmError {
    TsmError::InvalidFormat(format!("Arrow schema error: {error}"))
}

fn parquet_error(error: parquet::errors::ParquetError) -> TsmError {
    TsmError::InvalidFormat(format!("Parquet read failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{ParquetReader, ParquetReaderConfig};
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::buffer::{FlushPolicy, MeasurementBuffer};
    use crate::store::manifest::{ActiveFile, ManifestStore, ManifestUpdate};
    use crate::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
    use crate::store::seq::{IngestSeq, SequencedRow};

    fn sequenced(
        sequence: u64,
        measurement: &str,
        tags: Tags,
        timestamp: i64,
        fields: Fields,
    ) -> SequencedRow {
        SequencedRow::new(
            IngestSeq::new(sequence),
            WideRow::new(SeriesKey::new(measurement, tags), timestamp, fields),
        )
    }

    fn persist(
        store: &ManifestStore,
        measurement: &str,
        name: &str,
        rows: &[SequencedRow],
    ) -> ActiveFile {
        let mut buffer = MeasurementBuffer::new(
            measurement,
            FlushPolicy::new(100, 1024 * 1024).expect("policy"),
        );
        for row in rows {
            buffer.append(row).expect("buffer");
        }
        let batch = buffer.drain_sorted().expect("batch");
        let written = ParquetWriter::new(ParquetWriterConfig::default())
            .write_atomic(store.segments_dir().join(name), &batch)
            .expect("write");
        let min_timestamp = rows
            .iter()
            .map(|row| row.row().timestamp())
            .min()
            .expect("non-empty rows");
        let max_timestamp = rows
            .iter()
            .map(|row| row.row().timestamp())
            .max()
            .expect("non-empty rows");
        ActiveFile::new(
            measurement,
            name,
            written.row_count() as u64,
            written.file_bytes(),
            min_timestamp,
            max_timestamp,
        )
        .expect("active file")
    }

    #[test]
    fn active_measurement_round_trips_sparse_rows_and_all_field_types() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let first = sequenced(
            1,
            "cpu",
            Tags::from([("host".into(), "edge-a".into())]),
            10,
            Fields::from([
                ("float".into(), FieldValue::Float(1.25)),
                ("integer".into(), FieldValue::Integer(-2)),
                ("string".into(), FieldValue::String("ready".into())),
            ]),
        );
        let second = sequenced(
            2,
            "cpu",
            Tags::from([
                ("host".into(), "edge-b".into()),
                ("region".into(), "east".into()),
            ]),
            20,
            Fields::from([
                ("boolean".into(), FieldValue::Boolean(true)),
                ("unsigned".into(), FieldValue::Unsigned(3)),
            ]),
        );
        let cpu = persist(
            &store,
            "cpu",
            "cpu.parquet",
            &[first.clone(), second.clone()],
        );
        let memory_row = sequenced(3, "memory", Tags::new(), 30, Fields::new());
        let memory = persist(&store, "memory", "memory.parquet", &[memory_row]);
        store
            .publish(ManifestUpdate::new().add_file(cpu).add_file(memory))
            .expect("publish");

        let rows = ParquetReader::new(ParquetReaderConfig::default())
            .read_measurement(&store.state().expect("state"), store.segments_dir(), "cpu")
            .expect("read cpu");

        assert_eq!(rows, [first, second]);
        assert_eq!(rows[0].row().field("boolean"), None);
        assert_eq!(rows[1].row().field("float"), None);
        assert_eq!(
            rows[1]
                .row()
                .series()
                .tags()
                .get("region")
                .map(String::as_str),
            Some("east")
        );
    }

    #[test]
    fn manifest_row_limit_is_checked_before_read_allocation() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [
            sequenced(1, "cpu", Tags::new(), 1, Fields::new()),
            sequenced(2, "cpu", Tags::new(), 2, Fields::new()),
        ];
        let cpu = persist(&store, "cpu", "cpu.parquet", &rows);
        store
            .publish(ManifestUpdate::new().add_file(cpu))
            .expect("publish");

        let reader = ParquetReader::new(ParquetReaderConfig::new(1, 1).expect("config"));
        assert!(reader
            .read_measurement(&store.state().expect("state"), store.segments_dir(), "cpu",)
            .is_err());
    }

    #[test]
    fn manifest_timestamp_range_must_match_the_parquet_rows() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [sequenced(
            1,
            "cpu",
            Tags::new(),
            42,
            Fields::from([("value".into(), FieldValue::Integer(1))]),
        )];
        let actual = persist(&store, "cpu", "cpu.parquet", &rows);
        let wrong = ActiveFile::new(
            "cpu",
            actual.name(),
            actual.row_count(),
            actual.file_bytes(),
            0,
            0,
        )
        .expect("wrong metadata");
        store
            .publish(ManifestUpdate::new().add_file(wrong))
            .expect("publish");

        assert!(ParquetReader::new(ParquetReaderConfig::default())
            .read_measurement(&store.state().expect("state"), store.segments_dir(), "cpu")
            .is_err());
    }
}
