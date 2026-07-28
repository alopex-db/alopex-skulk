//! Minimal active-set Parquet reader for wide rows.

use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use crate::store::buffer::{
    COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND, INGEST_SEQ_COLUMN, INGEST_SEQ_COLUMN_KIND,
    TAG_COLUMN_KIND, TIME_COLUMN, TIME_COLUMN_KIND,
};
use crate::store::manifest::ManifestState;
use crate::store::seq::{IngestSeq, SequencedRow};
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

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
            .collect::<Vec<_>>();
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
            let file = File::open(segments_dir.as_ref().join(active_file.name()))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(parquet_error)?
                .with_batch_size(self.config.batch_rows)
                .build()
                .map_err(parquet_error)?;
            for batch in reader {
                decode_batch(measurement, &batch.map_err(arrow_error)?, &mut rows)?;
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
        }
        rows.sort_by_key(SequencedRow::ingest_seq);
        Ok(rows)
    }
}

fn decode_batch(
    measurement: &str,
    batch: &RecordBatch,
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

    let mut tags = Vec::new();
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
            Some(TAG_COLUMN_KIND) if field.data_type() == &DataType::Utf8 => {
                tags.push((index, field.name().clone()));
            }
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
        let mut row_tags = Tags::new();
        for (index, name) in &tags {
            let values = batch
                .column(*index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| TsmError::Corruption("tag column is not Utf8".into()))?;
            if !values.is_null(row_index) {
                row_tags.insert(name.clone(), values.value(row_index).to_owned());
            }
        }
        let mut row_fields = Fields::new();
        for (index, name) in &fields {
            if let Some(value) = decode_field(batch, *index, row_index)? {
                row_fields.insert(name.clone(), value);
            }
        }
        output.push(SequencedRow::new(
            IngestSeq::new(sequences.value(row_index)),
            WideRow::new(
                SeriesKey::new(measurement, row_tags),
                times.value(row_index),
                row_fields,
            ),
        ));
    }
    Ok(())
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

fn validate_system_kind(field: &arrow::datatypes::Field, expected: &str) -> Result<()> {
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

fn arrow_error(error: arrow::error::ArrowError) -> TsmError {
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
            buffer.append(row.clone()).expect("buffer");
        }
        let batch = buffer.drain_sorted().expect("batch");
        let written = ParquetWriter::new(ParquetWriterConfig::default())
            .write_atomic(store.segments_dir().join(name), &batch)
            .expect("write");
        ActiveFile::new(
            measurement,
            name,
            written.row_count() as u64,
            written.file_bytes(),
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
}
