//! Pruning-aware storage scan contract and local implementation.

use crate::error::{Result, TsmError};
use crate::model::Timestamp;
use crate::store::buffer::{
    COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND, INGEST_SEQ_COLUMN, INGEST_SEQ_COLUMN_KIND,
    TAG_COLUMN_KIND, TIME_COLUMN, TIME_COLUMN_KIND,
};
use crate::store::manifest::ManifestState;
use crate::store::parquet_reader::decode_batch;
use crate::store::seq::SequencedRow;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Inclusive timestamp bounds used by the storage scan contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanTimeRange {
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

impl ScanTimeRange {
    /// Creates validated optional inclusive bounds.
    pub fn new(start: Option<Timestamp>, end: Option<Timestamp>) -> Result<Self> {
        if matches!((start, end), (Some(start), Some(end)) if start > end) {
            return Err(TsmError::InvalidInput(
                "scan timestamp range is inverted".into(),
            ));
        }
        Ok(Self { start, end })
    }

    /// Creates a bounded inclusive timestamp range.
    pub fn bounded(start: Timestamp, end: Timestamp) -> Result<Self> {
        Self::new(Some(start), Some(end))
    }

    /// Creates an unbounded timestamp range.
    pub const fn all() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    /// Returns the inclusive lower bound.
    pub const fn start(&self) -> Option<Timestamp> {
        self.start
    }

    /// Returns the inclusive upper bound.
    pub const fn end(&self) -> Option<Timestamp> {
        self.end
    }

    fn overlaps(&self, min: Timestamp, max: Timestamp) -> bool {
        self.start.is_none_or(|start| max >= start) && self.end.is_none_or(|end| min <= end)
    }

    fn contains(&self, timestamp: Timestamp) -> bool {
        self.start.is_none_or(|start| timestamp >= start)
            && self.end.is_none_or(|end| timestamp <= end)
    }
}

/// One immutable storage scan request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRequest {
    measurement: String,
    time_range: ScanTimeRange,
    field_projection: Option<BTreeSet<String>>,
}

impl ScanRequest {
    /// Creates a scan over one measurement and timestamp range.
    pub fn new(measurement: impl Into<String>, time_range: ScanTimeRange) -> Self {
        Self {
            measurement: measurement.into(),
            time_range,
            field_projection: None,
        }
    }

    /// Restricts decoded field columns while retaining system and tag columns.
    pub fn with_field_projection<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.field_projection = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Returns the requested measurement name.
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns the inclusive timestamp bounds.
    pub const fn time_range(&self) -> ScanTimeRange {
        self.time_range
    }

    /// Returns the requested field names, or `None` when all fields are needed.
    pub fn field_projection(&self) -> Option<&BTreeSet<String>> {
        self.field_projection.as_ref()
    }
}

/// Hard allocation limits and batch sizing for storage scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageReaderConfig {
    batch_rows: usize,
    max_decoded_rows: usize,
    max_decoded_bytes: usize,
}

impl StorageReaderConfig {
    /// Creates non-zero scan limits.
    pub fn new(
        batch_rows: usize,
        max_decoded_rows: usize,
        max_decoded_bytes: usize,
    ) -> Result<Self> {
        if batch_rows == 0 || max_decoded_rows == 0 || max_decoded_bytes == 0 {
            return Err(TsmError::InvalidInput(
                "storage reader batch, row, and byte limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            batch_rows,
            max_decoded_rows,
            max_decoded_bytes,
        })
    }

    /// Returns the Arrow record-batch row target.
    pub const fn batch_rows(&self) -> usize {
        self.batch_rows
    }

    /// Returns the maximum rows passed to the decoder after pruning.
    pub const fn max_decoded_rows(&self) -> usize {
        self.max_decoded_rows
    }

    /// Returns the maximum projected compressed bytes passed to the decoder.
    pub const fn max_decoded_bytes(&self) -> usize {
        self.max_decoded_bytes
    }
}

impl Default for StorageReaderConfig {
    fn default() -> Self {
        Self {
            batch_rows: 8_192,
            max_decoded_rows: 1_000_000,
            max_decoded_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Observable pruning and decode accounting for one scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanStats {
    files_considered: usize,
    files_pruned: usize,
    files_opened: usize,
    row_groups_considered: usize,
    row_groups_pruned: usize,
    row_groups_decoded: usize,
    decoded_rows: usize,
    decoded_bytes: usize,
    projected_field_columns: usize,
}

impl ScanStats {
    /// Returns manifest files belonging to the requested measurement.
    pub const fn files_considered(&self) -> usize {
        self.files_considered
    }

    /// Returns files rejected by manifest timestamp bounds.
    pub const fn files_pruned(&self) -> usize {
        self.files_pruned
    }

    /// Returns files opened for Parquet metadata inspection.
    pub const fn files_opened(&self) -> usize {
        self.files_opened
    }

    /// Returns row groups inspected using Parquet statistics.
    pub const fn row_groups_considered(&self) -> usize {
        self.row_groups_considered
    }

    /// Returns row groups rejected by timestamp statistics.
    pub const fn row_groups_pruned(&self) -> usize {
        self.row_groups_pruned
    }

    /// Returns row groups passed to the Parquet decoder.
    pub const fn row_groups_decoded(&self) -> usize {
        self.row_groups_decoded
    }

    /// Returns rows charged to the decoder after pruning.
    pub const fn decoded_rows(&self) -> usize {
        self.decoded_rows
    }

    /// Returns projected compressed column bytes charged after pruning.
    pub const fn decoded_bytes(&self) -> usize {
        self.decoded_bytes
    }

    /// Returns field-column projections applied across opened files.
    pub const fn projected_field_columns(&self) -> usize {
        self.projected_field_columns
    }
}

/// Rows and observability counters produced by one storage scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanResult {
    rows: Vec<SequencedRow>,
    stats: ScanStats,
}

impl ScanResult {
    /// Returns the rows that survived exact timestamp filtering.
    pub fn rows(&self) -> &[SequencedRow] {
        &self.rows
    }

    /// Consumes the result and returns its rows.
    pub fn into_rows(self) -> Vec<SequencedRow> {
        self.rows
    }

    /// Returns pruning and decode accounting.
    pub const fn stats(&self) -> &ScanStats {
        &self.stats
    }
}

/// Read-only query boundary implemented by local and future distributed stores.
pub trait StorageReader {
    /// Scans one measurement using storage-level pruning and projection.
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult>;
}

/// A pruning-aware reader over one immutable manifest snapshot.
pub struct ManifestStorageReader<'a> {
    manifest: &'a ManifestState,
    segments_dir: PathBuf,
    config: StorageReaderConfig,
}

impl<'a> ManifestStorageReader<'a> {
    /// Binds an immutable manifest snapshot and its segment directory.
    pub fn new(
        manifest: &'a ManifestState,
        segments_dir: impl Into<PathBuf>,
        config: StorageReaderConfig,
    ) -> Self {
        Self {
            manifest,
            segments_dir: segments_dir.into(),
            config,
        }
    }
}

impl StorageReader for ManifestStorageReader<'_> {
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult> {
        if request.measurement().is_empty() {
            return Err(TsmError::InvalidInput(
                "scan measurement must be non-empty".into(),
            ));
        }

        let mut stats = ScanStats::default();
        let mut rows = Vec::new();
        for active_file in self
            .manifest
            .active_files()
            .values()
            .filter(|file| file.measurement() == request.measurement())
        {
            stats.files_considered = checked_add(stats.files_considered, 1, "file count")?;
            if !request
                .time_range()
                .overlaps(active_file.min_timestamp(), active_file.max_timestamp())
            {
                stats.files_pruned = checked_add(stats.files_pruned, 1, "pruned file count")?;
                continue;
            }

            stats.files_opened = checked_add(stats.files_opened, 1, "opened file count")?;
            scan_file(
                active_file.name(),
                active_file.row_count(),
                &self.segments_dir,
                request,
                self.config,
                &mut stats,
                &mut rows,
            )?;
        }
        rows.sort_by_key(SequencedRow::ingest_seq);
        Ok(ScanResult { rows, stats })
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_file(
    file_name: &str,
    manifest_rows: u64,
    segments_dir: &Path,
    request: &ScanRequest,
    config: StorageReaderConfig,
    stats: &mut ScanStats,
    output: &mut Vec<SequencedRow>,
) -> Result<()> {
    let file = File::open(segments_dir.join(file_name))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_error)?;
    let metadata_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| TsmError::Corruption("Parquet file has a negative row count".into()))?;
    if metadata_rows != manifest_rows {
        return Err(TsmError::Corruption(format!(
            "Parquet '{file_name}' row count {metadata_rows} differs from manifest {manifest_rows}"
        )));
    }

    let schema = builder.schema();
    let time_index = schema.index_of(TIME_COLUMN).map_err(arrow_error)?;
    let sequence_index = schema.index_of(INGEST_SEQ_COLUMN).map_err(arrow_error)?;
    validate_system_column(schema.field(time_index), TIME_COLUMN_KIND)?;
    validate_system_column(schema.field(sequence_index), INGEST_SEQ_COLUMN_KIND)?;
    let (projected_indices, projected_fields) =
        projected_indices(schema.fields(), request.field_projection())?;
    stats.projected_field_columns = checked_add(
        stats.projected_field_columns,
        projected_fields,
        "projected field count",
    )?;

    let mut selected_row_groups = Vec::new();
    for (index, row_group) in builder.metadata().row_groups().iter().enumerate() {
        stats.row_groups_considered =
            checked_add(stats.row_groups_considered, 1, "row-group count")?;
        if !row_group_overlaps(row_group, time_index, request.time_range())? {
            stats.row_groups_pruned =
                checked_add(stats.row_groups_pruned, 1, "pruned row-group count")?;
            continue;
        }
        charge_row_group(row_group, &projected_indices, config, stats)?;
        selected_row_groups.push(index);
    }
    stats.row_groups_decoded = checked_add(
        stats.row_groups_decoded,
        selected_row_groups.len(),
        "decoded row-group count",
    )?;
    if selected_row_groups.is_empty() {
        return Ok(());
    }

    let expected_rows = selected_row_groups
        .iter()
        .try_fold(0_usize, |total, index| {
            let rows = non_negative_usize(
                builder.metadata().row_group(*index).num_rows(),
                "Parquet row group has a negative row count",
            )?;
            checked_add(total, rows, "selected row count")
        })?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), projected_indices);
    let reader = builder
        .with_row_groups(selected_row_groups)
        .with_projection(projection)
        .with_batch_size(config.batch_rows())
        .build()
        .map_err(parquet_error)?;
    let mut decoded = Vec::with_capacity(expected_rows);
    for batch in reader {
        decode_batch(
            request.measurement(),
            &batch.map_err(arrow_error)?,
            &mut decoded,
        )?;
    }
    if decoded.len() != expected_rows {
        return Err(TsmError::Corruption(format!(
            "Parquet '{file_name}' decoded {} rows but selected metadata declares {expected_rows}",
            decoded.len()
        )));
    }
    output.extend(
        decoded
            .into_iter()
            .filter(|row| request.time_range().contains(row.row().timestamp())),
    );
    Ok(())
}

fn projected_indices(
    fields: &arrow_schema::Fields,
    requested_fields: Option<&BTreeSet<String>>,
) -> Result<(Vec<usize>, usize)> {
    let mut indices = Vec::new();
    let mut projected_fields = 0;
    for (index, field) in fields.iter().enumerate() {
        let kind = field
            .metadata()
            .get(COLUMN_KIND_METADATA_KEY)
            .map(String::as_str)
            .ok_or_else(|| {
                TsmError::Corruption(format!(
                    "Parquet column '{}' lacks Skulk metadata",
                    field.name()
                ))
            })?;
        let selected = match kind {
            TIME_COLUMN_KIND | INGEST_SEQ_COLUMN_KIND | TAG_COLUMN_KIND => true,
            FIELD_COLUMN_KIND => {
                let selected = requested_fields.is_none_or(|fields| fields.contains(field.name()));
                if selected {
                    projected_fields = checked_add(projected_fields, 1, "projected field count")?;
                }
                selected
            }
            _ => {
                return Err(TsmError::Corruption(format!(
                    "Parquet column '{}' has invalid Skulk metadata",
                    field.name()
                )));
            }
        };
        if selected {
            indices.push(index);
        }
    }
    Ok((indices, projected_fields))
}

fn row_group_overlaps(
    row_group: &RowGroupMetaData,
    time_index: usize,
    range: ScanTimeRange,
) -> Result<bool> {
    let Some(statistics) = row_group.column(time_index).statistics() else {
        return Ok(true);
    };
    let Statistics::Int64(statistics) = statistics else {
        return Err(TsmError::Corruption(
            "Parquet timestamp statistics are not i64".into(),
        ));
    };
    match (statistics.min_opt(), statistics.max_opt()) {
        (Some(min), Some(max)) => Ok(range.overlaps(*min, *max)),
        _ => Ok(true),
    }
}

fn charge_row_group(
    row_group: &RowGroupMetaData,
    projected_indices: &[usize],
    config: StorageReaderConfig,
    stats: &mut ScanStats,
) -> Result<()> {
    let rows = non_negative_usize(
        row_group.num_rows(),
        "Parquet row group has a negative row count",
    )?;
    let bytes = projected_indices.iter().try_fold(0_usize, |total, index| {
        let bytes = non_negative_usize(
            row_group.column(*index).compressed_size(),
            "Parquet column has a negative compressed size",
        )?;
        checked_add(total, bytes, "projected compressed byte count")
    })?;
    let decoded_rows = checked_add(stats.decoded_rows, rows, "decoded row count")?;
    if decoded_rows > config.max_decoded_rows() {
        return Err(TsmError::ResourceLimit(format!(
            "pruned scan would decode {decoded_rows} rows, limit is {}",
            config.max_decoded_rows()
        )));
    }
    let decoded_bytes = checked_add(stats.decoded_bytes, bytes, "decoded byte count")?;
    if decoded_bytes > config.max_decoded_bytes() {
        return Err(TsmError::ResourceLimit(format!(
            "pruned scan would decode {decoded_bytes} bytes, limit is {}",
            config.max_decoded_bytes()
        )));
    }
    stats.decoded_rows = decoded_rows;
    stats.decoded_bytes = decoded_bytes;
    Ok(())
}

fn checked_add(left: usize, right: usize, context: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| TsmError::ResourceLimit(format!("{context} overflow")))
}

fn non_negative_usize(value: i64, message: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| TsmError::Corruption(message.into()))
}

fn validate_system_column(field: &arrow_schema::Field, expected: &str) -> Result<()> {
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

fn arrow_error(error: arrow_schema::ArrowError) -> TsmError {
    TsmError::InvalidFormat(format!("Arrow schema error: {error}"))
}

fn parquet_error(error: parquet::errors::ParquetError) -> TsmError {
    TsmError::InvalidFormat(format!("Parquet read failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        ManifestStorageReader, ScanRequest, ScanTimeRange, StorageReader, StorageReaderConfig,
    };
    use crate::error::{Result, TsmError};
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::buffer::{FlushPolicy, MeasurementBuffer};
    use crate::store::manifest::{ActiveFile, ManifestStore, ManifestUpdate};
    use crate::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
    use crate::store::seq::{IngestSeq, SequencedRow};

    fn sequenced(sequence: u64, measurement: &str, timestamp: i64, fields: Fields) -> SequencedRow {
        SequencedRow::new(
            IngestSeq::new(sequence),
            WideRow::new(
                SeriesKey::new(measurement, Tags::from([("host".into(), "edge-a".into())])),
                timestamp,
                fields,
            ),
        )
    }

    fn persist(
        store: &ManifestStore,
        measurement: &str,
        name: &str,
        rows: &[SequencedRow],
        row_group_rows: usize,
    ) -> ActiveFile {
        let mut buffer = MeasurementBuffer::new(
            measurement,
            FlushPolicy::new(rows.len() + 1, 1024 * 1024).expect("policy"),
        );
        for row in rows {
            buffer.append(row).expect("buffer");
        }
        let batch = buffer.drain_sorted().expect("batch");
        let written =
            ParquetWriter::new(ParquetWriterConfig::new(row_group_rows).expect("writer config"))
                .write_atomic(store.segments_dir().join(name), &batch)
                .expect("write");
        ActiveFile::new(
            measurement,
            name,
            written.row_count() as u64,
            written.file_bytes(),
            rows.iter()
                .map(|row| row.row().timestamp())
                .min()
                .expect("rows"),
            rows.iter()
                .map(|row| row.row().timestamp())
                .max()
                .expect("rows"),
        )
        .expect("active file")
    }

    fn reader_config(max_rows: usize, max_bytes: usize) -> StorageReaderConfig {
        StorageReaderConfig::new(2, max_rows, max_bytes).expect("reader config")
    }

    fn scan_via_contract(
        reader: &dyn StorageReader,
        request: &ScanRequest,
    ) -> Result<Vec<SequencedRow>> {
        Ok(reader.scan(request)?.into_rows())
    }

    #[test]
    fn manifest_and_row_group_time_ranges_are_pruned_before_decode() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let old_rows = [
            sequenced(1, "cpu", 1, Fields::new()),
            sequenced(2, "cpu", 2, Fields::new()),
        ];
        let mixed_rows = [
            sequenced(3, "cpu", 10, Fields::new()),
            sequenced(4, "cpu", 11, Fields::new()),
            sequenced(5, "cpu", 100, Fields::new()),
            sequenced(6, "cpu", 101, Fields::new()),
        ];
        let old = persist(&store, "cpu", "old.parquet", &old_rows, 2);
        let mixed = persist(&store, "cpu", "mixed.parquet", &mixed_rows, 2);
        store
            .publish(ManifestUpdate::new().add_file(old).add_file(mixed))
            .expect("publish");

        let state = store.state().expect("state");
        let reader =
            ManifestStorageReader::new(&state, store.segments_dir(), reader_config(10, usize::MAX));
        let result = reader
            .scan(&ScanRequest::new(
                "cpu",
                ScanTimeRange::bounded(100, 101).expect("range"),
            ))
            .expect("scan");

        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row.row().timestamp())
                .collect::<Vec<_>>(),
            [100, 101]
        );
        assert_eq!(result.stats().files_considered(), 2);
        assert_eq!(result.stats().files_pruned(), 1);
        assert_eq!(result.stats().files_opened(), 1);
        assert_eq!(result.stats().row_groups_considered(), 2);
        assert_eq!(result.stats().row_groups_pruned(), 1);
        assert_eq!(result.stats().row_groups_decoded(), 1);
        assert_eq!(result.stats().decoded_rows(), 2);
    }

    #[test]
    fn projection_decodes_only_system_tags_and_requested_fields() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [sequenced(
            1,
            "cpu",
            10,
            Fields::from([
                ("keep".into(), FieldValue::Float(1.5)),
                ("drop".into(), FieldValue::String("large".into())),
            ]),
        )];
        let file = persist(&store, "cpu", "cpu.parquet", &rows, 1);
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");

        let state = store.state().expect("state");
        let reader =
            ManifestStorageReader::new(&state, store.segments_dir(), reader_config(10, usize::MAX));
        let request = ScanRequest::new("cpu", ScanTimeRange::all()).with_field_projection(["keep"]);
        let result = reader.scan(&request).expect("scan");

        assert_eq!(
            result.rows()[0].row().field("keep"),
            Some(&FieldValue::Float(1.5))
        );
        assert_eq!(result.rows()[0].row().field("drop"), None);
        assert_eq!(
            result.rows()[0]
                .row()
                .series()
                .tags()
                .get("host")
                .map(String::as_str),
            Some("edge-a")
        );
        assert_eq!(result.stats().projected_field_columns(), 1);
    }

    #[test]
    fn limits_are_charged_after_pruning_not_from_manifest_candidates() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [
            sequenced(1, "cpu", 1, Fields::new()),
            sequenced(2, "cpu", 2, Fields::new()),
            sequenced(3, "cpu", 100, Fields::new()),
            sequenced(4, "cpu", 101, Fields::new()),
        ];
        let file = persist(&store, "cpu", "cpu.parquet", &rows, 2);
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");
        let state = store.state().expect("state");
        let reader =
            ManifestStorageReader::new(&state, store.segments_dir(), reader_config(2, usize::MAX));

        let narrow = ScanRequest::new("cpu", ScanTimeRange::bounded(100, 101).expect("range"));
        assert_eq!(
            scan_via_contract(&reader, &narrow)
                .expect("narrow query must pass")
                .len(),
            2
        );

        let broad = ScanRequest::new("cpu", ScanTimeRange::all());
        assert!(matches!(
            reader.scan(&broad),
            Err(TsmError::ResourceLimit(message)) if message.contains("rows")
        ));
    }

    #[test]
    fn projected_compressed_bytes_are_bounded_before_decode() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [sequenced(
            1,
            "cpu",
            10,
            Fields::from([("value".into(), FieldValue::String("payload".repeat(64)))]),
        )];
        let file = persist(&store, "cpu", "cpu.parquet", &rows, 1);
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");
        let state = store.state().expect("state");
        let request =
            ScanRequest::new("cpu", ScanTimeRange::all()).with_field_projection(["value"]);
        let measuring =
            ManifestStorageReader::new(&state, store.segments_dir(), reader_config(10, usize::MAX))
                .scan(&request)
                .expect("measure");
        let decoded_bytes = measuring.stats().decoded_bytes();
        assert!(decoded_bytes > 0);

        let limited = ManifestStorageReader::new(
            &state,
            store.segments_dir(),
            reader_config(10, decoded_bytes - 1),
        );
        assert!(matches!(
            limited.scan(&request),
            Err(TsmError::ResourceLimit(message)) if message.contains("bytes")
        ));
    }

    #[test]
    fn inverted_time_range_and_zero_limits_are_rejected() {
        assert!(ScanTimeRange::bounded(2, 1).is_err());
        assert!(StorageReaderConfig::new(0, 1, 1).is_err());
        assert!(StorageReaderConfig::new(1, 0, 1).is_err());
        assert!(StorageReaderConfig::new(1, 1, 0).is_err());
    }
}
