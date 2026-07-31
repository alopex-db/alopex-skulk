//! Pruning-aware storage scan contract and local implementation.

use crate::error::{Result, TsmError};
use crate::model::{FieldValue, SeriesKey, Tags, Timestamp};
use crate::store::buffer::{
    COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND, INGEST_SEQ_COLUMN, INGEST_SEQ_COLUMN_KIND,
    TAG_COLUMN_KIND, TIME_COLUMN, TIME_COLUMN_KIND,
};
use crate::store::compaction::deduplicate_latest;
use crate::store::manifest::ManifestState;
use crate::store::parquet_reader::{
    decode_batch_with_series_cache, decode_float_batch_with_series_cache, DecodedFloatSample,
    DecodedSeriesCache,
};
use crate::store::schema::MeasurementSchema;
use crate::store::seq::{IngestSeq, SequencedRow};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::{Encoding, PageType};
use parquet::column::page::Page;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::statistics::Statistics;
use regex::{Regex, RegexBuilder};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_TAG_REGEX_BYTES: usize = 64 * 1024;
const MAX_TAG_REGEX_AUTOMATON_BYTES: usize = 2 * 1024 * 1024;
const MAX_TAG_DICTIONARY_PRUNING_BYTES: i64 = 4 * 1024 * 1024;

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

/// A storage-level tag comparison operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TagPredicateOp {
    /// The tag value must equal the predicate value.
    Equal,
    /// The tag value must not equal the predicate value.
    NotEqual,
    /// The entire tag value must match the regular expression.
    Regex,
    /// The entire tag value must not match the regular expression.
    NotRegex,
}

/// A predicate over one tag column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TagPredicate {
    name: String,
    op: TagPredicateOp,
    value: String,
}

impl TagPredicate {
    /// Creates a tag predicate from string-like values.
    pub fn new(name: impl Into<String>, op: TagPredicateOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }

    /// Returns the tag column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the comparison operation.
    pub const fn op(&self) -> TagPredicateOp {
        self.op
    }

    /// Returns the literal or regular-expression value.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Compiles and validates this predicate once for repeated row evaluation.
    pub fn prepare(&self) -> Result<PreparedTagPredicate> {
        self.prepare_with_limits(MAX_TAG_REGEX_BYTES, MAX_TAG_REGEX_AUTOMATON_BYTES)
    }

    /// Compiles this predicate using query-specific regex source and automaton limits.
    pub fn prepare_with_limits(
        &self,
        max_regex_bytes: usize,
        max_regex_automaton_bytes: usize,
    ) -> Result<PreparedTagPredicate> {
        if max_regex_bytes == 0 || max_regex_automaton_bytes == 0 {
            return Err(TsmError::InvalidInput(
                "tag regular-expression limits must be non-zero".to_string(),
            ));
        }
        if self.name.is_empty() {
            return Err(TsmError::InvalidInput(
                "tag predicate name must be non-empty".into(),
            ));
        }
        let operation = match self.op {
            TagPredicateOp::Equal => PreparedTagPredicateOp::Equal(self.value.clone()),
            TagPredicateOp::NotEqual => PreparedTagPredicateOp::NotEqual(self.value.clone()),
            TagPredicateOp::Regex | TagPredicateOp::NotRegex => {
                if self.value.len() > max_regex_bytes {
                    return Err(TsmError::ResourceLimit(format!(
                        "tag regular expression has {} bytes, limit is {max_regex_bytes}",
                        self.value.len()
                    )));
                }
                let anchored = format!(r"\A(?:{})\z", self.value);
                let regex = RegexBuilder::new(&anchored)
                    .size_limit(max_regex_automaton_bytes)
                    .dfa_size_limit(max_regex_automaton_bytes)
                    .build()
                    .map_err(|error| match error {
                        regex::Error::CompiledTooBig(limit) => TsmError::ResourceLimit(format!(
                            "tag regular expression exceeded the {limit}-byte compiled-size limit"
                        )),
                        other => TsmError::InvalidInput(format!(
                            "invalid tag regular expression '{}': {other}",
                            self.value
                        )),
                    })?;
                if self.op == TagPredicateOp::Regex {
                    PreparedTagPredicateOp::Regex(regex)
                } else {
                    PreparedTagPredicateOp::NotRegex(regex)
                }
            }
        };
        Ok(PreparedTagPredicate {
            name: self.name.clone(),
            operation,
        })
    }
}

enum PreparedTagPredicateOp {
    Equal(String),
    NotEqual(String),
    Regex(Regex),
    NotRegex(Regex),
}

/// A validated tag predicate ready for repeated matching.
pub struct PreparedTagPredicate {
    name: String,
    operation: PreparedTagPredicateOp,
}

impl PreparedTagPredicate {
    /// Returns the tag name inspected by this predicate.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Matches canonical tags, treating an absent tag as the empty string.
    pub fn matches(&self, tags: &Tags) -> bool {
        let value = tags.get(&self.name).map_or("", String::as_str);
        self.matches_value(value)
    }

    /// Matches one already-resolved label value.
    pub fn matches_value(&self, value: &str) -> bool {
        match &self.operation {
            PreparedTagPredicateOp::Equal(expected) => value == expected,
            PreparedTagPredicateOp::NotEqual(expected) => value != expected,
            PreparedTagPredicateOp::Regex(regex) => regex.is_match(value),
            PreparedTagPredicateOp::NotRegex(regex) => !regex.is_match(value),
        }
    }

    fn equal_value(&self) -> Option<&str> {
        match &self.operation {
            PreparedTagPredicateOp::Equal(value) => Some(value),
            _ => None,
        }
    }
}

/// One immutable storage scan request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRequest {
    measurement: String,
    time_range: ScanTimeRange,
    field_projection: Option<BTreeSet<String>>,
    tag_predicates: Vec<TagPredicate>,
    decode_limits: Option<ScanDecodeLimits>,
}

impl ScanRequest {
    /// Creates a scan over one measurement and timestamp range.
    pub fn new(measurement: impl Into<String>, time_range: ScanTimeRange) -> Self {
        Self {
            measurement: measurement.into(),
            time_range,
            field_projection: None,
            tag_predicates: Vec::new(),
            decode_limits: None,
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

    /// Adds tag predicates that must all match a row.
    pub fn with_tag_predicates<I>(mut self, predicates: I) -> Self
    where
        I: IntoIterator<Item = TagPredicate>,
    {
        self.tag_predicates = predicates.into_iter().collect();
        self
    }

    /// Narrows post-pruning decode limits for this request.
    pub fn with_decode_limits(mut self, limits: ScanDecodeLimits) -> Self {
        self.decode_limits = Some(limits);
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

    /// Returns the conjunctive tag predicates.
    pub fn tag_predicates(&self) -> &[TagPredicate] {
        &self.tag_predicates
    }

    /// Returns query-specific decode limits, when narrower limits were requested.
    pub const fn decode_limits(&self) -> Option<ScanDecodeLimits> {
        self.decode_limits
    }
}

/// Query-specific limits charged after pruning and before Parquet decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanDecodeLimits {
    max_decoded_rows: usize,
    max_decoded_bytes: usize,
}

impl ScanDecodeLimits {
    /// Creates post-pruning decode limits; zero prohibits that resource.
    pub const fn new(max_decoded_rows: usize, max_decoded_bytes: usize) -> Self {
        Self {
            max_decoded_rows,
            max_decoded_bytes,
        }
    }

    /// Returns the post-pruning row limit.
    pub const fn max_decoded_rows(self) -> usize {
        self.max_decoded_rows
    }

    /// Returns the projected compressed-byte limit.
    pub const fn max_decoded_bytes(self) -> usize {
        self.max_decoded_bytes
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
    /// The default embedded scan limits.
    pub const DEFAULT: Self = Self {
        batch_rows: 8_192,
        max_decoded_rows: 1_000_000,
        max_decoded_bytes: 256 * 1024 * 1024,
    };

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

    fn constrained_by(self, limits: Option<ScanDecodeLimits>) -> Self {
        let Some(limits) = limits else {
            return self;
        };
        Self {
            batch_rows: self.batch_rows,
            max_decoded_rows: self.max_decoded_rows.min(limits.max_decoded_rows()),
            max_decoded_bytes: self.max_decoded_bytes.min(limits.max_decoded_bytes()),
        }
    }
}

impl Default for StorageReaderConfig {
    fn default() -> Self {
        Self::DEFAULT
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
    row_groups_pruned_by_tag: usize,
    row_groups_decoded: usize,
    decoded_rows: usize,
    decoded_bytes: usize,
    projected_field_columns: usize,
    pending_rows_considered: usize,
    rows_returned: usize,
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

    /// Returns row groups rejected specifically by tag statistics or dictionaries.
    pub const fn row_groups_pruned_by_tag(&self) -> usize {
        self.row_groups_pruned_by_tag
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

    /// Returns unflushed rows considered for the requested measurement.
    pub const fn pending_rows_considered(&self) -> usize {
        self.pending_rows_considered
    }

    /// Returns rows remaining after exact filtering and deduplication.
    pub const fn rows_returned(&self) -> usize {
        self.rows_returned
    }
}

/// Rows and observability counters produced by one storage scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanResult {
    rows: Vec<SequencedRow>,
    stats: ScanStats,
}

impl ScanResult {
    /// Creates a scan result for custom local or distributed reader implementations.
    pub const fn new(rows: Vec<SequencedRow>, stats: ScanStats) -> Self {
        Self { rows, stats }
    }

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

/// One deduplicated float sample from the lightweight series scan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloatPoint {
    timestamp: Timestamp,
    ingest_seq: IngestSeq,
    value: Option<f64>,
}

impl FloatPoint {
    /// Creates a sample whose sequence participates in latest-write-wins merging.
    pub const fn new(timestamp: Timestamp, ingest_seq: IngestSeq, value: Option<f64>) -> Self {
        Self {
            timestamp,
            ingest_seq,
            value,
        }
    }

    /// Returns the nanosecond timestamp.
    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }

    /// Returns the durable ingest sequence.
    pub const fn ingest_seq(self) -> IngestSeq {
        self.ingest_seq
    }

    /// Returns the sample value, or `None` for a sparse row.
    pub const fn value(self) -> Option<f64> {
        self.value
    }
}

/// Chronologically ordered float samples for one canonical series.
#[derive(Debug, Clone, PartialEq)]
pub struct FloatSeries {
    series: SeriesKey,
    points: Vec<FloatPoint>,
}

impl FloatSeries {
    /// Creates one series result for custom or distributed storage readers.
    pub const fn new(series: SeriesKey, points: Vec<FloatPoint>) -> Self {
        Self { series, points }
    }

    /// Returns the canonical measurement and tags.
    pub const fn series(&self) -> &SeriesKey {
        &self.series
    }

    /// Returns timestamp-ordered, latest-write-wins samples.
    pub fn points(&self) -> &[FloatPoint] {
        &self.points
    }

    fn into_parts(self) -> (SeriesKey, Vec<FloatPoint>) {
        (self.series, self.points)
    }
}

/// Lightweight float-series output and the same pruning accounting as a row scan.
#[derive(Debug, Clone, PartialEq)]
pub struct FloatSeriesScanResult {
    series: Vec<FloatSeries>,
    stats: ScanStats,
}

impl FloatSeriesScanResult {
    /// Creates a result for custom or distributed storage readers.
    pub const fn new(series: Vec<FloatSeries>, stats: ScanStats) -> Self {
        Self { series, stats }
    }

    /// Returns canonical series in deterministic order.
    pub fn series(&self) -> &[FloatSeries] {
        &self.series
    }

    /// Consumes the result and returns its series.
    pub fn into_series(self) -> Vec<FloatSeries> {
        self.series
    }

    /// Returns pruning and decode accounting.
    pub const fn stats(&self) -> &ScanStats {
        &self.stats
    }

    fn into_parts(self) -> (Vec<FloatSeries>, ScanStats) {
        (self.series, self.stats)
    }
}

/// Read-only query boundary implemented by local and future distributed stores.
pub trait StorageReader {
    /// Scans one measurement using storage-level pruning and projection.
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult>;

    /// Tries a series-oriented float scan without materializing wide rows.
    ///
    /// Readers return `None` when the selected field is not float-compatible;
    /// the executor then uses the general row path.
    fn try_scan_float_series(
        &self,
        request: &ScanRequest,
        field: &str,
    ) -> Result<Option<Arc<FloatSeriesScanResult>>> {
        let result = self.scan(request)?;
        let stats = *result.stats();
        let Some(samples) = float_samples_from_rows(result.into_rows(), field)? else {
            return Ok(None);
        };
        let series = finish_float_samples(samples)?;
        let mut stats = stats;
        stats.rows_returned = float_point_count(&series)?;
        Ok(Some(Arc::new(FloatSeriesScanResult { series, stats })))
    }

    /// Lists queryable measurements in deterministic order when the reader
    /// supports matcher-only measurement selection.
    fn measurement_names(&self) -> Result<Vec<String>> {
        Err(TsmError::Unsupported {
            feature: "storage reader does not provide measurement enumeration".to_string(),
            line: 0,
            column: 0,
            offset: 0,
        })
    }

    /// Resolves one query-visible measurement schema when the reader provides
    /// schema catalog access.
    fn measurement_schema(&self, _measurement: &str) -> Result<MeasurementSchema> {
        Err(TsmError::Unsupported {
            feature: "storage reader does not provide schema resolution".to_string(),
            line: 0,
            column: 0,
            offset: 0,
        })
    }
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
        let tag_predicates = request
            .tag_predicates()
            .iter()
            .map(TagPredicate::prepare)
            .collect::<Result<Vec<_>>>()?;

        let mut stats = ScanStats::default();
        let mut rows = Vec::new();
        let config = self.config.constrained_by(request.decode_limits());
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
                &tag_predicates,
                config,
                &mut stats,
                &mut rows,
            )?;
        }
        rows = finalize_visible_rows(rows)?;
        stats.rows_returned = rows.len();
        Ok(ScanResult { rows, stats })
    }

    fn try_scan_float_series(
        &self,
        request: &ScanRequest,
        field: &str,
    ) -> Result<Option<Arc<FloatSeriesScanResult>>> {
        if request.measurement().is_empty() || field.is_empty() {
            return Err(TsmError::InvalidInput(
                "float-series scan measurement and field must be non-empty".into(),
            ));
        }
        let tag_predicates = request
            .tag_predicates()
            .iter()
            .map(TagPredicate::prepare)
            .collect::<Result<Vec<_>>>()?;
        let mut stats = ScanStats::default();
        let mut samples = Vec::new();
        let config = self.config.constrained_by(request.decode_limits());
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
            if !scan_float_file(
                active_file.name(),
                active_file.row_count(),
                &self.segments_dir,
                request,
                field,
                &tag_predicates,
                config,
                &mut stats,
                &mut samples,
            )? {
                return Ok(None);
            }
        }
        let series = finish_float_samples(samples)?;
        stats.rows_returned = float_point_count(&series)?;
        Ok(Some(Arc::new(FloatSeriesScanResult { series, stats })))
    }

    fn measurement_names(&self) -> Result<Vec<String>> {
        Ok(self
            .manifest
            .active_files()
            .values()
            .map(|file| file.measurement().to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}

pub(crate) fn merge_pending_float_series(
    durable: FloatSeriesScanResult,
    pending: &[SequencedRow],
    request: &ScanRequest,
    field: &str,
) -> Result<Option<FloatSeriesScanResult>> {
    let (durable, mut stats) = durable.into_parts();
    let mut samples = Vec::with_capacity(float_point_count(&durable)? + pending.len());
    for series in durable {
        let (series, points) = series.into_parts();
        let series = Arc::new(series);
        samples.extend(points.into_iter().map(|point| SequencedFloatSample {
            series: Arc::clone(&series),
            timestamp: point.timestamp(),
            ingest_seq: point.ingest_seq(),
            value: point.value(),
        }));
    }
    let predicates = request
        .tag_predicates()
        .iter()
        .map(TagPredicate::prepare)
        .collect::<Result<Vec<_>>>()?;
    stats.pending_rows_considered = checked_add(
        stats.pending_rows_considered,
        pending.len(),
        "pending row count",
    )?;
    for row in pending {
        if !request.time_range().contains(row.row().timestamp())
            || !predicates
                .iter()
                .all(|predicate| predicate.matches(row.row().series().tags()))
        {
            continue;
        }
        let value = match row.row().field(field) {
            Some(FieldValue::Float(value)) => Some(*value),
            Some(_) => return Ok(None),
            None => None,
        };
        samples.push(SequencedFloatSample {
            series: row.row().shared_series(),
            timestamp: row.row().timestamp(),
            ingest_seq: row.ingest_seq(),
            value,
        });
    }
    let series = finish_float_samples(samples)?;
    stats.rows_returned = float_point_count(&series)?;
    Ok(Some(FloatSeriesScanResult { series, stats }))
}

pub(crate) fn merge_pending_rows(
    mut result: ScanResult,
    pending: &[SequencedRow],
    request: &ScanRequest,
) -> Result<ScanResult> {
    let predicates = request
        .tag_predicates()
        .iter()
        .map(TagPredicate::prepare)
        .collect::<Result<Vec<_>>>()?;
    result.stats.pending_rows_considered = checked_add(
        result.stats.pending_rows_considered,
        pending.len(),
        "pending row count",
    )?;
    for row in pending {
        if request.time_range().contains(row.row().timestamp())
            && predicates
                .iter()
                .all(|predicate| predicate.matches(row.row().series().tags()))
        {
            result
                .rows
                .push(project_pending_row(row, request.field_projection()));
        }
    }

    result.rows = finalize_visible_rows(result.rows)?;
    result.stats.rows_returned = result.rows.len();
    Ok(result)
}

struct SequencedFloatSample {
    series: Arc<SeriesKey>,
    timestamp: Timestamp,
    ingest_seq: IngestSeq,
    value: Option<f64>,
}

impl From<DecodedFloatSample> for SequencedFloatSample {
    fn from(sample: DecodedFloatSample) -> Self {
        Self {
            series: sample.series,
            timestamp: sample.timestamp,
            ingest_seq: sample.ingest_seq,
            value: sample.value,
        }
    }
}

fn float_samples_from_rows(
    rows: Vec<SequencedRow>,
    field: &str,
) -> Result<Option<Vec<SequencedFloatSample>>> {
    let mut samples = Vec::with_capacity(rows.len());
    for row in rows {
        let value = match row.row().field(field) {
            Some(FieldValue::Float(value)) => Some(*value),
            Some(_) => return Ok(None),
            None => None,
        };
        samples.push(SequencedFloatSample {
            series: row.row().shared_series(),
            timestamp: row.row().timestamp(),
            ingest_seq: row.ingest_seq(),
            value,
        });
    }
    Ok(Some(samples))
}

fn finish_float_samples(samples: Vec<SequencedFloatSample>) -> Result<Vec<FloatSeries>> {
    let mut sequences = HashSet::with_capacity(samples.len());
    let mut grouped: BTreeMap<Arc<SeriesKey>, Vec<SequencedFloatSample>> = BTreeMap::new();
    for sample in samples {
        if !sequences.insert(sample.ingest_seq) {
            return Err(TsmError::Corruption(format!(
                "duplicate visible ingest sequence {}",
                sample.ingest_seq.get()
            )));
        }
        grouped
            .entry(Arc::clone(&sample.series))
            .or_default()
            .push(sample);
    }

    grouped
        .into_iter()
        .map(|(series, mut samples)| {
            samples.sort_by(|left, right| {
                left.timestamp
                    .cmp(&right.timestamp)
                    .then_with(|| left.ingest_seq.cmp(&right.ingest_seq))
            });
            let mut points: Vec<FloatPoint> = Vec::with_capacity(samples.len());
            for sample in samples {
                let point = FloatPoint::new(sample.timestamp, sample.ingest_seq, sample.value);
                if points
                    .last()
                    .is_some_and(|previous| previous.timestamp() == sample.timestamp)
                {
                    if let Some(previous) = points.last_mut() {
                        *previous = point;
                    }
                } else {
                    points.push(point);
                }
            }
            Ok(FloatSeries::new((*series).clone(), points))
        })
        .collect()
}

fn float_point_count(series: &[FloatSeries]) -> Result<usize> {
    series.iter().try_fold(0_usize, |total, series| {
        checked_add(total, series.points().len(), "float sample count")
    })
}

fn finalize_visible_rows(rows: Vec<SequencedRow>) -> Result<Vec<SequencedRow>> {
    let mut sequences = BTreeSet::new();
    if let Some(duplicate) = rows
        .iter()
        .map(SequencedRow::ingest_seq)
        .find(|sequence| !sequences.insert(*sequence))
    {
        return Err(TsmError::Corruption(format!(
            "duplicate visible ingest sequence {}",
            duplicate.get()
        )));
    }
    Ok(deduplicate_latest(rows).into_values().collect())
}

fn project_pending_row(row: &SequencedRow, projection: Option<&BTreeSet<String>>) -> SequencedRow {
    let (sequence, row) = row.clone().into_parts();
    let (series, timestamp, mut fields) = row.into_parts();
    if let Some(projection) = projection {
        fields.retain(|name, _| projection.contains(name));
    }
    SequencedRow::new(
        sequence,
        crate::model::WideRow::new(series, timestamp, fields),
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_file(
    file_name: &str,
    manifest_rows: u64,
    segments_dir: &Path,
    request: &ScanRequest,
    tag_predicates: &[PreparedTagPredicate],
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

    let path = segments_dir.join(file_name);
    let dictionary_reader = tag_predicates
        .iter()
        .any(|predicate| predicate.equal_value().is_some())
        .then(|| SerializedFileReader::new(File::open(&path)?).map_err(parquet_error))
        .transpose()?;
    let mut selected_row_groups = Vec::new();
    for (index, row_group) in builder.metadata().row_groups().iter().enumerate() {
        stats.row_groups_considered =
            checked_add(stats.row_groups_considered, 1, "row-group count")?;
        if !row_group_overlaps(row_group, time_index, request.time_range())? {
            stats.row_groups_pruned =
                checked_add(stats.row_groups_pruned, 1, "pruned row-group count")?;
            continue;
        }
        if !tag_predicates_allow_row_group(
            index,
            row_group,
            schema.fields(),
            tag_predicates,
            dictionary_reader.as_ref(),
        )? {
            stats.row_groups_pruned =
                checked_add(stats.row_groups_pruned, 1, "pruned row-group count")?;
            stats.row_groups_pruned_by_tag = checked_add(
                stats.row_groups_pruned_by_tag,
                1,
                "tag-pruned row-group count",
            )?;
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
    let mut series_cache = DecodedSeriesCache::default();
    for batch in reader {
        decode_batch_with_series_cache(
            request.measurement(),
            &batch.map_err(arrow_error)?,
            &mut series_cache,
            &mut decoded,
        )?;
    }
    if decoded.len() != expected_rows {
        return Err(TsmError::Corruption(format!(
            "Parquet '{file_name}' decoded {} rows but selected metadata declares {expected_rows}",
            decoded.len()
        )));
    }
    output.extend(decoded.into_iter().filter(|row| {
        request.time_range().contains(row.row().timestamp())
            && tag_predicates
                .iter()
                .all(|predicate| predicate.matches(row.row().series().tags()))
    }));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_float_file(
    file_name: &str,
    manifest_rows: u64,
    segments_dir: &Path,
    request: &ScanRequest,
    field: &str,
    tag_predicates: &[PreparedTagPredicate],
    config: StorageReaderConfig,
    stats: &mut ScanStats,
    output: &mut Vec<SequencedFloatSample>,
) -> Result<bool> {
    let path = segments_dir.join(file_name);
    let file = File::open(&path)?;
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
    let requested_fields = BTreeSet::from([field.to_string()]);
    let (projected_indices, projected_fields) =
        projected_indices(schema.fields(), Some(&requested_fields))?;
    stats.projected_field_columns = checked_add(
        stats.projected_field_columns,
        projected_fields,
        "projected field count",
    )?;

    let dictionary_reader = tag_predicates
        .iter()
        .any(|predicate| predicate.equal_value().is_some())
        .then(|| SerializedFileReader::new(File::open(&path)?).map_err(parquet_error))
        .transpose()?;
    let mut selected_row_groups = Vec::new();
    for (index, row_group) in builder.metadata().row_groups().iter().enumerate() {
        stats.row_groups_considered =
            checked_add(stats.row_groups_considered, 1, "row-group count")?;
        if !row_group_overlaps(row_group, time_index, request.time_range())? {
            stats.row_groups_pruned =
                checked_add(stats.row_groups_pruned, 1, "pruned row-group count")?;
            continue;
        }
        if !tag_predicates_allow_row_group(
            index,
            row_group,
            schema.fields(),
            tag_predicates,
            dictionary_reader.as_ref(),
        )? {
            stats.row_groups_pruned =
                checked_add(stats.row_groups_pruned, 1, "pruned row-group count")?;
            stats.row_groups_pruned_by_tag = checked_add(
                stats.row_groups_pruned_by_tag,
                1,
                "tag-pruned row-group count",
            )?;
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
        return Ok(true);
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
    let mut decoded = Vec::new();
    let mut decoded_rows = 0_usize;
    let mut series_cache = DecodedSeriesCache::default();
    for batch in reader {
        let batch = batch.map_err(arrow_error)?;
        decoded_rows = checked_add(decoded_rows, batch.num_rows(), "decoded row count")?;
        if !decode_float_batch_with_series_cache(
            request.measurement(),
            field,
            &batch,
            &mut series_cache,
            &mut decoded,
        )? {
            return Ok(false);
        }
    }
    if decoded_rows != expected_rows {
        return Err(TsmError::Corruption(format!(
            "Parquet '{file_name}' decoded {decoded_rows} rows but selected metadata declares {expected_rows}"
        )));
    }
    output.extend(
        decoded
            .into_iter()
            .filter(|sample| {
                request.time_range().contains(sample.timestamp)
                    && tag_predicates
                        .iter()
                        .all(|predicate| predicate.matches(sample.series.tags()))
            })
            .map(SequencedFloatSample::from),
    );
    Ok(true)
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

fn tag_predicates_allow_row_group(
    row_group_index: usize,
    row_group: &RowGroupMetaData,
    fields: &arrow_schema::Fields,
    predicates: &[PreparedTagPredicate],
    dictionary_reader: Option<&SerializedFileReader<File>>,
) -> Result<bool> {
    for predicate in predicates {
        let Some(expected) = predicate.equal_value() else {
            continue;
        };
        let column_index = fields.iter().position(|field| {
            field.name() == &predicate.name
                && field
                    .metadata()
                    .get(COLUMN_KIND_METADATA_KEY)
                    .is_some_and(|kind| kind == TAG_COLUMN_KIND)
        });
        let Some(column_index) = column_index else {
            if !expected.is_empty() {
                return Ok(false);
            }
            continue;
        };
        let column = row_group.column(column_index);
        let Some(statistics) = column.statistics() else {
            continue;
        };
        let null_count = statistics.null_count_opt();
        let Statistics::ByteArray(statistics) = statistics else {
            return Err(TsmError::Corruption(format!(
                "Parquet tag column '{}' statistics are not byte arrays",
                predicate.name
            )));
        };
        let row_group_rows = u64::try_from(row_group.num_rows()).map_err(|_| {
            TsmError::Corruption("Parquet row group has a negative row count".into())
        })?;
        if expected.is_empty() && null_count.is_some_and(|count| count > 0) {
            continue;
        }
        if !expected.is_empty() && null_count.is_some_and(|count| count == row_group_rows) {
            return Ok(false);
        }
        if let (Some(min), Some(max)) = (statistics.min_opt(), statistics.max_opt()) {
            if expected.as_bytes() < min.data() || expected.as_bytes() > max.data() {
                return Ok(false);
            }
        }
        if dictionary_data_pages_only(column)
            && column.compressed_size() >= 0
            && column.compressed_size() <= MAX_TAG_DICTIONARY_PRUNING_BYTES
            && (expected.is_empty() && null_count == Some(0) || !expected.is_empty())
        {
            let reader = dictionary_reader.ok_or_else(|| {
                TsmError::Corruption("dictionary pruning reader is unavailable".into())
            })?;
            if dictionary_contains(reader, row_group_index, column_index, expected)? == Some(false)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn dictionary_data_pages_only(column: &parquet::file::metadata::ColumnChunkMetaData) -> bool {
    let Some(statistics) = column.page_encoding_stats() else {
        return false;
    };
    let mut saw_data_page = false;
    for statistic in statistics {
        if matches!(
            statistic.page_type,
            PageType::DATA_PAGE | PageType::DATA_PAGE_V2
        ) {
            saw_data_page = true;
            if !matches!(
                statistic.encoding,
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
            ) {
                return false;
            }
        }
    }
    saw_data_page
}

fn dictionary_contains(
    reader: &SerializedFileReader<File>,
    row_group_index: usize,
    column_index: usize,
    expected: &str,
) -> Result<Option<bool>> {
    let row_group = reader
        .get_row_group(row_group_index)
        .map_err(parquet_error)?;
    let mut pages = row_group
        .get_column_page_reader(column_index)
        .map_err(parquet_error)?;
    match pages.get_next_page().map_err(parquet_error)? {
        Some(Page::DictionaryPage {
            buf,
            num_values,
            encoding,
            ..
        }) => {
            if encoding != Encoding::PLAIN {
                return Ok(None);
            }
            plain_dictionary_contains(&buf, num_values, expected).map(Some)
        }
        Some(Page::DataPage { .. } | Page::DataPageV2 { .. }) | None => Ok(None),
    }
}

fn plain_dictionary_contains(bytes: &[u8], num_values: u32, expected: &str) -> Result<bool> {
    let mut offset = 0_usize;
    for _ in 0..num_values {
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| TsmError::Corruption("dictionary offset overflow".into()))?;
        let encoded_length: [u8; 4] = bytes
            .get(offset..length_end)
            .ok_or_else(|| TsmError::Corruption("truncated dictionary value length".into()))?
            .try_into()
            .map_err(|_| TsmError::Corruption("invalid dictionary value length".into()))?;
        let length = usize::try_from(u32::from_le_bytes(encoded_length))
            .map_err(|_| TsmError::ResourceLimit("dictionary value exceeds usize".into()))?;
        let value_end = length_end
            .checked_add(length)
            .ok_or_else(|| TsmError::Corruption("dictionary value offset overflow".into()))?;
        let value = bytes
            .get(length_end..value_end)
            .ok_or_else(|| TsmError::Corruption("truncated dictionary value".into()))?;
        if value == expected.as_bytes() {
            return Ok(true);
        }
        offset = value_end;
    }
    Ok(false)
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
        TagPredicate, TagPredicateOp,
    };
    use crate::error::{Result, TsmError};
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::buffer::{FlushPolicy, MeasurementBuffer};
    use crate::store::manifest::{ActiveFile, ManifestStore, ManifestUpdate};
    use crate::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
    use crate::store::seq::{IngestSeq, SequencedRow};
    use proptest::prelude::*;

    fn sequenced(sequence: u64, measurement: &str, timestamp: i64, fields: Fields) -> SequencedRow {
        sequenced_with_tags(
            sequence,
            measurement,
            timestamp,
            Tags::from([("host".into(), "edge-a".into())]),
            fields,
        )
    }

    fn sequenced_with_tags(
        sequence: u64,
        measurement: &str,
        timestamp: i64,
        tags: Tags,
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
        assert!(matches!(
            TagPredicate::new("", TagPredicateOp::Equal, "x").prepare(),
            Err(TsmError::InvalidInput(_))
        ));
        assert!(matches!(
            TagPredicate::new("foo", TagPredicateOp::Regex, "(").prepare(),
            Err(TsmError::InvalidInput(_))
        ));
        assert!(matches!(
            TagPredicate::new(
                "foo",
                TagPredicateOp::Regex,
                "x".repeat(super::MAX_TAG_REGEX_BYTES + 1),
            )
            .prepare(),
            Err(TsmError::ResourceLimit(_))
        ));
    }

    #[test]
    fn tag_dictionary_prunes_an_in_range_but_absent_equal_value() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [
            sequenced_with_tags(
                1,
                "cpu",
                1,
                Tags::from([("foo".into(), "a".into())]),
                Fields::new(),
            ),
            sequenced_with_tags(
                2,
                "cpu",
                2,
                Tags::from([("foo".into(), "z".into())]),
                Fields::new(),
            ),
        ];
        let file = persist(&store, "cpu", "cpu.parquet", &rows, 2);
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");
        let state = store.state().expect("state");
        let reader =
            ManifestStorageReader::new(&state, store.segments_dir(), reader_config(10, usize::MAX));
        let request = ScanRequest::new("cpu", ScanTimeRange::all())
            .with_tag_predicates([TagPredicate::new("foo", TagPredicateOp::Equal, "m")]);
        let result = reader.scan(&request).expect("scan");

        assert!(result.rows().is_empty());
        assert_eq!(result.stats().row_groups_pruned_by_tag(), 1);
        assert_eq!(result.stats().row_groups_decoded(), 0);
    }

    #[test]
    fn missing_tag_is_evaluated_as_empty_for_every_operator() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let rows = [sequenced_with_tags(1, "cpu", 1, Tags::new(), Fields::new())];
        let file = persist(&store, "cpu", "cpu.parquet", &rows, 1);
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");
        let state = store.state().expect("state");
        let reader =
            ManifestStorageReader::new(&state, store.segments_dir(), reader_config(10, usize::MAX));

        for (op, value, should_match) in [
            (TagPredicateOp::Equal, "", true),
            (TagPredicateOp::NotEqual, "x", true),
            (TagPredicateOp::Regex, ".*", true),
            (TagPredicateOp::NotRegex, ".*", false),
        ] {
            let request = ScanRequest::new("cpu", ScanTimeRange::all())
                .with_tag_predicates([TagPredicate::new("foo", op, value)]);
            assert_eq!(
                reader.scan(&request).expect("scan").rows().len(),
                usize::from(should_match),
                "{op:?} {value:?}"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn pruning_matches_full_scan_then_filter_for_arbitrary_tag_rows(
            values in proptest::collection::vec(prop::option::of("[a-c]{0,2}"), 1..8),
            op_index in 0_u8..4,
            target in "[a-c]{0,2}",
        ) {
            let root = tempfile::tempdir().expect("tempdir");
            let store = ManifestStore::open(root.path()).expect("manifest");
            let rows = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let mut tags = Tags::new();
                    if let Some(value) = value {
                        tags.insert("foo".into(), value.clone());
                    }
                    sequenced_with_tags(
                        index as u64 + 1,
                        "cpu",
                        index as i64,
                        tags,
                        Fields::new(),
                    )
                })
                .collect::<Vec<_>>();
            let file = persist(&store, "cpu", "cpu.parquet", &rows, 2);
            store
                .publish(ManifestUpdate::new().add_file(file))
                .expect("publish");
            let op = match op_index {
                0 => TagPredicateOp::Equal,
                1 => TagPredicateOp::NotEqual,
                2 => TagPredicateOp::Regex,
                _ => TagPredicateOp::NotRegex,
            };
            let state = store.state().expect("state");
            let reader = ManifestStorageReader::new(
                &state,
                store.segments_dir(),
                reader_config(100, usize::MAX),
            );
            let request = ScanRequest::new("cpu", ScanTimeRange::all())
                .with_tag_predicates([TagPredicate::new("foo", op, target.clone())]);
            let actual = reader
                .scan(&request)
                .expect("scan")
                .rows()
                .iter()
                .map(|row| row.row().timestamp())
                .collect::<Vec<_>>();
            let mut expected = values
                .iter()
                .enumerate()
                .filter_map(|(index, value)| {
                    let actual = value.as_deref().unwrap_or("");
                    let equal = actual == target;
                    let matches = match op {
                        TagPredicateOp::Equal | TagPredicateOp::Regex => equal,
                        TagPredicateOp::NotEqual | TagPredicateOp::NotRegex => !equal,
                    };
                    matches.then(|| {
                        let mut tags = Tags::new();
                        if let Some(value) = value {
                            tags.insert("foo".into(), value.clone());
                        }
                        (tags, index as i64)
                    })
                })
                .collect::<Vec<_>>();
            expected.sort_by(|left, right| {
                left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
            });
            let expected = expected
                .into_iter()
                .map(|(_, timestamp)| timestamp)
                .collect::<Vec<_>>();

            prop_assert_eq!(actual, expected);
        }
    }
}
