//! Protocol-independent ingestion and protocol decoder boundaries.

/// Structured JSON batch and single-point decoding.
pub mod json;
pub mod line_protocol;
/// Prometheus Remote Write v1 decoding.
pub mod remote_write;

use crate::error::{Result, TsmError};
use crate::model::{FieldValue, SeriesId, WideRow};
use crate::store::buffer::{estimated_row_bytes, INGEST_SEQ_COLUMN, TIME_COLUMN};
use crate::store::recovery::RecoveryStore;
use crate::store::seq::IngestSeq;
use crate::store::wal::encoded_frame_size;
use std::collections::BTreeSet;
use std::mem::size_of;

const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_EXPANDED_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_ROWS: usize = 100_000;
const DEFAULT_MAX_SERIES: usize = 50_000;
const DEFAULT_MAX_TAGS: usize = 256;
const DEFAULT_MAX_FIELDS: usize = 256;
const DEFAULT_MAX_NAME_BYTES: usize = 1_024;
const DEFAULT_MAX_STRING_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_BUFFERED_ROWS: usize = 131_072;
const DEFAULT_MAX_BUFFERED_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_WAL_BYTES: usize = 512 * 1024 * 1024;
const RESERVED_PREFIX: &str = "_skulk_";

/// Protocol-specific position used to correlate acceptance and rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceLocation {
    /// The entire request rather than one decoded item.
    Request,
    /// A one-based Line Protocol line number.
    Line(usize),
    /// A decoder-defined batch item index.
    Item(usize),
    /// A Prometheus TimeSeries index.
    Series(usize),
}

#[derive(Debug)]
struct CandidateRow {
    source: SourceLocation,
    row: WideRow,
}

/// Decoder output passed to the shared ingestion boundary.
#[derive(Debug)]
pub struct IngestBatch {
    encoded_bytes: usize,
    expanded_bytes: usize,
    rows: Vec<CandidateRow>,
    rejections: Vec<IngestRejection>,
}

impl IngestBatch {
    /// Creates an empty decoder result with encoded and post-decode byte sizes.
    pub const fn new(encoded_bytes: usize, expanded_bytes: usize) -> Self {
        Self {
            encoded_bytes,
            expanded_bytes,
            rows: Vec::new(),
            rejections: Vec::new(),
        }
    }

    /// Adds one protocol-decoded wide row.
    pub fn push_row(&mut self, source: SourceLocation, row: WideRow) {
        self.rows.push(CandidateRow { source, row });
    }

    /// Adds a protocol-specific rejection while allowing later items to continue.
    pub fn reject(&mut self, source: SourceLocation, reason: impl Into<String>) {
        self.rejections
            .push(IngestRejection::new(source, reason.into()));
    }

    /// Returns the original encoded request size.
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Returns the decoder-observed expanded request size.
    pub const fn expanded_bytes(&self) -> usize {
        self.expanded_bytes
    }

    /// Returns the number of decoded rows plus protocol-specific rejections.
    pub fn item_count(&self) -> usize {
        self.rows.len().saturating_add(self.rejections.len())
    }
}

/// One rejected source item and its actionable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRejection {
    source: SourceLocation,
    reason: String,
}

impl IngestRejection {
    fn new(source: SourceLocation, reason: String) -> Self {
        Self { source, reason }
    }

    /// Returns the decoder-defined source position.
    pub const fn source(&self) -> SourceLocation {
        self.source
    }

    /// Returns why the item was rejected.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// One durably accepted source item and its ingest sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedRow {
    source: SourceLocation,
    sequence: IngestSeq,
}

impl AcceptedRow {
    /// Returns the decoder-defined source position.
    pub const fn source(&self) -> SourceLocation {
        self.source
    }

    /// Returns the durable row-level ingest sequence.
    pub const fn sequence(&self) -> IngestSeq {
        self.sequence
    }
}

/// Complete accepted/rejected result for one partially successful batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    accepted: Vec<AcceptedRow>,
    rejections: Vec<IngestRejection>,
}

impl IngestOutcome {
    /// Returns the number of durably accepted rows.
    pub fn accepted_count(&self) -> usize {
        self.accepted.len()
    }

    /// Returns the number of decoder or common-validation rejections.
    pub fn rejected_count(&self) -> usize {
        self.rejections.len()
    }

    /// Returns accepted rows in decoder input order.
    pub fn accepted(&self) -> &[AcceptedRow] {
        &self.accepted
    }

    /// Returns rejected items ordered by source location.
    pub fn rejections(&self) -> &[IngestRejection] {
        &self.rejections
    }
}

/// Request-level decoded input limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLimits {
    max_encoded_bytes: usize,
    max_expanded_bytes: usize,
    max_rows: usize,
    max_series: usize,
}

impl RequestLimits {
    /// Creates non-zero request, expansion, row, and series limits.
    pub fn new(
        max_encoded_bytes: usize,
        max_expanded_bytes: usize,
        max_rows: usize,
        max_series: usize,
    ) -> Result<Self> {
        if [max_encoded_bytes, max_expanded_bytes, max_rows, max_series].contains(&0) {
            return Err(TsmError::InvalidInput(
                "ingest request limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            max_encoded_bytes,
            max_expanded_bytes,
            max_rows,
            max_series,
        })
    }

    /// Returns the maximum encoded request size.
    pub const fn max_encoded_bytes(self) -> usize {
        self.max_encoded_bytes
    }

    /// Returns the maximum post-decode size.
    pub const fn max_expanded_bytes(self) -> usize {
        self.max_expanded_bytes
    }

    /// Returns the maximum decoded item count.
    pub const fn max_rows(self) -> usize {
        self.max_rows
    }

    /// Returns the maximum distinct series count.
    pub const fn max_series(self) -> usize {
        self.max_series
    }
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_expanded_bytes: DEFAULT_MAX_EXPANDED_BYTES,
            max_rows: DEFAULT_MAX_ROWS,
            max_series: DEFAULT_MAX_SERIES,
        }
    }
}

/// Per-row identifier, column-count, and string limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowLimits {
    max_tags: usize,
    max_fields: usize,
    max_name_bytes: usize,
    max_string_bytes: usize,
}

impl RowLimits {
    /// Creates non-zero tag, field, name, and string limits.
    pub fn new(
        max_tags: usize,
        max_fields: usize,
        max_name_bytes: usize,
        max_string_bytes: usize,
    ) -> Result<Self> {
        if [max_tags, max_fields, max_name_bytes, max_string_bytes].contains(&0) {
            return Err(TsmError::InvalidInput(
                "ingest row limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            max_tags,
            max_fields,
            max_name_bytes,
            max_string_bytes,
        })
    }

    /// Returns the maximum tags per row.
    pub const fn max_tags(self) -> usize {
        self.max_tags
    }

    /// Returns the maximum fields per row.
    pub const fn max_fields(self) -> usize {
        self.max_fields
    }

    /// Returns the maximum UTF-8 byte length of any identifier.
    pub const fn max_name_bytes(self) -> usize {
        self.max_name_bytes
    }

    /// Returns the maximum UTF-8 byte length of a tag or string field value.
    pub const fn max_string_bytes(self) -> usize {
        self.max_string_bytes
    }
}

impl Default for RowLimits {
    fn default() -> Self {
        Self {
            max_tags: DEFAULT_MAX_TAGS,
            max_fields: DEFAULT_MAX_FIELDS,
            max_name_bytes: DEFAULT_MAX_NAME_BYTES,
            max_string_bytes: DEFAULT_MAX_STRING_BYTES,
        }
    }
}

/// Admission thresholds for pending rows, buffer memory, and WAL bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    max_buffered_rows: usize,
    max_buffered_bytes: usize,
    max_wal_bytes: usize,
}

impl AdmissionLimits {
    /// Creates non-zero storage-pressure thresholds.
    pub fn new(
        max_buffered_rows: usize,
        max_buffered_bytes: usize,
        max_wal_bytes: usize,
    ) -> Result<Self> {
        if [max_buffered_rows, max_buffered_bytes, max_wal_bytes].contains(&0) {
            return Err(TsmError::InvalidInput(
                "ingest admission limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            max_buffered_rows,
            max_buffered_bytes,
            max_wal_bytes,
        })
    }

    /// Returns the maximum unflushed row count.
    pub const fn max_buffered_rows(self) -> usize {
        self.max_buffered_rows
    }

    /// Returns the maximum estimated unflushed memory.
    pub const fn max_buffered_bytes(self) -> usize {
        self.max_buffered_bytes
    }

    /// Returns the maximum durable-log file size.
    pub const fn max_wal_bytes(self) -> usize {
        self.max_wal_bytes
    }
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            max_buffered_rows: DEFAULT_MAX_BUFFERED_ROWS,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
            max_wal_bytes: DEFAULT_MAX_WAL_BYTES,
        }
    }
}

/// Shared resource and admission policy for every protocol decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IngestLimits {
    request: RequestLimits,
    row: RowLimits,
    admission: AdmissionLimits,
}

impl IngestLimits {
    /// Combines independently validated request, row, and admission policies.
    pub const fn new(request: RequestLimits, row: RowLimits, admission: AdmissionLimits) -> Self {
        Self {
            request,
            row,
            admission,
        }
    }

    /// Returns request-level limits for early decoder checks.
    pub const fn request(self) -> RequestLimits {
        self.request
    }

    /// Returns per-row limits.
    pub const fn row(self) -> RowLimits {
        self.row
    }

    /// Returns storage admission limits.
    pub const fn admission(self) -> AdmissionLimits {
        self.admission
    }

    /// Rejects encoded or expanded request sizes before rows are stored.
    pub fn validate_request_bytes(self, encoded_bytes: usize, expanded_bytes: usize) -> Result<()> {
        ensure_at_most(
            "request bytes",
            encoded_bytes,
            self.request.max_encoded_bytes,
        )?;
        ensure_at_most(
            "expanded request bytes",
            expanded_bytes,
            self.request.max_expanded_bytes,
        )
    }
}

/// Current storage pressure observed before admission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestPressure {
    buffered_rows: usize,
    buffered_bytes: usize,
    wal_bytes: usize,
}

impl IngestPressure {
    /// Creates a pressure snapshot.
    pub const fn new(buffered_rows: usize, buffered_bytes: usize, wal_bytes: usize) -> Self {
        Self {
            buffered_rows,
            buffered_bytes,
            wal_bytes,
        }
    }

    /// Returns the current unflushed row count.
    pub const fn buffered_rows(self) -> usize {
        self.buffered_rows
    }

    /// Returns the current estimated buffer bytes.
    pub const fn buffered_bytes(self) -> usize {
        self.buffered_bytes
    }

    /// Returns the current WAL file bytes.
    pub const fn wal_bytes(self) -> usize {
        self.wal_bytes
    }
}

/// Dependency boundary used by the protocol-independent ingestion service.
pub trait IngestSink {
    /// Returns pressure used for request admission.
    fn pressure(&self) -> Result<IngestPressure>;

    /// Durably writes a prevalidated batch before returning sequences.
    fn write_batch(&mut self, rows: Vec<WideRow>, now: i64) -> Result<Vec<IngestSeq>>;
}

impl IngestSink for RecoveryStore {
    fn pressure(&self) -> Result<IngestPressure> {
        let wal_bytes = usize::try_from(self.wal_file_bytes()?)
            .map_err(|_| TsmError::ResourceLimit("WAL size exceeds usize".into()))?;
        Ok(IngestPressure::new(
            self.pending_row_count(),
            self.pending_estimated_bytes()?,
            wal_bytes,
        ))
    }

    fn write_batch(&mut self, rows: Vec<WideRow>, now: i64) -> Result<Vec<IngestSeq>> {
        self.ingest_batch_at(rows, now)
    }
}

/// Shared validation, admission, and durable-write service for all decoders.
pub struct Ingestor<S> {
    sink: S,
    limits: IngestLimits,
}

impl<S: IngestSink> Ingestor<S> {
    /// Creates an ingestion service around an injected durable sink.
    pub const fn new(sink: S, limits: IngestLimits) -> Self {
        Self { sink, limits }
    }

    /// Returns the underlying sink.
    pub const fn sink(&self) -> &S {
        &self.sink
    }

    /// Returns the underlying sink mutably for lifecycle operations such as flush.
    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    /// Consumes the service and returns its sink.
    pub fn into_sink(self) -> S {
        self.sink
    }

    /// Validates, admits, and durably writes one decoder batch.
    pub fn ingest(&mut self, batch: IngestBatch, now: i64) -> Result<IngestOutcome> {
        self.limits
            .validate_request_bytes(batch.encoded_bytes, batch.expanded_bytes)?;
        let item_count = batch
            .rows
            .len()
            .checked_add(batch.rejections.len())
            .ok_or_else(|| TsmError::ResourceLimit("ingest item count overflow".into()))?;
        ensure_at_most("decoded rows", item_count, self.limits.request.max_rows)?;

        let mut observed_series = BTreeSet::<SeriesId>::new();
        let mut candidate_bytes = 0_usize;
        let mut valid = Vec::new();
        let mut rejections = batch.rejections;
        for candidate in batch.rows {
            observed_series.insert(candidate.row.series_id());
            ensure_at_most(
                "distinct series",
                observed_series.len(),
                self.limits.request.max_series,
            )?;

            let row_bytes = estimate_and_validate_row_resources(&candidate.row, self.limits.row)?;
            candidate_bytes = checked_add(
                candidate_bytes,
                row_bytes,
                "expanded row byte estimate overflow",
            )?;
            match validate_common_names(&candidate.row, self.limits.row) {
                Ok(()) => {
                    let buffer_bytes = estimated_row_bytes(&candidate.row)?;
                    let wal_bytes = encoded_frame_size(&candidate.row)?;
                    valid.push((candidate, buffer_bytes, wal_bytes));
                }
                Err(reason) => rejections.push(IngestRejection::new(candidate.source, reason)),
            }
        }
        ensure_at_most(
            "expanded request bytes",
            candidate_bytes.max(batch.expanded_bytes),
            self.limits.request.max_expanded_bytes,
        )?;

        if valid.is_empty() {
            rejections.sort_by_key(IngestRejection::source);
            return Ok(IngestOutcome {
                accepted: Vec::new(),
                rejections,
            });
        }

        let valid_buffer_bytes = valid.iter().try_fold(0_usize, |total, (_, bytes, _)| {
            checked_add(total, *bytes, "admission byte estimate overflow")
        })?;
        let valid_wal_bytes = valid.iter().try_fold(0_usize, |total, (_, _, bytes)| {
            checked_add(total, *bytes, "admission WAL byte estimate overflow")
        })?;
        enforce_admission(
            self.sink.pressure()?,
            valid.len(),
            valid_buffer_bytes,
            valid_wal_bytes,
            self.limits.admission,
        )?;

        let mut sources = Vec::with_capacity(valid.len());
        let mut rows = Vec::with_capacity(valid.len());
        for (candidate, _, _) in valid {
            sources.push(candidate.source);
            rows.push(candidate.row);
        }
        let sequences = self.sink.write_batch(rows, now)?;
        if sequences.len() != sources.len() {
            return Err(TsmError::Corruption(format!(
                "ingest sink returned {} sequences for {} rows",
                sequences.len(),
                sources.len()
            )));
        }
        let accepted = sources
            .into_iter()
            .zip(sequences)
            .map(|(source, sequence)| AcceptedRow { source, sequence })
            .collect();
        rejections.sort_by_key(IngestRejection::source);
        Ok(IngestOutcome {
            accepted,
            rejections,
        })
    }
}

fn estimate_and_validate_row_resources(row: &WideRow, limits: RowLimits) -> Result<usize> {
    ensure_at_most("tags per row", row.series().tags().len(), limits.max_tags)?;
    ensure_at_most("fields per row", row.fields().len(), limits.max_fields)?;

    let mut bytes = checked_add(
        size_of::<i64>() + size_of::<u64>(),
        row.series().measurement().len(),
        "row byte estimate overflow",
    )?;
    for (name, value) in row.series().tags() {
        ensure_at_most("tag value bytes", value.len(), limits.max_string_bytes)?;
        bytes = checked_add(bytes, name.len(), "row byte estimate overflow")?;
        bytes = checked_add(bytes, value.len(), "row byte estimate overflow")?;
    }
    for (name, value) in row.fields() {
        bytes = checked_add(bytes, name.len(), "row byte estimate overflow")?;
        let value_bytes = match value {
            FieldValue::Float(_) => size_of::<f64>(),
            FieldValue::Integer(_) => size_of::<i64>(),
            FieldValue::Unsigned(_) => size_of::<u64>(),
            FieldValue::Boolean(_) => size_of::<bool>(),
            FieldValue::String(value) => {
                ensure_at_most("string field bytes", value.len(), limits.max_string_bytes)?;
                value.len()
            }
        };
        bytes = checked_add(bytes, value_bytes, "row byte estimate overflow")?;
    }
    Ok(bytes)
}

fn validate_common_names(row: &WideRow, limits: RowLimits) -> std::result::Result<(), String> {
    validate_identifier("measurement", row.series().measurement(), limits)?;
    if row.fields().is_empty() {
        return Err("row must contain at least one field".into());
    }
    for name in row.series().tags().keys() {
        validate_identifier("tag", name, limits)?;
        if row.fields().contains_key(name) {
            return Err(format!("column '{name}' cannot be both a tag and a field"));
        }
    }
    for name in row.fields().keys() {
        validate_identifier("field", name, limits)?;
    }
    Ok(())
}

fn validate_identifier(
    kind: &str,
    name: &str,
    limits: RowLimits,
) -> std::result::Result<(), String> {
    if name.is_empty() {
        return Err(format!("{kind} name must not be empty"));
    }
    if name.len() > limits.max_name_bytes {
        return Err(format!(
            "{kind} name exceeds {} bytes",
            limits.max_name_bytes
        ));
    }
    if name.chars().any(char::is_control) {
        return Err(format!("{kind} name contains a control character"));
    }
    if matches!(name, TIME_COLUMN | INGEST_SEQ_COLUMN) || name.starts_with(RESERVED_PREFIX) {
        return Err(format!("{kind} name '{name}' is reserved by Skulk"));
    }
    Ok(())
}

fn enforce_admission(
    pressure: IngestPressure,
    incoming_rows: usize,
    incoming_buffer_bytes: usize,
    incoming_wal_bytes: usize,
    limits: AdmissionLimits,
) -> Result<()> {
    let projected_rows = checked_add(
        pressure.buffered_rows,
        incoming_rows,
        "admission row count overflow",
    )?;
    let projected_buffer = checked_add(
        pressure.buffered_bytes,
        incoming_buffer_bytes,
        "admission buffer byte overflow",
    )?;
    let projected_wal = checked_add(
        pressure.wal_bytes,
        incoming_wal_bytes,
        "admission WAL byte overflow",
    )?;

    if projected_rows > limits.max_buffered_rows {
        return Err(backpressure_error(
            "buffered rows",
            projected_rows,
            limits.max_buffered_rows,
        ));
    }
    if projected_buffer > limits.max_buffered_bytes {
        return Err(backpressure_error(
            "buffer bytes",
            projected_buffer,
            limits.max_buffered_bytes,
        ));
    }
    if projected_wal > limits.max_wal_bytes {
        return Err(backpressure_error(
            "WAL bytes",
            projected_wal,
            limits.max_wal_bytes,
        ));
    }
    Ok(())
}

fn ensure_at_most(label: &str, actual: usize, limit: usize) -> Result<()> {
    if actual > limit {
        return Err(TsmError::ResourceLimit(format!(
            "{label} {actual} exceeds limit {limit}"
        )));
    }
    Ok(())
}

fn checked_add(left: usize, right: usize, message: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| TsmError::ResourceLimit(message.into()))
}

fn backpressure_error(resource: &str, projected: usize, limit: usize) -> TsmError {
    TsmError::ResourceLimit(format!(
        "ingest backpressure: projected {resource} {projected} exceeds limit {limit}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        AdmissionLimits, IngestBatch, IngestLimits, IngestPressure, IngestSink, Ingestor,
        RequestLimits, RowLimits, SourceLocation,
    };
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::buffer::FlushPolicy;
    use crate::store::parquet_reader::ParquetReaderConfig;
    use crate::store::parquet_writer::ParquetWriterConfig;
    use crate::store::recovery::{RecoveryConfig, RecoveryStore};
    use crate::store::seq::{IngestSeq, SequencedRow};
    use crate::store::wal::WalConfig;
    use crate::Result;
    use std::time::Duration;

    #[derive(Default)]
    struct FakeSink {
        pressure: IngestPressure,
        writes: Vec<WideRow>,
        calls: usize,
    }

    impl IngestSink for FakeSink {
        fn pressure(&self) -> Result<IngestPressure> {
            Ok(self.pressure)
        }

        fn write_batch(&mut self, rows: Vec<WideRow>, _now: i64) -> Result<Vec<IngestSeq>> {
            self.calls += 1;
            let first = self.writes.len() as u64 + 1;
            let sequences = (0..rows.len())
                .map(|offset| IngestSeq::new(first + offset as u64))
                .collect();
            self.writes.extend(rows);
            Ok(sequences)
        }
    }

    fn row(measurement: &str, tags: Tags, fields: Fields) -> WideRow {
        WideRow::new(SeriesKey::new(measurement, tags), 1, fields)
    }

    fn value_fields(name: &str) -> Fields {
        Fields::from([(name.to_owned(), FieldValue::Float(1.0))])
    }

    #[test]
    fn common_validation_rejects_invalid_and_reserved_names_per_row() {
        let mut batch = IngestBatch::new(100, 200);
        batch.push_row(
            SourceLocation::Item(0),
            row("cpu", Tags::new(), value_fields("value")),
        );
        batch.push_row(
            SourceLocation::Item(1),
            row("", Tags::new(), value_fields("value")),
        );
        batch.push_row(
            SourceLocation::Item(2),
            row(
                "cpu",
                Tags::from([("bad\nname".into(), "edge".into())]),
                value_fields("value"),
            ),
        );
        batch.push_row(
            SourceLocation::Item(3),
            row("cpu", Tags::new(), value_fields("_time")),
        );
        batch.push_row(
            SourceLocation::Item(4),
            row(
                "cpu",
                Tags::from([("same".into(), "tag".into())]),
                value_fields("same"),
            ),
        );

        let mut ingestor = Ingestor::new(FakeSink::default(), IngestLimits::default());
        let outcome = ingestor.ingest(batch, 1).expect("partial success");

        assert_eq!(outcome.accepted_count(), 1);
        assert_eq!(outcome.rejected_count(), 4);
        assert_eq!(ingestor.sink().writes.len(), 1);
        assert!(outcome
            .rejections()
            .iter()
            .any(|rejection| rejection.reason().contains("reserved")));
    }

    #[test]
    fn request_limit_rejects_the_whole_batch_before_sink_mutation() {
        let limits = IngestLimits::new(
            RequestLimits::new(4, 16, 2, 2).expect("request limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let mut batch = IngestBatch::new(5, 5);
        batch.push_row(
            SourceLocation::Item(0),
            row("cpu", Tags::new(), value_fields("value")),
        );
        let mut ingestor = Ingestor::new(FakeSink::default(), limits);

        let error = ingestor.ingest(batch, 1).expect_err("oversized request");

        assert!(error.to_string().contains("request bytes"));
        assert_eq!(ingestor.sink().calls, 0);
    }

    #[test]
    fn every_request_and_row_resource_dimension_is_bounded() {
        let request_cases = [
            (
                RequestLimits::new(100, 4, 10, 10).expect("limits"),
                IngestBatch::new(1, 5),
                "expanded request bytes",
            ),
            (
                RequestLimits::new(100, 100, 1, 10).expect("limits"),
                {
                    let mut batch = IngestBatch::new(1, 1);
                    batch.push_row(
                        SourceLocation::Item(0),
                        row("cpu", Tags::new(), value_fields("value")),
                    );
                    batch.push_row(
                        SourceLocation::Item(1),
                        row("cpu", Tags::new(), value_fields("value")),
                    );
                    batch
                },
                "decoded rows",
            ),
            (
                RequestLimits::new(100, 1_000, 10, 1).expect("limits"),
                {
                    let mut batch = IngestBatch::new(1, 1);
                    batch.push_row(
                        SourceLocation::Item(0),
                        row("cpu", Tags::new(), value_fields("value")),
                    );
                    batch.push_row(
                        SourceLocation::Item(1),
                        row("memory", Tags::new(), value_fields("value")),
                    );
                    batch
                },
                "distinct series",
            ),
        ];
        for (request, batch, expected) in request_cases {
            let limits =
                IngestLimits::new(request, RowLimits::default(), AdmissionLimits::default());
            assert_resource_error(limits, batch, expected);
        }

        let row_cases = [
            (
                RowLimits::new(1, 10, 100, 100).expect("limits"),
                row(
                    "cpu",
                    Tags::from([("a".into(), "1".into()), ("b".into(), "2".into())]),
                    value_fields("value"),
                ),
                "tags per row",
            ),
            (
                RowLimits::new(10, 1, 100, 100).expect("limits"),
                row(
                    "cpu",
                    Tags::new(),
                    Fields::from([
                        ("a".into(), FieldValue::Float(1.0)),
                        ("b".into(), FieldValue::Float(2.0)),
                    ]),
                ),
                "fields per row",
            ),
            (
                RowLimits::new(10, 10, 100, 3).expect("limits"),
                row(
                    "cpu",
                    Tags::from([("host".into(), "edge".into())]),
                    value_fields("value"),
                ),
                "tag value bytes",
            ),
        ];
        for (row_limits, row, expected) in row_cases {
            let mut batch = IngestBatch::new(1, 1);
            batch.push_row(SourceLocation::Item(0), row);
            let limits = IngestLimits::new(
                RequestLimits::default(),
                row_limits,
                AdmissionLimits::default(),
            );
            assert_resource_error(limits, batch, expected);
        }

        let mut batch = IngestBatch::new(1, 1);
        batch.push_row(
            SourceLocation::Item(0),
            row("long", Tags::new(), value_fields("value")),
        );
        let limits = IngestLimits::new(
            RequestLimits::default(),
            RowLimits::new(10, 10, 3, 100).expect("limits"),
            AdmissionLimits::default(),
        );
        let mut ingestor = Ingestor::new(FakeSink::default(), limits);
        let outcome = ingestor.ingest(batch, 1).expect("row-level rejection");
        assert_eq!(outcome.accepted_count(), 0);
        assert_eq!(outcome.rejected_count(), 1);
        assert!(outcome.rejections()[0].reason().contains("name exceeds"));
        assert_eq!(ingestor.sink().calls, 0);
    }

    #[test]
    fn admission_rejects_before_wal_when_buffer_or_log_budget_would_be_exceeded() {
        let limits = IngestLimits::new(
            RequestLimits::default(),
            RowLimits::default(),
            AdmissionLimits::new(2, 1_024, 1_024).expect("admission limits"),
        );
        let sink = FakeSink {
            pressure: IngestPressure::new(2, 128, 128),
            ..FakeSink::default()
        };
        let mut batch = IngestBatch::new(10, 10);
        batch.push_row(
            SourceLocation::Item(0),
            row("cpu", Tags::new(), value_fields("value")),
        );
        let mut ingestor = Ingestor::new(sink, limits);

        let error = ingestor.ingest(batch, 1).expect_err("backpressure");

        assert!(error.to_string().contains("backpressure"));
        assert_eq!(ingestor.sink().calls, 0);
    }

    #[test]
    fn admission_bounds_buffer_and_wal_bytes_independently() {
        for admission in [
            AdmissionLimits::new(10, 1, 100_000).expect("buffer limit"),
            AdmissionLimits::new(10, 100_000, 1).expect("WAL limit"),
        ] {
            let limits =
                IngestLimits::new(RequestLimits::default(), RowLimits::default(), admission);
            let mut batch = IngestBatch::new(1, 1);
            batch.push_row(
                SourceLocation::Item(0),
                row("cpu", Tags::new(), value_fields("value")),
            );
            let mut ingestor = Ingestor::new(FakeSink::default(), limits);

            let error = ingestor.ingest(batch, 1).expect_err("backpressure");

            assert!(error.to_string().contains("backpressure"));
            assert_eq!(ingestor.sink().calls, 0);
        }
    }

    #[test]
    fn recovery_store_pressure_blocks_until_pending_rows_are_flushed() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
        let limits = IngestLimits::new(
            RequestLimits::default(),
            RowLimits::default(),
            AdmissionLimits::new(1, 1024 * 1024, 1024 * 1024).expect("admission"),
        );
        let mut ingestor = Ingestor::new(store, limits);
        let mut first = IngestBatch::new(10, 10);
        first.push_row(
            SourceLocation::Item(0),
            row("cpu", Tags::new(), value_fields("value")),
        );
        assert_eq!(
            ingestor.ingest(first, 1).expect("first").accepted_count(),
            1
        );

        let mut blocked = IngestBatch::new(10, 10);
        blocked.push_row(
            SourceLocation::Item(1),
            row("cpu", Tags::new(), value_fields("value")),
        );
        let error = ingestor.ingest(blocked, 1).expect_err("backpressure");
        assert!(error.to_string().contains("backpressure"));

        ingestor.sink_mut().flush_all().expect("flush");
        let mut after_flush = IngestBatch::new(10, 10);
        after_flush.push_row(
            SourceLocation::Item(2),
            row("cpu", Tags::new(), value_fields("value")),
        );
        let outcome = ingestor.ingest(after_flush, 1).expect("readmitted");
        assert_eq!(outcome.accepted()[0].sequence(), IngestSeq::new(2));
    }

    #[test]
    fn result_combines_decoder_rejections_with_durable_store_acceptance() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
        let mut ingestor = Ingestor::new(store, IngestLimits::default());
        let mut batch = IngestBatch::new(100, 200);
        batch.reject(SourceLocation::Line(2), "decoder syntax error");
        batch.push_row(
            SourceLocation::Line(1),
            row("cpu", Tags::new(), value_fields("value")),
        );

        let outcome = ingestor.ingest(batch, 1).expect("ingest");

        assert_eq!(outcome.accepted_count(), 1);
        assert_eq!(outcome.rejected_count(), 1);
        assert_eq!(outcome.accepted()[0].source(), SourceLocation::Line(1));
        assert_eq!(outcome.accepted()[0].sequence(), IngestSeq::new(1));
        assert_eq!(outcome.rejections()[0].source(), SourceLocation::Line(2));
        assert!(outcome.rejections()[0].reason().contains("decoder syntax"));
        assert_eq!(
            ingestor
                .sink()
                .read_measurement("cpu")
                .expect("visible rows"),
            vec![SequencedRow::new(
                IngestSeq::new(1),
                row("cpu", Tags::new(), value_fields("value"))
            )]
        );
    }

    #[test]
    fn wal_entry_limit_is_checked_before_sequence_mutation() {
        let root = tempfile::tempdir().expect("tempdir");
        let config = RecoveryConfig::new(
            WalConfig::new(Duration::from_secs(1), 80).expect("WAL config"),
            FlushPolicy::default(),
            ParquetWriterConfig::default(),
            ParquetReaderConfig::default(),
        );
        let store = RecoveryStore::open(root.path(), config).expect("store");
        let mut ingestor = Ingestor::new(store, IngestLimits::default());
        let mut oversized = IngestBatch::new(100, 300);
        oversized.push_row(
            SourceLocation::Item(0),
            row(
                "cpu",
                Tags::new(),
                Fields::from([("value".into(), FieldValue::String("x".repeat(200)))]),
            ),
        );

        let error = ingestor
            .ingest(oversized, 1)
            .expect_err("oversized WAL row");
        assert!(error.to_string().contains("encoded WAL entry"));

        let mut valid = IngestBatch::new(10, 10);
        valid.push_row(
            SourceLocation::Item(1),
            row("cpu", Tags::new(), value_fields("value")),
        );
        let outcome = ingestor.ingest(valid, 1).expect("valid row");
        assert_eq!(outcome.accepted()[0].sequence(), IngestSeq::new(1));
    }

    fn assert_resource_error(limits: IngestLimits, batch: IngestBatch, expected: &str) {
        let mut ingestor = Ingestor::new(FakeSink::default(), limits);
        let error = ingestor.ingest(batch, 1).expect_err("resource limit");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
        assert_eq!(ingestor.sink().calls, 0);
    }
}
