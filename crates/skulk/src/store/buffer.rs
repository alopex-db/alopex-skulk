//! Measurement-scoped Arrow buffer for sparse wide rows.

use crate::error::{Result, TsmError};
use crate::model::{FieldType, FieldValue, WideRow};
use crate::store::seq::SequencedRow;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use arrow_select::take::take;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem::size_of;
use std::sync::Arc;

/// Physical Arrow column containing the nanosecond timestamp.
pub const TIME_COLUMN: &str = "_time";
/// Physical Arrow column containing the row-level ingest sequence.
pub const INGEST_SEQ_COLUMN: &str = "_ingest_seq";
/// Arrow field metadata key identifying a tag, field, or system column.
pub const COLUMN_KIND_METADATA_KEY: &str = "skulk.column.kind";
/// Metadata value for an individual tag column.
pub const TAG_COLUMN_KIND: &str = "tag";
/// Metadata value for an individual field column.
pub const FIELD_COLUMN_KIND: &str = "field";
/// Metadata value for the timestamp system column.
pub const TIME_COLUMN_KIND: &str = "time";
/// Metadata value for the ingest-sequence system column.
pub const INGEST_SEQ_COLUMN_KIND: &str = "ingest_seq";

/// Row-count and estimated-memory thresholds that trigger a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushPolicy {
    max_rows: usize,
    max_estimated_bytes: usize,
}

impl FlushPolicy {
    /// Creates non-zero row and memory thresholds.
    pub fn new(max_rows: usize, max_estimated_bytes: usize) -> Result<Self> {
        if max_rows == 0 || max_estimated_bytes == 0 {
            return Err(TsmError::InvalidInput(
                "flush row and estimated-byte thresholds must be non-zero".into(),
            ));
        }
        Ok(Self {
            max_rows,
            max_estimated_bytes,
        })
    }

    /// Returns the row-count threshold.
    pub const fn max_rows(&self) -> usize {
        self.max_rows
    }

    /// Returns the estimated-memory threshold.
    pub const fn max_estimated_bytes(&self) -> usize {
        self.max_estimated_bytes
    }
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self {
            max_rows: 65_536,
            max_estimated_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Mutable Arrow builders for one measurement/table.
pub struct MeasurementBuffer {
    measurement: String,
    policy: FlushPolicy,
    columns: BTreeMap<String, DataColumn>,
    timestamps: TimestampNanosecondBuilder,
    sequences: UInt64Builder,
    sort_rows: Vec<SortRow>,
    estimated_bytes: usize,
}

impl MeasurementBuffer {
    /// Creates an empty measurement-scoped buffer.
    pub fn new(measurement: impl Into<String>, policy: FlushPolicy) -> Self {
        Self {
            measurement: measurement.into(),
            policy,
            columns: BTreeMap::new(),
            timestamps: TimestampNanosecondBuilder::new(),
            sequences: UInt64Builder::new(),
            sort_rows: Vec::new(),
            estimated_bytes: 0,
        }
    }

    /// Returns the measurement/table name accepted by this buffer.
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns the number of buffered rows.
    pub fn row_count(&self) -> usize {
        self.sort_rows.len()
    }

    /// Returns the memory estimate used by the flush policy.
    pub const fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Returns whether either configured flush threshold has been reached.
    pub fn should_flush(&self) -> bool {
        self.row_count() >= self.policy.max_rows
            || self.estimated_bytes >= self.policy.max_estimated_bytes
    }

    /// Adds one sequenced row after validating the table and column union.
    pub fn append(&mut self, sequenced: SequencedRow) -> Result<()> {
        let row = sequenced.row();
        let estimated_bytes = self.validate_append(row)?;

        let prior_rows = self.row_count();
        let mut present = BTreeSet::new();
        for (name, value) in row.series().tags() {
            present.insert(name.clone());
            let column = self
                .columns
                .entry(name.clone())
                .or_insert_with(|| DataColumn::new(ColumnRole::Tag, prior_rows));
            column.append_tag(value)?;
        }
        for (name, value) in row.fields() {
            present.insert(name.clone());
            let role = ColumnRole::Field(value.field_type());
            let column = self
                .columns
                .entry(name.clone())
                .or_insert_with(|| DataColumn::new(role, prior_rows));
            column.append_field(value)?;
        }
        for (name, column) in &mut self.columns {
            if !present.contains(name) {
                column.append_null();
            }
        }

        self.timestamps.append_value(row.timestamp());
        self.sequences.append_value(sequenced.ingest_seq().get());
        self.sort_rows.push(SortRow {
            tags: row.series().tags().clone(),
            timestamp: row.timestamp(),
            ingest_seq: sequenced.ingest_seq().get(),
        });
        self.estimated_bytes = estimated_bytes;
        Ok(())
    }

    pub(crate) fn validate_append(&self, row: &WideRow) -> Result<usize> {
        self.validate_row(row)?;
        let row_bytes = estimated_row_bytes(row)?;
        self.estimated_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| TsmError::ResourceLimit("buffer memory estimate overflow".into()))
    }

    /// Builds a schema-union RecordBatch sorted by tags, time, then sequence.
    pub fn to_sorted_record_batch(&self) -> Result<RecordBatch> {
        if self.sort_rows.is_empty() {
            return Err(TsmError::InvalidInput(
                "cannot build an Arrow batch from an empty buffer".into(),
            ));
        }

        let (fields, columns) = self.unsorted_schema_and_columns();
        let schema = Arc::new(Schema::new(fields));
        let unsorted = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow_error)?;
        let indices = UInt64Array::from(self.sorted_indices()?);
        let sorted_columns = unsorted
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &indices, None).map_err(arrow_error))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(schema, sorted_columns).map_err(arrow_error)
    }

    /// Builds the sorted batch and clears buffered rows only after success.
    pub fn drain_sorted(&mut self) -> Result<RecordBatch> {
        let batch = self.to_sorted_record_batch()?;
        self.columns.clear();
        self.timestamps = TimestampNanosecondBuilder::new();
        self.sequences = UInt64Builder::new();
        self.sort_rows.clear();
        self.estimated_bytes = 0;
        Ok(batch)
    }

    fn validate_row(&self, row: &WideRow) -> Result<()> {
        if row.series().measurement() != self.measurement {
            return Err(TsmError::InvalidInput(format!(
                "measurement '{}' does not match buffer '{}'",
                row.series().measurement(),
                self.measurement
            )));
        }

        for name in row.series().tags().keys() {
            validate_user_column_name(name)?;
            if row.fields().contains_key(name) {
                return Err(TsmError::InvalidInput(format!(
                    "column '{name}' cannot be both a tag and a field"
                )));
            }
            if let Some(column) = self.columns.get(name) {
                if column.role != ColumnRole::Tag {
                    return Err(column_role_conflict(name));
                }
            }
        }
        for (name, value) in row.fields() {
            validate_user_column_name(name)?;
            let expected = ColumnRole::Field(value.field_type());
            if let Some(column) = self.columns.get(name) {
                if column.role != expected {
                    return Err(column_role_conflict(name));
                }
            }
        }
        Ok(())
    }

    fn unsorted_schema_and_columns(&self) -> (Vec<Field>, Vec<ArrayRef>) {
        let mut fields = Vec::with_capacity(self.columns.len() + 2);
        let mut columns = Vec::with_capacity(self.columns.len() + 2);

        for (name, column) in self
            .columns
            .iter()
            .filter(|(_, column)| column.role == ColumnRole::Tag)
        {
            fields.push(arrow_field(name, column.data_type(), true, TAG_COLUMN_KIND));
            columns.push(column.as_arrow());
        }
        fields.push(arrow_field(
            TIME_COLUMN,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
            TIME_COLUMN_KIND,
        ));
        columns.push(Arc::new(self.timestamps.finish_cloned()));
        fields.push(arrow_field(
            INGEST_SEQ_COLUMN,
            DataType::UInt64,
            false,
            INGEST_SEQ_COLUMN_KIND,
        ));
        columns.push(Arc::new(self.sequences.finish_cloned()));
        for (name, column) in self
            .columns
            .iter()
            .filter(|(_, column)| matches!(column.role, ColumnRole::Field(_)))
        {
            fields.push(arrow_field(
                name,
                column.data_type(),
                true,
                FIELD_COLUMN_KIND,
            ));
            columns.push(column.as_arrow());
        }
        (fields, columns)
    }

    fn sorted_indices(&self) -> Result<Vec<u64>> {
        let tag_names = self
            .columns
            .iter()
            .filter_map(|(name, column)| (column.role == ColumnRole::Tag).then_some(name.as_str()))
            .collect::<Vec<_>>();
        let mut indices = (0..self.sort_rows.len()).collect::<Vec<_>>();
        indices.sort_by(|left, right| {
            compare_sort_rows(&self.sort_rows[*left], &self.sort_rows[*right], &tag_names)
        });
        indices
            .into_iter()
            .map(|index| {
                u64::try_from(index)
                    .map_err(|_| TsmError::ResourceLimit("Arrow row index exceeds u64".into()))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnRole {
    Tag,
    Field(FieldType),
}

struct DataColumn {
    role: ColumnRole,
    builder: ColumnBuilder,
}

impl DataColumn {
    fn new(role: ColumnRole, prior_rows: usize) -> Self {
        let mut builder = ColumnBuilder::new(role);
        builder.append_nulls(prior_rows);
        Self { role, builder }
    }

    fn append_tag(&mut self, value: &str) -> Result<()> {
        match &mut self.builder {
            ColumnBuilder::String(builder) if self.role == ColumnRole::Tag => {
                builder.append_value(value);
                Ok(())
            }
            _ => Err(TsmError::Corruption(
                "tag column builder does not match its role".into(),
            )),
        }
    }

    fn append_field(&mut self, value: &FieldValue) -> Result<()> {
        match (&mut self.builder, value) {
            (ColumnBuilder::Float(builder), FieldValue::Float(value)) => {
                builder.append_value(*value)
            }
            (ColumnBuilder::Integer(builder), FieldValue::Integer(value)) => {
                builder.append_value(*value)
            }
            (ColumnBuilder::Unsigned(builder), FieldValue::Unsigned(value)) => {
                builder.append_value(*value)
            }
            (ColumnBuilder::Boolean(builder), FieldValue::Boolean(value)) => {
                builder.append_value(*value)
            }
            (ColumnBuilder::String(builder), FieldValue::String(value)) => {
                builder.append_value(value)
            }
            _ => {
                return Err(TsmError::Corruption(
                    "field column builder does not match its role".into(),
                ));
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn data_type(&self) -> DataType {
        self.builder.data_type()
    }

    fn as_arrow(&self) -> ArrayRef {
        self.builder.as_arrow()
    }
}

enum ColumnBuilder {
    Float(Float64Builder),
    Integer(Int64Builder),
    Unsigned(UInt64Builder),
    Boolean(BooleanBuilder),
    String(StringBuilder),
}

impl ColumnBuilder {
    fn new(role: ColumnRole) -> Self {
        match role {
            ColumnRole::Tag | ColumnRole::Field(FieldType::String) => {
                Self::String(StringBuilder::new())
            }
            ColumnRole::Field(FieldType::Float) => Self::Float(Float64Builder::new()),
            ColumnRole::Field(FieldType::Integer) => Self::Integer(Int64Builder::new()),
            ColumnRole::Field(FieldType::Unsigned) => Self::Unsigned(UInt64Builder::new()),
            ColumnRole::Field(FieldType::Boolean) => Self::Boolean(BooleanBuilder::new()),
        }
    }

    fn append_nulls(&mut self, count: usize) {
        match self {
            Self::Float(builder) => builder.append_nulls(count),
            Self::Integer(builder) => builder.append_nulls(count),
            Self::Unsigned(builder) => builder.append_nulls(count),
            Self::Boolean(builder) => builder.append_nulls(count),
            Self::String(builder) => builder.append_nulls(count),
        }
    }

    fn append_null(&mut self) {
        self.append_nulls(1);
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Float(_) => DataType::Float64,
            Self::Integer(_) => DataType::Int64,
            Self::Unsigned(_) => DataType::UInt64,
            Self::Boolean(_) => DataType::Boolean,
            Self::String(_) => DataType::Utf8,
        }
    }

    fn as_arrow(&self) -> ArrayRef {
        match self {
            Self::Float(builder) => Arc::new(builder.finish_cloned()),
            Self::Integer(builder) => Arc::new(builder.finish_cloned()),
            Self::Unsigned(builder) => Arc::new(builder.finish_cloned()),
            Self::Boolean(builder) => Arc::new(builder.finish_cloned()),
            Self::String(builder) => Arc::new(builder.finish_cloned()),
        }
    }
}

struct SortRow {
    tags: BTreeMap<String, String>,
    timestamp: i64,
    ingest_seq: u64,
}

fn compare_sort_rows(left: &SortRow, right: &SortRow, tag_names: &[&str]) -> Ordering {
    for tag_name in tag_names {
        let ordering = left.tags.get(*tag_name).cmp(&right.tags.get(*tag_name));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.timestamp
        .cmp(&right.timestamp)
        .then_with(|| left.ingest_seq.cmp(&right.ingest_seq))
}

fn validate_user_column_name(name: &str) -> Result<()> {
    if matches!(name, TIME_COLUMN | INGEST_SEQ_COLUMN) {
        return Err(TsmError::InvalidInput(format!(
            "column '{name}' is reserved by Skulk"
        )));
    }
    Ok(())
}

fn column_role_conflict(name: &str) -> TsmError {
    TsmError::InvalidInput(format!(
        "column '{name}' changed tag/field role or field type"
    ))
}

fn arrow_field(name: &str, data_type: DataType, nullable: bool, kind: &str) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        COLUMN_KIND_METADATA_KEY.to_owned(),
        kind.to_owned(),
    )]))
}

pub(crate) fn estimated_row_bytes(row: &WideRow) -> Result<usize> {
    let mut bytes = size_of::<i64>() + size_of::<u64>();
    for (name, value) in row.series().tags() {
        bytes = checked_add(bytes, name.len())?;
        let duplicated_value_bytes = value
            .len()
            .checked_mul(2)
            .ok_or_else(|| TsmError::ResourceLimit("buffer memory estimate overflow".into()))?;
        bytes = checked_add(bytes, duplicated_value_bytes)?;
    }
    for (name, value) in row.fields() {
        bytes = checked_add(bytes, name.len())?;
        bytes = checked_add(
            bytes,
            match value {
                FieldValue::Float(_) => size_of::<f64>(),
                FieldValue::Integer(_) => size_of::<i64>(),
                FieldValue::Unsigned(_) => size_of::<u64>(),
                FieldValue::Boolean(_) => size_of::<bool>(),
                FieldValue::String(value) => value.len(),
            },
        )?;
    }
    Ok(bytes)
}

fn checked_add(current: usize, additional: usize) -> Result<usize> {
    current
        .checked_add(additional)
        .ok_or_else(|| TsmError::ResourceLimit("buffer memory estimate overflow".into()))
}

fn arrow_error(error: ArrowError) -> TsmError {
    TsmError::Serialization(format!("Arrow record batch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        FlushPolicy, MeasurementBuffer, COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND,
        INGEST_SEQ_COLUMN, TAG_COLUMN_KIND, TIME_COLUMN,
    };
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::seq::{IngestSeq, SequencedRow};
    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt64Array,
    };
    use arrow_schema::DataType;

    fn sequenced(
        sequence: u64,
        measurement: &str,
        tags: &[(&str, &str)],
        timestamp: i64,
        fields: Fields,
    ) -> SequencedRow {
        SequencedRow::new(
            IngestSeq::new(sequence),
            WideRow::new(
                SeriesKey::new(
                    measurement,
                    tags.iter()
                        .map(|(name, value)| ((*name).into(), (*value).into()))
                        .collect::<Tags>(),
                ),
                timestamp,
                fields,
            ),
        )
    }

    fn unlimited_policy() -> FlushPolicy {
        FlushPolicy::new(1_000, 1024 * 1024).expect("valid policy")
    }

    #[test]
    fn sparse_fields_and_late_columns_are_arrow_nulls() {
        let mut buffer = MeasurementBuffer::new("weather", unlimited_policy());
        buffer
            .append(sequenced(
                1,
                "weather",
                &[("region", "east")],
                10,
                Fields::from([("temperature".into(), FieldValue::Float(21.5))]),
            ))
            .expect("first row");
        buffer
            .append(sequenced(
                2,
                "weather",
                &[("region", "east"), ("zone", "1a")],
                20,
                Fields::from([("status".into(), FieldValue::String("ok".into()))]),
            ))
            .expect("second row");

        let batch = buffer.to_sorted_record_batch().expect("record batch");
        let temperature = batch
            .column_by_name("temperature")
            .expect("temperature")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column");
        let status = batch
            .column_by_name("status")
            .expect("status")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");

        assert_eq!(temperature.value(0), 21.5);
        assert!(temperature.is_null(1));
        assert!(status.is_null(0));
        assert_eq!(status.value(1), "ok");
        let zone = batch
            .column_by_name("zone")
            .expect("zone")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tag column");
        assert!(zone.is_null(0));
        assert_eq!(zone.value(1), "1a");
    }

    #[test]
    fn every_field_type_keeps_its_arrow_type_and_value() {
        let mut buffer = MeasurementBuffer::new("mixed", unlimited_policy());
        buffer
            .append(sequenced(
                7,
                "mixed",
                &[],
                42,
                Fields::from([
                    ("boolean".into(), FieldValue::Boolean(true)),
                    ("float".into(), FieldValue::Float(1.25)),
                    ("integer".into(), FieldValue::Integer(-2)),
                    ("string".into(), FieldValue::String("ready".into())),
                    ("unsigned".into(), FieldValue::Unsigned(3)),
                ]),
            ))
            .expect("mixed row");

        let batch = buffer.to_sorted_record_batch().expect("record batch");
        assert_eq!(
            batch
                .schema()
                .field_with_name("float")
                .expect("float")
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            batch
                .column_by_name("float")
                .expect("float")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float array")
                .value(0),
            1.25
        );
        assert_eq!(
            batch
                .column_by_name("integer")
                .expect("integer")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("integer array")
                .value(0),
            -2
        );
        assert_eq!(
            batch
                .column_by_name("unsigned")
                .expect("unsigned")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("unsigned array")
                .value(0),
            3
        );
        assert!(batch
            .column_by_name("boolean")
            .expect("boolean")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean array")
            .value(0));
        assert_eq!(
            batch
                .column_by_name("string")
                .expect("string")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .value(0),
            "ready"
        );
    }

    #[test]
    fn batch_is_sorted_by_tags_then_timestamp_then_ingest_sequence() {
        let mut buffer = MeasurementBuffer::new("cpu", unlimited_policy());
        for row in [
            sequenced(
                2,
                "cpu",
                &[("host", "b")],
                10,
                Fields::from([("value".into(), FieldValue::Integer(2))]),
            ),
            sequenced(
                3,
                "cpu",
                &[("host", "a")],
                20,
                Fields::from([("value".into(), FieldValue::Integer(3))]),
            ),
            sequenced(
                4,
                "cpu",
                &[("host", "a")],
                10,
                Fields::from([("value".into(), FieldValue::Integer(4))]),
            ),
            sequenced(
                1,
                "cpu",
                &[("host", "a")],
                10,
                Fields::from([("value".into(), FieldValue::Integer(1))]),
            ),
        ] {
            buffer.append(row).expect("append row");
        }

        let batch = buffer.to_sorted_record_batch().expect("record batch");
        assert_eq!(
            batch
                .schema()
                .field_with_name("host")
                .expect("host")
                .metadata()
                .get(COLUMN_KIND_METADATA_KEY)
                .map(String::as_str),
            Some(TAG_COLUMN_KIND)
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("value")
                .expect("value")
                .metadata()
                .get(COLUMN_KIND_METADATA_KEY)
                .map(String::as_str),
            Some(FIELD_COLUMN_KIND)
        );
        let hosts = batch
            .column_by_name("host")
            .expect("host")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("host strings");
        let times = batch
            .column_by_name(TIME_COLUMN)
            .expect("time")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamps");
        let sequences = batch
            .column_by_name(INGEST_SEQ_COLUMN)
            .expect("sequence")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("sequences");

        assert_eq!(
            (0..4).map(|index| hosts.value(index)).collect::<Vec<_>>(),
            ["a", "a", "a", "b"]
        );
        assert_eq!(
            (0..4).map(|index| times.value(index)).collect::<Vec<_>>(),
            [10, 10, 20, 10]
        );
        assert_eq!(
            (0..4)
                .map(|index| sequences.value(index))
                .collect::<Vec<_>>(),
            [1, 4, 3, 2]
        );
    }

    #[test]
    fn measurement_or_field_type_conflicts_do_not_partially_mutate_the_buffer() {
        let mut buffer = MeasurementBuffer::new("cpu", unlimited_policy());
        buffer
            .append(sequenced(
                1,
                "cpu",
                &[],
                1,
                Fields::from([("value".into(), FieldValue::Integer(1))]),
            ))
            .expect("initial row");

        assert!(buffer
            .append(sequenced(
                2,
                "memory",
                &[],
                2,
                Fields::from([("value".into(), FieldValue::Integer(2))]),
            ))
            .is_err());
        assert!(buffer
            .append(sequenced(
                3,
                "cpu",
                &[],
                3,
                Fields::from([("value".into(), FieldValue::String("bad".into()))]),
            ))
            .is_err());
        assert_eq!(buffer.row_count(), 1);
    }

    #[test]
    fn flush_policy_triggers_on_rows_or_estimated_memory() {
        let mut row_limited =
            MeasurementBuffer::new("cpu", FlushPolicy::new(2, usize::MAX).expect("policy"));
        row_limited
            .append(sequenced(1, "cpu", &[], 1, Fields::new()))
            .expect("first row");
        assert!(!row_limited.should_flush());
        row_limited
            .append(sequenced(2, "cpu", &[], 2, Fields::new()))
            .expect("second row");
        assert!(row_limited.should_flush());

        let mut memory_limited =
            MeasurementBuffer::new("logs", FlushPolicy::new(100, 32).expect("policy"));
        memory_limited
            .append(sequenced(
                1,
                "logs",
                &[],
                1,
                Fields::from([("message".into(), FieldValue::String("x".repeat(64)))]),
            ))
            .expect("large row");
        assert!(memory_limited.should_flush());
    }

    #[test]
    fn draining_a_sorted_batch_resets_only_after_success() {
        let mut buffer = MeasurementBuffer::new("cpu", unlimited_policy());
        buffer
            .append(sequenced(1, "cpu", &[], 1, Fields::new()))
            .expect("row");

        let batch = buffer.drain_sorted().expect("drain");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(buffer.row_count(), 0);
        assert!(!buffer.should_flush());
        assert!(buffer.to_sorted_record_batch().is_err());
    }
}
