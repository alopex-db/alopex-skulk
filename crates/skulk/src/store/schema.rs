//! Composite query schema resolution for durable and pending columns.

use crate::error::{Result, TsmError};
use crate::model::FieldType;
use crate::store::buffer::{
    ColumnRole, MeasurementState, COLUMN_KIND_METADATA_KEY, FIELD_COLUMN_KIND, INGEST_SEQ_COLUMN,
    INGEST_SEQ_COLUMN_KIND, TAG_COLUMN_KIND, TIME_COLUMN, TIME_COLUMN_KIND,
};
use crate::store::manifest::ManifestState;
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Mutex;

/// The query-visible role of one measurement column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnKind {
    /// The system nanosecond timestamp.
    Time,
    /// A UTF-8 series tag.
    Tag,
    /// A typed, nullable measurement field.
    Field,
}

/// Logical scalar types supported by the Skulk wide model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnDataType {
    /// Nanoseconds since the Unix epoch.
    TimestampNanosecond,
    /// IEEE-754 double precision.
    Float64,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 64-bit integer.
    UInt64,
    /// Boolean.
    Boolean,
    /// UTF-8 string.
    Utf8,
}

/// The role and logical type of one composite schema column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnSchema {
    kind: ColumnKind,
    data_type: ColumnDataType,
}

impl ColumnSchema {
    /// Creates one role/type pair.
    pub const fn new(kind: ColumnKind, data_type: ColumnDataType) -> Self {
        Self { kind, data_type }
    }

    /// Returns the query-visible column role.
    pub const fn kind(&self) -> ColumnKind {
        self.kind
    }

    /// Returns the logical scalar type.
    pub const fn data_type(&self) -> ColumnDataType {
        self.data_type
    }
}

/// A fully composed schema for one measurement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementSchema {
    measurement: String,
    columns: BTreeMap<String, ColumnSchema>,
}

impl MeasurementSchema {
    fn new(measurement: impl Into<String>) -> Self {
        let mut columns = BTreeMap::new();
        columns.insert(
            TIME_COLUMN.to_owned(),
            ColumnSchema::new(ColumnKind::Time, ColumnDataType::TimestampNanosecond),
        );
        Self {
            measurement: measurement.into(),
            columns,
        }
    }

    /// Returns the measurement name.
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns every column in deterministic name order.
    pub fn columns(&self) -> &BTreeMap<String, ColumnSchema> {
        &self.columns
    }

    /// Returns one named column.
    pub fn column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.get(name)
    }

    fn merge(&mut self, name: &str, incoming: ColumnSchema) -> Result<()> {
        if matches!(name, "time" | "__name__") {
            return Err(schema_conflict(
                &self.measurement,
                name,
                "reserved query identifier",
                describe(incoming),
            ));
        }
        match self.columns.get(name) {
            Some(existing) if *existing != incoming => Err(schema_conflict(
                &self.measurement,
                name,
                describe(*existing),
                describe(incoming),
            )),
            Some(_) => Ok(()),
            None => {
                self.columns.insert(name.to_owned(), incoming);
                Ok(())
            }
        }
    }
}

#[derive(Default)]
struct ParquetSchemaCache {
    generation: Option<u64>,
    schemas: BTreeMap<String, MeasurementSchema>,
}

/// Lazily resolves and caches durable schemas by manifest generation.
#[derive(Default)]
pub struct SchemaResolver {
    parquet_cache: Mutex<ParquetSchemaCache>,
}

impl SchemaResolver {
    /// Creates an empty lazy resolver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves one measurement from durable Parquet plus current pending state.
    pub fn resolve(
        &self,
        manifest: &ManifestState,
        segments_dir: &Path,
        measurement: &str,
        state: Option<&MeasurementState>,
    ) -> Result<MeasurementSchema> {
        if measurement.is_empty() {
            return Err(TsmError::InvalidInput(
                "schema measurement must be non-empty".into(),
            ));
        }
        if state.is_some_and(|state| state.measurement() != measurement) {
            return Err(TsmError::InvalidInput(
                "measurement state belongs to another measurement".into(),
            ));
        }

        let mut cache = self
            .parquet_cache
            .lock()
            .map_err(|_| TsmError::Corruption("schema cache lock is poisoned".into()))?;
        if cache.generation != Some(manifest.generation()) {
            cache.generation = Some(manifest.generation());
            cache.schemas.clear();
        }
        let durable_exists = manifest
            .active_files()
            .values()
            .any(|file| file.measurement() == measurement);
        let mut schema = if let Some(cached) = cache.schemas.get(measurement) {
            cached.clone()
        } else if durable_exists {
            let resolved = resolve_parquet_schema(manifest, segments_dir, measurement)?;
            cache
                .schemas
                .insert(measurement.to_owned(), resolved.clone());
            resolved
        } else {
            MeasurementSchema::new(measurement)
        };
        drop(cache);

        if let Some(state) = state {
            for (name, role) in state.columns() {
                schema.merge(name, buffered_column(*role))?;
            }
        }
        if !durable_exists && state.is_none_or(|state| state.row_count() == 0) {
            return Err(TsmError::InvalidInput(format!(
                "measurement '{measurement}' does not exist"
            )));
        }
        Ok(schema)
    }

    /// Returns the sorted union of durable and pending measurement names.
    pub fn measurement_names<I, S>(&self, manifest: &ManifestState, pending: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut names = manifest
            .active_files()
            .values()
            .map(|file| file.measurement().to_owned())
            .collect::<BTreeSet<_>>();
        names.extend(pending.into_iter().map(|name| name.as_ref().to_owned()));
        names.into_iter().collect()
    }
}

fn resolve_parquet_schema(
    manifest: &ManifestState,
    segments_dir: &Path,
    measurement: &str,
) -> Result<MeasurementSchema> {
    let mut schema = MeasurementSchema::new(measurement);
    for active_file in manifest
        .active_files()
        .values()
        .filter(|file| file.measurement() == measurement)
    {
        let file = File::open(segments_dir.join(active_file.name()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_error)?;
        for field in builder.schema().fields() {
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
            match kind {
                TIME_COLUMN_KIND => {
                    if field.name() != TIME_COLUMN
                        || field.data_type() != &DataType::Timestamp(TimeUnit::Nanosecond, None)
                    {
                        return Err(TsmError::Corruption(
                            "Parquet time column has an invalid name or type".into(),
                        ));
                    }
                }
                INGEST_SEQ_COLUMN_KIND => {
                    if field.name() != INGEST_SEQ_COLUMN || field.data_type() != &DataType::UInt64 {
                        return Err(TsmError::Corruption(
                            "Parquet ingest sequence has an invalid name or type".into(),
                        ));
                    }
                }
                TAG_COLUMN_KIND => {
                    if field.data_type() != &DataType::Utf8 {
                        return Err(TsmError::Corruption(format!(
                            "Parquet tag column '{}' is not Utf8",
                            field.name()
                        )));
                    }
                    schema.merge(
                        field.name(),
                        ColumnSchema::new(ColumnKind::Tag, ColumnDataType::Utf8),
                    )?;
                }
                FIELD_COLUMN_KIND => {
                    schema.merge(
                        field.name(),
                        ColumnSchema::new(ColumnKind::Field, arrow_field_type(field.data_type())?),
                    )?;
                }
                _ => {
                    return Err(TsmError::Corruption(format!(
                        "Parquet column '{}' has invalid Skulk metadata",
                        field.name()
                    )));
                }
            }
        }
    }
    Ok(schema)
}

const fn buffered_column(role: ColumnRole) -> ColumnSchema {
    match role {
        ColumnRole::Tag => ColumnSchema::new(ColumnKind::Tag, ColumnDataType::Utf8),
        ColumnRole::Field(field_type) => {
            ColumnSchema::new(ColumnKind::Field, model_field_type(field_type))
        }
    }
}

const fn model_field_type(field_type: FieldType) -> ColumnDataType {
    match field_type {
        FieldType::Float => ColumnDataType::Float64,
        FieldType::Integer => ColumnDataType::Int64,
        FieldType::Unsigned => ColumnDataType::UInt64,
        FieldType::Boolean => ColumnDataType::Boolean,
        FieldType::String => ColumnDataType::Utf8,
    }
}

fn arrow_field_type(data_type: &DataType) -> Result<ColumnDataType> {
    match data_type {
        DataType::Float64 => Ok(ColumnDataType::Float64),
        DataType::Int64 => Ok(ColumnDataType::Int64),
        DataType::UInt64 => Ok(ColumnDataType::UInt64),
        DataType::Boolean => Ok(ColumnDataType::Boolean),
        DataType::Utf8 => Ok(ColumnDataType::Utf8),
        _ => Err(TsmError::InvalidFormat(format!(
            "unsupported query schema field type {data_type}"
        ))),
    }
}

fn describe(column: ColumnSchema) -> String {
    format!("{:?}/{:?}", column.kind(), column.data_type())
}

fn schema_conflict(
    measurement: &str,
    column: &str,
    existing: impl Into<String>,
    incoming: impl Into<String>,
) -> TsmError {
    TsmError::SchemaConflict {
        measurement: measurement.to_owned(),
        column: column.to_owned(),
        existing: existing.into(),
        incoming: incoming.into(),
    }
}

fn parquet_error(error: parquet::errors::ParquetError) -> TsmError {
    TsmError::InvalidFormat(format!("Parquet schema read failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{ColumnDataType, ColumnKind, SchemaResolver};
    use crate::error::TsmError;
    use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
    use crate::store::buffer::{FlushPolicy, MeasurementBuffer, MeasurementState};
    use crate::store::manifest::{ActiveFile, ManifestStore, ManifestUpdate};
    use crate::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
    use crate::store::seq::{IngestSeq, SequencedRow};

    fn row(measurement: &str, tags: Tags, fields: Fields) -> WideRow {
        WideRow::new(SeriesKey::new(measurement, tags), 10, fields)
    }

    fn persist(store: &ManifestStore, measurement: &str, name: &str, row: WideRow) -> ActiveFile {
        let mut buffer = MeasurementBuffer::new(
            measurement,
            FlushPolicy::new(2, 1024 * 1024).expect("policy"),
        );
        buffer
            .append(&SequencedRow::new(IngestSeq::new(1), row))
            .expect("append");
        let batch = buffer.drain_sorted().expect("batch");
        let written = ParquetWriter::new(ParquetWriterConfig::default())
            .write_atomic(store.segments_dir().join(name), &batch)
            .expect("write");
        ActiveFile::new(
            measurement,
            name,
            written.row_count() as u64,
            written.file_bytes(),
            10,
            10,
        )
        .expect("active file")
    }

    #[test]
    fn parquet_and_current_measurement_state_are_composed() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let file = persist(
            &store,
            "cpu",
            "cpu.parquet",
            row(
                "cpu",
                Tags::from([("host".into(), "edge-a".into())]),
                Fields::from([("value".into(), FieldValue::Float(1.0))]),
            ),
        );
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");
        let mut state = MeasurementState::new("cpu");
        state
            .record(&row(
                "cpu",
                Tags::from([("region".into(), "east".into())]),
                Fields::from([("count".into(), FieldValue::Unsigned(2))]),
            ))
            .expect("state");

        let schema = SchemaResolver::new()
            .resolve(
                &store.state().expect("manifest state"),
                store.segments_dir(),
                "cpu",
                Some(&state),
            )
            .expect("resolve");

        assert_eq!(
            schema.column("_time").expect("time").kind(),
            ColumnKind::Time
        );
        assert_eq!(schema.column("host").expect("host").kind(), ColumnKind::Tag);
        assert_eq!(
            schema.column("value").expect("value").data_type(),
            ColumnDataType::Float64
        );
        assert_eq!(
            schema.column("region").expect("region").kind(),
            ColumnKind::Tag
        );
        assert_eq!(
            schema.column("count").expect("count").data_type(),
            ColumnDataType::UInt64
        );
    }

    #[test]
    fn field_type_and_tag_role_conflicts_are_machine_readable() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let float_file = persist(
            &store,
            "cpu",
            "float.parquet",
            row(
                "cpu",
                Tags::new(),
                Fields::from([("value".into(), FieldValue::Float(1.0))]),
            ),
        );
        let integer_file = persist(
            &store,
            "cpu",
            "integer.parquet",
            row(
                "cpu",
                Tags::new(),
                Fields::from([("value".into(), FieldValue::Integer(1))]),
            ),
        );
        store
            .publish(
                ManifestUpdate::new()
                    .add_file(float_file)
                    .add_file(integer_file),
            )
            .expect("publish");

        let error = SchemaResolver::new()
            .resolve(
                &store.state().expect("state"),
                store.segments_dir(),
                "cpu",
                None,
            )
            .expect_err("type conflict");
        assert!(matches!(
            error,
            TsmError::SchemaConflict {
                measurement,
                column,
                ..
            } if measurement == "cpu" && column == "value"
        ));

        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let tag_file = persist(
            &store,
            "cpu",
            "tag.parquet",
            row(
                "cpu",
                Tags::from([("host".into(), "edge-a".into())]),
                Fields::new(),
            ),
        );
        let field_file = persist(
            &store,
            "cpu",
            "field.parquet",
            row(
                "cpu",
                Tags::new(),
                Fields::from([("host".into(), FieldValue::String("edge-a".into()))]),
            ),
        );
        store
            .publish(
                ManifestUpdate::new()
                    .add_file(tag_file)
                    .add_file(field_file),
            )
            .expect("publish");
        assert!(matches!(
            SchemaResolver::new().resolve(
                &store.state().expect("state"),
                store.segments_dir(),
                "cpu",
                None,
            ),
            Err(TsmError::SchemaConflict { column, .. }) if column == "host"
        ));
    }

    #[test]
    fn pending_state_is_never_hidden_by_the_manifest_generation_cache() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let first = persist(
            &store,
            "cpu",
            "first.parquet",
            row(
                "cpu",
                Tags::new(),
                Fields::from([("first".into(), FieldValue::Boolean(true))]),
            ),
        );
        store
            .publish(ManifestUpdate::new().add_file(first))
            .expect("publish");
        let resolver = SchemaResolver::new();
        let generation_one = store.state().expect("generation one");
        assert!(resolver
            .resolve(&generation_one, store.segments_dir(), "cpu", None,)
            .expect("first resolve")
            .column("pending")
            .is_none());

        let mut pending = MeasurementState::new("cpu");
        pending
            .record(&row(
                "cpu",
                Tags::new(),
                Fields::from([("pending".into(), FieldValue::Integer(2))]),
            ))
            .expect("pending");
        assert!(resolver
            .resolve(&generation_one, store.segments_dir(), "cpu", Some(&pending),)
            .expect("same generation with pending")
            .column("pending")
            .is_some());

        let second = persist(
            &store,
            "cpu",
            "second.parquet",
            row(
                "cpu",
                Tags::new(),
                Fields::from([("second".into(), FieldValue::String("new".into()))]),
            ),
        );
        let generation_two = store
            .publish(ManifestUpdate::new().add_file(second))
            .expect("generation two");
        assert!(resolver
            .resolve(&generation_two, store.segments_dir(), "cpu", None,)
            .expect("next generation")
            .column("second")
            .is_some());
    }

    #[test]
    fn measurement_names_union_manifest_and_pending_names() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        let file = persist(
            &store,
            "cpu",
            "cpu.parquet",
            row("cpu", Tags::new(), Fields::new()),
        );
        store
            .publish(ManifestUpdate::new().add_file(file))
            .expect("publish");

        assert_eq!(
            SchemaResolver::new()
                .measurement_names(&store.state().expect("state"), ["memory", "cpu"],),
            ["cpu", "memory"]
        );
    }

    #[test]
    fn query_reserved_time_and_name_columns_are_explicit_conflicts() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = ManifestStore::open(root.path()).expect("manifest");
        for (name, tags, fields) in [
            (
                "time",
                Tags::new(),
                Fields::from([("time".into(), FieldValue::Integer(1))]),
            ),
            (
                "__name__",
                Tags::from([("__name__".into(), "cpu".into())]),
                Fields::new(),
            ),
        ] {
            let mut state = MeasurementState::new("cpu");
            state
                .record(&row("cpu", tags, fields))
                .expect("ingest state accepts user column");
            assert!(matches!(
                SchemaResolver::new().resolve(
                    &store.state().expect("state"),
                    store.segments_dir(),
                    "cpu",
                    Some(&state),
                ),
                Err(TsmError::SchemaConflict { column, .. }) if column == name
            ));
        }
    }
}
