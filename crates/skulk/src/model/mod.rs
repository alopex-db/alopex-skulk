//! Wide time-series data model and series identity.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::sync::Arc;
use xxhash_rust::xxh64::Xxh64;

/// Timestamp in nanoseconds since the Unix epoch.
pub type Timestamp = i64;

/// Canonically ordered tag names and values.
pub type Tags = BTreeMap<String, String>;

/// Canonically ordered field names and typed values.
pub type Fields = BTreeMap<String, FieldValue>;

/// Stable identifier derived from a measurement and its tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeriesId(u64);

impl SeriesId {
    /// Returns the underlying deterministic identifier.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Measurement and tags that define one time-series identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    measurement: String,
    tags: Tags,
}

impl SeriesKey {
    /// Creates a canonical series key.
    pub fn new(measurement: impl Into<String>, tags: Tags) -> Self {
        Self {
            measurement: measurement.into(),
            tags,
        }
    }

    /// Returns the measurement name.
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Returns the canonically ordered tags.
    pub fn tags(&self) -> &Tags {
        &self.tags
    }

    /// Computes the stable series identifier.
    pub fn id(&self) -> SeriesId {
        let mut hasher = Xxh64::new(0);
        hash_part(&mut hasher, self.measurement.as_bytes());
        for (name, value) in &self.tags {
            hash_part(&mut hasher, name.as_bytes());
            hash_part(&mut hasher, value.as_bytes());
        }
        SeriesId(hasher.finish())
    }

    /// Splits the key into its owned measurement and tags.
    pub fn into_parts(self) -> (String, Tags) {
        (self.measurement, self.tags)
    }
}

fn hash_part(hasher: &mut Xxh64, value: &[u8]) {
    hasher.write_u64(value.len() as u64);
    hasher.write(value);
}

/// Logical type of a field value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// IEEE-754 double precision value.
    Float,
    /// Signed 64-bit integer.
    Integer,
    /// Unsigned 64-bit integer.
    Unsigned,
    /// Boolean value.
    Boolean,
    /// UTF-8 string value.
    String,
}

/// One typed value in a wide time-series row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    /// IEEE-754 double precision value.
    Float(f64),
    /// Signed 64-bit integer.
    Integer(i64),
    /// Unsigned 64-bit integer.
    Unsigned(u64),
    /// Boolean value.
    Boolean(bool),
    /// UTF-8 string value.
    String(String),
}

impl FieldValue {
    /// Returns the logical field type without coercing the value.
    pub const fn field_type(&self) -> FieldType {
        match self {
            Self::Float(_) => FieldType::Float,
            Self::Integer(_) => FieldType::Integer,
            Self::Unsigned(_) => FieldType::Unsigned,
            Self::Boolean(_) => FieldType::Boolean,
            Self::String(_) => FieldType::String,
        }
    }
}

/// One timestamped row containing all fields for a series.
///
/// The series identity is shared (`Arc`) so hot decode paths can reuse one
/// interned key across many rows, and its hash is memoized at construction.
#[derive(Debug, Clone, PartialEq)]
pub struct WideRow {
    series: Arc<SeriesKey>,
    series_id: SeriesId,
    timestamp: Timestamp,
    fields: Fields,
}

impl WideRow {
    /// Creates a wide row without splitting its fields into separate series.
    pub fn new(series: SeriesKey, timestamp: Timestamp, fields: Fields) -> Self {
        let series_id = series.id();
        Self {
            series: Arc::new(series),
            series_id,
            timestamp,
            fields,
        }
    }

    /// Creates a row sharing an interned series identity and memoized id.
    pub fn with_shared_series(
        series: Arc<SeriesKey>,
        series_id: SeriesId,
        timestamp: Timestamp,
        fields: Fields,
    ) -> Self {
        debug_assert_eq!(series.id(), series_id);
        Self {
            series,
            series_id,
            timestamp,
            fields,
        }
    }

    /// Returns the series identity.
    pub fn series(&self) -> &SeriesKey {
        &self.series
    }

    /// Returns the stable identifier for this row's series.
    pub const fn series_id(&self) -> SeriesId {
        self.series_id
    }

    /// Returns the nanosecond timestamp.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns all present fields.
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    /// Returns a named field, or `None` when the field is sparse in this row.
    pub fn field(&self, name: &str) -> Option<&FieldValue> {
        self.fields.get(name)
    }

    /// Splits the row into owned parts for storage adapters.
    pub fn into_parts(self) -> (SeriesKey, Timestamp, Fields) {
        let series = Arc::try_unwrap(self.series).unwrap_or_else(|shared| (*shared).clone());
        (series, self.timestamp, self.fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn map<const N: usize>(entries: [(&str, &str); N]) -> BTreeMap<String, String> {
        entries
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect()
    }

    #[test]
    fn series_identity_is_stable_for_tag_order_and_excludes_fields() {
        let first = SeriesKey::new("weather", map([("region", "east"), ("host", "edge-1")]));
        let reordered = SeriesKey::new("weather", map([("host", "edge-1"), ("region", "east")]));

        let mut fields_a = Fields::new();
        fields_a.insert("temperature".into(), FieldValue::Float(22.5));
        let mut fields_b = Fields::new();
        fields_b.insert("status".into(), FieldValue::String("ok".into()));

        let row_a = WideRow::new(first.clone(), 10, fields_a);
        let row_b = WideRow::new(reordered.clone(), 20, fields_b);

        assert_eq!(first, reordered);
        assert_eq!(row_a.series_id(), row_b.series_id());
        assert_ne!(
            row_a.series_id(),
            SeriesKey::new("different", first.tags().clone()).id()
        );
    }

    #[test]
    fn wide_row_preserves_all_five_field_types() {
        let fields = Fields::from([
            ("float".into(), FieldValue::Float(1.5)),
            ("integer".into(), FieldValue::Integer(-2)),
            ("unsigned".into(), FieldValue::Unsigned(3)),
            ("boolean".into(), FieldValue::Boolean(true)),
            ("string".into(), FieldValue::String("ready".into())),
        ]);
        let row = WideRow::new(SeriesKey::new("mixed", Tags::new()), 42, fields);

        assert_eq!(row.field("float"), Some(&FieldValue::Float(1.5)));
        assert_eq!(row.field("integer"), Some(&FieldValue::Integer(-2)));
        assert_eq!(row.field("unsigned"), Some(&FieldValue::Unsigned(3)));
        assert_eq!(row.field("boolean"), Some(&FieldValue::Boolean(true)));
        assert_eq!(
            row.field("string"),
            Some(&FieldValue::String("ready".into()))
        );
        assert_eq!(row.fields().len(), 5);
    }

    #[test]
    fn missing_field_is_distinguishable_from_a_present_value() {
        let row = WideRow::new(
            SeriesKey::new("sparse", Tags::new()),
            42,
            Fields::from([("present".into(), FieldValue::Integer(0))]),
        );

        assert_eq!(row.field("present"), Some(&FieldValue::Integer(0)));
        assert_eq!(row.field("missing"), None);
    }
}
