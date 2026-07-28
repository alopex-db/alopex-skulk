//! Structured JSON ingest decoder.

use super::{IngestBatch, IngestLimits, SourceLocation};
use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use serde_json::{Map, Number, Value};

const BATCH_KEY: &str = "metrics";
const CANONICAL_NAME_KEY: &str = "name";
const SINGLE_NAME_KEY: &str = "metric";
const TAGS_KEY: &str = "tags";
const FIELDS_KEY: &str = "fields";
const TIMESTAMP_KEY: &str = "timestamp";

/// Pure byte-to-wide-row decoder for the canonical batch and single-point JSON forms.
#[derive(Debug, Clone, Copy)]
pub struct JsonIngestDecoder {
    limits: IngestLimits,
}

impl JsonIngestDecoder {
    /// Creates a decoder that shares the unified ingestion resource policy.
    pub const fn new(limits: IngestLimits) -> Self {
        Self { limits }
    }

    /// Decodes JSON, retaining item-local schema errors for partial success.
    pub fn decode(&self, input: &[u8], default_timestamp: i64) -> Result<IngestBatch> {
        self.limits
            .validate_request_bytes(input.len(), input.len())?;
        let root: Value = serde_json::from_slice(input)
            .map_err(|error| TsmError::InvalidFormat(format!("JSON decode failed: {error}")))?;
        let object = root.as_object().ok_or_else(|| {
            TsmError::InvalidFormat(
                "JSON ingest root must be an object containing metrics or one metric".into(),
            )
        })?;

        if let Some(metrics) = object.get(BATCH_KEY) {
            validate_batch_root(object)?;
            let items = metrics
                .as_array()
                .ok_or_else(|| TsmError::InvalidFormat("JSON metrics must be an array".into()))?;
            self.decode_items(items, CANONICAL_NAME_KEY, input.len(), default_timestamp)
        } else {
            self.validate_item_count(1)?;
            let mut batch = IngestBatch::new(input.len(), input.len());
            decode_item(
                object,
                SINGLE_NAME_KEY,
                SourceLocation::Item(0),
                default_timestamp,
                &mut batch,
            );
            Ok(batch)
        }
    }

    fn decode_items(
        &self,
        items: &[Value],
        name_key: &str,
        encoded_bytes: usize,
        default_timestamp: i64,
    ) -> Result<IngestBatch> {
        self.validate_item_count(items.len())?;
        let mut batch = IngestBatch::new(encoded_bytes, encoded_bytes);
        for (index, item) in items.iter().enumerate() {
            let source = SourceLocation::Item(index);
            match item.as_object() {
                Some(object) => {
                    decode_item(object, name_key, source, default_timestamp, &mut batch)
                }
                None => batch.reject(source, "JSON metric item must be an object"),
            }
        }
        Ok(batch)
    }

    fn validate_item_count(&self, item_count: usize) -> Result<()> {
        let request = self.limits.request();
        if item_count > request.max_rows() {
            return Err(TsmError::ResourceLimit(format!(
                "JSON items {item_count} exceeds row limit {}",
                request.max_rows()
            )));
        }
        if item_count > request.max_series() {
            return Err(TsmError::ResourceLimit(format!(
                "JSON items {item_count} exceeds series limit {}",
                request.max_series()
            )));
        }
        Ok(())
    }
}

fn validate_batch_root(object: &Map<String, Value>) -> Result<()> {
    if let Some(key) = object.keys().find(|key| key.as_str() != BATCH_KEY) {
        return Err(TsmError::InvalidFormat(format!(
            "unexpected JSON batch key '{key}'"
        )));
    }
    Ok(())
}

fn decode_item(
    object: &Map<String, Value>,
    name_key: &str,
    source: SourceLocation,
    default_timestamp: i64,
    batch: &mut IngestBatch,
) {
    match wide_row(object, name_key, default_timestamp) {
        Ok(row) => batch.push_row(source, row),
        Err(reason) => batch.reject(source, reason),
    }
}

fn wide_row(
    object: &Map<String, Value>,
    name_key: &str,
    default_timestamp: i64,
) -> std::result::Result<WideRow, String> {
    validate_item_keys(object, name_key)?;
    let measurement = object
        .get(name_key)
        .ok_or_else(|| format!("missing JSON {name_key}"))?
        .as_str()
        .ok_or_else(|| format!("JSON {name_key} must be a string"))?
        .to_owned();
    let tags = parse_tags(object.get(TAGS_KEY))?;
    let fields = parse_fields(object.get(FIELDS_KEY))?;
    let timestamp = parse_timestamp(object.get(TIMESTAMP_KEY), default_timestamp)?;
    Ok(WideRow::new(
        SeriesKey::new(measurement, tags),
        timestamp,
        fields,
    ))
}

fn validate_item_keys(
    object: &Map<String, Value>,
    name_key: &str,
) -> std::result::Result<(), String> {
    if let Some(key) = object.keys().find(|key| {
        !matches!(key.as_str(), TAGS_KEY | FIELDS_KEY | TIMESTAMP_KEY) && key.as_str() != name_key
    }) {
        return Err(format!("unexpected JSON metric key '{key}'"));
    }
    Ok(())
}

fn parse_tags(value: Option<&Value>) -> std::result::Result<Tags, String> {
    let Some(value) = value else {
        return Ok(Tags::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| "JSON tags must be an object".to_owned())?;
    object
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_owned()))
                .ok_or_else(|| format!("JSON tag '{name}' must be a string"))
        })
        .collect()
}

fn parse_fields(value: Option<&Value>) -> std::result::Result<Fields, String> {
    let object = value
        .ok_or_else(|| "missing JSON fields".to_owned())?
        .as_object()
        .ok_or_else(|| "JSON fields must be an object".to_owned())?;
    object
        .iter()
        .map(|(name, value)| {
            json_field(value)
                .map(|value| (name.clone(), value))
                .ok_or_else(|| format!("JSON field '{name}' must be a number, boolean, or string"))
        })
        .collect()
}

fn json_field(value: &Value) -> Option<FieldValue> {
    match value {
        Value::Number(number) => number_field(number),
        Value::Bool(value) => Some(FieldValue::Boolean(*value)),
        Value::String(value) => Some(FieldValue::String(value.clone())),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn number_field(number: &Number) -> Option<FieldValue> {
    if let Some(value) = number.as_i64() {
        Some(FieldValue::Integer(value))
    } else if let Some(value) = number.as_u64() {
        Some(FieldValue::Unsigned(value))
    } else {
        number.as_f64().map(FieldValue::Float)
    }
}

fn parse_timestamp(value: Option<&Value>, default: i64) -> std::result::Result<i64, String> {
    match value {
        None => Ok(default),
        Some(Value::Number(number)) => number
            .as_i64()
            .ok_or_else(|| "JSON timestamp must be an i64 nanosecond integer".into()),
        Some(_) => Err("JSON timestamp must be an i64 nanosecond integer".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::JsonIngestDecoder;
    use crate::ingest::{AdmissionLimits, IngestLimits, RequestLimits, RowLimits, SourceLocation};
    use crate::model::FieldValue;
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn canonical_batch_preserves_tags_timestamp_and_all_json_scalar_types() {
        let batch = decode(
            br#"{
                "metrics": [{
                    "name": "system",
                    "tags": {"host": "edge", "region": "ap"},
                    "fields": {
                        "float": 1.5,
                        "signed": -2,
                        "integer": 3,
                        "unsigned": 18446744073709551615,
                        "boolean": true,
                        "string": "ready"
                    },
                    "timestamp": 42
                }]
            }"#,
            999,
        )
        .expect("canonical batch");

        assert_eq!(batch.rows.len(), 1);
        let row = &batch.rows[0].row;
        assert_eq!(row.series().measurement(), "system");
        assert_eq!(
            row.series().tags().get("host").map(String::as_str),
            Some("edge")
        );
        assert_eq!(row.timestamp(), 42);
        assert_eq!(row.field("float"), Some(&FieldValue::Float(1.5)));
        assert_eq!(row.field("signed"), Some(&FieldValue::Integer(-2)));
        assert_eq!(row.field("integer"), Some(&FieldValue::Integer(3)));
        assert_eq!(row.field("unsigned"), Some(&FieldValue::Unsigned(u64::MAX)));
        assert_eq!(row.field("boolean"), Some(&FieldValue::Boolean(true)));
        assert_eq!(
            row.field("string"),
            Some(&FieldValue::String("ready".into()))
        );
    }

    #[test]
    fn single_point_metric_form_is_one_item_sugar_and_uses_default_timestamp() {
        let batch = decode(
            br#"{
                "metric": "cpu",
                "tags": {"host": "edge"},
                "fields": {"usage": 23.5}
            }"#,
            123,
        )
        .expect("single point");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].source, SourceLocation::Item(0));
        assert_eq!(batch.rows[0].row.series().measurement(), "cpu");
        assert_eq!(batch.rows[0].row.timestamp(), 123);
    }

    #[test]
    fn schema_violations_reject_only_the_bad_items_and_later_items_continue() {
        let batch = decode(
            br#"{
                "metrics": [
                    {"name": "first", "fields": {"value": 1}},
                    {"tags": {}, "fields": {"value": 2}},
                    {"name": "bad_tag", "tags": {"host": 3}, "fields": {"value": 3}},
                    {"name": "bad_field", "fields": {"value": null}},
                    {"name": "later", "fields": {"ok": true}}
                ]
            }"#,
            7,
        )
        .expect("partial success");

        assert_eq!(batch.rows.len(), 2);
        assert_eq!(batch.rows[0].row.series().measurement(), "first");
        assert_eq!(batch.rows[1].row.series().measurement(), "later");
        assert_eq!(batch.rejections.len(), 3);
        assert_eq!(
            batch
                .rejections
                .iter()
                .map(|rejection| rejection.source())
                .collect::<Vec<_>>(),
            vec![
                SourceLocation::Item(1),
                SourceLocation::Item(2),
                SourceLocation::Item(3)
            ]
        );
        assert!(batch.rejections[0].reason().contains("name"));
        assert!(batch.rejections[1].reason().contains("tag"));
        assert!(batch.rejections[2].reason().contains("field"));
    }

    #[test]
    fn invalid_root_json_or_root_schema_rejects_the_request() {
        let syntax = decode(br#"{"metrics":["#, 0).expect_err("syntax");
        assert!(syntax.to_string().contains("JSON"));

        let schema = decode(br#"{"metrics": {}}"#, 0).expect_err("root schema");
        assert!(schema.to_string().contains("metrics"));

        let unknown =
            decode(br#"{"metrics": [], "unexpected": true}"#, 0).expect_err("unknown root key");
        assert!(unknown.to_string().contains("unexpected"));
    }

    #[test]
    fn invalid_timestamp_and_unknown_entry_key_are_item_local() {
        let batch = decode(
            br#"{
                "metrics": [
                    {"name": "fractional", "fields": {"v": 1}, "timestamp": 1.5},
                    {"name": "unknown", "fields": {"v": 2}, "extra": true},
                    {"name": "valid", "fields": {"v": 3}, "timestamp": -4}
                ]
            }"#,
            0,
        )
        .expect("partial success");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row.timestamp(), -4);
        assert_eq!(batch.rejections.len(), 2);
        assert!(batch.rejections[0].reason().contains("timestamp"));
        assert!(batch.rejections[1].reason().contains("extra"));
    }

    #[test]
    fn enforces_encoded_and_item_count_limits() {
        let limits = IngestLimits::new(
            RequestLimits::new(4, 1_024, 1, 1).expect("limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let error = JsonIngestDecoder::new(limits)
            .decode(br#"{"metrics":[]}"#, 0)
            .expect_err("encoded limit");
        assert!(error.to_string().contains("request bytes"));

        let limits = IngestLimits::new(
            RequestLimits::new(1_024, 1_024, 1, 1).expect("limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let error = JsonIngestDecoder::new(limits)
            .decode(
                br#"{"metrics":[
                    {"name":"one","fields":{"v":1}},
                    {"name":"two","fields":{"v":2}}
                ]}"#,
                0,
            )
            .expect_err("item limit");
        assert!(error.to_string().contains("items"));
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_return_results_without_panicking(
            input in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let decoder = JsonIngestDecoder::new(IngestLimits::default());
            let outcome = catch_unwind(AssertUnwindSafe(|| decoder.decode(&input, 0)));
            prop_assert!(outcome.is_ok());
        }
    }

    fn decode(input: &[u8], default_timestamp: i64) -> crate::Result<crate::ingest::IngestBatch> {
        JsonIngestDecoder::new(IngestLimits::default()).decode(input, default_timestamp)
    }
}
