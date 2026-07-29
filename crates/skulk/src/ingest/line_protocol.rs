//! InfluxDB Line Protocol decoder.

use super::{IngestBatch, IngestLimits, SourceLocation};
use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use influxdb_line_protocol::{parse_lines, split_lines, FieldValue as LineFieldValue, ParsedLine};
use std::collections::HashMap;

/// Upper bound for the per-request series cache (bypassed once full).
const SERIES_CACHE_MAX_ENTRIES: usize = 10_000;

/// Pure byte-to-wide-row decoder for InfluxDB Line Protocol.
#[derive(Debug, Clone, Copy)]
pub struct LineProtocolDecoder {
    limits: IngestLimits,
}

impl LineProtocolDecoder {
    /// Creates a decoder that shares the unified ingestion resource policy.
    pub const fn new(limits: IngestLimits) -> Self {
        Self { limits }
    }

    /// Decodes a request, retaining line-local errors for partial success.
    pub fn decode(&self, input: &[u8], default_timestamp: i64) -> Result<IngestBatch> {
        self.limits
            .validate_request_bytes(input.len(), input.len())?;
        let text = std::str::from_utf8(input).map_err(|error| {
            TsmError::InvalidFormat(format!("Line Protocol is not UTF-8: {error}"))
        })?;
        let mut batch = IngestBatch::new(input.len(), input.len());
        let mut physical_line = 1_usize;
        let mut series_cache: HashMap<&str, SeriesKey> = HashMap::new();

        for raw_line in split_lines(text) {
            let source = SourceLocation::Line(physical_line);
            physical_line = physical_line
                .checked_add(raw_line.bytes().filter(|byte| *byte == b'\n').count())
                .and_then(|line| line.checked_add(1))
                .ok_or_else(|| {
                    TsmError::ResourceLimit("Line Protocol line number overflow".into())
                })?;

            let invalid_multiline =
                raw_line.contains('\n') && matches!(parse_lines(raw_line).next(), Some(Err(_)));
            if invalid_multiline {
                for (offset, physical) in raw_line.split('\n').enumerate() {
                    let line = source_line(source, offset)?;
                    self.decode_one(
                        physical,
                        line,
                        default_timestamp,
                        &mut batch,
                        &mut series_cache,
                    )?;
                }
            } else {
                self.decode_one(
                    raw_line,
                    source,
                    default_timestamp,
                    &mut batch,
                    &mut series_cache,
                )?;
            }
        }
        Ok(batch)
    }

    fn decode_one<'a>(
        &self,
        raw_line: &'a str,
        source: SourceLocation,
        default_timestamp: i64,
        batch: &mut IngestBatch,
        series_cache: &mut HashMap<&'a str, SeriesKey>,
    ) -> Result<()> {
        match parse_lines(raw_line).next() {
            None => return Ok(()),
            Some(Err(error)) => batch.reject(source, error.to_string()),
            Some(Ok(line)) => {
                let series_raw = &raw_line[..raw_series_end(raw_line)];
                match wide_row(line, series_raw, default_timestamp, series_cache) {
                    Ok(row) => batch.push_row(source, row),
                    Err(reason) => batch.reject(source, reason),
                }
            }
        }
        if batch.item_count() > self.limits.request().max_rows() {
            return Err(TsmError::ResourceLimit(format!(
                "decoded rows {} exceeds limit {}",
                batch.item_count(),
                self.limits.request().max_rows()
            )));
        }
        Ok(())
    }
}

fn source_line(source: SourceLocation, offset: usize) -> Result<SourceLocation> {
    let SourceLocation::Line(line) = source else {
        return Err(TsmError::Corruption(
            "Line Protocol source is not a line".into(),
        ));
    };
    line.checked_add(offset)
        .map(SourceLocation::Line)
        .ok_or_else(|| TsmError::ResourceLimit("Line Protocol line number overflow".into()))
}

/// Returns the byte length of the measurement-and-tags prefix of one line
/// (everything before the first space that is not escaped by a backslash).
fn raw_series_end(line: &str) -> usize {
    let bytes = line.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = (index + 2).min(bytes.len()),
            b' ' => return index,
            _ => index += 1,
        }
    }
    bytes.len()
}

fn wide_row<'a>(
    line: ParsedLine<'_>,
    series_raw: &'a str,
    default_timestamp: i64,
    series_cache: &mut HashMap<&'a str, SeriesKey>,
) -> std::result::Result<WideRow, String> {
    let series = if let Some(cached) = series_cache.get(series_raw) {
        cached.clone()
    } else {
        let mut tags = Tags::new();
        for (name, value) in line.series.tag_set.unwrap_or_default() {
            let name = name.to_string();
            if tags.insert(name.clone(), value.to_string()).is_some() {
                return Err(format!("duplicate tag '{name}'"));
            }
        }
        let key = SeriesKey::new(line.series.measurement.to_string(), tags);
        if series_cache.len() < SERIES_CACHE_MAX_ENTRIES {
            series_cache.insert(series_raw, key.clone());
        }
        key
    };

    let mut fields = Fields::new();
    for (name, value) in line.field_set {
        let name = name.to_string();
        let value = match value {
            LineFieldValue::F64(value) => FieldValue::Float(value),
            LineFieldValue::I64(value) => FieldValue::Integer(value),
            LineFieldValue::U64(value) => FieldValue::Unsigned(value),
            LineFieldValue::Boolean(value) => FieldValue::Boolean(value),
            LineFieldValue::String(value) => FieldValue::String(value.to_string()),
        };
        if fields.insert(name.clone(), value).is_some() {
            return Err(format!("duplicate field '{name}'"));
        }
    }

    Ok(WideRow::new(
        series,
        line.timestamp.unwrap_or(default_timestamp),
        fields,
    ))
}

#[cfg(test)]
mod tests {
    use super::LineProtocolDecoder;
    use crate::ingest::{AdmissionLimits, IngestLimits, RequestLimits, RowLimits, SourceLocation};
    use crate::model::FieldValue;
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn repeated_series_lines_hit_the_cache_and_stay_equivalent() {
        let limits = IngestLimits::default();
        let mut input = String::new();
        for index in 0..1_000 {
            input.push_str(&format!(
                "cpu,host=web\\ 01,region=ap value={index},status=\"ok\" {index}\n"
            ));
        }
        let batch = LineProtocolDecoder::new(limits)
            .decode(input.as_bytes(), 42)
            .expect("decode");
        assert_eq!(batch.rows.len(), 1_000);
        let first = batch.rows[0].row.series().clone();
        for entry in &batch.rows {
            assert_eq!(entry.row.series(), &first);
            assert_eq!(
                entry.row.series().tags().get("host").map(String::as_str),
                Some("web 01")
            );
        }
        // A different raw series (swapped tag order) must miss the cache and
        // still canonicalize to the same key.
        let swapped = LineProtocolDecoder::new(limits)
            .decode(b"cpu,region=ap,host=web\\ 01 value=1 7", 42)
            .expect("decode swapped");
        assert_eq!(swapped.rows[0].row.series(), &first);
        // Duplicate tags keep erroring even with a warm cache shape.
        let dup = LineProtocolDecoder::new(limits)
            .decode(b"cpu,host=a,host=b value=1 7", 42)
            .expect("decode duplicate");
        assert_eq!(dup.rows.len(), 0);
        assert_eq!(dup.rejections.len(), 1);
    }

    #[test]
    fn decodes_multi_field_rows_and_all_line_protocol_types() {
        let decoder = LineProtocolDecoder::new(IngestLimits::default());
        let batch = decoder
            .decode(
                b"weather,host=edge load=1.5,count=-2i,total=3u,ok=true,status=\"ready\" 42",
                999,
            )
            .expect("decode");

        assert_eq!(batch.rows.len(), 1);
        let row = &batch.rows[0].row;
        assert_eq!(row.series().measurement(), "weather");
        assert_eq!(
            row.series().tags().get("host").map(String::as_str),
            Some("edge")
        );
        assert_eq!(row.timestamp(), 42);
        assert_eq!(row.fields().len(), 5);
        assert_eq!(row.field("load"), Some(&FieldValue::Float(1.5)));
        assert_eq!(row.field("count"), Some(&FieldValue::Integer(-2)));
        assert_eq!(row.field("total"), Some(&FieldValue::Unsigned(3)));
        assert_eq!(row.field("ok"), Some(&FieldValue::Boolean(true)));
        assert_eq!(
            row.field("status"),
            Some(&FieldValue::String("ready".into()))
        );
    }

    #[test]
    fn unescapes_identifiers_and_values_and_supplies_the_local_timestamp() {
        let decoder = LineProtocolDecoder::new(IngestLimits::default());
        let batch = decoder
            .decode(
                br#"weather\ station,host=edge\,a,region=west\ coast field\ key="a \"quoted\" value""#,
                123,
            )
            .expect("decode");

        let row = &batch.rows[0].row;
        assert_eq!(row.series().measurement(), "weather station");
        assert_eq!(
            row.series().tags().get("host").map(String::as_str),
            Some("edge,a")
        );
        assert_eq!(
            row.series().tags().get("region").map(String::as_str),
            Some("west coast")
        );
        assert_eq!(row.timestamp(), 123);
        assert_eq!(
            row.field("field key"),
            Some(&FieldValue::String("a \"quoted\" value".into()))
        );
    }

    #[test]
    fn malformed_line_is_rejected_with_physical_line_number_and_later_lines_continue() {
        let decoder = LineProtocolDecoder::new(IngestLimits::default());
        let batch = decoder
            .decode(b"good value=1i 1\n\nnot valid\nlater ok=false 3", 99)
            .expect("partial decode");

        assert_eq!(batch.rows.len(), 2);
        assert_eq!(batch.rows[0].source, SourceLocation::Line(1));
        assert_eq!(batch.rows[1].source, SourceLocation::Line(4));
        assert_eq!(batch.rejections.len(), 1);
        assert_eq!(batch.rejections[0].source(), SourceLocation::Line(3));
        assert!(!batch.rejections[0].reason().is_empty());
    }

    #[test]
    fn unterminated_string_does_not_swallow_the_following_physical_line() {
        let decoder = LineProtocolDecoder::new(IngestLimits::default());
        let batch = decoder
            .decode(b"bad value=\"unterminated\nlater value=2i 3", 99)
            .expect("partial decode");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].source, SourceLocation::Line(2));
        assert_eq!(batch.rows[0].row.series().measurement(), "later");
        assert_eq!(batch.rejections.len(), 1);
        assert_eq!(batch.rejections[0].source(), SourceLocation::Line(1));
    }

    #[test]
    fn duplicate_tag_or_field_is_rejected_without_hiding_following_rows() {
        let decoder = LineProtocolDecoder::new(IngestLimits::default());
        let batch = decoder
            .decode(
                b"bad,a=1,a=2 value=1i\nbad value=1i,value=2i\nok value=3i",
                99,
            )
            .expect("partial decode");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].source, SourceLocation::Line(3));
        assert_eq!(batch.rejections.len(), 2);
        assert!(batch
            .rejections
            .iter()
            .all(|rejection| rejection.reason().contains("duplicate")));
    }

    #[test]
    fn encoded_request_limit_is_applied_before_parsing() {
        let limits = IngestLimits::new(
            RequestLimits::new(4, 100, 10, 10).expect("limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let decoder = LineProtocolDecoder::new(limits);

        let error = decoder
            .decode(b"cpu value=1i", 0)
            .expect_err("request limit");

        assert!(error.to_string().contains("request bytes"));
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_return_results_without_panicking(input in prop::collection::vec(any::<u8>(), 0..4096)) {
            let decoder = LineProtocolDecoder::new(IngestLimits::default());
            let outcome = catch_unwind(AssertUnwindSafe(|| decoder.decode(&input, 0)));
            prop_assert!(outcome.is_ok());
        }
    }
}
