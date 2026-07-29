//! InfluxDB Line Protocol decoder.

use super::{IngestBatch, IngestLimits, SourceLocation};
use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesId, SeriesKey, Tags, WideRow};
use influxdb_line_protocol::{parse_lines, split_lines, FieldValue as LineFieldValue, ParsedLine};
use std::collections::HashMap;
use std::sync::Arc;

/// Upper bound for the per-request series cache (bypassed once full).
const SERIES_CACHE_MAX_ENTRIES: usize = 10_000;

/// Pure byte-to-wide-row decoder for InfluxDB Line Protocol.
#[derive(Debug, Clone, Copy)]
pub struct LineProtocolDecoder {
    limits: IngestLimits,
    fastpath: bool,
}

impl LineProtocolDecoder {
    /// Creates a decoder that shares the unified ingestion resource policy.
    pub const fn new(limits: IngestLimits) -> Self {
        Self {
            limits,
            fastpath: true,
        }
    }

    /// Test hook: disables the escape-free fast path so differential tests
    /// can compare it against the reference parser.
    #[cfg(test)]
    pub(crate) const fn without_fastpath(limits: IngestLimits) -> Self {
        Self {
            limits,
            fastpath: false,
        }
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
        let mut series_cache: HashMap<&str, (Arc<SeriesKey>, SeriesId)> = HashMap::new();

        for raw_line in split_lines(text) {
            let source = SourceLocation::Line(physical_line);
            let has_newline = raw_line.contains('\n');
            let embedded_newlines = if has_newline {
                raw_line.bytes().filter(|byte| *byte == b'\n').count()
            } else {
                0
            };
            physical_line = physical_line
                .checked_add(embedded_newlines)
                .and_then(|line| line.checked_add(1))
                .ok_or_else(|| {
                    TsmError::ResourceLimit("Line Protocol line number overflow".into())
                })?;

            let invalid_multiline =
                has_newline && matches!(parse_lines(raw_line).next(), Some(Err(_)));
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
        series_cache: &mut HashMap<&'a str, (Arc<SeriesKey>, SeriesId)>,
    ) -> Result<()> {
        if self.fastpath {
            if let Some(row) = fast_parse(raw_line, default_timestamp, series_cache) {
                match row {
                    Ok(row) => batch.push_row(source, row),
                    Err(reason) => batch.reject(source, reason),
                }
                if batch.item_count() > self.limits.request().max_rows() {
                    return Err(TsmError::ResourceLimit(format!(
                        "decoded rows {} exceeds limit {}",
                        batch.item_count(),
                        self.limits.request().max_rows()
                    )));
                }
                return Ok(());
            }
        }
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
    series_cache: &mut HashMap<&'a str, (Arc<SeriesKey>, SeriesId)>,
) -> std::result::Result<WideRow, String> {
    let (series, series_id) = if let Some((series, series_id)) = series_cache.get(series_raw) {
        (Arc::clone(series), *series_id)
    } else {
        let mut tags = Tags::new();
        for (name, value) in line.series.tag_set.unwrap_or_default() {
            let name = name.to_string();
            if tags.insert(name.clone(), value.to_string()).is_some() {
                return Err(format!("duplicate tag '{name}'"));
            }
        }
        let key = Arc::new(SeriesKey::new(line.series.measurement.to_string(), tags));
        let key_id = key.id();
        if series_cache.len() < SERIES_CACHE_MAX_ENTRIES {
            series_cache.insert(series_raw, (Arc::clone(&key), key_id));
        }
        (key, key_id)
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

    Ok(WideRow::with_shared_series(
        series,
        series_id,
        line.timestamp.unwrap_or(default_timestamp),
        fields,
    ))
}

/// Escape-free fast path: parses one line without the reference nom parser
/// when the line provably contains none of the constructs that need it
/// (backslash escapes or quoted strings). Any ambiguity returns `None` so the
/// reference parser stays the authority; equivalence is enforced by
/// differential tests.
fn fast_parse<'a>(
    line: &'a str,
    default_timestamp: i64,
    series_cache: &mut HashMap<&'a str, (Arc<SeriesKey>, SeriesId)>,
) -> Option<std::result::Result<WideRow, String>> {
    let bytes = line.as_bytes();
    if bytes.is_empty()
        || bytes[0] == b'#'
        || bytes[0].is_ascii_whitespace()
        || bytes
            .iter()
            .any(|byte| matches!(byte, b'\\' | b'"' | b'\t' | b'\r' | b'\n') || *byte == 0)
    {
        return None;
    }

    let mut parts = line.splitn(3, ' ');
    let series_raw = parts.next()?;
    let fields_raw = parts.next()?;
    let rest = parts.next();
    if series_raw.is_empty() || fields_raw.is_empty() {
        return None;
    }
    let timestamp = match rest {
        None => default_timestamp,
        Some(ts_raw) => {
            let ts_raw = ts_raw.trim_end_matches(' ');
            if ts_raw.is_empty() || ts_raw.contains(' ') {
                return None;
            }
            parse_plain_i64(ts_raw)?
        }
    };

    // Validate every field value before reporting any semantic error, so a
    // line the reference parser would reject at parse level falls back to it.
    let mut parsed_fields: Vec<(&str, FieldValue)> = Vec::new();
    for pair in fields_raw.split(',') {
        let (name, value_raw) = pair.split_once('=')?;
        if name.is_empty() || value_raw.is_empty() {
            return None;
        }
        parsed_fields.push((name, parse_fast_field_value(value_raw)?));
    }
    if parsed_fields.is_empty() {
        return None;
    }

    let cached = series_cache.get(series_raw).cloned();
    let (series, series_id) = if let Some((series, series_id)) = cached {
        (series, series_id)
    } else {
        let mut series_parts = series_raw.split(',');
        let measurement = series_parts.next()?;
        if measurement.is_empty() {
            return None;
        }
        let mut parsed_tags: Vec<(&str, &str)> = Vec::new();
        for pair in series_parts {
            let (name, value) = pair.split_once('=')?;
            if name.is_empty() || value.is_empty() || value.contains('=') {
                return None;
            }
            parsed_tags.push((name, value));
        }
        // The whole line is fast-parseable: semantic errors may be reported now.
        let mut tags = Tags::new();
        for (name, value) in parsed_tags {
            if tags.insert(name.to_string(), value.to_string()).is_some() {
                return Some(Err(format!("duplicate tag '{name}'")));
            }
        }
        let key = Arc::new(SeriesKey::new(measurement.to_string(), tags));
        let key_id = key.id();
        if series_cache.len() < SERIES_CACHE_MAX_ENTRIES {
            series_cache.insert(series_raw, (Arc::clone(&key), key_id));
        }
        (key, key_id)
    };

    let mut fields = Fields::new();
    for (name, value) in parsed_fields {
        if fields.insert(name.to_string(), value).is_some() {
            return Some(Err(format!("duplicate field '{name}'")));
        }
    }

    Some(Ok(WideRow::with_shared_series(
        series, series_id, timestamp, fields,
    )))
}

/// Conservative field value typing per the Line Protocol rules; anything not
/// provably identical to the reference outcome returns `None` (fallback).
fn parse_fast_field_value(raw: &str) -> Option<FieldValue> {
    match raw {
        "t" | "T" | "true" | "True" | "TRUE" => return Some(FieldValue::Boolean(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Some(FieldValue::Boolean(false)),
        _ => {}
    }
    if let Some(int_raw) = raw.strip_suffix('i') {
        return parse_plain_i64(int_raw).map(FieldValue::Integer);
    }
    if let Some(uint_raw) = raw.strip_suffix('u') {
        if uint_raw.is_empty() || !uint_raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        return uint_raw.parse::<u64>().ok().map(FieldValue::Unsigned);
    }
    // Floats: only the conservative shape [-]digits[.digits][eE[+-]digits].
    let mut body = raw.as_bytes();
    if body.first() == Some(&b'-') {
        body = &body[1..];
    }
    let mut seen_digit = false;
    let mut seen_dot = false;
    let mut index = 0;
    while index < body.len() {
        match body[index] {
            b'0'..=b'9' => {
                seen_digit = true;
                index += 1;
            }
            b'.' if !seen_dot => {
                seen_dot = true;
                index += 1;
            }
            b'e' | b'E' if seen_digit && index + 1 < body.len() => {
                let mut exp = &body[index + 1..];
                if exp.first() == Some(&b'+') || exp.first() == Some(&b'-') {
                    exp = &exp[1..];
                }
                if exp.is_empty() || !exp.iter().all(u8::is_ascii_digit) {
                    return None;
                }
                index = body.len();
            }
            _ => return None,
        }
    }
    if !seen_digit {
        return None;
    }
    raw.parse::<f64>().ok().map(FieldValue::Float)
}

fn parse_plain_i64(raw: &str) -> Option<i64> {
    let body = raw.strip_prefix('-').unwrap_or(raw);
    if body.is_empty() || !body.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    raw.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::LineProtocolDecoder;
    use crate::ingest::{AdmissionLimits, IngestLimits, RequestLimits, RowLimits, SourceLocation};
    use crate::model::FieldValue;
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn outcomes(batch: &crate::ingest::IngestBatch) -> (Vec<String>, Vec<String>) {
        let rows = batch
            .rows
            .iter()
            .map(|entry| format!("{:?}|{:?}", entry.source, entry.row))
            .collect();
        let rejects = batch
            .rejected_debug()
            .iter()
            .map(|(source, reason)| format!("{source:?}|{reason}"))
            .collect();
        (rows, rejects)
    }

    proptest! {
        /// The escape-free fast path must be byte-for-byte equivalent to the
        /// reference parser on every input it chooses to handle.
        #[test]
        fn fast_path_is_equivalent_to_the_reference_parser(
            lines in proptest::collection::vec(
                proptest::string::string_regex(
                    "[a-c]{1,4}(,[a-c]{1,3}=[a-d0-9]{1,3}){0,3} [a-c]{1,3}=((-?[0-9]{1,4}(\\.[0-9]{1,3})?([eE][+-]?[0-9]{1,2})?)|(-?[0-9]{1,4}[iu])|t|f|true|false|TRUE|FALSE|nan|inf|1\\.2\\.3|--4|0x1f)(,[a-d]{1,3}=[0-9]{1,3})?( -?[0-9]{1,10})?"
                ).expect("regex"),
                0..8,
            ),
            junk in proptest::collection::vec("[ -~]{0,40}", 0..4),
        ) {
            let mut input = lines.join("\n");
            for line in junk {
                input.push('\n');
                input.push_str(&line);
            }
            let limits = IngestLimits::default();
            let fast = LineProtocolDecoder::new(limits).decode(input.as_bytes(), 77);
            let reference = LineProtocolDecoder::without_fastpath(limits).decode(input.as_bytes(), 77);
            match (fast, reference) {
                (Ok(fast), Ok(reference)) => {
                    prop_assert_eq!(outcomes(&fast), outcomes(&reference));
                }
                (Err(fast), Err(reference)) => {
                    prop_assert_eq!(fast.to_string(), reference.to_string());
                }
                (fast, reference) => {
                    return Err(proptest::test_runner::TestCaseError::fail(format!(
                        "fast/reference disagreed: {fast:?} vs {reference:?}"
                    )));
                }
            }
        }
    }

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
