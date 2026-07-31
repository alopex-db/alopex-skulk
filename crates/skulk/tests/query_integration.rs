#![cfg(all(feature = "promql", feature = "sql-ts"))]

use alopex_skulk::ingest::json::JsonIngestDecoder;
use alopex_skulk::ingest::line_protocol::LineProtocolDecoder;
use alopex_skulk::ingest::remote_write::RemoteWriteDecoder;
use alopex_skulk::ingest::{IngestLimits, Ingestor, O3Config, TooOldPolicy};
use alopex_skulk::query::{LabelMatcher, MatchOp, MetadataRequest, QueryEngine, QueryResult};
use alopex_skulk::store::reader::{ScanRequest, ScanResult, ScanTimeRange, StorageReader};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::retention::{LateWritePolicy, RetentionPolicy, HOUR_NANOS};
use alopex_skulk::store::schema::MeasurementSchema;
use alopex_skulk::{Result, TsmError};
use arrow_array::{Array, Float64Array};
use prost::Message;
use snap::raw::Encoder;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

const SECOND: i64 = 1_000_000_000;
const OLD_BASE: i64 = 2 * HOUR_NANOS;
const OLD_EVAL: i64 = OLD_BASE + 30 * SECOND;
const FRESH_TIME: i64 = 3 * HOUR_NANOS + 10 * SECOND;

fn result_floats(result: &QueryResult, column: &str) -> Vec<f64> {
    let mut values = Vec::new();
    for batch in result.batches() {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing '{column}' column"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("'{column}' is not Float64"));
        values.extend(array.values());
    }
    values
}

fn assert_query_state(
    store: &RecoveryStore,
    at: i64,
    expected_promql: &[f64],
    expected_sql_average: Option<f64>,
) {
    let engine = QueryEngine::new(store);
    let promql = engine.query_promql("metric", at).expect("PromQL query");
    assert_eq!(result_floats(&promql, "_value"), expected_promql);

    let sql_raw = engine
        .query_sql(
            "SELECT value FROM metric \
             WHERE time > NOW() - INTERVAL '5 minutes' AND time <= NOW() \
             ORDER BY host",
            at,
        )
        .expect("SQL raw query");
    assert_eq!(result_floats(&sql_raw, "value"), expected_promql);

    let sql = engine
        .query_sql("SELECT AVG(value) AS avg_value FROM metric", at)
        .expect("SQL query");
    let averages = result_floats(&sql, "avg_value");
    match expected_sql_average {
        Some(expected) => assert_eq!(averages, [expected]),
        None => assert!(averages.is_empty(), "unexpected SQL rows: {averages:?}"),
    }
}

fn metadata_hosts(store: &RecoveryStore) -> Vec<String> {
    let request = MetadataRequest::new(
        vec![LabelMatcher::new("__name__", MatchOp::Equal, "metric")],
        ScanTimeRange::bounded(0, 5 * HOUR_NANOS).expect("metadata range"),
    );
    let engine = QueryEngine::new(store);
    assert_eq!(
        engine.label_names(&request).expect("label names"),
        ["__name__", "host"]
    );
    engine
        .label_values("host", &request)
        .expect("host label values")
}

fn ingest_line(
    ingestor: &mut Ingestor<RecoveryStore>,
    limits: IngestLimits,
    host: &str,
    timestamp: i64,
    value: f64,
    now: i64,
) {
    let body = format!("metric,host={host} value={value} {timestamp}");
    let batch = LineProtocolDecoder::new(limits)
        .decode(body.as_bytes(), now)
        .expect("line decode");
    let outcome = ingestor.ingest(batch, now).expect("line ingest");
    assert_eq!(outcome.accepted_count(), 1);
}

fn ingest_remote(
    ingestor: &mut Ingestor<RecoveryStore>,
    limits: IngestLimits,
    host: &str,
    timestamp: i64,
    value: f64,
    now: i64,
) {
    let body = remote_write_body("metric", host, value, timestamp / 1_000_000);
    let batch = RemoteWriteDecoder::new(limits)
        .decode("application/x-protobuf", "snappy", &body)
        .expect("Remote Write decode");
    let outcome = ingestor.ingest(batch, now).expect("Remote Write ingest");
    assert_eq!(outcome.accepted_count(), 1);
}

fn ingest_json(
    ingestor: &mut Ingestor<RecoveryStore>,
    limits: IngestLimits,
    host: &str,
    timestamp: i64,
    value: f64,
    now: i64,
) {
    let value = format!("{value:.1}");
    let body = format!(
        r#"{{"metrics":[{{"name":"metric","tags":{{"host":"{host}"}},"fields":{{"value":{value}}},"timestamp":{timestamp}}}]}}"#
    );
    let batch = JsonIngestDecoder::new(limits)
        .decode(body.as_bytes(), now)
        .expect("JSON decode");
    let outcome = ingestor.ingest(batch, now).expect("JSON ingest");
    assert_eq!(outcome.accepted_count(), 1);
}

#[test]
fn ingest_query_results_survive_flush_compaction_retention_and_reopen() {
    let root = tempfile::tempdir().expect("tempdir");
    let limits = IngestLimits::default();
    let store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open recovery store");
    let mut ingestor = Ingestor::with_o3_config(
        store,
        limits,
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Drop, false).expect("O3 config"),
    );
    let ingest_now = OLD_BASE + 45 * SECOND;

    ingest_line(
        &mut ingestor,
        limits,
        "a",
        OLD_BASE + 10 * SECOND,
        1.0,
        ingest_now,
    );
    ingest_remote(
        &mut ingestor,
        limits,
        "b",
        OLD_BASE + 20 * SECOND,
        2.0,
        ingest_now,
    );
    ingest_json(
        &mut ingestor,
        limits,
        "c",
        OLD_BASE + 30 * SECOND,
        3.0,
        ingest_now,
    );

    let too_old_body = format!(
        r#"{{"metrics":[{{"name":"metric","tags":{{"host":"dropped"}},"fields":{{"value":99.0}},"timestamp":{}}}]}}"#,
        OLD_BASE - HOUR_NANOS - SECOND
    );
    let too_old = JsonIngestDecoder::new(limits)
        .decode(too_old_body.as_bytes(), ingest_now)
        .expect("old JSON decode");
    let dropped = ingestor
        .ingest(too_old, ingest_now)
        .expect("observable O3 drop");
    assert_eq!(dropped.accepted_count(), 0);
    assert_eq!(dropped.dropped_count(), 1);

    assert_query_state(ingestor.sink(), OLD_EVAL, &[1.0, 2.0, 3.0], Some(2.0));
    assert_eq!(metadata_hosts(ingestor.sink()), ["a", "b", "c"]);

    ingestor.sink_mut().flush_all().expect("first flush");
    assert_query_state(ingestor.sink(), OLD_EVAL, &[1.0, 2.0, 3.0], Some(2.0));

    ingestor.set_o3_config(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Drop, true)
            .expect("backfill config"),
    );
    ingest_json(
        &mut ingestor,
        limits,
        "a",
        OLD_BASE + 10 * SECOND,
        4.0,
        FRESH_TIME,
    );
    ingestor.set_o3_config(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Drop, false).expect("live config"),
    );
    ingest_line(&mut ingestor, limits, "fresh", FRESH_TIME, 10.0, FRESH_TIME);
    ingestor.sink_mut().flush_all().expect("second flush");

    assert_query_state(ingestor.sink(), OLD_EVAL, &[4.0, 2.0, 3.0], Some(4.75));
    assert_query_state(ingestor.sink(), FRESH_TIME, &[10.0], Some(4.75));
    assert_eq!(metadata_hosts(ingestor.sink()), ["a", "b", "c", "fresh"]);

    let compacted = ingestor
        .sink_mut()
        .compact_measurement("metric")
        .expect("compaction")
        .expect("two old-partition files");
    assert_eq!(compacted.input_file_count(), 2);
    assert_eq!(compacted.output_row_count(), 3);
    assert_query_state(ingestor.sink(), OLD_EVAL, &[4.0, 2.0, 3.0], Some(4.75));

    ingestor
        .sink_mut()
        .set_retention_policy(
            "metric",
            RetentionPolicy::new(HOUR_NANOS as u64, LateWritePolicy::Accept)
                .expect("retention policy"),
        )
        .expect("set retention");
    let expired = ingestor
        .sink_mut()
        .expire_retention("metric", 4 * HOUR_NANOS)
        .expect("expire old partition");
    assert_eq!(expired.expired_partition_count(), 1);

    assert_query_state(ingestor.sink(), OLD_EVAL, &[], Some(10.0));
    assert_query_state(ingestor.sink(), FRESH_TIME, &[10.0], Some(10.0));
    assert_eq!(metadata_hosts(ingestor.sink()), ["fresh"]);

    drop(ingestor.into_sink());
    let reopened =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen recovery store");
    assert_query_state(&reopened, OLD_EVAL, &[], Some(10.0));
    assert_query_state(&reopened, FRESH_TIME, &[10.0], Some(10.0));
    assert_eq!(metadata_hosts(&reopened), ["fresh"]);
}

struct SchemaConflictReader<'a> {
    inner: &'a RecoveryStore,
}

impl StorageReader for SchemaConflictReader<'_> {
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult> {
        StorageReader::scan(self.inner, request)
    }

    fn measurement_names(&self) -> Result<Vec<String>> {
        StorageReader::measurement_names(self.inner)
    }

    fn measurement_schema(&self, measurement: &str) -> Result<MeasurementSchema> {
        if measurement == "metric" {
            return Err(TsmError::SchemaConflict {
                measurement: measurement.to_string(),
                column: "value".to_string(),
                existing: "field Float64".to_string(),
                incoming: "tag Utf8".to_string(),
            });
        }
        StorageReader::measurement_schema(self.inner, measurement)
    }
}

#[test]
fn unified_sql_entry_preserves_machine_readable_schema_conflicts() {
    let root = tempfile::tempdir().expect("tempdir");
    let store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open recovery store");
    let reader = SchemaConflictReader { inner: &store };
    let error = QueryEngine::new(&reader)
        .query_sql("SELECT value FROM metric", OLD_EVAL)
        .expect_err("schema conflict");

    assert!(matches!(
        error,
        TsmError::SchemaConflict {
            measurement,
            column,
            ..
        } if measurement == "metric" && column == "value"
    ));
}

#[test]
fn malicious_text_queries_return_errors_without_unwinding() {
    let root = tempfile::tempdir().expect("tempdir");
    let store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open recovery store");
    let engine = QueryEngine::new(&store);
    let huge_input = "x".repeat((1 << 20) + 1);
    let huge_regex = format!("metric{{host=~\"{}\"}}", "a".repeat((32 << 10) + 1));
    let deep_sql = format!(
        "SELECT {}value{} FROM metric",
        "(".repeat(100),
        ")".repeat(100)
    );

    for query in ["\0".to_string(), "(".repeat(1_000), huge_input] {
        let result = catch_unwind(AssertUnwindSafe(|| engine.query_promql(&query, OLD_EVAL)));
        assert!(
            result.is_ok(),
            "PromQL unwound for input length {}",
            query.len()
        );
        assert!(
            result.expect("no unwind").is_err(),
            "malicious PromQL was accepted"
        );
    }
    let regex_result = catch_unwind(AssertUnwindSafe(|| {
        engine.query_promql(&huge_regex, OLD_EVAL)
    }));
    assert!(regex_result.is_ok(), "PromQL regex unwound");
    assert!(matches!(
        regex_result.expect("no unwind"),
        Err(TsmError::ResourceLimit(_))
    ));

    for query in ["\0".to_string(), "SELECT".to_string(), deep_sql] {
        let result = catch_unwind(AssertUnwindSafe(|| engine.query_sql(&query, OLD_EVAL)));
        assert!(
            result.is_ok(),
            "SQL-TS unwound for input length {}",
            query.len()
        );
        assert!(
            result.expect("no unwind").is_err(),
            "malicious SQL-TS was accepted"
        );
    }
}

fn remote_write_body(measurement: &str, host: &str, value: f64, timestamp_millis: i64) -> Vec<u8> {
    let request = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: measurement.into(),
                },
                Label {
                    name: "host".into(),
                    value: host.into(),
                },
            ],
            samples: vec![Sample {
                value,
                timestamp: timestamp_millis,
            }],
        }],
    };
    Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("compress Remote Write fixture")
}

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}
