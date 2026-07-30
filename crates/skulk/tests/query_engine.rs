use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::EvaluationRange;
use alopex_skulk::query::plan::{
    LogicalPlan, MeasurementSelection, PlanNode, PlanTimeRange, PlanValueType, ScanNode,
    ScanResolution, SeriesGroupKind, SeriesGroupNode, SeriesWindow, SeriesWindowKind,
};
use alopex_skulk::query::{LabelMatcher, MatchOp, MetadataRequest, QueryEngine, QueryResult};
use alopex_skulk::store::reader::ScanTimeRange;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
#[cfg(any(feature = "promql", feature = "sql-ts"))]
use alopex_skulk::TsmError;
use arrow_array::{Array, Float64Array, TimestampNanosecondArray};
#[cfg(feature = "sql-ts")]
use arrow_array::{StringArray, UInt64Array};
use arrow_schema::{DataType, TimeUnit};
use std::collections::BTreeSet;

const SECOND: i64 = 1_000_000_000;

fn ingest(
    store: &mut RecoveryStore,
    host: &str,
    region: &str,
    timestamp: i64,
    value: f64,
    events: u64,
    status: &str,
) {
    store
        .ingest(WideRow::new(
            SeriesKey::new(
                "metric",
                Tags::from([
                    ("host".to_string(), host.to_string()),
                    ("region".to_string(), region.to_string()),
                ]),
            ),
            timestamp,
            Fields::from([
                ("value".to_string(), FieldValue::Float(value)),
                ("events".to_string(), FieldValue::Unsigned(events)),
                ("status".to_string(), FieldValue::String(status.to_string())),
            ]),
        ))
        .expect("ingest");
}

fn selector(at: i64, range: bool) -> LogicalPlan {
    let duration = 60 * SECOND;
    LogicalPlan {
        root: PlanNode::SeriesGroup(SeriesGroupNode {
            input: Box::new(PlanNode::Scan(ScanNode {
                measurement: MeasurementSelection {
                    exact: Some("metric".to_string()),
                    matchers: Vec::new(),
                },
                time_range: PlanTimeRange::prometheus_window(at - duration, at).expect("window"),
                tag_equalities: Vec::new(),
                field_projection: Some(BTreeSet::from(["value".to_string()])),
                resolution: ScanResolution::raw(),
            })),
            kind: SeriesGroupKind::Window(SeriesWindow {
                kind: if range {
                    SeriesWindowKind::Range
                } else {
                    SeriesWindowKind::Instant
                },
                evaluation_time: at,
                duration_ns: duration,
            }),
        }),
        output_type: if range {
            PlanValueType::RangeVector
        } else {
            PlanValueType::InstantVector
        },
    }
}

fn float_values(result: &QueryResult) -> Vec<f64> {
    result.batches()[0]
        .column_by_name("_value")
        .expect("value column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float values")
        .values()
        .to_vec()
}

#[test]
fn core_programmatic_api_materializes_scalar_vector_and_matrix_in_canonical_order() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "b", "west", 10 * SECOND, 30.0, 3, "up");
    ingest(&mut store, "a", "east", 5 * SECOND, 1.0, 1, "up");
    ingest(&mut store, "a", "east", 10 * SECOND, 2.0, 2, "down");
    let engine = QueryEngine::new(&store);

    let scalar = engine
        .execute(
            &LogicalPlan {
                root: PlanNode::Scalar(7.0),
                output_type: PlanValueType::Scalar,
            },
            10 * SECOND,
        )
        .expect("scalar");
    assert!(matches!(scalar, QueryResult::Scalar(_)));
    assert_eq!(float_values(&scalar), [7.0]);

    let vector = engine
        .execute(&selector(10 * SECOND, false), 10 * SECOND)
        .expect("vector");
    assert!(matches!(vector, QueryResult::Vector(_)));
    assert_eq!(float_values(&vector), [2.0, 30.0]);
    let schema = vector.batches()[0].schema();
    assert_eq!(schema.field(0).name(), "_time");
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(schema.field(1).name(), "_value");
    assert!(matches!(
        schema.field(2).data_type(),
        DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::UInt32 && value.as_ref() == &DataType::Utf8
    ));
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["_time", "_value", "__name__", "host", "region"]
    );

    let matrix = engine
        .execute(&selector(10 * SECOND, true), 10 * SECOND)
        .expect("matrix");
    assert!(matches!(matrix, QueryResult::Matrix(_)));
    assert_eq!(float_values(&matrix), [1.0, 2.0, 30.0]);
    let timestamps = matrix.batches()[0]
        .column_by_name("_time")
        .expect("time")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("timestamps");
    assert_eq!(timestamps.values(), &[5 * SECOND, 10 * SECOND, 10 * SECOND]);

    let range = engine
        .execute_range(
            &selector(5 * SECOND, false),
            EvaluationRange::new(5 * SECOND, 10 * SECOND, 5 * SECOND).expect("schedule"),
        )
        .expect("programmatic range");
    assert!(matches!(range, QueryResult::Matrix(_)));
    assert_eq!(float_values(&range), [1.0, 2.0, 30.0]);
}

#[test]
fn metadata_enumeration_applies_matchers_and_time_range_to_all_three_views() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "a", "east", 10 * SECOND, 1.0, 1, "up");
    ingest(&mut store, "b", "west", 15 * SECOND, 2.0, 2, "up");
    ingest(&mut store, "c", "west", 30 * SECOND, 3.0, 3, "up");
    let request = MetadataRequest::new(
        vec![
            LabelMatcher::new("__name__", MatchOp::Equal, "metric"),
            LabelMatcher::new("host", MatchOp::Regex, "a|b"),
        ],
        ScanTimeRange::bounded(0, 20 * SECOND).expect("range"),
    );
    let engine = QueryEngine::new(&store);

    let series = engine.series(&request).expect("series");
    assert_eq!(series.len(), 2);
    assert_eq!(series[0].tags().get("host").map(String::as_str), Some("a"));
    assert_eq!(series[1].tags().get("host").map(String::as_str), Some("b"));
    assert_eq!(
        engine.label_names(&request).expect("label names"),
        ["__name__", "host", "region"]
    );
    assert_eq!(
        engine.label_values("host", &request).expect("label values"),
        ["a", "b"]
    );
    assert_eq!(
        engine
            .label_values("__name__", &request)
            .expect("metric names"),
        ["metric"]
    );
}

#[cfg(feature = "promql")]
#[test]
fn promql_text_entries_execute_instant_and_range_queries_and_keep_error_positions() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "a", "east", 10 * SECOND, 2.0, 1, "up");
    ingest(&mut store, "a", "east", 20 * SECOND, 3.0, 2, "up");
    store
        .ingest(WideRow::new(
            SeriesKey::new(
                "other_metric",
                Tags::from([("host".to_string(), "a".to_string())]),
            ),
            20 * SECOND,
            Fields::from([("value".to_string(), FieldValue::Float(4.0))]),
        ))
        .expect("ingest second measurement");
    let engine = QueryEngine::new(&store);

    let instant = engine
        .query_promql("metric{host=\"a\"} + 2", 20 * SECOND)
        .expect("instant query");
    assert_eq!(float_values(&instant), [5.0]);

    let range = engine
        .query_promql_range("metric{host=\"a\"}", 10 * SECOND, 20 * SECOND, 10 * SECOND)
        .expect("range query");
    assert!(matches!(range, QueryResult::Matrix(_)));
    assert_eq!(float_values(&range), [2.0, 3.0]);

    let matcher_only = engine
        .query_promql(
            "{__name__=~\"metric|other_metric\",host=\"a\"} + 1",
            20 * SECOND,
        )
        .expect("measurement matcher query");
    assert_eq!(float_values(&matcher_only), [5.0, 4.0]);

    let scalar_range = engine
        .query_promql_range("2 + 3", 10 * SECOND, 20 * SECOND, 10 * SECOND)
        .expect("scalar range query");
    assert!(matches!(scalar_range, QueryResult::Matrix(_)));
    assert_eq!(float_values(&scalar_range), [5.0, 5.0]);

    let error = engine
        .query_promql("sum(", 20 * SECOND)
        .expect_err("invalid PromQL");
    assert!(matches!(
        error,
        TsmError::Parse {
            line,
            column,
            offset,
            ..
        } if line > 0 && column > 0 && offset > 0
    ));
}

#[cfg(feature = "sql-ts")]
#[test]
fn sql_text_entry_materializes_a_typed_table_and_reports_type_positions() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "b", "west", 10 * SECOND, 4.0, 3, "up");
    ingest(&mut store, "a", "east", 10 * SECOND, 2.0, 7, "down");
    let engine = QueryEngine::new(&store);

    let table = engine
        .query_sql(
            "SELECT host, AVG(value) AS avg_value, SUM(events) AS total \
             FROM metric GROUP BY host ORDER BY host",
            20 * SECOND,
        )
        .expect("SQL query");
    assert!(matches!(table, QueryResult::Table(_)));
    let batch = &table.batches()[0];
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["host", "avg_value", "total"]
    );
    let hosts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("hosts");
    assert_eq!(hosts.iter().collect::<Vec<_>>(), [Some("a"), Some("b")]);
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("totals")
            .values(),
        &[7, 3]
    );

    let error = engine
        .query_sql("SELECT AVG(status) FROM metric", 20 * SECOND)
        .expect_err("invalid aggregate type");
    assert!(
        matches!(
            error,
            TsmError::Type {
                line,
                column,
                ..
            } if line > 0 && column > 0
        ),
        "{error:?}"
    );
}
