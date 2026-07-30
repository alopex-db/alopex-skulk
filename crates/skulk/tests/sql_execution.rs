#![cfg(feature = "sql-ts")]

use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::{Executor, TableValue};
use alopex_skulk::query::plan::{plan_sql, PlanContext};
use alopex_skulk::query::sqlts::parse;
use alopex_skulk::query::sqlts::typecheck::typecheck;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};

const SECOND: i64 = 1_000_000_000;

fn ingest(
    store: &mut RecoveryStore,
    host: &str,
    timestamp: i64,
    value: f64,
    events: u64,
    counter: f64,
    status: &str,
) {
    store
        .ingest(WideRow::new(
            SeriesKey::new(
                "metrics",
                Tags::from([("host".to_string(), host.to_string())]),
            ),
            timestamp,
            Fields::from([
                ("value".to_string(), FieldValue::Float(value)),
                ("events".to_string(), FieldValue::Unsigned(events)),
                ("counter".to_string(), FieldValue::Float(counter)),
                ("status".to_string(), FieldValue::String(status.to_string())),
            ]),
        ))
        .expect("ingest");
}

fn plan(store: &RecoveryStore, sql: &str) -> alopex_skulk::query::plan::LogicalPlan {
    let query = parse(sql).expect("parse");
    let schema = store
        .measurement_schema(&query.measurement)
        .expect("schema");
    let typed = typecheck(query, &schema).expect("typecheck");
    plan_sql(&typed, PlanContext::instant(100 * SECOND)).expect("plan")
}

fn assert_float(value: &TableValue, expected: f64) {
    let TableValue::Float64(actual) = value else {
        panic!("expected float, got {value:?}");
    };
    assert!(
        (*actual - expected).abs() <= expected.abs().max(1.0) * 1e-12,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn sql_executes_floor_bucketing_aggregates_functions_filter_sort_limit_and_aliases() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    for row in [
        ("edge", -9, 1.0, 10, 100.0, "a"),
        ("edge", -1, 3.0, 20, 5.0, "b"),
        ("edge", 1, 5.0, 30, 5.0, "c"),
        ("edge", 9, 7.0, 40, 15.0, "d"),
        ("edge", 11, 9.0, 50, 15.0, "e"),
        ("edge", 19, 11.0, 60, 2.0, "f"),
        ("drop", 19, 100.0, 1, 1.0, "ignored"),
    ] {
        ingest(
            &mut store,
            row.0,
            row.1 * SECOND,
            row.2,
            row.3,
            row.4,
            row.5,
        );
    }
    let sql = "SELECT TIME_BUCKET('10 seconds', time) AS bucket, host, \
               AVG(value) AS avg_value, SUM(events) AS total_events, \
               MIN(value) AS min_value, MAX(value) AS max_value, \
               COUNT(*) AS samples, FIRST(status, time) AS first_status, \
               LAST(status, time) AS last_status, DELTA(events) AS delta_events, \
               DERIVATIVE(value) AS slope, RATE(counter) AS counter_rate \
               FROM metrics WHERE value >= 0 AND host != 'drop' \
               GROUP BY bucket, host ORDER BY bucket DESC LIMIT 3";
    let table = Executor::new(&store)
        .execute_table(&plan(&store, sql))
        .expect("execute SQL");

    assert_eq!(
        table
            .columns()
            .iter()
            .map(|column| column.name())
            .collect::<Vec<_>>(),
        [
            "bucket",
            "host",
            "avg_value",
            "total_events",
            "min_value",
            "max_value",
            "samples",
            "first_status",
            "last_status",
            "delta_events",
            "slope",
            "counter_rate",
        ]
    );
    assert_eq!(table.rows().len(), 3);

    let expected = [
        (10 * SECOND, 10.0, 110_u64, "e", "f", 10_u64, 0.25),
        (0, 6.0, 70_u64, "c", "d", 10_u64, 1.25),
        (-10 * SECOND, 2.0, 30_u64, "a", "b", 10_u64, 0.625),
    ];
    for (row, expected) in table.rows().iter().zip(expected) {
        assert_eq!(row[0], TableValue::TimestampNanosecond(expected.0));
        assert_eq!(row[1], TableValue::Utf8("edge".to_string()));
        assert_float(&row[2], expected.1);
        assert_eq!(row[3], TableValue::UInt64(expected.2));
        assert_float(&row[4], expected.1 - 1.0);
        assert_float(&row[5], expected.1 + 1.0);
        assert_eq!(row[6], TableValue::UInt64(2));
        assert_eq!(row[7], TableValue::Utf8(expected.3.to_string()));
        assert_eq!(row[8], TableValue::Utf8(expected.4.to_string()));
        assert_eq!(row[9], TableValue::UInt64(expected.5));
        assert_float(&row[10], 0.25);
        assert_float(&row[11], expected.6);
    }
}

#[test]
fn sql_wildcard_projection_preserves_typed_schema_and_row_order() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 5.0, 1, 1.0, "a");
    ingest(&mut store, "edge", 2 * SECOND, 11.0, 2, 2.0, "b");

    let sql = "SELECT * FROM metrics WHERE value > 5 AND host = 'edge' ORDER BY time DESC LIMIT 1";
    let table = Executor::new(&store)
        .execute_table(&plan(&store, sql))
        .expect("execute wildcard");

    assert_eq!(
        table
            .columns()
            .iter()
            .map(|column| column.name())
            .collect::<Vec<_>>(),
        ["time", "counter", "events", "host", "status", "value"]
    );
    assert_eq!(table.rows().len(), 1);
    assert_eq!(
        table.rows()[0],
        [
            TableValue::TimestampNanosecond(2 * SECOND),
            TableValue::Float64(2.0),
            TableValue::UInt64(2),
            TableValue::Utf8("edge".to_string()),
            TableValue::Utf8("b".to_string()),
            TableValue::Float64(11.0),
        ]
    );
}

#[test]
fn sql_executes_distinct_count_and_explicit_projection_aliases() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 5.0, 9, 1.0, "a");
    ingest(&mut store, "edge", 2 * SECOND, 11.0, 9, 2.0, "b");
    ingest(&mut store, "edge", 3 * SECOND, 7.0, 10, 3.0, "c");

    let distinct = Executor::new(&store)
        .execute_table(&plan(
            &store,
            "SELECT COUNT(DISTINCT events) AS unique_events FROM metrics",
        ))
        .expect("execute distinct count");
    assert_eq!(distinct.rows(), &[vec![TableValue::UInt64(2)]]);

    let projected = Executor::new(&store)
        .execute_table(&plan(
            &store,
            "SELECT value * 2.0 AS doubled FROM metrics \
             WHERE value >= 5.0 ORDER BY time DESC LIMIT 1",
        ))
        .expect("execute explicit projection");
    assert_eq!(projected.columns()[0].name(), "doubled");
    assert_eq!(projected.rows(), &[vec![TableValue::Float64(14.0)]]);
}

#[test]
fn sql_reports_float_division_by_zero() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 5.0, 1, 1.0, "a");

    let error = Executor::new(&store)
        .execute_table(&plan(
            &store,
            "SELECT value / 0.0 AS invalid FROM metrics LIMIT 1",
        ))
        .expect_err("division by zero must fail");
    assert!(error.to_string().contains("divides by zero"), "{error}");
}

#[test]
fn sql_preserves_select_list_order_around_an_expanded_wildcard() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 5.0, 7, 1.0, "a");

    let table = Executor::new(&store)
        .execute_table(&plan(
            &store,
            "SELECT value AS before, *, events AS after FROM metrics LIMIT 1",
        ))
        .expect("execute mixed wildcard projection");
    assert_eq!(
        table
            .columns()
            .iter()
            .map(|column| column.name())
            .collect::<Vec<_>>(),
        ["before", "time", "counter", "events", "host", "status", "value", "after",]
    );
    assert_eq!(table.rows()[0][0], TableValue::Float64(5.0));
    assert_eq!(table.rows()[0][7], TableValue::UInt64(7));
}
