#![cfg(feature = "sql-ts")]

use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::{Executor, TableValue};
use alopex_skulk::query::plan::{plan_sql, PlanContext};
use alopex_skulk::query::sqlts::parse;
use alopex_skulk::query::sqlts::typecheck::typecheck;
use alopex_skulk::query::QueryEngine;
use alopex_skulk::store::reader::{
    FloatSeriesScanResult, ScanRequest, ScanResult, ScanTimeRange, StorageReader,
};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::schema::MeasurementSchema;
use alopex_skulk::Result;
use std::cell::Cell;
use std::sync::Arc;

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

struct TrackingReader<'a> {
    inner: &'a RecoveryStore,
    row_scans: Cell<usize>,
    float_series_scans: Cell<usize>,
}

impl<'a> TrackingReader<'a> {
    fn new(inner: &'a RecoveryStore) -> Self {
        Self {
            inner,
            row_scans: Cell::new(0),
            float_series_scans: Cell::new(0),
        }
    }
}

impl StorageReader for TrackingReader<'_> {
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult> {
        self.row_scans.set(self.row_scans.get() + 1);
        self.inner.scan(request)
    }

    fn try_scan_float_series(
        &self,
        request: &ScanRequest,
        field: &str,
    ) -> Result<Option<Arc<FloatSeriesScanResult>>> {
        self.float_series_scans
            .set(self.float_series_scans.get() + 1);
        self.inner.try_scan_float_series(request, field)
    }

    fn measurement_schema(&self, measurement: &str) -> Result<MeasurementSchema> {
        self.inner.measurement_schema(measurement)
    }
}

struct RowOnlyReader<'a> {
    inner: &'a RecoveryStore,
}

impl StorageReader for RowOnlyReader<'_> {
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult> {
        self.inner.scan(request)
    }

    fn measurement_schema(&self, measurement: &str) -> Result<MeasurementSchema> {
        self.inner.measurement_schema(measurement)
    }
}

#[test]
fn sql_time_bucket_float_average_uses_the_lightweight_series_scan() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    for (timestamp, value) in [(SECOND, 1.0), (11 * SECOND, 3.0), (21 * SECOND, 5.0)] {
        ingest(&mut store, "edge", timestamp, value, 1, value, "up");
    }
    let reader = TrackingReader::new(&store);
    let result = QueryEngine::new(&reader)
        .query_sql(
            "SELECT TIME_BUCKET('10 seconds', time) AS bucket, \
             AVG(value) AS average FROM metrics \
             WHERE time > NOW() - INTERVAL '30 seconds' \
             GROUP BY bucket",
            30 * SECOND,
        )
        .expect("time-bucket average");

    assert_eq!(
        result
            .batches()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3
    );
    assert_eq!(reader.float_series_scans.get(), 1);
    assert_eq!(
        reader.row_scans.get(),
        0,
        "optimized aggregate must not materialize WideRow values"
    );
}

#[test]
fn recovery_store_reuses_float_scan_until_storage_generation_changes() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 1.0, 1, 1.0, "up");
    store.flush_all().expect("flush cached row");
    let request =
        ScanRequest::new("metrics", ScanTimeRange::all()).with_field_projection(["value"]);

    let first = store
        .try_scan_float_series(&request, "value")
        .expect("first float scan")
        .expect("float-compatible field");
    let second = store
        .try_scan_float_series(&request, "value")
        .expect("second float scan")
        .expect("float-compatible field");
    assert!(
        Arc::ptr_eq(&first, &second),
        "unchanged storage generation should reuse decoded float points"
    );

    ingest(&mut store, "edge", 2 * SECOND, 2.0, 1, 2.0, "up");
    let changed = store
        .try_scan_float_series(&request, "value")
        .expect("changed float scan")
        .expect("float-compatible field");
    assert!(!Arc::ptr_eq(&second, &changed));
    assert_eq!(
        changed
            .series()
            .iter()
            .map(|series| series.points().len())
            .sum::<usize>(),
        2
    );
}

#[test]
fn lightweight_float_scan_matches_row_path_for_sparse_latest_write() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 10.0, 1, 1.0, "original");
    store.flush_all().expect("flush original");
    store
        .ingest(WideRow::new(
            SeriesKey::new(
                "metrics",
                Tags::from([("host".to_string(), "edge".to_string())]),
            ),
            SECOND,
            Fields::from([(
                "status".to_string(),
                FieldValue::String("sparse overwrite".to_string()),
            )]),
        ))
        .expect("ingest sparse overwrite");

    let sql = "SELECT TIME_BUCKET('10 seconds', time) AS bucket, \
               AVG(value) AS average FROM metrics GROUP BY bucket";
    let logical = plan(&store, sql);
    let optimized = Executor::new(&store)
        .execute_table(&logical)
        .expect("optimized sparse aggregate");
    let row_reader = RowOnlyReader { inner: &store };
    let row_path = Executor::new(&row_reader)
        .execute_table(&logical)
        .expect("row sparse aggregate");

    assert_eq!(optimized, row_path);
    assert_eq!(optimized.rows().len(), 1);
    assert_eq!(optimized.rows()[0][1], TableValue::Null);
}

#[test]
fn lightweight_float_aggregates_match_row_path_across_series_and_negative_buckets() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    for row in [
        ("edge-a", -11, 1.0),
        ("edge-a", -1, 3.0),
        ("edge-b", 1, 5.0),
        ("edge-b", 9, 7.0),
        ("edge-a", 11, 9.0),
    ] {
        ingest(
            &mut store,
            row.0,
            row.1 * SECOND,
            row.2,
            1,
            row.2,
            "durable",
        );
    }
    store.flush_all().expect("flush durable rows");
    ingest(&mut store, "edge-b", 9 * SECOND, 11.0, 1, 11.0, "latest");

    let sql = "SELECT TIME_BUCKET('10 seconds', time) AS bucket, \
               AVG(value) AS average, SUM(value) AS total, \
               MIN(value) AS minimum, MAX(value) AS maximum, \
               COUNT(value) AS samples FROM metrics GROUP BY bucket";
    let logical = plan(&store, sql);
    let optimized = Executor::new(&store)
        .execute_table(&logical)
        .expect("optimized float aggregates");
    let row_reader = RowOnlyReader { inner: &store };
    let row_path = Executor::new(&row_reader)
        .execute_table(&logical)
        .expect("row float aggregates");

    assert_eq!(optimized, row_path);
    assert_eq!(optimized.rows().len(), 4);
}

#[test]
fn non_float_time_bucket_average_falls_back_to_row_scan() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    ingest(&mut store, "edge", SECOND, 1.0, 4, 1.0, "up");
    let reader = TrackingReader::new(&store);

    let result = QueryEngine::new(&reader)
        .query_sql(
            "SELECT TIME_BUCKET('10 seconds', time) AS bucket, \
             AVG(events) AS average FROM metrics GROUP BY bucket",
            30 * SECOND,
        )
        .expect("integer average fallback");

    assert_eq!(
        result
            .batches()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(reader.float_series_scans.get(), 1);
    assert_eq!(reader.row_scans.get(), 1);
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
