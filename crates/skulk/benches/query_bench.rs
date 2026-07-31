use alopex_skulk::ingest::line_protocol::LineProtocolDecoder;
use alopex_skulk::ingest::{IngestLimits, Ingestor};
use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::{QueryEngine, QueryResult};
use alopex_skulk::store::reader::{
    ScanRequest, ScanTimeRange, StorageReader, TagPredicate, TagPredicateOp,
};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

const SECOND_NS: i64 = 1_000_000_000;
const TARGET_MEASUREMENT: &str = "query_gate";
const TARGET_SERIES: usize = 100;
const TARGET_INTERVAL_NS: i64 = 10 * SECOND_NS;
const TARGET_TICKS: usize = 24 * 60 * 60 / 10;
const TARGET_POINTS: usize = TARGET_SERIES * TARGET_TICKS;
const MIN_DATABASE_POINTS: usize = 10_000_000;
const BACKGROUND_POINTS: usize = MIN_DATABASE_POINTS - TARGET_POINTS;
const BACKGROUND_SERIES: usize = 1_000;
const LOAD_BATCH_POINTS: usize = 100_000;
const P99_RUNS: usize = 100;
const INGEST_BATCH_POINTS: usize = 10_000;
const QUERY_GATE: Duration = Duration::from_millis(100);
const INGEST_GOAL: Duration = Duration::from_millis(10);
const EVALUATION_TIME: i64 = 1_700_000_000_000_000_000;
const QUERY: &str = "SELECT TIME_BUCKET('1 hour', time) AS bucket, \
                     AVG(value) AS average FROM query_gate \
                     WHERE time > NOW() - INTERVAL '24 hours' \
                     GROUP BY bucket";

const _: () = {
    assert!(TARGET_SERIES == 100);
    assert!(TARGET_INTERVAL_NS == 10 * SECOND_NS);
    assert!(TARGET_TICKS == 8_640);
    assert!(TARGET_POINTS == 864_000);
    assert!(MIN_DATABASE_POINTS >= 10_000_000);
    assert!(P99_RUNS >= 100);
};

fn query_release_gate(criterion: &mut Criterion) {
    let root = tempfile::tempdir().expect("query benchmark tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open benchmark store");
    load_database(&mut store);

    let ingest_p99 = measure_ingest_p99();
    let engine = QueryEngine::new(&store);
    assert_query_result(engine.query_sql(QUERY, EVALUATION_TIME));
    let query_durations = measure_p99(P99_RUNS, || {
        assert_query_result(engine.query_sql(QUERY, EVALUATION_TIME));
    });
    let query_p99 = nearest_rank_p99(&query_durations);
    let (time_pruned_files, tag_pruned_groups, tag_considered_groups) = measure_pruning(&store);
    let tag_reduction = tag_pruned_groups as f64 / tag_considered_groups.max(1) as f64 * 100.0;

    eprintln!(
        "SKULK_QUERY_BASELINE \
         database_points={MIN_DATABASE_POINTS} target_points={TARGET_POINTS} \
         target_series={TARGET_SERIES} interval_seconds=10 query_runs={P99_RUNS} \
         query_p99_ms={:.3} query_gate_ms={} query_gate_pass={} \
         ingest_points={INGEST_BATCH_POINTS} ingest_runs={P99_RUNS} \
         ingest_p99_ms={:.3} ingest_goal_ms={} ingest_goal_pass={} \
         time_pruned_files={time_pruned_files} time_decoded_rows=0 \
         tag_pruned_row_groups={tag_pruned_groups} \
         tag_considered_row_groups={tag_considered_groups} \
         tag_reduction_percent={tag_reduction:.2}",
        milliseconds(query_p99),
        QUERY_GATE.as_millis(),
        query_p99 < QUERY_GATE,
        milliseconds(ingest_p99),
        INGEST_GOAL.as_millis(),
        ingest_p99 < INGEST_GOAL,
    );

    assert!(
        query_p99 < QUERY_GATE,
        "release blocked: 24h aggregate p99 {:.3} ms is not below {} ms",
        milliseconds(query_p99),
        QUERY_GATE.as_millis()
    );
    assert!(
        tag_reduction >= 50.0,
        "release blocked: tag-pruning reduction {tag_reduction:.2}% is below 50%"
    );

    let mut group = criterion.benchmark_group("query_release_gate");
    group.throughput(Throughput::Elements(TARGET_POINTS as u64));
    group.bench_function("sql_ts_24h_time_bucket_average", |bencher| {
        bencher.iter(|| {
            black_box(
                engine
                    .query_sql(black_box(QUERY), EVALUATION_TIME)
                    .expect("Nim-parsed SQL-TS benchmark query"),
            )
        });
    });
    group.finish();
}

fn load_database(store: &mut RecoveryStore) {
    let target_start = EVALUATION_TIME - (TARGET_TICKS as i64 - 1) * TARGET_INTERVAL_NS;
    load_measurement(
        store,
        TARGET_MEASUREMENT,
        TARGET_SERIES,
        TARGET_POINTS,
        target_start,
        TARGET_INTERVAL_NS,
    );
    load_measurement(
        store,
        "query_background",
        BACKGROUND_SERIES,
        BACKGROUND_POINTS,
        target_start - 2 * 24 * 60 * 60 * SECOND_NS,
        TARGET_INTERVAL_NS,
    );

    let manifest = store.manifest_state().expect("benchmark manifest");
    let manifest_points = manifest
        .active_files()
        .values()
        .map(|file| file.row_count())
        .sum::<u64>();
    assert_eq!(
        manifest_points, MIN_DATABASE_POINTS as u64,
        "benchmark database must contain the fixed point count"
    );
}

fn load_measurement(
    store: &mut RecoveryStore,
    measurement: &str,
    series_count: usize,
    point_count: usize,
    start: i64,
    interval: i64,
) {
    let series = (0..series_count)
        .map(|index| {
            let key = Arc::new(SeriesKey::new(
                measurement,
                Tags::from([("host".to_string(), format!("edge-{index:04}"))]),
            ));
            let id = key.id();
            (key, id)
        })
        .collect::<Vec<_>>();

    for batch_start in (0..point_count).step_by(LOAD_BATCH_POINTS) {
        let batch_end = (batch_start + LOAD_BATCH_POINTS).min(point_count);
        let mut rows = Vec::with_capacity(batch_end - batch_start);
        for index in batch_start..batch_end {
            let series_index = index % series_count;
            let tick = index / series_count;
            let (key, id) = &series[series_index];
            rows.push(WideRow::with_shared_series(
                Arc::clone(key),
                *id,
                start + tick as i64 * interval,
                Fields::from([(
                    "value".to_string(),
                    FieldValue::Float((tick % 10_000) as f64 + series_index as f64 / 1_000.0),
                )]),
            ));
        }
        store
            .ingest_batch_at(rows, EVALUATION_TIME)
            .expect("load durable benchmark batch");
        store.flush_all().expect("flush benchmark batch");
        eprintln!(
            "SKULK_QUERY_LOAD measurement={measurement} loaded={batch_end} total={point_count}"
        );
    }
}

fn measure_ingest_p99() -> Duration {
    let input = line_protocol_input();
    let decoder = LineProtocolDecoder::new(IngestLimits::default());
    let mut durations = Vec::with_capacity(P99_RUNS);
    for run in 0..P99_RUNS {
        let root = tempfile::tempdir().expect("ingest p99 tempdir");
        let store =
            RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("ingest p99 store");
        let now = EVALUATION_TIME + run as i64 * SECOND_NS;
        let started = Instant::now();
        let batch = decoder
            .decode(black_box(&input), now)
            .expect("decode ingest batch");
        let mut ingestor = Ingestor::new(store, IngestLimits::default());
        let outcome = ingestor.ingest(batch, now).expect("durable ingest batch");
        let elapsed = started.elapsed();
        assert_eq!(outcome.accepted_count(), INGEST_BATCH_POINTS);
        durations.push(elapsed);
    }
    nearest_rank_p99(&durations)
}

fn line_protocol_input() -> Vec<u8> {
    let mut input = String::with_capacity(INGEST_BATCH_POINTS * 60);
    for index in 0..INGEST_BATCH_POINTS {
        writeln!(
            input,
            "ingest_gate,host=edge-{:03} value={:.1} {}",
            index % TARGET_SERIES,
            index as f64 / 10.0,
            EVALUATION_TIME + (index / TARGET_SERIES) as i64 * SECOND_NS
        )
        .expect("write ingest fixture");
    }
    input.into_bytes()
}

fn measure_p99(mut runs: usize, mut operation: impl FnMut()) -> Vec<Duration> {
    runs = runs.max(P99_RUNS);
    let mut durations = Vec::with_capacity(runs);
    for _ in 0..runs {
        let started = Instant::now();
        operation();
        durations.push(started.elapsed());
    }
    durations
}

fn nearest_rank_p99(durations: &[Duration]) -> Duration {
    assert!(durations.len() >= P99_RUNS);
    let mut sorted = durations.to_vec();
    sorted.sort_unstable();
    let rank = (99 * sorted.len()).div_ceil(100);
    sorted[rank - 1]
}

fn measure_pruning(store: &RecoveryStore) -> (usize, usize, usize) {
    let outside = ScanRequest::new(
        TARGET_MEASUREMENT,
        ScanTimeRange::bounded(
            EVALUATION_TIME + SECOND_NS,
            EVALUATION_TIME + 24 * 60 * 60 * SECOND_NS,
        )
        .expect("outside time range"),
    )
    .with_field_projection(["value"]);
    let outside = store.scan(&outside).expect("time-pruning scan");
    assert!(outside.rows().is_empty());
    assert!(outside.stats().files_considered() > 0);
    assert_eq!(
        outside.stats().files_pruned(),
        outside.stats().files_considered()
    );
    assert_eq!(outside.stats().files_opened(), 0);
    assert_eq!(outside.stats().row_groups_decoded(), 0);
    assert_eq!(outside.stats().decoded_rows(), 0);

    let target_start = EVALUATION_TIME - (TARGET_TICKS as i64 - 1) * TARGET_INTERVAL_NS;
    let absent_tag = ScanRequest::new(
        TARGET_MEASUREMENT,
        ScanTimeRange::bounded(target_start, EVALUATION_TIME).expect("target time range"),
    )
    .with_field_projection(["value"])
    .with_tag_predicates([TagPredicate::new(
        "host",
        TagPredicateOp::Equal,
        "absent-host",
    )]);
    let absent_tag = store.scan(&absent_tag).expect("tag-pruning scan");
    assert!(absent_tag.rows().is_empty());
    assert!(absent_tag.stats().row_groups_considered() > 0);
    assert_eq!(absent_tag.stats().row_groups_decoded(), 0);
    assert_eq!(absent_tag.stats().decoded_rows(), 0);

    (
        outside.stats().files_pruned(),
        absent_tag.stats().row_groups_pruned_by_tag(),
        absent_tag.stats().row_groups_considered(),
    )
}

fn assert_query_result(result: alopex_skulk::Result<QueryResult>) {
    let result = result.expect("Nim-parsed SQL-TS 24h time-bucket aggregate");
    assert!(
        result
            .batches()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
            > 0,
        "24h aggregate must produce a result"
    );
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = query_release_gate
}
criterion_main!(benches);
