//! Load and stress coverage for the durable ingest path.
//!
//! The shapes follow the reference projects: prometheus `tsdb/wlog` tests
//! drive large record volumes through append/reopen/corruption carry-on,
//! `tsdb/head_bench_test.go` stresses series-cardinality paths, and
//! `influxdb3_wal` verifies buffered batch WAL contents across flush cycles.
//! Timing guards are coarse ratios, not absolute latencies, so they stay
//! stable on slow filesystems while still catching superlinear regressions.

use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::wal::WAL_FILE_NAME;
use std::time::Instant;

const NOW: i64 = 10_000_000;

fn wide_row(measurement: &str, host: usize, ts: i64, value: f64) -> WideRow {
    let mut tags = Tags::new();
    tags.insert("host".to_string(), format!("host-{host:05}"));
    WideRow::new(
        SeriesKey::new(measurement, tags),
        ts,
        Fields::from([("value".to_string(), FieldValue::Float(value))]),
    )
}

/// Load: 50 durable batches across 4 measurements (100K rows total) must all
/// be acknowledged and survive an unclean reopen (I1 at scale).
#[test]
fn sustained_batches_recover_all_rows_after_reopen() {
    let root = tempfile::tempdir().expect("tempdir");
    let measurements = ["cpu", "mem", "disk", "net"];
    {
        let mut store =
            RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
        for batch in 0..50_i64 {
            let mut rows = Vec::with_capacity(2_000);
            for measurement in measurements {
                for point in 0..500_i64 {
                    let host = (point % 100) as usize;
                    let ts = 1_000_000 + batch * 500 + point;
                    rows.push(wide_row(
                        measurement,
                        host,
                        ts,
                        (batch * 500 + point) as f64,
                    ));
                }
            }
            let accepted = store.ingest_batch_at(rows, NOW).expect("batch ingest");
            assert_eq!(accepted.len(), 2_000);
        }
        // Dropped without flush: everything must come back from the WAL alone.
    }
    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen store");
    for measurement in measurements {
        let rows = store.read_measurement(measurement).expect("readback");
        assert_eq!(rows.len(), 25_000, "measurement {measurement}");
    }
}

/// Stress guard: per-batch ingest cost must not grow superlinearly while
/// pending rows accumulate between flushes (catches quadratic validation).
#[test]
fn per_batch_cost_does_not_degrade_as_pending_accumulates() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    let mut batch_times = Vec::with_capacity(30);
    for batch in 0..30_i64 {
        let rows: Vec<WideRow> = (0..1_500_i64)
            .map(|point| {
                let host = (point % 100) as usize;
                wide_row("cpu", host, 1_000_000 + batch * 1_500 + point, point as f64)
            })
            .collect();
        let started = Instant::now();
        store.ingest_batch_at(rows, NOW).expect("batch ingest");
        batch_times.push(started.elapsed());
    }
    let first: f64 = batch_times[..5]
        .iter()
        .map(|elapsed| elapsed.as_secs_f64())
        .sum::<f64>()
        / 5.0;
    let last: f64 = batch_times[25..]
        .iter()
        .map(|elapsed| elapsed.as_secs_f64())
        .sum::<f64>()
        / 5.0;
    assert!(
        last < first * 4.0 + 0.005,
        "per-batch cost degraded: first-5 avg {first:.4}s, last-5 avg {last:.4}s"
    );
}

/// Cardinality stress: 10K distinct series in one measurement flush to
/// Parquet and read back completely (prometheus head-bench shape).
#[test]
fn high_cardinality_series_flush_and_readback() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    for chunk in 0..10_i64 {
        let rows: Vec<WideRow> = (0..5_000_i64)
            .map(|index| {
                let host = (chunk * 1_000 + index / 5) as usize;
                wide_row("cpu", host, 1_000_000 + chunk * 5_000 + index, index as f64)
            })
            .collect();
        store.ingest_batch_at(rows, NOW).expect("batch ingest");
        store.flush_all().expect("flush");
    }
    let rows = store.read_measurement("cpu").expect("readback");
    assert_eq!(rows.len(), 50_000);
}

/// Flush/checkpoint cycles: the WAL must shrink back after every flush and
/// stay bounded across cycles instead of growing monotonically.
#[test]
fn flush_cycles_keep_wal_bounded_and_data_complete() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_path = root.path().join(WAL_FILE_NAME);
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    let mut wal_sizes_after_flush = Vec::new();
    for cycle in 0..5_i64 {
        for batch in 0..5_i64 {
            let rows: Vec<WideRow> = (0..20_i64)
                .flat_map(|m| {
                    (0..100_i64)
                        .map(move |point| (m, point))
                        .collect::<Vec<_>>()
                })
                .map(|(m, point)| {
                    let measurement = format!("m{m:02}");
                    let ts = 1_000_000 + cycle * 1_000 + batch * 200 + point;
                    let mut tags = Tags::new();
                    tags.insert("host".to_string(), format!("host-{:03}", point % 50));
                    WideRow::new(
                        SeriesKey::new(measurement, tags),
                        ts,
                        Fields::from([("value".to_string(), FieldValue::Float(point as f64))]),
                    )
                })
                .collect();
            store.ingest_batch_at(rows, NOW).expect("batch ingest");
        }
        store.flush_all().expect("flush");
        let size = std::fs::metadata(&wal_path).expect("wal metadata").len();
        wal_sizes_after_flush.push(size);
    }
    let first = wal_sizes_after_flush[0];
    for (cycle, size) in wal_sizes_after_flush.iter().enumerate() {
        assert!(
            *size <= first.max(4_096),
            "WAL grew across flush cycles: cycle {cycle} size {size}, first {first}"
        );
    }
    let total: usize = (0..20)
        .map(|m| {
            store
                .read_measurement(&format!("m{m:02}"))
                .expect("readback")
                .len()
        })
        .sum();
    assert_eq!(total, 20 * 100 * 5 * 5);
}

/// Failure stress: an all-or-nothing batch rejection (oversized row) must not
/// poison the store; later batches and reopen stay consistent.
#[test]
fn rejected_batch_under_load_leaves_store_usable_and_consistent() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let mut store =
            RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
        let good: Vec<WideRow> = (0..1_000_i64)
            .map(|point| wide_row("cpu", (point % 10) as usize, 1_000_000 + point, 1.0))
            .collect();
        store.ingest_batch_at(good, NOW).expect("first batch");

        let mut poisoned_batch: Vec<WideRow> = (0..10_i64)
            .map(|point| wide_row("cpu", 0, 2_000_000 + point, 2.0))
            .collect();
        poisoned_batch.push(WideRow::new(
            SeriesKey::new("cpu", Tags::new()),
            3_000_000,
            Fields::from([(
                "message".to_string(),
                FieldValue::String("x".repeat(2 * 1024 * 1024)),
            )]),
        ));
        assert!(store.ingest_batch_at(poisoned_batch, NOW).is_err());

        let after: Vec<WideRow> = (0..1_000_i64)
            .map(|point| wide_row("cpu", (point % 10) as usize, 4_000_000 + point, 3.0))
            .collect();
        store
            .ingest_batch_at(after, NOW)
            .expect("store stays usable");
    }
    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen store");
    let rows = store.read_measurement("cpu").expect("readback");
    assert_eq!(rows.len(), 2_000, "rejected batch must contribute no rows");
}
