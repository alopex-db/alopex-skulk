//! Benchmarks for Skulk TSM components.
//!
//! Run with: cargo bench --package skulk

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use skulk::tsm::{CompressedBlock, DataPoint, TimePartition, TimeSeriesMemTable};
use std::time::Duration;

/// Generate typical time series data (regular intervals, slowly varying values).
fn generate_typical_timeseries(count: usize) -> Vec<(i64, f64)> {
    let mut points = Vec::with_capacity(count);
    let start_ts = 1_000_000_000_i64;
    let interval = 1_000_000_000_i64; // 1 second in nanos

    let mut value = 50.0;
    for i in 0..count {
        // Simulate slowly varying metric
        value += (i as f64 * 0.1).sin() * 0.1;
        points.push((start_ts + (i as i64) * interval, value));
    }

    points
}

fn bench_gorilla_compress(c: &mut Criterion) {
    let points = generate_typical_timeseries(10_000);

    c.bench_function("gorilla_compress_10k", |b| {
        b.iter(|| CompressedBlock::compress(black_box(&points)))
    });
}

fn bench_gorilla_decompress(c: &mut Criterion) {
    let points = generate_typical_timeseries(10_000);
    let block = CompressedBlock::compress(&points);

    c.bench_function("gorilla_decompress_10k", |b| b.iter(|| block.decompress()));
}

fn bench_gorilla_roundtrip(c: &mut Criterion) {
    let points = generate_typical_timeseries(10_000);

    c.bench_function("gorilla_roundtrip_10k", |b| {
        b.iter(|| {
            let block = CompressedBlock::compress(black_box(&points));
            black_box(block.decompress())
        })
    });
}

fn bench_memtable_insert(c: &mut Criterion) {
    let partition = TimePartition::new(0, Duration::from_secs(86400)); // 1 day

    c.bench_function("memtable_insert_1k", |b| {
        b.iter_batched(
            || {
                let memtable = TimeSeriesMemTable::new(partition.clone());
                let points: Vec<_> = (0..1000)
                    .map(|i| {
                        DataPoint::new(
                            "cpu.usage",
                            vec![("host".to_string(), format!("server{}", i % 10))],
                            i * 1_000_000,
                            0.5 + (i as f64 * 0.001),
                        )
                    })
                    .collect();
                (memtable, points)
            },
            |(mut memtable, points)| {
                for point in &points {
                    memtable.insert(point).unwrap();
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_compression_ratio(c: &mut Criterion) {
    let points = generate_typical_timeseries(10_000);
    let raw_size = points.len() * std::mem::size_of::<(i64, f64)>();

    c.bench_function("compression_ratio_measurement", |b| {
        b.iter(|| {
            let block = CompressedBlock::compress(&points);
            let compressed_size = (block.timestamps.len() + block.values.len()).div_ceil(8)
                + std::mem::size_of::<u32>();

            // Return ratio for verification (not part of benchmark timing)
            black_box(raw_size as f64 / compressed_size as f64)
        })
    });
}

criterion_group!(
    benches,
    bench_gorilla_compress,
    bench_gorilla_decompress,
    bench_gorilla_roundtrip,
    bench_memtable_insert,
    bench_compression_ratio,
);
criterion_main!(benches);
