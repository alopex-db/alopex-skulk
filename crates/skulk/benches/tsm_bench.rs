//! Benchmarks for Skulk TSM components.
//!
//! Run with: cargo bench --package skulk
//!
//! ## Benchmark Categories
//!
//! - **Gorilla Compression**: Encode/decode performance
//! - **MemTable Operations**: Insert, lookup, scan
//! - **WAL Operations**: Write, sync, recovery
//! - **TSM File I/O**: Write, read, scan
//! - **End-to-End**: Full write path benchmarks

use alopex_skulk::tsm::{
    CompressedBlock, DataPoint, PartitionManager, PartitionManagerConfig, SeriesMeta,
    TimePartition, TimeRange, TimeSeriesMemTable, TsmReader, TsmWriter,
};
use alopex_skulk::wal::{SyncMode, Wal, WalConfig};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

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

// ============================================================================
// Gorilla Compression Benchmarks (varied sizes)
// ============================================================================

fn bench_gorilla_compress_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_compress");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let points = generate_typical_timeseries(*size);
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &points, |b, points| {
            b.iter(|| CompressedBlock::compress(black_box(points)))
        });
    }

    group.finish();
}

fn bench_gorilla_decompress_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_decompress");

    for size in [100, 1_000, 10_000, 100_000].iter() {
        let points = generate_typical_timeseries(*size);
        let block = CompressedBlock::compress(&points);
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &block, |b, block| {
            b.iter(|| block.decompress())
        });
    }

    group.finish();
}

// ============================================================================
// MemTable Benchmarks
// ============================================================================

fn bench_memtable_insert_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_insert");

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let partition = TimePartition::new(0, Duration::from_secs(86400));
                    let memtable = TimeSeriesMemTable::new(partition);
                    let points: Vec<_> = (0..size)
                        .map(|i| {
                            DataPoint::new(
                                "cpu.usage",
                                vec![("host".to_string(), format!("server{}", i % 10))],
                                (i as i64) * 1_000_000,
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

    group.finish();
}

fn bench_memtable_scan(c: &mut Criterion) {
    let partition = TimePartition::new(0, Duration::from_secs(86400));
    let mut memtable = TimeSeriesMemTable::new(partition);

    // Insert 10K points
    for i in 0..10_000 {
        let point = DataPoint::new(
            "cpu.usage",
            vec![("host".to_string(), "server1".to_string())],
            (i as i64) * 1_000_000,
            0.5 + (i as f64 * 0.001),
        );
        memtable.insert(&point).unwrap();
    }

    let mut group = c.benchmark_group("memtable_scan");

    // Full scan
    group.bench_function("full_10k", |b| {
        b.iter(|| {
            let range = TimeRange::new(0, 10_000 * 1_000_000);
            let results: Vec<_> = memtable.scan(range).collect();
            black_box(results)
        })
    });

    // Partial scan (10%)
    group.bench_function("partial_1k", |b| {
        b.iter(|| {
            let range = TimeRange::new(4_500 * 1_000_000, 5_500 * 1_000_000);
            let results: Vec<_> = memtable.scan(range).collect();
            black_box(results)
        })
    });

    group.finish();
}

fn bench_memtable_lookup(c: &mut Criterion) {
    let partition = TimePartition::new(0, Duration::from_secs(86400));
    let mut memtable = TimeSeriesMemTable::new(partition);

    // Insert 100 series with 100 points each
    for series_idx in 0..100 {
        for i in 0..100 {
            let point = DataPoint::new(
                format!("metric.{}", series_idx),
                vec![("id".to_string(), series_idx.to_string())],
                (i as i64) * 1_000_000,
                (series_idx * 100 + i) as f64,
            );
            memtable.insert(&point).unwrap();
        }
    }

    let series_id =
        TimeSeriesMemTable::compute_series_id("metric.50", &[("id".to_string(), "50".to_string())]);

    c.bench_function("memtable_lookup_series", |b| {
        b.iter(|| {
            let data = memtable.get_series(black_box(series_id));
            black_box(data)
        })
    });
}

// ============================================================================
// WAL Benchmarks
// ============================================================================

fn bench_wal_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_write");

    for sync_mode in [SyncMode::None, SyncMode::Fsync].iter() {
        let mode_name = match sync_mode {
            SyncMode::None => "nosync",
            SyncMode::Fsync => "fsync",
            SyncMode::Fdatasync => "fdatasync",
        };

        group.throughput(Throughput::Elements(1000));
        group.bench_function(BenchmarkId::new("1k_points", mode_name), |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let wal_config = WalConfig {
                        batch_size: 100,
                        batch_timeout: Duration::from_millis(10),
                        segment_size: 10 * 1024 * 1024,
                        sync_mode: *sync_mode,
                    };
                    let wal = Wal::new(temp_dir.path(), wal_config).unwrap();
                    let partition = TimePartition::new(0, Duration::from_secs(86400));
                    let memtable = TimeSeriesMemTable::new(partition);
                    let points: Vec<_> = (0..1000)
                        .map(|i| {
                            DataPoint::new(
                                "cpu.usage",
                                vec![("host".to_string(), "server1".to_string())],
                                (i as i64) * 1_000_000,
                                i as f64,
                            )
                        })
                        .collect();
                    (temp_dir, wal, memtable, points)
                },
                |(_temp_dir, mut wal, mut memtable, points)| {
                    for point in &points {
                        memtable.insert_with_wal(point, &mut wal).unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_wal_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_recovery");

    for size in [100, 1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let wal_dir = temp_dir.path().to_path_buf();

                    // Write entries
                    {
                        let wal_config = WalConfig {
                            batch_size: 100,
                            batch_timeout: Duration::from_millis(10),
                            segment_size: 10 * 1024 * 1024,
                            sync_mode: SyncMode::Fsync,
                        };
                        let mut wal = Wal::new(&wal_dir, wal_config).unwrap();
                        let partition = TimePartition::new(0, Duration::from_secs(86400));
                        let mut memtable = TimeSeriesMemTable::new(partition);

                        for i in 0..size {
                            let point =
                                DataPoint::new("metric", vec![], (i as i64) * 1_000_000, i as f64);
                            memtable.insert_with_wal(&point, &mut wal).unwrap();
                        }
                    }

                    (temp_dir, wal_dir)
                },
                |(_temp_dir, wal_dir)| {
                    let recovered = Wal::recover(&wal_dir).unwrap();
                    black_box(recovered)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// TSM File I/O Benchmarks
// ============================================================================

fn bench_tsm_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsm_write");

    for size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let file_path = temp_dir.path().join("test.skulk");

                    let series_id = 1u64;
                    let meta = SeriesMeta::new(
                        "cpu.usage",
                        vec![("host".to_string(), "server1".to_string())],
                    );
                    let points: BTreeMap<i64, (f64, u64)> = (0..size)
                        .map(|i| ((i as i64) * 1_000_000, (i as f64 * 0.1, i as u64)))
                        .collect();

                    (temp_dir, file_path, series_id, meta, points)
                },
                |(_temp_dir, file_path, series_id, meta, points)| {
                    let mut writer = TsmWriter::new(&file_path).unwrap();
                    writer.write_series(series_id, &meta, &points).unwrap();
                    writer.finish().unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_tsm_read_series(c: &mut Criterion) {
    // Setup: Create a TSM file with data
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("read_test.skulk");

    let series_id = 1u64;
    let meta = SeriesMeta::new(
        "cpu.usage",
        vec![("host".to_string(), "server1".to_string())],
    );
    let points: BTreeMap<i64, (f64, u64)> = (0..10_000)
        .map(|i| ((i as i64) * 1_000_000, (i as f64 * 0.1, i as u64)))
        .collect();

    {
        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(series_id, &meta, &points).unwrap();
        writer.finish().unwrap();
    }

    let reader = TsmReader::open(&file_path).unwrap();

    c.bench_function("tsm_read_series_10k", |b| {
        b.iter(|| {
            let result = reader.read_series(black_box(series_id)).unwrap();
            black_box(result)
        })
    });
}

fn bench_tsm_scan(c: &mut Criterion) {
    // Setup: Create a TSM file with data
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("scan_test.skulk");

    let series_id = 1u64;
    let meta = SeriesMeta::new("cpu.usage", vec![]);
    let points: BTreeMap<i64, (f64, u64)> = (0..10_000)
        .map(|i| ((i as i64) * 1_000_000, (i as f64, i as u64)))
        .collect();

    {
        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(series_id, &meta, &points).unwrap();
        writer.finish().unwrap();
    }

    let reader = TsmReader::open(&file_path).unwrap();

    let mut group = c.benchmark_group("tsm_scan");

    // Full scan
    group.bench_function("full_10k", |b| {
        b.iter(|| {
            let range = TimeRange::new(0, 10_000 * 1_000_000);
            let results: Vec<_> = reader.scan(range).collect();
            black_box(results)
        })
    });

    // Partial scan (10%)
    group.bench_function("partial_1k", |b| {
        b.iter(|| {
            let range = TimeRange::new(4_500 * 1_000_000, 5_500 * 1_000_000);
            let results: Vec<_> = reader.scan(range).collect();
            black_box(results)
        })
    });

    group.finish();
}

// ============================================================================
// PartitionManager Benchmarks
// ============================================================================

fn bench_partition_manager_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_manager_insert");

    for size in [1_000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let config = PartitionManagerConfig::default()
                        .with_partition_duration(Duration::from_secs(3600));
                    let manager = PartitionManager::new(config);
                    let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

                    // Create points spanning multiple partitions
                    let points: Vec<_> = (0..size)
                        .map(|i| {
                            DataPoint::new(
                                "cpu.usage",
                                vec![],
                                ((i / 1000) as i64) * duration_nanos
                                    + (i % 1000) as i64 * 1_000_000,
                                i as f64,
                            )
                        })
                        .collect();

                    (manager, points)
                },
                |(mut manager, points)| {
                    for point in &points {
                        manager.insert(point).unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_partition_manager_scan(c: &mut Criterion) {
    let config =
        PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
    let mut manager = PartitionManager::new(config);

    let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

    // Insert data across 3 partitions, 1K points each
    for partition_idx in 0..3 {
        for i in 0..1000 {
            let point = DataPoint::new(
                "cpu.usage",
                vec![],
                partition_idx * duration_nanos + i * 1_000_000,
                (partition_idx * 1000 + i) as f64,
            );
            manager.insert(&point).unwrap();
        }
    }

    let mut group = c.benchmark_group("partition_manager_scan");

    // Single partition
    group.bench_function("single_partition_1k", |b| {
        b.iter(|| {
            let range = TimeRange::new(0, duration_nanos);
            let results: Vec<_> = manager.scan(range).collect();
            black_box(results)
        })
    });

    // Cross-partition
    group.bench_function("cross_partition_3k", |b| {
        b.iter(|| {
            let range = TimeRange::new(0, 3 * duration_nanos);
            let results: Vec<_> = manager.scan(range).collect();
            black_box(results)
        })
    });

    group.finish();
}

fn bench_partition_manager_flush(c: &mut Criterion) {
    c.bench_function("partition_manager_flush_1k", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let config = PartitionManagerConfig::default()
                    .with_partition_duration(Duration::from_secs(3600));
                let mut manager = PartitionManager::new(config);

                for i in 0..1000 {
                    let point = DataPoint::new("cpu.usage", vec![], i * 1_000_000, i as f64);
                    manager.insert(&point).unwrap();
                }

                (temp_dir, manager)
            },
            |(temp_dir, mut manager): (TempDir, PartitionManager)| {
                manager.flush_partition(0, temp_dir.path()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

// ============================================================================
// End-to-End Benchmarks
// ============================================================================

fn bench_full_write_path(c: &mut Criterion) {
    c.bench_function("full_write_path_1k", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let wal_dir = temp_dir.path().join("wal");
                let tsm_dir = temp_dir.path().join("tsm");
                std::fs::create_dir_all(&tsm_dir).unwrap();

                let wal_config = WalConfig {
                    batch_size: 100,
                    batch_timeout: Duration::from_millis(10),
                    segment_size: 10 * 1024 * 1024,
                    sync_mode: SyncMode::Fsync,
                };
                let wal = Wal::new(&wal_dir, wal_config).unwrap();
                let partition = TimePartition::new(0, Duration::from_secs(3600));
                let memtable = TimeSeriesMemTable::new(partition);

                let points: Vec<_> = (0..1000)
                    .map(|i| {
                        DataPoint::new(
                            "cpu.usage",
                            vec![("host".to_string(), "server1".to_string())],
                            (i as i64) * 1_000_000,
                            i as f64,
                        )
                    })
                    .collect();

                (temp_dir, wal, memtable, tsm_dir, points)
            },
            |(_temp_dir, mut wal, mut memtable, tsm_dir, points): (
                TempDir,
                Wal,
                TimeSeriesMemTable,
                PathBuf,
                Vec<DataPoint>,
            )| {
                // WAL + MemTable insert
                for point in &points {
                    memtable.insert_with_wal(point, &mut wal).unwrap();
                }

                // Flush to TSM
                let tsm_path = tsm_dir.join("test.skulk");
                memtable.flush(&tsm_path).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    // Gorilla compression
    bench_gorilla_compress,
    bench_gorilla_decompress,
    bench_gorilla_roundtrip,
    bench_compression_ratio,
    bench_gorilla_compress_sizes,
    bench_gorilla_decompress_sizes,
    // MemTable
    bench_memtable_insert,
    bench_memtable_insert_sizes,
    bench_memtable_scan,
    bench_memtable_lookup,
    // WAL
    bench_wal_write,
    bench_wal_recovery,
    // TSM File I/O
    bench_tsm_write,
    bench_tsm_read_series,
    bench_tsm_scan,
    // PartitionManager
    bench_partition_manager_insert,
    bench_partition_manager_scan,
    bench_partition_manager_flush,
    // End-to-End
    bench_full_write_path,
);
criterion_main!(benches);
