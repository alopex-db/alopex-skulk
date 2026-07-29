use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::buffer::{FlushPolicy, MeasurementBuffer};
use alopex_skulk::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::seq::{IngestSeq, SequencedRow};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::time::Duration;

const POINTS: usize = 10_000;

fn value(index: usize, repeated: bool) -> f64 {
    if repeated {
        if index & 1 == 0 {
            0.0
        } else {
            1.0
        }
    } else {
        let mixed = (index as u64)
            .wrapping_mul(6_364_136_223_846_793_005)
            .rotate_left(17);
        (mixed as f64) / (u64::MAX as f64) * 1_000.0
    }
}

fn row(index: usize, repeated: bool) -> WideRow {
    WideRow::new(
        SeriesKey::new(
            "gauge",
            Tags::from([(
                "host".into(),
                if index & 3 == 0 {
                    "edge-a".into()
                } else {
                    "edge-b".into()
                },
            )]),
        ),
        1_000_000_000 + index as i64 * 1_000_000,
        Fields::from([("value".into(), FieldValue::Float(value(index, repeated)))]),
    )
}

fn batch(repeated: bool) -> arrow_array::RecordBatch {
    let mut buffer = MeasurementBuffer::new(
        "gauge",
        FlushPolicy::new(POINTS + 1, 128 * 1024 * 1024).expect("fixed policy"),
    );
    for index in 0..POINTS {
        buffer
            .append(&SequencedRow::new(
                IngestSeq::new(index as u64 + 1),
                row(index, repeated),
            ))
            .expect("fixed row");
    }
    buffer.drain_sorted().expect("fixed batch")
}

fn storage_benchmarks(criterion: &mut Criterion) {
    let mut parquet = criterion.benchmark_group("parquet_fsync_10k");
    parquet.throughput(Throughput::Elements(POINTS as u64));
    for (name, repeated) in [("volatile", false), ("repeated", true)] {
        let batch = batch(repeated);
        parquet.bench_with_input(
            BenchmarkId::new("dataset", name),
            &batch,
            |bencher, batch| {
                bencher.iter_batched(
                    || {
                        let root = tempfile::tempdir().expect("tempdir");
                        let path = root.path().join("bench.parquet");
                        (root, path)
                    },
                    |(_root, path)| {
                        black_box(
                            ParquetWriter::new(ParquetWriterConfig::default())
                                .write_atomic(path, batch)
                                .expect("durable parquet"),
                        );
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    parquet.finish();

    let rows = (0..POINTS)
        .map(|index| row(index, true))
        .collect::<Vec<_>>();
    let mut end_to_end = criterion.benchmark_group("recovery_fsync_flush_10k");
    end_to_end.throughput(Throughput::Elements(POINTS as u64));
    end_to_end.bench_function("single_writer", |bencher| {
        bencher.iter_batched(
            || (tempfile::tempdir().expect("tempdir"), rows.clone()),
            |(root, rows)| {
                let mut store =
                    RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
                black_box(
                    store
                        .ingest_batch_at(rows, 20_000_000_000)
                        .expect("durable batch Ack"),
                );
                black_box(store.flush_all().expect("durable flush"));
            },
            BatchSize::LargeInput,
        );
    });
    end_to_end.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = storage_benchmarks
}
criterion_main!(benches);
