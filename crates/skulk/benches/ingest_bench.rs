use alopex_skulk::ingest::line_protocol::LineProtocolDecoder;
use alopex_skulk::ingest::remote_write::RemoteWriteDecoder;
use alopex_skulk::ingest::{IngestLimits, Ingestor};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use prost::Message;
use snap::raw::Encoder;
use std::fmt::Write as _;
use std::time::Duration;

const POINTS: usize = 10_000;
const SERIES: usize = 100;
const NOW: i64 = 20_000_000_000;

fn line_protocol_input() -> Vec<u8> {
    let mut input = String::with_capacity(POINTS * 50);
    for index in 0..POINTS {
        writeln!(
            input,
            "cpu,host=edge-{} value={} {}",
            index % SERIES,
            index as f64 / 10.0,
            1_000_000_000_i64 + index as i64
        )
        .expect("write fixture");
    }
    input.into_bytes()
}

fn remote_write_input() -> Vec<u8> {
    let timeseries = (0..SERIES)
        .map(|series| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "cpu".into(),
                },
                Label {
                    name: "host".into(),
                    value: format!("edge-{series}"),
                },
            ],
            samples: (0..(POINTS / SERIES))
                .map(|offset| {
                    let index = series * (POINTS / SERIES) + offset;
                    Sample {
                        value: index as f64 / 10.0,
                        timestamp: 1_000 + index as i64,
                    }
                })
                .collect(),
        })
        .collect();
    Encoder::new()
        .compress_vec(&WriteRequest { timeseries }.encode_to_vec())
        .expect("compress fixture")
}

fn protocol_decode(criterion: &mut Criterion) {
    let limits = IngestLimits::default();
    let line_input = line_protocol_input();
    let remote_input = remote_write_input();
    let mut group = criterion.benchmark_group("protocol_decode_10k");
    group.throughput(Throughput::Elements(POINTS as u64));
    group.bench_with_input(
        BenchmarkId::new("line_protocol", POINTS),
        &line_input,
        |bencher, input| {
            let decoder = LineProtocolDecoder::new(limits);
            bencher.iter(|| {
                black_box(
                    decoder
                        .decode(black_box(input), NOW)
                        .expect("Line Protocol decode")
                        .item_count(),
                )
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new("remote_write_v1", POINTS),
        &remote_input,
        |bencher, input| {
            let decoder = RemoteWriteDecoder::new(limits);
            bencher.iter(|| {
                black_box(
                    decoder
                        .decode("application/x-protobuf", "snappy", black_box(input))
                        .expect("Remote Write decode")
                        .item_count(),
                )
            });
        },
    );
    group.finish();
}

fn durable_ingest(criterion: &mut Criterion) {
    let limits = IngestLimits::default();
    let line_input = line_protocol_input();
    let remote_input = remote_write_input();
    let mut group = criterion.benchmark_group("decode_wal_fsync_ack_10k");
    group.throughput(Throughput::Elements(POINTS as u64));

    group.bench_function("line_protocol", |bencher| {
        bencher.iter_batched(
            || {
                let root = tempfile::tempdir().expect("tempdir");
                let store =
                    RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
                (root, Ingestor::new(store, limits))
            },
            |(_root, mut ingestor)| {
                let batch = LineProtocolDecoder::new(limits)
                    .decode(black_box(&line_input), NOW)
                    .expect("Line Protocol decode");
                black_box(
                    ingestor
                        .ingest(batch, NOW)
                        .expect("durable Line Protocol ingest")
                        .accepted_count(),
                );
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("remote_write_v1", |bencher| {
        bencher.iter_batched(
            || {
                let root = tempfile::tempdir().expect("tempdir");
                let store =
                    RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
                (root, Ingestor::new(store, limits))
            },
            |(_root, mut ingestor)| {
                let batch = RemoteWriteDecoder::new(limits)
                    .decode("application/x-protobuf", "snappy", black_box(&remote_input))
                    .expect("Remote Write decode");
                black_box(
                    ingestor
                        .ingest(batch, NOW)
                        .expect("durable Remote Write ingest")
                        .accepted_count(),
                );
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
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

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = protocol_decode, durable_ingest
}
criterion_main!(benches);
