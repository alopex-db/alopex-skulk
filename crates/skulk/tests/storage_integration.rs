use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::buffer::{FlushPolicy, MeasurementBuffer};
use alopex_skulk::store::parquet_writer::{ParquetWriter, ParquetWriterConfig};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::retention::{LateWritePolicy, RetentionPolicy, HOUR_NANOS};
use alopex_skulk::store::seq::{IngestSeq, SequencedRow};

const METRIC_ROWS: usize = 20_000;

fn row(host: &str, timestamp: i64, fields: Fields) -> WideRow {
    WideRow::new(
        SeriesKey::new("cpu", Tags::from([("host".into(), host.into())])),
        timestamp,
        fields,
    )
}

#[test]
fn phase_one_write_compact_retain_reopen_and_read_round_trip() {
    let root = tempfile::tempdir().expect("tempdir");
    let winner_sequence;
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store
            .set_retention_policy(
                "cpu",
                RetentionPolicy::new(HOUR_NANOS as u64, LateWritePolicy::Accept).expect("policy"),
            )
            .expect("set policy");
        store
            .ingest_at(
                row(
                    "old",
                    0,
                    Fields::from([("value".into(), FieldValue::Float(1.0))]),
                ),
                2 * HOUR_NANOS,
            )
            .expect("old accepted by policy");
        store
            .ingest_at(
                row(
                    "edge-a",
                    2 * HOUR_NANOS,
                    Fields::from([
                        ("value".into(), FieldValue::Float(2.0)),
                        ("left".into(), FieldValue::String("first".into())),
                    ]),
                ),
                2 * HOUR_NANOS,
            )
            .expect("first fresh");
        store.flush_all().expect("first flush");

        winner_sequence = store
            .ingest_at(
                row(
                    "edge-a",
                    2 * HOUR_NANOS,
                    Fields::from([
                        ("value".into(), FieldValue::Float(3.0)),
                        ("right".into(), FieldValue::Boolean(true)),
                    ]),
                ),
                2 * HOUR_NANOS,
            )
            .expect("later duplicate");
        store
            .ingest_at(
                row(
                    "edge-b",
                    2 * HOUR_NANOS + 1,
                    Fields::from([("count".into(), FieldValue::Unsigned(4))]),
                ),
                2 * HOUR_NANOS,
            )
            .expect("sparse row");
        store.flush_all().expect("second flush");

        let compacted = store
            .compact_measurement("cpu")
            .expect("compact")
            .expect("fresh partition has two files");
        assert_eq!(compacted.input_file_count(), 2);
        assert_eq!(compacted.output_row_count(), 2);
        let expired = store
            .expire_retention("cpu", 2 * HOUR_NANOS)
            .expect("expire old hour");
        assert_eq!(expired.expired_partition_count(), 1);
        assert_eq!(store.list_partitions("cpu").expect("partitions").len(), 1);
    }

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen");
    let rows = store.read_measurement("cpu").expect("read");
    assert_eq!(rows.len(), 2);
    let winner = rows
        .iter()
        .find(|row| row.row().series().tags().get("host").map(String::as_str) == Some("edge-a"))
        .expect("dedup winner");
    assert_eq!(winner.ingest_seq(), winner_sequence);
    assert_eq!(winner.row().field("value"), Some(&FieldValue::Float(3.0)));
    assert_eq!(
        winner.row().field("right"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(winner.row().field("left"), None);
    assert_eq!(
        store
            .retention_policy("cpu")
            .expect("policy")
            .expect("configured")
            .late_write_policy(),
        LateWritePolicy::Accept
    );
}

#[test]
fn fixed_volatile_and_repeated_parquet_files_beat_v02_gorilla() {
    for (name, repeated) in [("volatile", false), ("repeated", true)] {
        let root = tempfile::tempdir().expect("tempdir");
        let mut buffer = MeasurementBuffer::new(
            "gauge",
            FlushPolicy::new(METRIC_ROWS + 1, 256 * 1024 * 1024).expect("policy"),
        );
        let mut baseline = Vec::with_capacity(METRIC_ROWS);
        for index in 0..METRIC_ROWS {
            let value = benchmark_value(index, repeated);
            let timestamp = 1_000_000_000 + index as i64 * 1_000_000;
            baseline.push((timestamp, value));
            buffer
                .append(SequencedRow::new(
                    IngestSeq::new(index as u64 + 1),
                    WideRow::new(
                        SeriesKey::new(
                            "gauge",
                            Tags::from([(
                                "host".into(),
                                if index % 4 == 0 {
                                    "edge-a".into()
                                } else {
                                    "edge-b".into()
                                },
                            )]),
                        ),
                        timestamp,
                        Fields::from([("value".into(), FieldValue::Float(value))]),
                    ),
                ))
                .expect("row");
        }
        let batch = buffer.drain_sorted().expect("batch");
        let written = ParquetWriter::new(ParquetWriterConfig::default())
            .write_atomic(root.path().join(format!("{name}.parquet")), &batch)
            .expect("write");
        let gorilla_bytes = v02_gorilla_size_bytes(baseline);
        eprintln!(
            "{name}: parquet={} gorilla={} ratio={:.3}",
            written.file_bytes(),
            gorilla_bytes,
            gorilla_bytes as f64 / written.file_bytes() as f64
        );
        assert!(
            written.file_bytes() < gorilla_bytes as u64,
            "{name} Parquet {} must be smaller than Gorilla {gorilla_bytes}",
            written.file_bytes()
        );
    }
}

#[test]
fn durable_batch_ack_uses_contiguous_sequences_and_recovers_every_row() {
    let root = tempfile::tempdir().expect("tempdir");
    let sequences = {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store
            .ingest_batch_at(
                (0..100)
                    .map(|index| {
                        row(
                            "batch",
                            index,
                            Fields::from([("value".into(), FieldValue::Integer(index))]),
                        )
                    })
                    .collect(),
                100,
            )
            .expect("batch Ack")
    };
    assert_eq!(sequences.len(), 100);
    assert_eq!(sequences.first().expect("first").get(), 1);
    assert_eq!(sequences.last().expect("last").get(), 100);

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert_eq!(store.read_measurement("cpu").expect("rows").len(), 100);
}

#[test]
fn invalid_batch_is_rejected_before_wal_or_sequence_mutation() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    let conflicting = vec![
        row(
            "batch",
            1,
            Fields::from([("value".into(), FieldValue::Integer(1))]),
        ),
        row(
            "batch",
            2,
            Fields::from([("value".into(), FieldValue::Float(2.0))]),
        ),
    ];
    assert!(store.ingest_batch_at(conflicting, 2).is_err());

    let sequence = store
        .ingest_at(
            row(
                "batch",
                3,
                Fields::from([("value".into(), FieldValue::Integer(3))]),
            ),
            3,
        )
        .expect("first accepted row");
    assert_eq!(sequence.get(), 1);
}

fn benchmark_value(index: usize, repeated: bool) -> f64 {
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
        mixed as f64 / u64::MAX as f64 * 1_000.0
    }
}

fn v02_gorilla_size_bytes(points: impl IntoIterator<Item = (i64, f64)>) -> usize {
    let mut timestamp_bits = 0_usize;
    let mut value_bits = 0_usize;
    let mut prior_timestamp: Option<i64> = None;
    let mut prior_delta = 0_i64;
    let mut prior_value: Option<u64> = None;
    let mut prior_leading = 0_u32;
    let mut prior_trailing = 0_u32;

    for (timestamp, value) in points {
        if let Some(previous) = prior_timestamp {
            let delta = timestamp - previous;
            let delta_of_delta = delta - prior_delta;
            timestamp_bits += if delta_of_delta == 0 {
                1
            } else if (-63..=64).contains(&delta_of_delta) {
                9
            } else if (-255..=256).contains(&delta_of_delta) {
                12
            } else if (-2047..=2048).contains(&delta_of_delta) {
                16
            } else {
                36
            };
            prior_delta = delta;
        } else {
            timestamp_bits += 64;
        }
        prior_timestamp = Some(timestamp);

        let bits = value.to_bits();
        if let Some(previous) = prior_value {
            let xor = bits ^ previous;
            if xor == 0 {
                value_bits += 1;
            } else {
                let leading = xor.leading_zeros();
                let trailing = xor.trailing_zeros();
                if leading >= prior_leading && trailing >= prior_trailing {
                    value_bits += 2 + (64 - prior_leading - prior_trailing) as usize;
                } else {
                    value_bits += 13 + (64 - leading - trailing) as usize;
                    prior_leading = leading;
                    prior_trailing = trailing;
                }
            }
        } else {
            value_bits += 64;
        }
        prior_value = Some(bits);
    }
    (timestamp_bits + value_bits).div_ceil(8)
}
