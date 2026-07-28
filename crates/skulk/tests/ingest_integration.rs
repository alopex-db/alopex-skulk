use alopex_skulk::ingest::json::JsonIngestDecoder;
use alopex_skulk::ingest::line_protocol::LineProtocolDecoder;
use alopex_skulk::ingest::remote_write::RemoteWriteDecoder;
use alopex_skulk::ingest::{
    AdmissionLimits, IngestLimits, Ingestor, RequestLimits, RowLimits, SourceLocation,
};
use alopex_skulk::model::FieldValue;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::seq::IngestSeq;
use prost::Message;
use snap::raw::Encoder;

const NOW: i64 = 10_000_000;

#[test]
fn three_protocol_bytes_share_durable_flush_and_parquet_readback() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let store =
            RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
        let limits = IngestLimits::default();
        let mut ingestor = Ingestor::new(store, limits);

        let line = LineProtocolDecoder::new(limits)
            .decode(
                br#"weather,host=edge temperature=21.5,status="ready" 1000000"#,
                NOW,
            )
            .expect("line decode");
        let line_outcome = ingestor.ingest(line, NOW).expect("line ingest");
        assert_eq!(line_outcome.accepted_count(), 1);
        assert_eq!(line_outcome.rejected_count(), 0);
        assert_eq!(line_outcome.accepted()[0].sequence(), IngestSeq::new(1));

        let remote_body = remote_write_body("cpu_usage", "edge", &[(0.5, 2)]);
        let remote = RemoteWriteDecoder::new(limits)
            .decode("application/x-protobuf", "snappy", &remote_body)
            .expect("remote decode");
        let remote_outcome = ingestor.ingest(remote, NOW).expect("remote ingest");
        assert_eq!(remote_outcome.accepted_count(), 1);
        assert_eq!(remote_outcome.accepted()[0].sequence(), IngestSeq::new(2));

        let json = JsonIngestDecoder::new(limits)
            .decode(
                br#"{"metrics":[{
                    "name":"events",
                    "tags":{"host":"edge"},
                    "fields":{"count":3,"healthy":true},
                    "timestamp":3000000
                }]}"#,
                NOW,
            )
            .expect("json decode");
        let json_outcome = ingestor.ingest(json, NOW).expect("json ingest");
        assert_eq!(json_outcome.accepted_count(), 1);
        assert_eq!(json_outcome.accepted()[0].sequence(), IngestSeq::new(3));

        let published = ingestor.sink_mut().flush_all().expect("durable flush");
        assert_eq!(published.len(), 3);
    }

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen");
    let weather = store.read_measurement("weather").expect("weather");
    assert_eq!(weather.len(), 1);
    assert_eq!(
        weather[0].row().field("temperature"),
        Some(&FieldValue::Float(21.5))
    );
    assert_eq!(
        weather[0].row().field("status"),
        Some(&FieldValue::String("ready".into()))
    );
    assert_eq!(
        weather[0]
            .row()
            .series()
            .tags()
            .get("host")
            .map(String::as_str),
        Some("edge")
    );

    let cpu = store.read_measurement("cpu_usage").expect("cpu");
    assert_eq!(cpu.len(), 1);
    assert_eq!(cpu[0].row().timestamp(), 2_000_000);
    assert_eq!(cpu[0].row().field("value"), Some(&FieldValue::Float(0.5)));

    let events = store.read_measurement("events").expect("events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].row().timestamp(), 3_000_000);
    assert_eq!(
        events[0].row().field("count"),
        Some(&FieldValue::Integer(3))
    );
    assert_eq!(
        events[0].row().field("healthy"),
        Some(&FieldValue::Boolean(true))
    );
}

#[test]
fn partial_success_and_real_store_backpressure_compose_across_protocols() {
    let root = tempfile::tempdir().expect("tempdir");
    let limits = IngestLimits::new(
        RequestLimits::default(),
        RowLimits::default(),
        AdmissionLimits::new(1, 1024 * 1024, 1024 * 1024).expect("admission"),
    );
    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    let mut ingestor = Ingestor::new(store, limits);

    let partial = JsonIngestDecoder::new(limits)
        .decode(
            br#"{"metrics":[
                {"name":"events","fields":{"count":1},"timestamp":1},
                {"fields":{"count":2},"timestamp":2},
                {"name":"_time","fields":{"count":3},"timestamp":3}
            ]}"#,
            NOW,
        )
        .expect("partial decode");
    let outcome = ingestor.ingest(partial, NOW).expect("partial ingest");
    assert_eq!(outcome.accepted_count(), 1);
    assert_eq!(outcome.rejected_count(), 2);
    assert_eq!(outcome.accepted()[0].source(), SourceLocation::Item(0));
    assert_eq!(outcome.rejections()[0].source(), SourceLocation::Item(1));
    assert_eq!(outcome.rejections()[1].source(), SourceLocation::Item(2));
    assert!(outcome.rejections()[0].reason().contains("name"));
    assert!(outcome.rejections()[1].reason().contains("reserved"));

    let blocked = LineProtocolDecoder::new(limits)
        .decode(b"cpu value=2.0 2", NOW)
        .expect("line decode");
    let error = ingestor
        .ingest(blocked, NOW)
        .expect_err("backpressure before WAL");
    assert!(error.to_string().contains("backpressure"));
    assert!(ingestor
        .sink()
        .read_measurement("cpu")
        .expect("cpu absent")
        .is_empty());

    ingestor.sink_mut().flush_all().expect("release pressure");
    let retried = LineProtocolDecoder::new(limits)
        .decode(b"cpu value=2.0 2", NOW)
        .expect("line decode");
    let retry_outcome = ingestor.ingest(retried, NOW).expect("readmitted");
    assert_eq!(retry_outcome.accepted_count(), 1);
    assert_eq!(retry_outcome.accepted()[0].sequence(), IngestSeq::new(2));
    ingestor.sink_mut().flush_all().expect("final flush");
    assert_eq!(
        ingestor
            .sink()
            .read_measurement("events")
            .expect("events")
            .len(),
        1
    );
    assert_eq!(
        ingestor.sink().read_measurement("cpu").expect("cpu").len(),
        1
    );
}

fn remote_write_body(measurement: &str, host: &str, samples: &[(f64, i64)]) -> Vec<u8> {
    let request = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: measurement.into(),
                },
                Label {
                    name: "host".into(),
                    value: host.into(),
                },
            ],
            samples: samples
                .iter()
                .map(|(value, timestamp)| Sample {
                    value: *value,
                    timestamp: *timestamp,
                })
                .collect(),
        }],
    };
    Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("compress Remote Write fixture")
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
