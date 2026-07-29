//! Prometheus Remote Write v1 decoder.
//!
//! The protobuf declarations below are the complete wire-message closure used by
//! `prometheus.WriteRequest` in Prometheus
//! `ab225f6ef5a833551ea0130be468f1d1b1968daa`. They are transcribed from
//! `prompb/{remote,types}.proto`; gogo options affect generated Go shape, not the
//! protobuf wire schema. Keeping the generated shape checked in avoids requiring
//! `protoc` or a C toolchain when building Skulk.

use super::{IngestBatch, IngestLimits, SourceLocation};
use crate::error::{Result, TsmError};
use crate::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use prost::Message;
use snap::raw::{decompress_len, Decoder};

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
const V1_MESSAGE: &str = "prometheus.WriteRequest";
const V2_MESSAGE: &str = "io.prometheus.write.v2.Request";
const VALUE_FIELD: &str = "value";
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Pure Snappy/protobuf-to-wide-row decoder for Prometheus Remote Write v1.
#[derive(Debug, Clone, Copy)]
pub struct RemoteWriteDecoder {
    limits: IngestLimits,
}

impl RemoteWriteDecoder {
    /// Creates a decoder that shares the unified ingestion resource policy.
    pub const fn new(limits: IngestLimits) -> Self {
        Self { limits }
    }

    /// Decodes one Remote Write v1 request without performing network I/O.
    ///
    /// Decompression, protobuf errors, unsupported request features, and
    /// timestamp overflow reject the entire request. Invalid `__name__` labels
    /// are retained as series-local rejections so other valid series can proceed.
    pub fn decode(
        &self,
        content_type: &str,
        content_encoding: &str,
        body: &[u8],
    ) -> Result<IngestBatch> {
        self.limits.validate_request_bytes(body.len(), 0)?;
        validate_content_type(content_type)?;
        if !content_encoding.eq_ignore_ascii_case("snappy") {
            return Err(TsmError::InvalidFormat(format!(
                "Remote Write requires snappy Content-Encoding, got '{content_encoding}'"
            )));
        }

        let expanded_len = decompress_len(body).map_err(|error| {
            TsmError::InvalidFormat(format!("Remote Write Snappy decompression failed: {error}"))
        })?;
        self.limits
            .validate_request_bytes(body.len(), expanded_len)?;

        let mut protobuf = Vec::new();
        protobuf.try_reserve_exact(expanded_len).map_err(|error| {
            TsmError::ResourceLimit(format!(
                "Remote Write decompression allocation of {expanded_len} bytes failed: {error}"
            ))
        })?;
        protobuf.resize(expanded_len, 0);
        let written = Decoder::new()
            .decompress(body, &mut protobuf)
            .map_err(|error| {
                TsmError::InvalidFormat(format!(
                    "Remote Write Snappy decompression failed: {error}"
                ))
            })?;
        if written != expanded_len {
            return Err(TsmError::InvalidFormat(format!(
                "Remote Write Snappy length mismatch: expected {expanded_len}, decoded {written}"
            )));
        }

        let request = proto::WriteRequest::decode(protobuf.as_slice()).map_err(|error| {
            TsmError::InvalidFormat(format!("Remote Write protobuf decode failed: {error}"))
        })?;
        self.convert_request(request, body.len(), expanded_len)
    }

    fn convert_request(
        &self,
        request: proto::WriteRequest,
        encoded_bytes: usize,
        expanded_bytes: usize,
    ) -> Result<IngestBatch> {
        if !request.metadata.is_empty() {
            return Err(TsmError::InvalidFormat(
                "Remote Write metadata is unsupported in v0.3".into(),
            ));
        }
        if let Some(index) = request
            .timeseries
            .iter()
            .position(|series| !series.exemplars.is_empty())
        {
            return Err(TsmError::InvalidFormat(format!(
                "Remote Write exemplars are unsupported in v0.3 (series {index})"
            )));
        }
        if let Some(index) = request
            .timeseries
            .iter()
            .position(|series| !series.histograms.is_empty())
        {
            return Err(TsmError::InvalidFormat(format!(
                "Remote Write histograms are unsupported in v0.3 (series {index})"
            )));
        }
        if request.timeseries.len() > self.limits.request().max_series() {
            return Err(TsmError::ResourceLimit(format!(
                "Remote Write series {} exceeds limit {}",
                request.timeseries.len(),
                self.limits.request().max_series()
            )));
        }

        let mut batch = IngestBatch::new(encoded_bytes, expanded_bytes);
        let mut sample_index = 0_usize;
        for (series_index, series) in request.timeseries.into_iter().enumerate() {
            let source = SourceLocation::Series(series_index);
            let sample_count = series.samples.len();
            match series_key(&series.labels) {
                Ok(series_key) => {
                    for sample in series.samples {
                        let timestamp =
                            sample
                                .timestamp
                                .checked_mul(NANOS_PER_MILLI)
                                .ok_or_else(|| {
                                    TsmError::InvalidInput(format!(
                                        "Remote Write timestamp overflow in series {series_index}"
                                    ))
                                })?;
                        let mut fields = Fields::new();
                        fields.insert(VALUE_FIELD.into(), FieldValue::Float(sample.value));
                        batch.push_row(
                            SourceLocation::Item(sample_index),
                            WideRow::new(series_key.clone(), timestamp, fields),
                        );
                        sample_index = sample_index.checked_add(1).ok_or_else(|| {
                            TsmError::ResourceLimit("Remote Write sample index overflow".into())
                        })?;
                        enforce_item_limit(&batch, self.limits)?;
                    }
                }
                Err(reason) => {
                    batch.reject(source, reason);
                    sample_index = sample_index.checked_add(sample_count).ok_or_else(|| {
                        TsmError::ResourceLimit("Remote Write sample index overflow".into())
                    })?;
                    enforce_item_limit(&batch, self.limits)?;
                }
            }
        }
        Ok(batch)
    }
}

fn validate_content_type(content_type: &str) -> Result<()> {
    let mut parts = content_type.split(';');
    let media_type = parts.next().unwrap_or_default().trim();
    if !media_type.eq_ignore_ascii_case(PROTOBUF_CONTENT_TYPE) {
        return Err(TsmError::InvalidFormat(format!(
            "Remote Write requires {PROTOBUF_CONTENT_TYPE}, got '{media_type}'"
        )));
    }

    let mut message = None;
    for parameter in parts {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            return Err(TsmError::InvalidFormat(format!(
                "invalid Remote Write Content-Type parameter '{parameter}'"
            )));
        };
        if name.trim().eq_ignore_ascii_case("proto") {
            if message.is_some() {
                return Err(TsmError::InvalidFormat(
                    "duplicate Remote Write proto Content-Type parameter".into(),
                ));
            }
            message = Some(value.trim().trim_matches('"'));
        }
    }

    match message {
        None | Some(V1_MESSAGE) => Ok(()),
        Some(V2_MESSAGE) => Err(TsmError::InvalidFormat(
            "Remote Write v2 is unsupported in v0.3".into(),
        )),
        Some(other) => Err(TsmError::InvalidFormat(format!(
            "unsupported Remote Write protobuf message '{other}'"
        ))),
    }
}

fn series_key(labels: &[proto::Label]) -> std::result::Result<SeriesKey, String> {
    let mut measurement = None;
    let mut tags = Tags::new();
    for label in labels {
        if label.name == "__name__" {
            if measurement.is_some() {
                return Err("duplicate __name__ label".into());
            }
            measurement = Some(label.value.clone());
        } else if tags
            .insert(label.name.clone(), label.value.clone())
            .is_some()
        {
            return Err(format!("duplicate label '{}'", label.name));
        }
    }

    match measurement {
        None => Err("missing __name__ label".into()),
        Some(name) if name.is_empty() => Err("empty __name__ label".into()),
        Some(name) => Ok(SeriesKey::new(name, tags)),
    }
}

fn enforce_item_limit(batch: &IngestBatch, limits: IngestLimits) -> Result<()> {
    if batch.item_count() > limits.request().max_rows() {
        return Err(TsmError::ResourceLimit(format!(
            "Remote Write decoded rows {} exceeds limit {}",
            batch.item_count(),
            limits.request().max_rows()
        )));
    }
    Ok(())
}

#[allow(clippy::module_name_repetitions)]
mod proto {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WriteRequest {
        #[prost(message, repeated, tag = "1")]
        pub timeseries: Vec<TimeSeries>,
        #[prost(message, repeated, tag = "3")]
        pub metadata: Vec<MetricMetadata>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct MetricMetadata {
        #[prost(enumeration = "metric_metadata::MetricType", tag = "1")]
        pub r#type: i32,
        #[prost(string, tag = "2")]
        pub metric_family_name: String,
        #[prost(string, tag = "4")]
        pub help: String,
        #[prost(string, tag = "5")]
        pub unit: String,
    }

    pub mod metric_metadata {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
        )]
        #[repr(i32)]
        pub enum MetricType {
            Unknown = 0,
            Counter = 1,
            Gauge = 2,
            Histogram = 3,
            Gaugehistogram = 4,
            Summary = 5,
            Info = 6,
            Stateset = 7,
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Sample {
        #[prost(double, tag = "1")]
        pub value: f64,
        #[prost(int64, tag = "2")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Exemplar {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(double, tag = "2")]
        pub value: f64,
        #[prost(int64, tag = "3")]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Histogram {
        #[prost(oneof = "histogram::Count", tags = "1, 2")]
        pub count: Option<histogram::Count>,
        #[prost(double, tag = "3")]
        pub sum: f64,
        #[prost(sint32, tag = "4")]
        pub schema: i32,
        #[prost(double, tag = "5")]
        pub zero_threshold: f64,
        #[prost(oneof = "histogram::ZeroCount", tags = "6, 7")]
        pub zero_count: Option<histogram::ZeroCount>,
        #[prost(message, repeated, tag = "8")]
        pub negative_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "9")]
        pub negative_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "10")]
        pub negative_counts: Vec<f64>,
        #[prost(message, repeated, tag = "11")]
        pub positive_spans: Vec<BucketSpan>,
        #[prost(sint64, repeated, tag = "12")]
        pub positive_deltas: Vec<i64>,
        #[prost(double, repeated, tag = "13")]
        pub positive_counts: Vec<f64>,
        #[prost(enumeration = "histogram::ResetHint", tag = "14")]
        pub reset_hint: i32,
        #[prost(int64, tag = "15")]
        pub timestamp: i64,
        #[prost(double, repeated, tag = "16")]
        pub custom_values: Vec<f64>,
    }

    pub mod histogram {
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Count {
            #[prost(uint64, tag = "1")]
            CountInt(u64),
            #[prost(double, tag = "2")]
            CountFloat(f64),
        }

        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum ZeroCount {
            #[prost(uint64, tag = "6")]
            ZeroCountInt(u64),
            #[prost(double, tag = "7")]
            ZeroCountFloat(f64),
        }

        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
        )]
        #[repr(i32)]
        pub enum ResetHint {
            Unknown = 0,
            Yes = 1,
            No = 2,
            Gauge = 3,
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct BucketSpan {
        #[prost(sint32, tag = "1")]
        pub offset: i32,
        #[prost(uint32, tag = "2")]
        pub length: u32,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TimeSeries {
        #[prost(message, repeated, tag = "1")]
        pub labels: Vec<Label>,
        #[prost(message, repeated, tag = "2")]
        pub samples: Vec<Sample>,
        #[prost(message, repeated, tag = "3")]
        pub exemplars: Vec<Exemplar>,
        #[prost(message, repeated, tag = "4")]
        pub histograms: Vec<Histogram>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Label {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        proto::{Exemplar, Histogram, Label, MetricMetadata, Sample, TimeSeries, WriteRequest},
        RemoteWriteDecoder,
    };
    use crate::ingest::{AdmissionLimits, IngestLimits, RequestLimits, RowLimits, SourceLocation};
    use crate::model::FieldValue;
    use proptest::prelude::*;
    use prost::Message;
    use snap::raw::Encoder;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    const V1_CONTENT_TYPE: &str = "application/x-protobuf";
    const V2_CONTENT_TYPE: &str = "application/x-protobuf;proto=io.prometheus.write.v2.Request";

    #[test]
    fn decodes_v1_float_samples_to_wide_rows_and_converts_ms_to_ns() {
        let request = WriteRequest {
            timeseries: vec![series(
                vec![("__name__", "cpu_usage"), ("host", "edge-1")],
                vec![(1.25, 42), (-0.5, -2)],
            )],
            metadata: Vec::new(),
        };

        let batch = decode(request).expect("v1 decode");

        assert_eq!(batch.rows.len(), 2);
        assert_eq!(batch.rows[0].source, SourceLocation::Item(0));
        assert_eq!(batch.rows[1].source, SourceLocation::Item(1));
        let first = &batch.rows[0].row;
        assert_eq!(first.series().measurement(), "cpu_usage");
        assert_eq!(
            first.series().tags().get("host").map(String::as_str),
            Some("edge-1")
        );
        assert_eq!(first.timestamp(), 42_000_000);
        assert_eq!(first.field("value"), Some(&FieldValue::Float(1.25)));
        assert_eq!(batch.rows[1].row.timestamp(), -2_000_000);
        assert_eq!(
            batch.rows[1].row.field("value"),
            Some(&FieldValue::Float(-0.5))
        );
    }

    #[test]
    fn decodes_a_wire_fixture_independent_of_the_rust_message_types() {
        let protobuf = [
            0x0a, 0x1e, 0x0a, 0x0f, 0x0a, 0x08, b'_', b'_', b'n', b'a', b'm', b'e', b'_', b'_',
            0x12, 0x03, b'c', b'p', b'u', 0x12, 0x0b, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xf8, 0x3f, 0x10, 0x2a,
        ];
        let body = Encoder::new()
            .compress_vec(&protobuf)
            .expect("compress fixture");

        let batch = RemoteWriteDecoder::new(IngestLimits::default())
            .decode(V1_CONTENT_TYPE, "snappy", &body)
            .expect("decode official wire shape");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row.series().measurement(), "cpu");
        assert_eq!(batch.rows[0].row.timestamp(), 42_000_000);
        assert_eq!(
            batch.rows[0].row.field("value"),
            Some(&FieldValue::Float(1.5))
        );
    }

    #[test]
    fn rejects_v2_content_type_before_decoding() {
        let decoder = RemoteWriteDecoder::new(IngestLimits::default());

        let error = decoder
            .decode(V2_CONTENT_TYPE, "snappy", &[])
            .expect_err("v2 must be rejected");

        assert!(error.to_string().contains("v2"));
    }

    #[test]
    fn rejects_metadata_exemplars_and_histograms_explicitly() {
        let metadata = WriteRequest {
            timeseries: Vec::new(),
            metadata: vec![MetricMetadata::default()],
        };
        assert_error_contains(metadata, "metadata");

        let mut exemplars = series(vec![("__name__", "cpu")], vec![(1.0, 1)]);
        exemplars.exemplars.push(Exemplar::default());
        assert_error_contains(
            WriteRequest {
                timeseries: vec![exemplars],
                metadata: Vec::new(),
            },
            "exemplar",
        );

        let mut histograms = series(vec![("__name__", "cpu")], vec![(1.0, 1)]);
        histograms.histograms.push(Histogram::default());
        assert_error_contains(
            WriteRequest {
                timeseries: vec![histograms],
                metadata: Vec::new(),
            },
            "histogram",
        );
    }

    #[test]
    fn rejects_missing_empty_and_duplicate_metric_names_per_series() {
        let request = WriteRequest {
            timeseries: vec![
                series(vec![("host", "a")], vec![(1.0, 1)]),
                series(vec![("__name__", "")], vec![(2.0, 2)]),
                series(
                    vec![("__name__", "first"), ("__name__", "second")],
                    vec![(3.0, 3)],
                ),
                series(vec![("__name__", "good")], vec![(4.0, 4)]),
            ],
            metadata: Vec::new(),
        };

        let batch = decode(request).expect("series-local validation");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row.series().measurement(), "good");
        assert_eq!(batch.rejections.len(), 3);
        assert_eq!(
            batch
                .rejections
                .iter()
                .map(|rejection| rejection.source())
                .collect::<Vec<_>>(),
            vec![
                SourceLocation::Series(0),
                SourceLocation::Series(1),
                SourceLocation::Series(2)
            ]
        );
        assert!(batch.rejections[0].reason().contains("missing"));
        assert!(batch.rejections[1].reason().contains("empty"));
        assert!(batch.rejections[2].reason().contains("duplicate"));
    }

    #[test]
    fn rejects_duplicate_non_name_labels_without_hiding_later_series() {
        let request = WriteRequest {
            timeseries: vec![
                series(
                    vec![("__name__", "bad"), ("host", "a"), ("host", "b")],
                    vec![(1.0, 1)],
                ),
                series(vec![("__name__", "good")], vec![(2.0, 2)]),
            ],
            metadata: Vec::new(),
        };

        let batch = decode(request).expect("series-local validation");

        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].row.series().measurement(), "good");
        assert_eq!(batch.rejections.len(), 1);
        assert!(batch.rejections[0].reason().contains("duplicate label"));
    }

    #[test]
    fn malformed_snappy_or_protobuf_rejects_the_whole_request() {
        let decoder = RemoteWriteDecoder::new(IngestLimits::default());
        let snappy_error = decoder
            .decode(V1_CONTENT_TYPE, "snappy", b"not-snappy")
            .expect_err("snappy error");
        assert!(snappy_error.to_string().contains("Snappy"));

        let invalid_protobuf = Encoder::new()
            .compress_vec(&[0x0a, 0x80])
            .expect("compress fixture");
        let protobuf_error = decoder
            .decode(V1_CONTENT_TYPE, "snappy", &invalid_protobuf)
            .expect_err("protobuf error");
        assert!(protobuf_error.to_string().contains("protobuf"));
    }

    #[test]
    fn rejects_timestamp_conversion_overflow_without_partial_output() {
        let request = WriteRequest {
            timeseries: vec![
                series(vec![("__name__", "valid")], vec![(1.0, 1)]),
                series(vec![("__name__", "overflow")], vec![(2.0, i64::MAX)]),
            ],
            metadata: Vec::new(),
        };

        let error = decode(request).expect_err("timestamp overflow");

        assert!(error.to_string().contains("timestamp"));
        assert!(error.to_string().contains("overflow"));
    }

    #[test]
    fn enforces_encoded_and_decompressed_request_limits_before_protobuf_decode() {
        let request = WriteRequest {
            timeseries: vec![series(vec![("__name__", "cpu")], vec![(1.0, 1)])],
            metadata: Vec::new(),
        };
        let protobuf = request.encode_to_vec();
        let compressed = Encoder::new()
            .compress_vec(&protobuf)
            .expect("compress fixture");

        let encoded_limits = IngestLimits::new(
            RequestLimits::new(1, 1_024, 10, 10).expect("limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let error = RemoteWriteDecoder::new(encoded_limits)
            .decode(V1_CONTENT_TYPE, "snappy", &compressed)
            .expect_err("encoded limit");
        assert!(error.to_string().contains("request bytes"));

        let expanded_limits = IngestLimits::new(
            RequestLimits::new(1_024, 1, 10, 10).expect("limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let error = RemoteWriteDecoder::new(expanded_limits)
            .decode(V1_CONTENT_TYPE, "snappy", &compressed)
            .expect_err("expanded limit");
        assert!(error.to_string().contains("expanded request bytes"));
    }

    #[test]
    fn enforces_series_and_sample_count_limits() {
        let one_each = IngestLimits::new(
            RequestLimits::new(1_024, 1_024, 1, 1).expect("limits"),
            RowLimits::default(),
            AdmissionLimits::default(),
        );
        let two_series = WriteRequest {
            timeseries: vec![
                series(vec![("__name__", "one")], vec![(1.0, 1)]),
                series(vec![("__name__", "two")], vec![(2.0, 2)]),
            ],
            metadata: Vec::new(),
        };
        let error = RemoteWriteDecoder::new(one_each)
            .decode(V1_CONTENT_TYPE, "snappy", &compressed(two_series))
            .expect_err("series limit");
        assert!(error.to_string().contains("series"));

        let two_samples = WriteRequest {
            timeseries: vec![series(vec![("__name__", "one")], vec![(1.0, 1), (2.0, 2)])],
            metadata: Vec::new(),
        };
        let error = RemoteWriteDecoder::new(one_each)
            .decode(V1_CONTENT_TYPE, "snappy", &compressed(two_samples))
            .expect_err("sample limit");
        assert!(error.to_string().contains("decoded rows"));
    }

    #[test]
    fn rejects_unsupported_content_metadata() {
        let decoder = RemoteWriteDecoder::new(IngestLimits::default());
        let body = compressed(WriteRequest::default());

        let encoding_error = decoder
            .decode(V1_CONTENT_TYPE, "zstd", &body)
            .expect_err("encoding");
        assert!(encoding_error.to_string().contains("snappy"));

        let proto_error = decoder
            .decode(
                "application/x-protobuf;proto=example.Unknown",
                "snappy",
                &body,
            )
            .expect_err("unknown proto");
        assert!(proto_error.to_string().contains("unsupported"));
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_return_results_without_panicking(
            input in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let decoder = RemoteWriteDecoder::new(IngestLimits::default());
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                decoder.decode(V1_CONTENT_TYPE, "snappy", &input)
            }));
            prop_assert!(outcome.is_ok());
        }

        #[test]
        fn arbitrary_compressed_protobuf_returns_results_without_panicking(
            protobuf in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let decoder = RemoteWriteDecoder::new(IngestLimits::default());
            let body = Encoder::new().compress_vec(&protobuf).expect("compress fuzz case");
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                decoder.decode(V1_CONTENT_TYPE, "snappy", &body)
            }));
            prop_assert!(outcome.is_ok());
        }
    }

    fn decode(request: WriteRequest) -> crate::Result<crate::ingest::IngestBatch> {
        RemoteWriteDecoder::new(IngestLimits::default()).decode(
            V1_CONTENT_TYPE,
            "snappy",
            &compressed(request),
        )
    }

    fn assert_error_contains(request: WriteRequest, expected: &str) {
        let error = decode(request).expect_err("unsupported payload");
        assert!(
            error.to_string().contains(expected),
            "{error} did not contain {expected}"
        );
    }

    fn compressed(request: WriteRequest) -> Vec<u8> {
        Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .expect("compress fixture")
    }

    fn series(labels: Vec<(&str, &str)>, samples: Vec<(f64, i64)>) -> TimeSeries {
        TimeSeries {
            labels: labels
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect(),
            samples: samples
                .into_iter()
                .map(|(value, timestamp)| Sample { value, timestamp })
                .collect(),
            exemplars: Vec::new(),
            histograms: Vec::new(),
        }
    }
}
