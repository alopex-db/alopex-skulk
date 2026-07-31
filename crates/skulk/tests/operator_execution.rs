use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::{ExecutionValue, Executor};
use alopex_skulk::query::plan::{
    AggregateCall, AggregateInput, AggregateKind, AggregateNode, AggregationStage, ArithmeticKind,
    BinaryNode, LogicalPlan, MeasurementSelection, PlanNode, PlanTimeRange, PlanValueType,
    RangeFunctionKind, RangeFunctionNode, ScanNode, ScanResolution, SeriesGroupKind,
    SeriesGroupNode, SeriesGrouping, SeriesWindow, SeriesWindowKind,
};
use alopex_skulk::store::reader::{ScanRequest, ScanResult, ScanStats, StorageReader};
use alopex_skulk::store::seq::{IngestSeq, SequencedRow};
use alopex_skulk::Result;
use std::collections::BTreeSet;

const SECOND: i64 = 1_000_000_000;

struct MockReader {
    rows: Vec<SequencedRow>,
}

impl StorageReader for MockReader {
    fn scan(&self, _request: &ScanRequest) -> Result<ScanResult> {
        Ok(ScanResult::new(self.rows.clone(), ScanStats::default()))
    }
}

fn row(sequence: u64, tags: &[(&str, &str)], timestamp: i64, value: f64) -> SequencedRow {
    SequencedRow::new(
        IngestSeq::new(sequence),
        WideRow::new(
            SeriesKey::new(
                "metric",
                Tags::from_iter(
                    tags.iter()
                        .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
                ),
            ),
            timestamp,
            Fields::from([("value".to_string(), FieldValue::Float(value))]),
        ),
    )
}

fn selector(at: i64, range: bool) -> PlanNode {
    let duration = 60 * SECOND;
    PlanNode::SeriesGroup(SeriesGroupNode {
        input: Box::new(PlanNode::Scan(ScanNode {
            measurement: MeasurementSelection {
                exact: Some("metric".to_string()),
                matchers: Vec::new(),
            },
            time_range: PlanTimeRange::prometheus_window(at - duration, at).expect("window"),
            tag_equalities: Vec::new(),
            field_projection: Some(BTreeSet::from(["value".to_string()])),
            resolution: ScanResolution::raw(),
        })),
        kind: SeriesGroupKind::Window(SeriesWindow {
            kind: if range {
                SeriesWindowKind::Range
            } else {
                SeriesWindowKind::Instant
            },
            evaluation_time: at,
            duration_ns: duration,
        }),
    })
}

fn aggregate(
    input: PlanNode,
    kind: AggregateKind,
    grouping: Option<SeriesGrouping>,
) -> LogicalPlan {
    LogicalPlan {
        root: PlanNode::Aggregate(AggregateNode {
            input: Box::new(input),
            calls: vec![AggregateCall {
                kind,
                argument: AggregateInput::CurrentValue,
                parameter: None,
                auxiliary: Vec::new(),
                distinct: false,
            }],
            grouping,
            stage: AggregationStage::Single,
        }),
        output_type: PlanValueType::InstantVector,
    }
}

fn vector_values(value: ExecutionValue) -> Vec<(String, Tags, f64, bool)> {
    let ExecutionValue::InstantVector(vector) = value else {
        panic!("expected vector");
    };
    vector
        .into_iter()
        .map(|sample| {
            (
                sample.series().measurement().to_string(),
                sample.series().tags().clone(),
                sample.value(),
                sample.metric_name_is_dropped(),
            )
        })
        .collect()
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= expected.abs().max(1.0) * 1e-12,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn prometheus_aggregates_follow_by_without_and_output_label_rules() {
    let at = 100 * SECOND;
    let reader = MockReader {
        rows: vec![
            row(1, &[("host", "a"), ("region", "east")], at, 1.0),
            row(2, &[("host", "a"), ("region", "west")], at, 3.0),
            row(3, &[("host", "b"), ("region", "east")], at, 10.0),
        ],
    };
    let executor = Executor::new(&reader);

    for (kind, expected_a) in [
        (AggregateKind::Sum, 4.0),
        (AggregateKind::Avg, 2.0),
        (AggregateKind::Min, 1.0),
        (AggregateKind::Max, 3.0),
        (AggregateKind::Count, 2.0),
    ] {
        let plan = aggregate(
            selector(at, false),
            kind,
            Some(SeriesGrouping {
                labels: vec!["host".to_string()],
                without: false,
            }),
        );
        let values = vector_values(executor.evaluate(&plan, at).expect("aggregate"));
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].0, "");
        assert_eq!(values[0].1, Tags::from([("host".into(), "a".into())]));
        assert_close(values[0].2, expected_a);
        assert!(values[0].3);
    }

    let without = aggregate(
        selector(at, false),
        AggregateKind::Sum,
        Some(SeriesGrouping {
            labels: vec!["region".to_string()],
            without: true,
        }),
    );
    let values = vector_values(executor.evaluate(&without, at).expect("without"));
    assert_eq!(values[0].1, Tags::from([("host".into(), "a".into())]));
    assert_close(values[0].2, 4.0);

    let by_name = aggregate(
        selector(at, false),
        AggregateKind::Sum,
        Some(SeriesGrouping {
            labels: vec!["__name__".to_string()],
            without: false,
        }),
    );
    let values = vector_values(executor.evaluate(&by_name, at).expect("by name"));
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].0, "metric");
    assert!(values[0].1.is_empty());
    assert_close(values[0].2, 14.0);
    assert!(!values[0].3);
}

#[test]
fn delayed_metric_name_drop_survives_aggregation_by_name() {
    let at = 100 * SECOND;
    let reader = MockReader {
        rows: vec![
            row(1, &[("host", "a")], at - 10 * SECOND, 1.0),
            row(2, &[("host", "a")], at, 11.0),
        ],
    };
    let rate = PlanNode::RangeFunction(RangeFunctionNode {
        input: Box::new(selector(at, true)),
        function: RangeFunctionKind::Rate,
    });
    let plan = aggregate(
        rate,
        AggregateKind::Sum,
        Some(SeriesGrouping {
            labels: vec!["__name__".to_string()],
            without: false,
        }),
    );

    let values = vector_values(
        Executor::new(&reader)
            .evaluate(&plan, at)
            .expect("sum rate"),
    );
    assert_eq!(values[0].0, "metric");
    assert!(values[0].3);
}

#[test]
fn histogram_quantile_interpolates_and_forces_monotonic_buckets() {
    let at = 100 * SECOND;
    let reader = MockReader {
        rows: vec![
            row(1, &[("job", "api"), ("le", "1")], at, 1.0),
            row(2, &[("job", "api"), ("le", "2")], at, 3.0),
            row(3, &[("job", "api"), ("le", "+Inf")], at, 4.0),
            row(4, &[("job", "broken"), ("le", "1")], at, 2.0),
            row(5, &[("job", "broken"), ("le", "2")], at, 1.0),
            row(6, &[("job", "broken"), ("le", "+Inf")], at, 4.0),
        ],
    };
    let plan = LogicalPlan {
        root: PlanNode::Aggregate(AggregateNode {
            input: Box::new(selector(at, false)),
            calls: vec![AggregateCall {
                kind: AggregateKind::HistogramQuantile,
                argument: AggregateInput::CurrentValue,
                parameter: Some(Box::new(PlanNode::Scalar(0.5))),
                auxiliary: Vec::new(),
                distinct: false,
            }],
            grouping: None,
            stage: AggregationStage::Single,
        }),
        output_type: PlanValueType::InstantVector,
    };

    let values = vector_values(
        Executor::new(&reader)
            .evaluate(&plan, at)
            .expect("quantile"),
    );
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].1["job"], "api");
    assert_close(values[0].2, 1.5);
    assert_eq!(values[1].1["job"], "broken");
    assert_close(values[1].2, 1.0);
}

#[test]
fn scalar_vector_arithmetic_preserves_operand_order_and_scalar_arithmetic() {
    let at = 100 * SECOND;
    let reader = MockReader {
        rows: vec![row(1, &[("host", "a")], at, 10.0)],
    };
    let executor = Executor::new(&reader);

    let vector_minus_scalar = LogicalPlan {
        root: PlanNode::Binary(BinaryNode {
            left: Box::new(selector(at, false)),
            op: ArithmeticKind::Sub,
            right: Box::new(PlanNode::Scalar(2.0)),
        }),
        output_type: PlanValueType::InstantVector,
    };
    let values = vector_values(
        executor
            .evaluate(&vector_minus_scalar, at)
            .expect("vector - scalar"),
    );
    assert_close(values[0].2, 8.0);
    assert!(values[0].3);

    let scalar_minus_vector = LogicalPlan {
        root: PlanNode::Binary(BinaryNode {
            left: Box::new(PlanNode::Scalar(2.0)),
            op: ArithmeticKind::Sub,
            right: Box::new(selector(at, false)),
        }),
        output_type: PlanValueType::InstantVector,
    };
    let values = vector_values(
        executor
            .evaluate(&scalar_minus_vector, at)
            .expect("scalar - vector"),
    );
    assert_close(values[0].2, -8.0);

    let scalar = LogicalPlan {
        root: PlanNode::Binary(BinaryNode {
            left: Box::new(PlanNode::Scalar(9.0)),
            op: ArithmeticKind::Div,
            right: Box::new(PlanNode::Scalar(3.0)),
        }),
        output_type: PlanValueType::Scalar,
    };
    let ExecutionValue::Scalar(sample) = executor.evaluate(&scalar, at).expect("scalar") else {
        panic!("expected scalar");
    };
    assert_eq!(sample.timestamp(), at);
    assert_eq!(sample.value(), 3.0);
}

#[cfg(feature = "promql")]
#[test]
fn planned_offset_shifts_the_samples_used_by_binary_execution() {
    let at = 100 * SECOND;
    let reader = MockReader {
        rows: vec![
            row(1, &[("host", "edge")], at - 30 * SECOND, 5.0),
            row(2, &[("host", "edge")], at, 11.0),
        ],
    };
    let expression = alopex_skulk::query::promql::parse("2 - metric offset 30s")
        .expect("parse offset expression");
    let plan = alopex_skulk::query::plan::plan_promql(
        &expression,
        alopex_skulk::query::plan::PlanContext::instant(at),
    )
    .expect("plan offset expression");

    let values = vector_values(
        Executor::new(&reader)
            .evaluate(&plan, at)
            .expect("execute offset expression"),
    );
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].1, Tags::from([("host".into(), "edge".into())]));
    assert_close(values[0].2, -3.0);
}
