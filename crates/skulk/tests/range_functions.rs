use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::{EvaluationRange, Executor};
use alopex_skulk::query::plan::{
    LogicalPlan, MeasurementSelection, PlanNode, PlanTimeRange, PlanValueType, RangeFunctionKind,
    RangeFunctionNode, ScanNode, ScanResolution, SeriesGroupKind, SeriesGroupNode, SeriesWindow,
    SeriesWindowKind,
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

fn row(sequence: u64, host: &str, timestamp: i64, value: f64) -> SequencedRow {
    SequencedRow::new(
        IngestSeq::new(sequence),
        WideRow::new(
            SeriesKey::new(
                "requests_total",
                Tags::from([("host".to_string(), host.to_string())]),
            ),
            timestamp,
            Fields::from([("value".to_string(), FieldValue::Float(value))]),
        ),
    )
}

fn function_plan(
    evaluation_time: i64,
    duration_ns: i64,
    function: RangeFunctionKind,
) -> LogicalPlan {
    let scan = PlanNode::Scan(ScanNode {
        measurement: MeasurementSelection {
            exact: Some("requests_total".to_string()),
            matchers: Vec::new(),
        },
        time_range: PlanTimeRange::prometheus_window(
            evaluation_time - duration_ns,
            evaluation_time,
        )
        .expect("window"),
        tag_equalities: Vec::new(),
        field_projection: Some(BTreeSet::from(["value".to_string()])),
        resolution: ScanResolution::raw(),
    });
    let range = PlanNode::SeriesGroup(SeriesGroupNode {
        input: Box::new(scan),
        kind: SeriesGroupKind::Window(SeriesWindow {
            kind: SeriesWindowKind::Range,
            evaluation_time,
            duration_ns,
        }),
    });
    LogicalPlan {
        root: PlanNode::RangeFunction(RangeFunctionNode {
            input: Box::new(range),
            function,
        }),
        output_type: PlanValueType::InstantVector,
    }
}

fn evaluate(
    rows: Vec<SequencedRow>,
    at: i64,
    window: i64,
    function: RangeFunctionKind,
) -> Vec<(String, f64, bool)> {
    let reader = MockReader { rows };
    Executor::new(&reader)
        .execute_instant(&function_plan(at, window, function), at)
        .expect("range function")
        .into_iter()
        .map(|sample| {
            (
                sample.series().tags()["host"].clone(),
                sample.value(),
                sample.metric_name_is_dropped(),
            )
        })
        .collect()
}

fn assert_close(actual: f64, expected: f64) {
    let tolerance = expected.abs().max(1.0) * 1e-12;
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn rate_and_increase_correct_resets_and_extrapolate_to_window_boundaries() {
    let at = 60 * SECOND;
    let rows = vec![
        row(1, "reset", 10 * SECOND, 5.0),
        row(2, "reset", 20 * SECOND, 15.0),
        row(3, "reset", 40 * SECOND, 4.0),
        row(4, "reset", 50 * SECOND, 14.0),
    ];

    let increase = evaluate(rows.clone(), at, 60 * SECOND, RangeFunctionKind::Increase);
    assert_eq!(increase.len(), 1);
    assert_close(increase[0].1, 35.0);
    assert!(increase[0].2);

    let rate = evaluate(rows, at, 60 * SECOND, RangeFunctionKind::Rate);
    assert_eq!(rate.len(), 1);
    assert_close(rate[0].1, 35.0 / 60.0);
}

#[test]
fn increase_matches_the_prometheus_counter_reset_fixture() {
    let minute = 60 * SECOND;
    let at = 30 * minute;
    let rows = vec![
        row(1, "fixture", 0, 0.0),
        row(2, "fixture", 5 * minute, 1.0),
        row(3, "fixture", 10 * minute, 2.0),
        row(4, "fixture", 15 * minute, 3.0),
        row(5, "fixture", 20 * minute, 2.0),
        row(6, "fixture", 25 * minute, 3.0),
        row(7, "fixture", 30 * minute, 4.0),
    ];

    let increase = evaluate(rows, at, 30 * minute, RangeFunctionKind::Increase);
    assert_eq!(increase.len(), 1);
    assert_close(increase[0].1, 7.0);
}

#[test]
fn extrapolation_is_capped_at_half_the_average_sample_interval() {
    let at = 60 * SECOND;
    let rows = vec![
        row(1, "late", 30 * SECOND, 10.0),
        row(2, "late", 40 * SECOND, 20.0),
    ];

    let increase = evaluate(rows, at, 60 * SECOND, RangeFunctionKind::Increase);
    assert_eq!(increase.len(), 1);
    assert_close(increase[0].1, 20.0);
}

#[test]
fn range_query_rebases_range_function_windows_and_propagates_name_dropping() {
    let start = 60 * SECOND;
    let reader = MockReader {
        rows: vec![
            row(1, "matrix", 40 * SECOND, 10.0),
            row(2, "matrix", 50 * SECOND, 20.0),
            row(3, "matrix", 60 * SECOND, 30.0),
            row(4, "matrix", 70 * SECOND, 40.0),
        ],
    };
    let plan = function_plan(start, 60 * SECOND, RangeFunctionKind::Rate);
    let range = EvaluationRange::new(start, start + 10 * SECOND, 10 * SECOND).expect("schedule");

    let matrix = Executor::new(&reader)
        .execute_range(&plan, range)
        .expect("range query");
    assert_eq!(matrix.len(), 1);
    assert!(matrix[0].metric_name_is_dropped());
    assert_eq!(matrix[0].samples().len(), 2);
    assert_eq!(matrix[0].samples()[0].timestamp(), start);
    assert_eq!(matrix[0].samples()[1].timestamp(), start + 10 * SECOND);
}

#[test]
fn irate_uses_only_the_last_two_samples_and_corrects_a_reset() {
    let at = 60 * SECOND;
    let rows = vec![
        row(1, "edge", 10 * SECOND, 100.0),
        row(2, "edge", 30 * SECOND, 120.0),
        row(3, "edge", 50 * SECOND, 3.0),
    ];

    let rate = evaluate(rows, at, 60 * SECOND, RangeFunctionKind::IRate);
    assert_eq!(rate.len(), 1);
    assert_close(rate[0].1, 3.0 / 20.0);
}

#[test]
fn counter_functions_drop_series_with_fewer_than_two_samples() {
    let at = 60 * SECOND;
    for function in [
        RangeFunctionKind::Rate,
        RangeFunctionKind::IRate,
        RangeFunctionKind::Increase,
    ] {
        let result = evaluate(
            vec![row(1, "short", 50 * SECOND, 3.0)],
            at,
            60 * SECOND,
            function,
        );
        assert!(result.is_empty(), "{function:?} must drop the series");
    }
}

#[test]
fn over_time_functions_match_float_and_nan_semantics() {
    let at = 60 * SECOND;
    let finite = vec![
        row(1, "finite", 10 * SECOND, 1.0),
        row(2, "finite", 20 * SECOND, 2.0),
        row(3, "finite", 30 * SECOND, 3.0),
    ];
    let cases = [
        (RangeFunctionKind::AvgOverTime, 2.0),
        (RangeFunctionKind::MinOverTime, 1.0),
        (RangeFunctionKind::MaxOverTime, 3.0),
        (RangeFunctionKind::SumOverTime, 6.0),
        (RangeFunctionKind::CountOverTime, 3.0),
    ];
    for (function, expected) in cases {
        let result = evaluate(finite.clone(), at, 60 * SECOND, function);
        assert_eq!(result.len(), 1);
        assert_close(result[0].1, expected);
    }

    let with_nan = vec![
        row(1, "nan", 10 * SECOND, f64::NAN),
        row(2, "nan", 20 * SECOND, 5.0),
    ];
    for function in [
        RangeFunctionKind::MinOverTime,
        RangeFunctionKind::MaxOverTime,
    ] {
        let result = evaluate(with_nan.clone(), at, 60 * SECOND, function);
        assert_eq!(result[0].1, 5.0);
    }
    for function in [
        RangeFunctionKind::AvgOverTime,
        RangeFunctionKind::SumOverTime,
    ] {
        let result = evaluate(with_nan.clone(), at, 60 * SECOND, function);
        assert!(result[0].1.is_nan());
    }
    let count = evaluate(with_nan, at, 60 * SECOND, RangeFunctionKind::CountOverTime);
    assert_eq!(count[0].1, 2.0);
}

#[test]
fn avg_over_time_avoids_overflow_when_the_true_mean_is_finite() {
    let at = 60 * SECOND;
    let result = evaluate(
        vec![
            row(1, "large", 10 * SECOND, f64::MAX),
            row(2, "large", 20 * SECOND, f64::MAX),
        ],
        at,
        60 * SECOND,
        RangeFunctionKind::AvgOverTime,
    );

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, f64::MAX);
}
