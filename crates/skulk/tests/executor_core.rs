use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::{
    is_stale_nan, label_matches, EvaluationRange, ExecutionValue, Executor, ExecutorConfig,
    STALE_NAN_BITS,
};
use alopex_skulk::query::plan::{
    FilterNode, LogicalPlan, MeasurementSelection, PlanNode, PlanPredicate, PlanTimeRange,
    PlanValueType, ScanNode, ScanResolution, SeriesGroupKind, SeriesGroupNode, SeriesWindow,
    SeriesWindowKind,
};
use alopex_skulk::query::{LabelMatcher, MatchOp};
use alopex_skulk::store::reader::{ScanRequest, ScanResult, ScanStats, StorageReader};
use alopex_skulk::store::seq::{IngestSeq, SequencedRow};
use alopex_skulk::{Result, TsmError};
use std::cell::RefCell;
use std::collections::BTreeSet;

const SECOND: i64 = 1_000_000_000;

#[derive(Default)]
struct MockReader {
    rows: Vec<SequencedRow>,
    requests: RefCell<Vec<ScanRequest>>,
    failure: Option<String>,
}

impl MockReader {
    fn with_rows(rows: Vec<SequencedRow>) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    fn failing(message: &str) -> Self {
        Self {
            failure: Some(message.to_string()),
            ..Self::default()
        }
    }
}

impl StorageReader for MockReader {
    fn scan(&self, request: &ScanRequest) -> Result<ScanResult> {
        self.requests.borrow_mut().push(request.clone());
        if let Some(message) = &self.failure {
            return Err(TsmError::Corruption(message.clone()));
        }
        Ok(ScanResult::new(self.rows.clone(), ScanStats::default()))
    }
}

fn row(sequence: u64, host: &str, timestamp: i64, value: f64) -> SequencedRow {
    SequencedRow::new(
        IngestSeq::new(sequence),
        WideRow::new(
            SeriesKey::new("cpu", Tags::from([("host".to_string(), host.to_string())])),
            timestamp,
            Fields::from([("value".to_string(), FieldValue::Float(value))]),
        ),
    )
}

fn sparse_row(sequence: u64, host: &str, timestamp: i64) -> SequencedRow {
    SequencedRow::new(
        IngestSeq::new(sequence),
        WideRow::new(
            SeriesKey::new("cpu", Tags::from([("host".to_string(), host.to_string())])),
            timestamp,
            Fields::new(),
        ),
    )
}

fn selector_plan(
    evaluation_time: i64,
    duration_ns: i64,
    kind: SeriesWindowKind,
    predicates: Vec<PlanPredicate>,
) -> LogicalPlan {
    let scan = PlanNode::Scan(ScanNode {
        measurement: MeasurementSelection {
            exact: Some("cpu".to_string()),
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
    let input = if predicates.is_empty() {
        scan
    } else {
        PlanNode::Filter(FilterNode {
            input: Box::new(scan),
            predicates,
        })
    };
    LogicalPlan {
        root: PlanNode::SeriesGroup(SeriesGroupNode {
            input: Box::new(input),
            kind: SeriesGroupKind::Window(SeriesWindow {
                kind,
                evaluation_time,
                duration_ns,
            }),
        }),
        output_type: match kind {
            SeriesWindowKind::Instant => PlanValueType::InstantVector,
            SeriesWindowKind::Range => PlanValueType::RangeVector,
        },
    }
}

#[test]
fn instant_window_is_open_lower_closed_upper_and_uses_latest_field_sample() {
    let at = 600 * SECOND;
    let lookback = 300 * SECOND;
    let reader = MockReader::with_rows(vec![
        row(1, "lower-only", at - lookback, 1.0),
        row(2, "edge", at - lookback, 2.0),
        row(3, "edge", at - lookback + 1, 3.0),
        sparse_row(4, "edge", at - 10),
        row(5, "upper", at, 5.0),
        row(6, "future", at + 1, 6.0),
    ]);
    let executor = Executor::new(&reader);
    let plan = selector_plan(at, lookback, SeriesWindowKind::Instant, Vec::new());

    let vector = executor
        .execute_instant(&plan, at)
        .expect("instant evaluation");
    assert_eq!(vector.len(), 2);
    assert_eq!(vector[0].series().tags()["host"], "edge");
    assert_eq!(vector[0].source_timestamp(), at - lookback + 1);
    assert_eq!(vector[0].evaluation_timestamp(), at);
    assert_eq!(vector[0].value(), 3.0);
    assert_eq!(vector[1].series().tags()["host"], "upper");
    assert_eq!(vector[1].source_timestamp(), at);

    let requests = reader.requests.borrow();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].time_range().start(), Some(at - lookback + 1));
    assert_eq!(requests[0].time_range().end(), Some(at));
}

#[test]
fn stale_latest_sample_removes_instant_series_without_falling_back() {
    let at = 100 * SECOND;
    let stale = f64::from_bits(STALE_NAN_BITS);
    assert!(is_stale_nan(stale));
    assert!(!is_stale_nan(f64::NAN));

    let reader = MockReader::with_rows(vec![
        row(1, "stale", at - 2, 1.0),
        row(2, "stale", at - 1, stale),
        row(3, "normal-nan", at, f64::NAN),
        row(4, "live", at, 4.0),
    ]);
    let executor = Executor::new(&reader);
    let plan = selector_plan(at, 10 * SECOND, SeriesWindowKind::Instant, Vec::new());

    let vector = executor.execute_instant(&plan, at).expect("instant");
    assert_eq!(vector.len(), 2);
    assert_eq!(vector[0].series().tags()["host"], "live");
    assert_eq!(vector[1].series().tags()["host"], "normal-nan");
    assert!(vector[1].value().is_nan());
    assert!(!is_stale_nan(vector[1].value()));
}

#[test]
fn range_vector_input_excludes_only_staleness_markers() {
    let at = 100 * SECOND;
    let reader = MockReader::with_rows(vec![
        row(1, "edge", at - 10 * SECOND, 1.0),
        row(2, "edge", at - 9 * SECOND, f64::from_bits(STALE_NAN_BITS)),
        row(3, "edge", at - 8 * SECOND, f64::NAN),
        row(4, "edge", at, 4.0),
    ]);
    let executor = Executor::new(&reader);
    let plan = selector_plan(at, 10 * SECOND, SeriesWindowKind::Range, Vec::new());

    let ExecutionValue::RangeVector(series) = executor.evaluate(&plan, at).expect("range vector")
    else {
        panic!("expected range vector");
    };
    assert_eq!(series.len(), 1);
    assert_eq!(series[0].samples().len(), 2);
    assert_eq!(series[0].samples()[0].timestamp(), at - 8 * SECOND);
    assert!(series[0].samples()[0].value().is_nan());
    assert_eq!(series[0].samples()[1].timestamp(), at);
}

#[test]
fn range_query_rebases_windows_at_every_inclusive_step() {
    let start = 100 * SECOND;
    let reader = MockReader::with_rows(vec![
        row(1, "edge", start - 5 * SECOND, 1.0),
        row(
            2,
            "edge",
            start + 10 * SECOND,
            f64::from_bits(STALE_NAN_BITS),
        ),
        row(3, "edge", start + 15 * SECOND, 3.0),
    ]);
    let executor = Executor::new(&reader);
    let plan = selector_plan(start, 10 * SECOND, SeriesWindowKind::Instant, Vec::new());
    let range =
        EvaluationRange::new(start, start + 20 * SECOND, 10 * SECOND).expect("range request");

    let matrix = executor.execute_range(&plan, range).expect("range query");
    assert_eq!(matrix.len(), 1);
    assert_eq!(
        matrix[0]
            .samples()
            .iter()
            .map(|sample| (sample.timestamp(), sample.value()))
            .collect::<Vec<_>>(),
        [(start, 1.0), (start + 20 * SECOND, 3.0)]
    );
    let requests = reader.requests.borrow();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].time_range().start(),
        Some(start + 10 * SECOND + 1)
    );
    assert_eq!(requests[2].time_range().end(), Some(start + 20 * SECOND));
}

#[test]
fn missing_labels_are_empty_for_every_match_operator() {
    let series = SeriesKey::new("cpu", Tags::new());
    let cases = [
        (MatchOp::Equal, "", true),
        (MatchOp::Equal, "x", false),
        (MatchOp::NotEqual, "x", true),
        (MatchOp::NotEqual, "", false),
        (MatchOp::Regex, "", true),
        (MatchOp::Regex, ".+", false),
        (MatchOp::NotRegex, ".+", true),
        (MatchOp::NotRegex, "", false),
    ];
    for (op, pattern, expected) in cases {
        assert_eq!(
            label_matches(&LabelMatcher::new("missing", op, pattern), &series)
                .expect("valid matcher"),
            expected,
            "{op:?} {pattern:?}"
        );
    }
    assert!(label_matches(
        &LabelMatcher::new("__name__", MatchOp::Equal, "cpu"),
        &series
    )
    .expect("name matcher"));
}

#[test]
fn label_filter_is_composed_between_scan_and_grouping() {
    let at = 100 * SECOND;
    let reader = MockReader::with_rows(vec![row(1, "edge-1", at, 1.0), row(2, "db-1", at, 2.0)]);
    let executor = Executor::new(&reader);
    let plan = selector_plan(
        at,
        10 * SECOND,
        SeriesWindowKind::Instant,
        vec![PlanPredicate::Label(LabelMatcher::new(
            "host",
            MatchOp::Regex,
            "edge-.+",
        ))],
    );

    let vector = executor.execute_instant(&plan, at).expect("filtered");
    assert_eq!(vector.len(), 1);
    assert_eq!(vector[0].series().tags()["host"], "edge-1");
}

#[test]
fn intermediate_and_step_limits_fail_before_partial_results() {
    let at = 100 * SECOND;
    let reader = MockReader::with_rows(vec![
        row(1, "a", at, 1.0),
        row(2, "b", at, 2.0),
        row(3, "c", at, 3.0),
    ]);
    let config = ExecutorConfig::new(2, 10, 2).expect("config");
    let executor = Executor::with_config(&reader, config);
    let plan = selector_plan(at, 10 * SECOND, SeriesWindowKind::Instant, Vec::new());
    assert!(matches!(
        executor.execute_instant(&plan, at),
        Err(TsmError::ResourceLimit(_))
    ));

    let small_reader = MockReader::with_rows(vec![row(1, "a", at, 1.0)]);
    let executor = Executor::with_config(&small_reader, config);
    let range = EvaluationRange::new(at, at + 20 * SECOND, 10 * SECOND).expect("range");
    assert!(matches!(
        executor.execute_range(&plan, range),
        Err(TsmError::ResourceLimit(_))
    ));
    assert!(small_reader.requests.borrow().is_empty());
}

#[test]
fn storage_errors_are_propagated_without_partial_results() {
    let at = 100 * SECOND;
    let reader = MockReader::failing("broken segment");
    let executor = Executor::new(&reader);
    let plan = selector_plan(at, 10 * SECOND, SeriesWindowKind::Instant, Vec::new());

    assert!(matches!(
        executor.execute_instant(&plan, at),
        Err(TsmError::Corruption(message)) if message == "broken segment"
    ));
}

#[test]
fn invalid_evaluation_ranges_and_configs_are_rejected() {
    assert!(EvaluationRange::new(2, 1, 1).is_err());
    assert!(EvaluationRange::new(1, 2, 0).is_err());
    assert!(ExecutorConfig::new(0, 1, 1).is_err());
    assert!(ExecutorConfig::new(1, 0, 1).is_err());
    assert!(ExecutorConfig::new(1, 1, 0).is_err());

    let reader = MockReader::default();
    let executor = Executor::new(&reader);
    let mut plan = selector_plan(10, 1, SeriesWindowKind::Instant, Vec::new());
    let PlanNode::SeriesGroup(group) = &mut plan.root else {
        panic!("series group");
    };
    let SeriesGroupKind::Window(window) = &mut group.kind else {
        panic!("window");
    };
    window.duration_ns = 0;
    assert!(matches!(
        executor.execute_instant(&plan, 10),
        Err(TsmError::Plan { .. })
    ));
}
