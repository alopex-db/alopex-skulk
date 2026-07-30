use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::exec::limits::{
    CancellationToken, ExecutionLimits, ParsingLimits, QueryExecutionContext, QueryLimits,
    ScanLimits, DEFAULT_QUERY_TIMEOUT,
};
use alopex_skulk::query::plan::{
    LogicalPlan, MeasurementSelection, PlanNode, PlanTimeRange, PlanValueType, ScanNode,
    ScanResolution, SeriesGroupKind, SeriesGroupNode, SeriesWindow, SeriesWindowKind,
};
use alopex_skulk::query::{LabelMatcher, MatchOp, QueryEngine};
use alopex_skulk::store::reader::{ScanRequest, ScanResult, ScanStats, StorageReader};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::seq::{IngestSeq, SequencedRow};
use alopex_skulk::{Result, TsmError};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

const SECOND: i64 = 1_000_000_000;

fn row(sequence: u64, host: &str, timestamp: i64) -> SequencedRow {
    SequencedRow::new(
        IngestSeq::new(sequence),
        WideRow::new(
            SeriesKey::new("cpu", Tags::from([("host".to_string(), host.to_string())])),
            timestamp,
            Fields::from([("value".to_string(), FieldValue::Float(sequence as f64))]),
        ),
    )
}

fn selector_plan(at: i64) -> LogicalPlan {
    LogicalPlan {
        root: PlanNode::SeriesGroup(SeriesGroupNode {
            input: Box::new(PlanNode::Scan(ScanNode {
                measurement: MeasurementSelection {
                    exact: Some("cpu".to_string()),
                    matchers: Vec::new(),
                },
                time_range: PlanTimeRange::prometheus_window(at - 60 * SECOND, at).expect("window"),
                tag_equalities: Vec::new(),
                field_projection: Some(BTreeSet::from(["value".to_string()])),
                resolution: ScanResolution::raw(),
            })),
            kind: SeriesGroupKind::Window(SeriesWindow {
                kind: SeriesWindowKind::Instant,
                evaluation_time: at,
                duration_ns: 60 * SECOND,
            }),
        }),
        output_type: PlanValueType::InstantVector,
    }
}

fn matcher_selector_plan(at: i64) -> LogicalPlan {
    let mut plan = selector_plan(at);
    let PlanNode::SeriesGroup(group) = &mut plan.root else {
        unreachable!("selector helper always produces a series group");
    };
    let PlanNode::Scan(scan) = group.input.as_mut() else {
        unreachable!("selector helper always produces a scan");
    };
    scan.measurement = MeasurementSelection {
        exact: None,
        matchers: vec![LabelMatcher::new("__name__", MatchOp::Regex, "cpu|memory")],
    };
    plan
}

fn scalar_plan() -> LogicalPlan {
    LogicalPlan {
        root: PlanNode::Scalar(1.0),
        output_type: PlanValueType::Scalar,
    }
}

fn context(limits: QueryLimits, token: CancellationToken) -> QueryExecutionContext {
    QueryExecutionContext::with_timeout(limits, Duration::from_secs(60), token)
        .expect("query context")
}

#[derive(Default)]
struct MockReader {
    rows: Vec<SequencedRow>,
}

impl StorageReader for MockReader {
    fn scan(&self, _request: &ScanRequest) -> Result<ScanResult> {
        Ok(ScanResult::new(self.rows.clone(), ScanStats::default()))
    }
}

struct CancellingReader {
    token: CancellationToken,
}

impl StorageReader for CancellingReader {
    fn scan(&self, _request: &ScanRequest) -> Result<ScanResult> {
        self.token.cancel();
        Ok(ScanResult::new(
            vec![row(1, "a", 10 * SECOND)],
            ScanStats::default(),
        ))
    }
}

#[test]
fn one_limits_context_governs_series_decode_and_intermediate_results() {
    let rows = vec![row(1, "a", 10 * SECOND), row(2, "b", 10 * SECOND)];
    let reader = MockReader { rows };
    let engine = QueryEngine::new(&reader);

    let series_limits = QueryLimits::new(
        ParsingLimits::DEFAULT,
        ScanLimits::new(1, 1_000_000, 256 * 1024 * 1024).expect("scan limits"),
        ExecutionLimits::DEFAULT,
    );
    let error = engine
        .execute_with_context(
            &selector_plan(10 * SECOND),
            10 * SECOND,
            &context(series_limits, CancellationToken::new()),
        )
        .expect_err("series expansion must be bounded");
    assert!(matches!(error, TsmError::ResourceLimit(message) if message.contains("series")));

    let intermediate_limits = QueryLimits::new(
        ParsingLimits::DEFAULT,
        ScanLimits::DEFAULT,
        ExecutionLimits::new(1, 1_000_000, 100_000).expect("execution limits"),
    );
    let error = engine
        .execute_with_context(
            &selector_plan(10 * SECOND),
            10 * SECOND,
            &context(intermediate_limits, CancellationToken::new()),
        )
        .expect_err("intermediate rows must be bounded");
    assert!(matches!(error, TsmError::ResourceLimit(message) if message.contains("rows")));

    let root = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open store");
    store
        .ingest(row(1, "a", 10 * SECOND).into_parts().1)
        .expect("first ingest");
    store
        .ingest(row(2, "b", 10 * SECOND).into_parts().1)
        .expect("second ingest");
    store.flush_all().expect("flush");
    let engine = QueryEngine::new(&store);
    let decode_limits = QueryLimits::new(
        ParsingLimits::DEFAULT,
        ScanLimits::new(100, 1, 256 * 1024 * 1024).expect("decode limits"),
        ExecutionLimits::DEFAULT,
    );
    let error = engine
        .execute_with_context(
            &selector_plan(10 * SECOND),
            10 * SECOND,
            &context(decode_limits, CancellationToken::new()),
        )
        .expect_err("decode rows must be rejected before decoding");
    assert!(
        matches!(error, TsmError::ResourceLimit(message) if message.contains("decode") && message.contains("rows"))
    );

    let byte_limits = QueryLimits::new(
        ParsingLimits::DEFAULT,
        ScanLimits::new(100, 1_000_000, 1).expect("decode byte limits"),
        ExecutionLimits::DEFAULT,
    );
    let error = engine
        .execute_with_context(
            &selector_plan(10 * SECOND),
            10 * SECOND,
            &context(byte_limits, CancellationToken::new()),
        )
        .expect_err("decode bytes must be rejected before decoding");
    assert!(
        matches!(error, TsmError::ResourceLimit(message) if message.contains("decode") && message.contains("byte"))
    );

    store
        .ingest(WideRow::new(
            SeriesKey::new(
                "memory",
                Tags::from([("host".to_string(), "a".to_string())]),
            ),
            10 * SECOND,
            Fields::from([("value".to_string(), FieldValue::Float(3.0))]),
        ))
        .expect("other measurement");
    store.flush_all().expect("flush other measurement");
    let engine = QueryEngine::new(&store);
    let cumulative_limits = QueryLimits::new(
        ParsingLimits::DEFAULT,
        ScanLimits::new(100, 2, 256 * 1024 * 1024).expect("cumulative limits"),
        ExecutionLimits::DEFAULT,
    );
    let error = engine
        .execute_with_context(
            &matcher_selector_plan(10 * SECOND),
            10 * SECOND,
            &context(cumulative_limits, CancellationToken::new()),
        )
        .expect_err("decode budget must be shared across measurements");
    assert!(
        matches!(error, TsmError::ResourceLimit(message) if message.contains("decode") && message.contains("row"))
    );
}

#[cfg(feature = "promql")]
#[test]
fn parsing_limits_are_injected_before_ffi_mapping_and_regex_compilation() {
    let reader = MockReader::default();
    let engine = QueryEngine::new(&reader);

    let cases = [
        (
            ParsingLimits::new(4, 64, 65_536, 32 * 1024, 2 * 1024 * 1024).expect("input limits"),
            "12345",
            "input",
        ),
        (
            ParsingLimits::new(1 << 20, 1, 65_536, 32 * 1024, 2 * 1024 * 1024)
                .expect("depth limits"),
            "1 + 2",
            "depth",
        ),
        (
            ParsingLimits::new(1 << 20, 64, 1, 32 * 1024, 2 * 1024 * 1024).expect("node limits"),
            "1 + 2",
            "node",
        ),
        (
            ParsingLimits::new(1 << 20, 64, 65_536, 1, 2 * 1024 * 1024).expect("regex limits"),
            "{__name__=~\"ab\"}",
            "regex",
        ),
        (
            ParsingLimits::new(1 << 20, 64, 65_536, 32 * 1024, 1).expect("automaton limits"),
            "{__name__=~\"a\"}",
            "compiled",
        ),
    ];

    for (parsing, expression, expected) in cases {
        let limits = QueryLimits::new(parsing, ScanLimits::DEFAULT, ExecutionLimits::DEFAULT);
        let error = engine
            .query_promql_with_context(
                expression,
                10 * SECOND,
                &context(limits, CancellationToken::new()),
            )
            .expect_err("parsing limit must reject");
        assert!(
            matches!(error, TsmError::ResourceLimit(ref message) if message.to_ascii_lowercase().contains(expected)),
            "{error:?}"
        );
    }
}

#[cfg(feature = "sql-ts")]
#[test]
fn sql_parsing_limits_are_injected_before_ffi_and_during_ast_mapping() {
    let reader = MockReader::default();
    let engine = QueryEngine::new(&reader);
    let cases = [
        (
            ParsingLimits::new(4, 64, 65_536, 32 * 1024, 2 * 1024 * 1024).expect("input limits"),
            "SELECT value FROM cpu",
            "input",
        ),
        (
            ParsingLimits::new(1 << 20, 1, 65_536, 32 * 1024, 2 * 1024 * 1024)
                .expect("depth limits"),
            "SELECT value + 1 FROM cpu",
            "depth",
        ),
        (
            ParsingLimits::new(1 << 20, 64, 1, 32 * 1024, 2 * 1024 * 1024).expect("node limits"),
            "SELECT value + 1 FROM cpu",
            "node",
        ),
    ];

    for (parsing, query, expected) in cases {
        let limits = QueryLimits::new(parsing, ScanLimits::DEFAULT, ExecutionLimits::DEFAULT);
        let error = engine
            .query_sql_with_context(
                query,
                10 * SECOND,
                &context(limits, CancellationToken::new()),
            )
            .expect_err("SQL parsing limit must reject");
        assert!(
            matches!(error, TsmError::ResourceLimit(ref message) if message.to_ascii_lowercase().contains(expected)),
            "{error:?}"
        );
    }
}

#[test]
fn deadline_and_cancellation_abort_without_partial_results() {
    let reader = MockReader::default();
    let engine = QueryEngine::new(&reader);
    assert_eq!(engine.default_timeout(), DEFAULT_QUERY_TIMEOUT);

    let expired = QueryExecutionContext::with_deadline(
        QueryLimits::DEFAULT,
        Instant::now(),
        CancellationToken::new(),
    );
    assert!(matches!(
        engine.execute_with_context(&scalar_plan(), 0, &expired),
        Err(TsmError::Timeout)
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_context = context(QueryLimits::DEFAULT, cancelled);
    assert!(matches!(
        engine.execute_with_context(&scalar_plan(), 0, &cancelled_context),
        Err(TsmError::Cancelled)
    ));

    let cooperative_token = CancellationToken::new();
    let reader = CancellingReader {
        token: cooperative_token.clone(),
    };
    let engine = QueryEngine::new(&reader);
    let error = engine
        .execute_with_context(
            &selector_plan(10 * SECOND),
            10 * SECOND,
            &context(QueryLimits::DEFAULT, cooperative_token),
        )
        .expect_err("cancellation after scan must discard the result");
    assert!(matches!(error, TsmError::Cancelled));
}

#[test]
fn every_limit_and_timeout_configuration_must_be_non_zero() {
    assert!(ParsingLimits::new(0, 1, 1, 1, 1).is_err());
    assert!(ParsingLimits::new(1, 0, 1, 1, 1).is_err());
    assert!(ParsingLimits::new(1, 1, 0, 1, 1).is_err());
    assert!(ParsingLimits::new(1, 1, 1, 0, 1).is_err());
    assert!(ParsingLimits::new(1, 1, 1, 1, 0).is_err());
    assert!(ScanLimits::new(0, 1, 1).is_err());
    assert!(ScanLimits::new(1, 0, 1).is_err());
    assert!(ScanLimits::new(1, 1, 0).is_err());
    assert!(ExecutionLimits::new(0, 1, 1).is_err());
    assert!(ExecutionLimits::new(1, 0, 1).is_err());
    assert!(ExecutionLimits::new(1, 1, 0).is_err());
    assert!(QueryExecutionContext::with_timeout(
        QueryLimits::DEFAULT,
        Duration::ZERO,
        CancellationToken::new(),
    )
    .is_err());
}
