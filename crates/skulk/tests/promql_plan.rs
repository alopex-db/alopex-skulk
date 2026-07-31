#![cfg(feature = "promql")]

use alopex_skulk::query::plan::{
    plan_promql, AggregateKind, AggregationStage, PlanContext, PlanNode, PlanPredicate,
    RangeFunctionKind, SeriesGroupKind, SeriesWindowKind,
};
use alopex_skulk::query::promql::parse;
use alopex_skulk::query::MatchOp;
use alopex_skulk::TsmError;

const SECOND: i64 = 1_000_000_000;

fn plan(sql: &str, evaluation_time: i64) -> alopex_skulk::query::plan::LogicalPlan {
    plan_promql(
        &parse(sql).expect("parse"),
        PlanContext::instant(evaluation_time),
    )
    .expect("plan")
}

#[test]
fn builds_scan_filter_group_function_and_aggregate_pipeline() {
    let evaluation_time = 10_000 * SECOND;
    let logical = plan(
        "sum by (job) (rate(http_requests_total{instance=\"a\",env!=\"dev\"}[5m] offset 1h))",
        evaluation_time,
    );

    let PlanNode::Aggregate(aggregate) = &logical.root else {
        panic!("expected aggregate");
    };
    assert_eq!(aggregate.calls[0].kind, AggregateKind::Sum);
    assert_eq!(aggregate.stage, AggregationStage::Single);
    assert_eq!(
        aggregate.grouping.as_ref().expect("grouping").labels,
        ["job"]
    );

    let PlanNode::RangeFunction(function) = aggregate.input.as_ref() else {
        panic!("expected range function");
    };
    assert_eq!(function.function, RangeFunctionKind::Rate);
    let PlanNode::SeriesGroup(group) = function.input.as_ref() else {
        panic!("expected series group");
    };
    let SeriesGroupKind::Window(window) = group.kind else {
        panic!("expected range window");
    };
    assert_eq!(window.kind, SeriesWindowKind::Range);
    assert_eq!(window.duration_ns, 300 * SECOND);
    assert_eq!(window.evaluation_time, evaluation_time - 3_600 * SECOND);

    let PlanNode::Filter(filter) = group.input.as_ref() else {
        panic!("expected residual filter");
    };
    assert!(matches!(
        filter.predicates.as_slice(),
        [PlanPredicate::Label(matcher)]
            if matcher.name == "env" && matcher.op == MatchOp::NotEqual
    ));
    let PlanNode::Scan(scan) = filter.input.as_ref() else {
        panic!("expected scan");
    };
    assert_eq!(
        scan.measurement.exact.as_deref(),
        Some("http_requests_total")
    );
    assert!(matches!(
        scan.tag_equalities.as_slice(),
        [matcher]
            if matcher.name == "instance"
                && matcher.op == MatchOp::Equal
                && matcher.value == "a"
    ));
    assert_eq!(
        scan.field_projection
            .as_ref()
            .expect("projection")
            .iter()
            .collect::<Vec<_>>(),
        [&"value".to_string()]
    );
    assert_eq!(
        scan.time_range.start.expect("start").value,
        evaluation_time - 3_900 * SECOND,
    );
    assert_eq!(
        scan.time_range.end.expect("end").value,
        evaluation_time - 3_600 * SECOND,
    );
}

#[test]
fn extracts_reserved_measurement_and_field_matchers() {
    let logical = plan(
        "{__name__=~\"cpu_.+\",__field__=\"usage\",host=\"edge\",region=~\"us-.*\"}",
        1_000 * SECOND,
    );
    let PlanNode::SeriesGroup(group) = &logical.root else {
        panic!("expected group");
    };
    let PlanNode::Filter(filter) = group.input.as_ref() else {
        panic!("expected filter");
    };
    assert!(matches!(
        filter.predicates.as_slice(),
        [PlanPredicate::Label(matcher)]
            if matcher.name == "region" && matcher.op == MatchOp::Regex
    ));
    let PlanNode::Scan(scan) = filter.input.as_ref() else {
        panic!("expected scan");
    };
    assert_eq!(scan.measurement.exact, None);
    assert_eq!(scan.measurement.matchers.len(), 1);
    assert_eq!(scan.measurement.matchers[0].name, "__name__");
    assert_eq!(
        scan.field_projection
            .as_ref()
            .expect("projection")
            .iter()
            .collect::<Vec<_>>(),
        [&"usage".to_string()]
    );
    assert_eq!(scan.tag_equalities[0].name, "host");
}

#[test]
fn shifts_each_selector_range_for_offset_inside_binary_plans() {
    let evaluation_time = 2_000 * SECOND;
    let logical = plan("2 + cpu offset 5m", evaluation_time);
    let PlanNode::Binary(binary) = &logical.root else {
        panic!("expected binary");
    };
    assert!(matches!(binary.left.as_ref(), PlanNode::Scalar(value) if *value == 2.0));
    let PlanNode::SeriesGroup(group) = binary.right.as_ref() else {
        panic!("expected right vector");
    };
    let SeriesGroupKind::Window(window) = group.kind else {
        panic!("expected instant window");
    };
    assert_eq!(window.kind, SeriesWindowKind::Instant);
    assert_eq!(window.evaluation_time, evaluation_time - 300 * SECOND);
    assert_eq!(window.duration_ns, 300 * SECOND);
}

#[test]
fn preserves_scalar_vector_operand_order_and_scalar_arithmetic() {
    let vector_left = plan("cpu + 2", 2_000 * SECOND);
    assert!(matches!(
        vector_left.root,
        PlanNode::Binary(ref binary)
            if matches!(binary.left.as_ref(), PlanNode::SeriesGroup(_))
                && matches!(binary.right.as_ref(), PlanNode::Scalar(value) if *value == 2.0)
    ));

    let scalar_only = plan("2 * 3", 2_000 * SECOND);
    assert!(matches!(
        scalar_only.root,
        PlanNode::Binary(ref binary)
            if matches!(binary.left.as_ref(), PlanNode::Scalar(value) if *value == 2.0)
                && matches!(binary.right.as_ref(), PlanNode::Scalar(value) if *value == 3.0)
    ));
}

#[test]
fn maps_every_range_function_and_histogram_quantile() {
    for (name, expected) in [
        ("rate", RangeFunctionKind::Rate),
        ("irate", RangeFunctionKind::IRate),
        ("increase", RangeFunctionKind::Increase),
        ("avg_over_time", RangeFunctionKind::AvgOverTime),
        ("min_over_time", RangeFunctionKind::MinOverTime),
        ("max_over_time", RangeFunctionKind::MaxOverTime),
        ("sum_over_time", RangeFunctionKind::SumOverTime),
        ("count_over_time", RangeFunctionKind::CountOverTime),
    ] {
        let logical = plan(&format!("{name}(metric[5m])"), 1_000 * SECOND);
        assert!(
            matches!(
                logical.root,
                PlanNode::RangeFunction(ref function) if function.function == expected
            ),
            "{name}"
        );
    }

    let logical = plan(
        "histogram_quantile(0.95, http_request_duration_seconds_bucket)",
        1_000 * SECOND,
    );
    assert!(matches!(
        logical.root,
        PlanNode::Aggregate(ref aggregate)
            if aggregate.calls[0].kind == AggregateKind::HistogramQuantile
                && matches!(
                    aggregate.calls[0].parameter.as_deref(),
                    Some(PlanNode::Scalar(value)) if (*value - 0.95).abs() < f64::EPSILON
                )
    ));
}

#[test]
fn rejects_unbounded_field_selection_and_time_overflow_with_positions() {
    for query in [
        "cpu{__field__!=\"value\"}",
        "cpu{__field__=\"a\",__field__=\"b\"}",
    ] {
        assert!(matches!(
            plan_promql(
                &parse(query).expect("parse"),
                PlanContext::instant(1_000 * SECOND)
            ),
            Err(TsmError::Plan {
                line,
                column,
                ..
            }) if line > 0 && column > 0
        ));
    }

    assert!(matches!(
        plan_promql(
            &parse("cpu offset 1ms").expect("parse"),
            PlanContext::instant(i64::MIN)
        ),
        Err(TsmError::Plan { .. })
    ));
}
