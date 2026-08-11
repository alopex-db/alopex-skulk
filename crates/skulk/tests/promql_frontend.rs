#![cfg(feature = "promql")]

use alopex_skulk::query::promql::{
    parse, parser_contract_version, AggregationOp, BinaryOp, PromExprKind, PromFunction,
    PromValueType, MAX_PROMQL_INPUT_BYTES, MAX_PROMQL_REGEX_BYTES,
};
use alopex_skulk::TsmError;
use proptest::prelude::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn parses_selector_matchers_range_offset_and_anchored_regex() {
    let expression = parse(
        r#"rate(http_requests_total{job=~"api|web",env!="dev",zone="us",pod!~"canary.*"}[5m] offset 1m)"#,
    )
    .unwrap();

    assert_eq!(expression.value_type(), PromValueType::InstantVector);
    let PromExprKind::FunctionCall { function, args } = &expression.kind else {
        panic!("expected function call");
    };
    assert_eq!(*function, PromFunction::Rate);

    let PromExprKind::MatrixSelector {
        selector,
        range,
        offset,
    } = &args[0].kind
    else {
        panic!("expected matrix selector");
    };
    assert_eq!(range.milliseconds, 300_000);
    assert_eq!(offset.as_ref().unwrap().milliseconds, 60_000);

    let PromExprKind::VectorSelector {
        metric, matchers, ..
    } = &selector.kind
    else {
        panic!("expected vector selector");
    };
    assert_eq!(metric.as_deref(), Some("http_requests_total"));
    assert_eq!(matchers.len(), 4);
    assert!(matchers[0].matches_label_value("api"));
    assert!(matchers[0].matches_label_value("web"));
    assert!(!matchers[0].matches_label_value("xapi"));
    assert!(!matchers[0].matches_label_value("api\n"));
    assert!(matchers[1].matches_label_value("prod"));
    assert!(!matchers[1].matches_label_value("dev"));
    assert!(matchers[2].matches_label_value("us"));
    assert!(!matchers[2].matches_label_value("eu"));
    assert!(matchers[3].matches_label_value("stable"));
    assert!(!matchers[3].matches_label_value("canary-1"));
    assert_eq!(expression.span.start.offset, 0);
}

#[test]
fn decodes_every_supported_duration_unit() {
    for (duration, expected_milliseconds) in [
        ("1ms", 1),
        ("2s", 2_000),
        ("3m", 180_000),
        ("4h", 14_400_000),
        ("5d", 432_000_000),
        ("6w", 3_628_800_000),
        ("1y", 31_536_000_000),
        ("1h30m", 5_400_000),
    ] {
        let expression = parse(&format!("metric[{duration}]")).unwrap();
        let PromExprKind::MatrixSelector { range, .. } = expression.kind else {
            panic!("expected matrix selector");
        };
        assert_eq!(range.milliseconds, expected_milliseconds, "{duration}");
    }
}

#[test]
fn validates_supported_function_signatures_and_aggregation() {
    for query in [
        "rate(metric[5m])",
        "irate(metric[5m])",
        "increase(metric[5m])",
        "avg_over_time(metric[5m])",
        "min_over_time(metric[5m])",
        "max_over_time(metric[5m])",
        "sum_over_time(metric[5m])",
        "count_over_time(metric[5m])",
        "histogram_quantile(0.95, metric)",
    ] {
        assert_eq!(
            parse(query).unwrap().value_type(),
            PromValueType::InstantVector
        );
    }

    for (name, expected_op) in [
        ("sum", AggregationOp::Sum),
        ("avg", AggregationOp::Avg),
        ("max", AggregationOp::Max),
        ("min", AggregationOp::Min),
        ("count", AggregationOp::Count),
    ] {
        let expression = parse(&format!("{name} without (instance) (metric)")).unwrap();
        let PromExprKind::Aggregate {
            op,
            grouping,
            without,
            ..
        } = expression.kind
        else {
            panic!("expected aggregate");
        };
        assert_eq!(op, expected_op);
        assert_eq!(grouping.unwrap(), vec!["instance".to_string()]);
        assert!(without);
    }

    let expression = parse("sum by (job, instance) (metric)").unwrap();
    let PromExprKind::Aggregate {
        grouping, without, ..
    } = expression.kind
    else {
        panic!("expected aggregate");
    };
    assert_eq!(
        grouping.unwrap(),
        vec!["job".to_string(), "instance".to_string()]
    );
    assert!(!without);
}

#[test]
fn preserves_prometheus_arithmetic_precedence() {
    let expression = parse("1 + 2 * 3 ^ 2").unwrap();
    let PromExprKind::Binary { op, right, .. } = expression.kind else {
        panic!("expected top-level binary expression");
    };
    assert_eq!(op, BinaryOp::Add);

    let PromExprKind::Binary {
        op,
        left,
        right: power,
    } = right.kind
    else {
        panic!("expected multiplication on the right");
    };
    assert_eq!(op, BinaryOp::Mul);
    assert!(matches!(left.kind, PromExprKind::NumberLiteral { .. }));
    assert!(matches!(
        power.kind,
        PromExprKind::Binary {
            op: BinaryOp::Pow,
            ..
        }
    ));

    let unary = parse("-(1 + 2)").unwrap();
    assert_eq!(unary.value_type(), PromValueType::Scalar);
    assert!(matches!(unary.kind, PromExprKind::Unary { .. }));
    assert!(matches!(
        parse(r#""literal""#).unwrap().kind,
        PromExprKind::StringLiteral { .. }
    ));
}

#[test]
fn distinguishes_type_errors_from_unsupported_semantics() {
    assert!(matches!(parse("rate(metric)"), Err(TsmError::Type { .. })));
    assert!(matches!(
        parse("histogram_quantile(metric)"),
        Err(TsmError::Type { .. })
    ));
    assert!(matches!(
        parse(r#"label_replace(metric, "dst", "$1", "src", "(.*)")"#),
        Err(TsmError::Unsupported { .. })
    ));
    assert!(matches!(
        parse("left_metric + right_metric"),
        Err(TsmError::Unsupported { .. })
    ));
    assert!(matches!(
        parse("metric offset -1m"),
        Err(TsmError::Unsupported { .. })
    ));
}

#[test]
fn reports_positioned_parse_and_regex_errors_without_panicking() {
    let error = parse("metric{job=}").expect_err("invalid syntax must not be accepted");
    match error {
        TsmError::Parse {
            language,
            line,
            column,
            offset,
            ..
        } => {
            assert_eq!(language, "PromQL");
            assert_eq!(line, 1);
            assert!(column > 0);
            assert!(offset > 0);
        }
        other => panic!("expected positioned parse error, got {other:?}"),
    }

    assert!(matches!(
        parse(r#"metric{job=~"["}"#),
        Err(TsmError::Parse { .. })
    ));
}

#[test]
fn rejects_oversized_input_and_regex_before_unbounded_work() {
    let oversized = "x".repeat(MAX_PROMQL_INPUT_BYTES + 1);
    assert!(matches!(parse(&oversized), Err(TsmError::ResourceLimit(_))));

    let pattern = "a".repeat(MAX_PROMQL_REGEX_BYTES + 1);
    let query = format!(r#"metric{{job=~"{pattern}"}}"#);
    assert!(matches!(parse(&query), Err(TsmError::ResourceLimit(_))));
}

#[test]
fn rejects_excessive_nesting_and_interior_nul_without_panicking() {
    let deeply_nested = format!("{}1{}", "(".repeat(65), ")".repeat(65));
    assert!(matches!(
        parse(&deeply_nested),
        Err(TsmError::Parse { .. }) | Err(TsmError::ResourceLimit(_))
    ));
    assert!(matches!(
        parse("metric\0suffix"),
        Err(TsmError::InvalidInput(_))
    ));
}

#[test]
fn verifies_the_runtime_parser_contract_version() {
    assert_eq!(
        parser_contract_version().unwrap(),
        env!("SKULK_NIM_PARSER_CONTRACT_VERSION")
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn arbitrary_utf8_input_never_panics(
        characters in proptest::collection::vec(any::<char>(), 0..=256)
    ) {
        let input = characters.into_iter().collect::<String>();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _ = parse(&input);
        }));
        prop_assert!(outcome.is_ok());
    }
}
