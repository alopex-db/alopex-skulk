#![cfg(feature = "sql-ts")]

use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::plan::{
    plan_sql, AggregateKind, PlanContext, PlanExpressionKind, PlanNode, PlanPredicate,
    SeriesGroupKind,
};
use alopex_skulk::query::sqlts::parse;
use alopex_skulk::query::sqlts::typecheck::typecheck;
use alopex_skulk::query::MatchOp;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::TsmError;

const SECOND: i64 = 1_000_000_000;

fn with_typed_query<T>(
    sql: &str,
    test: impl FnOnce(alopex_skulk::query::plan::LogicalPlan) -> T,
) -> T {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .ingest(WideRow::new(
            SeriesKey::new(
                measurement(sql),
                Tags::from([
                    ("host".into(), "edge".into()),
                    ("region".into(), "prod".into()),
                ]),
            ),
            1,
            Fields::from([
                ("usage_user".into(), FieldValue::Float(1.0)),
                ("requests_total".into(), FieldValue::Unsigned(2)),
                ("price".into(), FieldValue::Float(3.0)),
                ("counter_total".into(), FieldValue::Unsigned(4)),
                ("status".into(), FieldValue::String("ready".into())),
            ]),
        ))
        .expect("ingest");
    let query = parse(sql).expect("parse");
    let schema = store
        .measurement_schema(&query.measurement)
        .expect("schema");
    let typed = typecheck(query, &schema).expect("typecheck");
    test(plan_sql(&typed, PlanContext::instant(100_000 * SECOND)).expect("logical SQL plan"))
}

fn measurement(sql: &str) -> &str {
    if sql.contains("cpu_metrics") {
        "cpu_metrics"
    } else if sql.contains("http_metrics") {
        "http_metrics"
    } else if sql.contains("stock_prices") {
        "stock_prices"
    } else {
        "metrics"
    }
}

#[test]
fn maps_the_time_bucket_spec_query_to_shared_operators() {
    with_typed_query(
        "SELECT TIME_BUCKET('1 hour', time) AS bucket, host, \
         AVG(usage_user) AS avg_usage, MAX(usage_user) AS max_usage \
         FROM cpu_metrics \
         WHERE time > NOW() - INTERVAL '24 hours' \
         GROUP BY bucket, host ORDER BY bucket DESC",
        |logical| {
            let PlanNode::Project(project) = &logical.root else {
                panic!("expected project");
            };
            assert_eq!(project.expressions.len(), 4);
            assert_eq!(project.expressions[0].alias.as_deref(), Some("bucket"));
            assert_eq!(project.expressions[2].alias.as_deref(), Some("avg_usage"));
            let PlanNode::Sort(sort) = project.input.as_ref() else {
                panic!("expected sort");
            };
            let PlanNode::Aggregate(aggregate) = sort.input.as_ref() else {
                panic!("expected aggregate");
            };
            assert_eq!(
                aggregate
                    .calls
                    .iter()
                    .map(|call| call.kind)
                    .collect::<Vec<_>>(),
                [AggregateKind::Avg, AggregateKind::Max]
            );
            let PlanNode::SeriesGroup(group) = aggregate.input.as_ref() else {
                panic!("expected SQL grouping");
            };
            let SeriesGroupKind::Keys(keys) = &group.kind else {
                panic!("expected SQL group keys");
            };
            assert_eq!(keys.len(), 2);
            assert!(matches!(
                keys[0].kind,
                PlanExpressionKind::TimeBucket {
                    interval_ns,
                    ref column,
                } if interval_ns == 3_600 * SECOND && column == "time"
            ));
            let PlanNode::Scan(scan) = group.input.as_ref() else {
                panic!("expected scan");
            };
            let lower = scan.time_range.start.expect("lower bound");
            assert_eq!(lower.value, 100_000 * SECOND - 86_400 * SECOND);
            assert!(!lower.inclusive);
            assert_eq!(scan.time_range.end, None);
            assert!(scan
                .field_projection
                .as_ref()
                .expect("projection")
                .contains("usage_user"));
        },
    );
}

#[test]
fn separates_time_and_tag_pushdown_from_residual_predicates() {
    with_typed_query(
        "SELECT usage_user FROM metrics \
         WHERE time >= NOW() - INTERVAL '1 hour' \
           AND host = 'edge' AND region != 'dev' AND usage_user > 0.5",
        |logical| {
            let PlanNode::Project(project) = &logical.root else {
                panic!("expected project");
            };
            let PlanNode::Filter(filter) = project.input.as_ref() else {
                panic!("expected filter");
            };
            assert_eq!(filter.predicates.len(), 2);
            assert!(filter
                .predicates
                .iter()
                .all(|predicate| matches!(predicate, PlanPredicate::Expression(_))));
            let PlanNode::Scan(scan) = filter.input.as_ref() else {
                panic!("expected scan");
            };
            assert!(matches!(
                scan.tag_equalities.as_slice(),
                [matcher]
                    if matcher.name == "host"
                        && matcher.op == MatchOp::Equal
                        && matcher.value == "edge"
            ));
            let lower = scan.time_range.start.expect("lower");
            assert!(lower.inclusive);
            assert_eq!(lower.value, 100_000 * SECOND - 3_600 * SECOND);
        },
    );
}

#[test]
fn maps_time_series_functions_order_and_limit_without_sql_only_plan_nodes() {
    with_typed_query(
        "SELECT RATE(requests_total) AS rate, DELTA(counter_total) AS delta, \
         DERIVATIVE(price) AS slope, FIRST(status, time) AS first_status, \
         LAST(price, time) AS last_price, COUNT(*) AS samples \
         FROM http_metrics ORDER BY rate DESC LIMIT 5",
        |logical| {
            let PlanNode::Project(project) = &logical.root else {
                panic!("expected project");
            };
            let PlanNode::Limit(limit) = project.input.as_ref() else {
                panic!("expected limit");
            };
            assert_eq!(limit.rows, 5);
            let PlanNode::Sort(sort) = limit.input.as_ref() else {
                panic!("expected sort");
            };
            let PlanNode::Aggregate(aggregate) = sort.input.as_ref() else {
                panic!("expected aggregate");
            };
            assert_eq!(
                aggregate
                    .calls
                    .iter()
                    .map(|call| call.kind)
                    .collect::<Vec<_>>(),
                [
                    AggregateKind::Rate,
                    AggregateKind::Delta,
                    AggregateKind::Derivative,
                    AggregateKind::First,
                    AggregateKind::Last,
                    AggregateKind::Count,
                ]
            );
        },
    );
}

#[test]
fn wildcard_requests_all_fields_and_conflicting_time_bounds_fail() {
    with_typed_query("SELECT * FROM metrics", |logical| {
        let PlanNode::Project(project) = &logical.root else {
            panic!("expected project");
        };
        let PlanNode::Scan(scan) = project.input.as_ref() else {
            panic!("expected scan");
        };
        assert_eq!(scan.field_projection, None);
    });

    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .ingest(WideRow::new(
            SeriesKey::new("metrics", Tags::new()),
            1,
            Fields::from([("usage_user".into(), FieldValue::Float(1.0))]),
        ))
        .expect("ingest");
    let query = parse(
        "SELECT usage_user FROM metrics \
         WHERE time > NOW() AND time < NOW() - INTERVAL '1 hour'",
    )
    .expect("parse");
    let schema = store.measurement_schema("metrics").expect("schema");
    let typed = typecheck(query, &schema).expect("typecheck");
    assert!(matches!(
        plan_sql(&typed, PlanContext::instant(100_000 * SECOND)),
        Err(TsmError::Plan { .. })
    ));
}

#[test]
fn wildcard_does_not_shift_alias_projection_indexes() {
    with_typed_query(
        "SELECT *, usage_user AS utilization FROM metrics ORDER BY utilization DESC",
        |logical| {
            let PlanNode::Project(project) = &logical.root else {
                panic!("expected project");
            };
            assert!(project.wildcard);
            assert_eq!(project.expressions.len(), 1);
            assert_eq!(project.expressions[0].alias.as_deref(), Some("utilization"));
            let PlanNode::Sort(sort) = project.input.as_ref() else {
                panic!("expected sort");
            };
            assert!(matches!(
                sort.keys.as_slice(),
                [key]
                    if key.descending
                        && matches!(
                            &key.expression.kind,
                            PlanExpressionKind::Column { name } if name == "usage_user"
                        )
            ));
        },
    );
}

#[test]
fn order_by_unprojected_column_runs_before_projection() {
    with_typed_query("SELECT usage_user FROM metrics ORDER BY host", |logical| {
        let PlanNode::Project(project) = &logical.root else {
            panic!("expected final project");
        };
        let PlanNode::Sort(sort) = project.input.as_ref() else {
            panic!("expected sort before projection");
        };
        assert!(matches!(
            sort.keys.as_slice(),
            [key]
                if matches!(
                    &key.expression.kind,
                    PlanExpressionKind::Column { name } if name == "host"
                )
        ));
        assert!(matches!(sort.input.as_ref(), PlanNode::Scan(_)));
    });
}
