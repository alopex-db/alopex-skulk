#![cfg(feature = "sql-ts")]

use alopex_skulk::query::sqlts::{
    parse, AggregateArgument, AggregateFunction, PredicateClass, SqlExprKind, SqlFunction,
    SqlGroupBy, SqlOrderKey, SqlProjection,
};
use alopex_skulk::query::TSFunction;
use alopex_skulk::TsmError;
use proptest::prelude::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

#[test]
fn maps_the_time_bucket_spec_example_and_aliases() {
    let query = parse(
        "SELECT TIME_BUCKET('1 hour', time) AS bucket, host, AVG(usage_user) AS avg_usage \
         FROM cpu_metrics \
         WHERE time > NOW() - INTERVAL '24 hours' \
         GROUP BY bucket, host \
         ORDER BY bucket DESC \
         LIMIT 10",
    )
    .unwrap();

    assert_eq!(query.measurement, "cpu_metrics");
    assert_eq!(query.projections.len(), 3);
    assert_eq!(query.limit, Some(10));

    let SqlProjection::Expr {
        expr,
        alias: Some(alias),
        ..
    } = &query.projections[0]
    else {
        panic!("expected aliased TIME_BUCKET projection");
    };
    assert_eq!(alias, "bucket");
    assert!(matches!(
        &expr.kind,
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::TimeBucket {
                interval,
                column,
            },
            ..
        }) if *interval == Duration::from_secs(3_600) && column == "time"
    ));

    assert!(matches!(
        &query.group_by[0],
        SqlGroupBy::ProjectionAlias {
            alias,
            projection_index: 0,
            ..
        } if alias == "bucket"
    ));
    assert!(matches!(
        &query.order_by[0].key,
        SqlOrderKey::ProjectionAlias {
            alias,
            projection_index: 0,
            ..
        } if alias == "bucket"
    ));
    assert!(!query.order_by[0].ascending);
    assert_eq!(query.predicates.len(), 1);
    assert_eq!(query.predicates[0].class, PredicateClass::Time);
    assert!(query.predicates[0].pushdown_eligible);
}

#[test]
fn resolves_all_required_time_series_and_aggregate_functions() {
    let query = parse(
        "SELECT \
         RATE(counter), DELTA(gauge), DERIVATIVE(position), \
         FIRST(price, time), LAST(price, time), \
         HISTOGRAM_QUANTILE(0.95, bucket), \
         AVG(value), SUM(value), MIN(value), MAX(value), COUNT(*) \
         FROM metrics",
    )
    .unwrap();

    let functions = query
        .projections
        .iter()
        .map(|projection| match projection {
            SqlProjection::Expr { expr, .. } => &expr.kind,
            SqlProjection::Wildcard { .. } => panic!("unexpected wildcard"),
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        functions[0],
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::Rate { column },
            ..
        })
            if column == "counter"
    ));
    assert!(matches!(
        functions[1],
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::Delta { column },
            ..
        })
            if column == "gauge"
    ));
    assert!(matches!(
        functions[2],
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::Derivative { column },
            ..
        })
            if column == "position"
    ));
    assert!(matches!(
        functions[3],
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::First {
                value_column,
                time_column,
            },
            ..
        }) if value_column == "price" && time_column == "time"
    ));
    assert!(matches!(
        functions[4],
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::Last {
                value_column,
                time_column,
            },
            ..
        }) if value_column == "price" && time_column == "time"
    ));
    assert!(matches!(
        functions[5],
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::HistogramQuantile {
                quantile,
                column,
            },
            ..
        }) if (*quantile - 0.95).abs() < f64::EPSILON && column == "bucket"
    ));

    for (kind, expected) in [
        (functions[6], AggregateFunction::Avg),
        (functions[7], AggregateFunction::Sum),
        (functions[8], AggregateFunction::Min),
        (functions[9], AggregateFunction::Max),
    ] {
        assert!(matches!(
            kind,
            SqlExprKind::Function(SqlFunction::Aggregate {
                function,
                argument: AggregateArgument::Expr(_),
                ..
            }) if *function == expected
        ));
    }
    assert!(matches!(
        functions[10],
        SqlExprKind::Function(SqlFunction::Aggregate {
            function: AggregateFunction::Count,
            argument: AggregateArgument::Wildcard,
            ..
        })
    ));
}

#[test]
fn classifies_time_tag_candidates_and_field_predicates() {
    let query = parse(
        "SELECT value FROM cpu \
         WHERE time >= NOW() - INTERVAL '1 hour' \
           AND host = 'api' \
           AND environment != 'dev' \
           AND region IN ('us', 'eu') \
           AND usage > 0.5",
    )
    .unwrap();

    assert_eq!(
        query
            .predicates
            .iter()
            .map(|predicate| predicate.class)
            .collect::<Vec<_>>(),
        vec![
            PredicateClass::Time,
            PredicateClass::Tag,
            PredicateClass::Tag,
            PredicateClass::Tag,
            PredicateClass::Field,
        ]
    );
    assert!(query.predicates[0].pushdown_eligible);
    assert!(query.predicates[1].pushdown_eligible);
    assert!(query.predicates[2].pushdown_eligible);
    assert!(query.predicates[3].pushdown_eligible);
    assert!(!query.predicates[4].pushdown_eligible);
}

#[test]
fn maps_the_rate_and_first_last_spec_examples() {
    let rate = parse(
        "SELECT TIME_BUCKET('5 minutes', time) AS bucket, \
         RATE(requests_total) AS requests_per_sec \
         FROM http_metrics",
    )
    .unwrap();
    assert_eq!(rate.measurement, "http_metrics");
    assert_eq!(rate.projections.len(), 2);

    let ohlc = parse(
        "SELECT TIME_BUCKET('1 day', time) AS day, \
         FIRST(price, time) AS open, LAST(price, time) AS close \
         FROM stock_prices GROUP BY day",
    )
    .unwrap();
    assert_eq!(ohlc.measurement, "stock_prices");
    assert!(matches!(
        ohlc.group_by[0],
        SqlGroupBy::ProjectionAlias {
            projection_index: 0,
            ..
        }
    ));
}

#[test]
fn supports_interval_arguments_ordinals_wildcards_and_measurement_aliases() {
    let query = parse(
        "SELECT TIME_BUCKET(INTERVAL '15 minutes', time) AS bucket, * \
         FROM metrics AS m GROUP BY 1 ORDER BY 1",
    )
    .unwrap();

    assert_eq!(query.measurement_alias.as_deref(), Some("m"));
    assert!(matches!(
        query.projections[1],
        SqlProjection::Wildcard { .. }
    ));
    assert!(matches!(
        query.group_by[0],
        SqlGroupBy::ProjectionOrdinal {
            ordinal: 1,
            projection_index: 0,
            ..
        }
    ));
    assert!(matches!(
        query.order_by[0].key,
        SqlOrderKey::ProjectionOrdinal {
            ordinal: 1,
            projection_index: 0,
            ..
        }
    ));
}

#[test]
fn rejects_out_of_scope_statements_and_select_constructs_explicitly() {
    for sql in [
        "SELECT a.value FROM a JOIN b ON a.id = b.id",
        "SELECT value FROM metrics HAVING value > 0",
        "SELECT value FROM (SELECT value FROM metrics) AS nested",
        "INSERT INTO metrics (value) VALUES (1)",
        "UPDATE metrics SET value = 1",
        "DELETE FROM metrics",
        "CREATE TABLE metrics (value FLOAT)",
        "SELECT value FROM metrics LIMIT 10 OFFSET 2",
        "SELECT value FROM metrics; SELECT value FROM metrics",
    ] {
        assert!(
            matches!(parse(sql), Err(TsmError::Unsupported { .. })),
            "{sql}"
        );
    }
}

#[test]
fn returns_positioned_result_errors_for_invalid_sql_and_signatures() {
    let error = parse("SELECT FROM metrics").unwrap_err();
    assert!(matches!(
        error,
        TsmError::Parse {
            language,
            line,
            column,
            ..
        } if language == "SQL-TS" && line > 0 && column > 0
    ));

    assert!(matches!(
        parse("SELECT RATE(value, time) FROM metrics"),
        Err(TsmError::Type { .. })
    ));
    assert!(matches!(
        parse("SELECT unknown_function(value) FROM metrics"),
        Err(TsmError::Unsupported { .. })
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn arbitrary_utf8_sql_never_panics(
        characters in proptest::collection::vec(any::<char>(), 0..=256)
    ) {
        let input = characters.into_iter().collect::<String>();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _ = parse(&input);
        }));
        prop_assert!(outcome.is_ok());
    }
}
