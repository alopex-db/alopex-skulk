//! SQL-TS to language-independent logical plan conversion.

use super::{
    AggregateCall, AggregateInput, AggregateKind, AggregateNode, AggregationStage, FilterNode,
    LimitNode, LogicalPlan, MeasurementSelection, PatternMatchKind, PlanContext, PlanExpression,
    PlanExpressionKind, PlanNode, PlanPredicate, PlanTimeRange, PlanValueType, ProjectNode,
    ProjectionExpression, ScalarBinaryKind, ScalarUnaryKind, ScanNode, ScanResolution,
    SeriesGroupKind, SeriesGroupNode, SortKey, SortNode, TimeBound,
};
use crate::query::sqlts::typecheck::{TypedProjection, TypedSqlTsQuery};
use crate::query::sqlts::{
    parse_duration, AggregateArgument, AggregateFunction, PatternKind, PredicateClass, SqlBinaryOp,
    SqlExpr, SqlExprKind, SqlFunction, SqlGroupBy, SqlLiteral, SqlOrderKey, SqlProjection, SqlSpan,
    SqlUnaryOp,
};
use crate::query::{LabelMatcher, MatchOp, TSFunction};
use crate::{Result, TsmError};
use std::collections::BTreeSet;
use std::time::Duration;

/// Converts one typechecked SQL-TS query into the shared logical operator tree.
pub fn plan_sql(query: &TypedSqlTsQuery, context: PlanContext) -> Result<LogicalPlan> {
    let mut time_range = PlanTimeRange::all();
    let mut tag_equalities = Vec::new();
    let mut residual = Vec::new();

    for predicate in &query.query.predicates {
        match predicate.class {
            PredicateClass::Time => {
                if let Some(range) = time_predicate_range(&predicate.expr, context)? {
                    time_range = intersect_ranges(time_range, range, predicate.expr.span)?;
                } else {
                    residual.push(PlanPredicate::Expression(map_scalar_expression(
                        &predicate.expr,
                        context,
                        None,
                    )?));
                }
            }
            PredicateClass::Tag => {
                if let Some(matcher) = tag_equality(&predicate.expr) {
                    tag_equalities.push(matcher);
                } else {
                    residual.push(PlanPredicate::Expression(map_scalar_expression(
                        &predicate.expr,
                        context,
                        None,
                    )?));
                }
            }
            PredicateClass::Field => residual.push(PlanPredicate::Expression(
                map_scalar_expression(&predicate.expr, context, None)?,
            )),
        }
    }

    let field_projection = collect_field_projection(&query.query);
    let scan = PlanNode::Scan(ScanNode {
        measurement: MeasurementSelection {
            exact: Some(query.query.measurement.clone()),
            matchers: Vec::new(),
        },
        time_range,
        tag_equalities,
        field_projection,
        resolution: ScanResolution::raw(),
    });
    let source = if residual.is_empty() {
        scan
    } else {
        PlanNode::Filter(FilterNode {
            input: Box::new(scan),
            predicates: residual,
        })
    };

    let mut calls = Vec::new();
    let mut projections = Vec::new();
    let mut projection_references = Vec::new();
    let mut wildcard = false;
    for (projection, typed) in query.query.projections.iter().zip(&query.projections) {
        match (projection, typed) {
            (SqlProjection::Wildcard { .. }, TypedProjection::Wildcard { .. }) => {
                wildcard = true;
                projection_references.push(None);
            }
            (SqlProjection::Expr { expr, alias, .. }, TypedProjection::Expr { .. }) => {
                let expression = map_scalar_expression(expr, context, Some(&mut calls))?;
                projection_references.push(Some(expression.clone()));
                projections.push(ProjectionExpression {
                    expression,
                    alias: alias.clone(),
                });
            }
            _ => {
                return Err(TsmError::FfiContract(
                    "typed SQL projection metadata is not aligned with its AST".to_string(),
                ));
            }
        }
    }

    let group_keys = query
        .query
        .group_by
        .iter()
        .map(|group| group_expression(group, &projection_references, context, &mut calls))
        .collect::<Result<Vec<_>>>()?;
    if group_keys.iter().any(contains_aggregate_result) {
        return Err(plan_error(
            query.query.span,
            "GROUP BY cannot contain an aggregate expression".to_string(),
        ));
    }

    let sort_keys = query
        .query
        .order_by
        .iter()
        .map(|order| {
            Ok(SortKey {
                expression: order_expression(
                    &order.key,
                    &projection_references,
                    context,
                    &mut calls,
                )?,
                descending: !order.ascending,
                nulls_first: order.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let aggregates = !calls.is_empty() || !group_keys.is_empty();
    if wildcard && aggregates {
        return Err(plan_error(
            query.query.span,
            "wildcard projection cannot be combined with grouping or aggregates".to_string(),
        ));
    }
    if aggregates {
        for projection in &projections {
            if !contains_aggregate_result(&projection.expression)
                && contains_column(&projection.expression)
                && !group_keys.contains(&projection.expression)
            {
                return Err(plan_error(
                    query.query.span,
                    "non-aggregate projection must appear in GROUP BY".to_string(),
                ));
            }
        }
    }

    let mut root = source;
    if aggregates {
        root = PlanNode::SeriesGroup(SeriesGroupNode {
            input: Box::new(root),
            kind: SeriesGroupKind::Keys(group_keys),
        });
        root = PlanNode::Aggregate(AggregateNode {
            input: Box::new(root),
            calls,
            grouping: None,
            stage: AggregationStage::Single,
        });
    }
    if !sort_keys.is_empty() {
        root = PlanNode::Sort(SortNode {
            input: Box::new(root),
            keys: sort_keys,
        });
    }
    if let Some(rows) = query.query.limit {
        root = PlanNode::Limit(LimitNode {
            input: Box::new(root),
            rows,
        });
    }
    root = PlanNode::Project(ProjectNode {
        input: Box::new(root),
        expressions: projections,
        wildcard,
    });

    Ok(LogicalPlan {
        root,
        output_type: PlanValueType::Table,
    })
}

fn group_expression(
    group: &SqlGroupBy,
    projections: &[Option<PlanExpression>],
    context: PlanContext,
    calls: &mut Vec<AggregateCall>,
) -> Result<PlanExpression> {
    match group {
        SqlGroupBy::Expression(expression) => {
            map_scalar_expression(expression, context, Some(calls))
        }
        SqlGroupBy::ProjectionAlias {
            projection_index,
            span,
            ..
        }
        | SqlGroupBy::ProjectionOrdinal {
            projection_index,
            span,
            ..
        } => projection_expression(*projection_index, projections, *span),
    }
}

fn order_expression(
    key: &SqlOrderKey,
    projections: &[Option<PlanExpression>],
    context: PlanContext,
    calls: &mut Vec<AggregateCall>,
) -> Result<PlanExpression> {
    match key {
        SqlOrderKey::Expression(expression) => {
            map_scalar_expression(expression, context, Some(calls))
        }
        SqlOrderKey::ProjectionAlias {
            projection_index,
            span,
            ..
        }
        | SqlOrderKey::ProjectionOrdinal {
            projection_index,
            span,
            ..
        } => projection_expression(*projection_index, projections, *span),
    }
}

fn projection_expression(
    index: usize,
    projections: &[Option<PlanExpression>],
    span: SqlSpan,
) -> Result<PlanExpression> {
    projections
        .get(index)
        .and_then(Clone::clone)
        .ok_or_else(|| {
            plan_error(
                span,
                format!(
                    "projection index {index} is outside the planned SELECT expressions or is a wildcard"
                ),
            )
        })
}

fn map_scalar_expression(
    expression: &SqlExpr,
    context: PlanContext,
    calls: Option<&mut Vec<AggregateCall>>,
) -> Result<PlanExpression> {
    let kind = match &expression.kind {
        SqlExprKind::Literal(literal) => match literal {
            SqlLiteral::Number(value) => PlanExpressionKind::Number(value.clone()),
            SqlLiteral::String(value) => PlanExpressionKind::String(value.clone()),
            SqlLiteral::Boolean(value) => PlanExpressionKind::Boolean(*value),
            SqlLiteral::Null => PlanExpressionKind::Null,
            SqlLiteral::Interval(raw) => {
                PlanExpressionKind::Interval(duration_literal_ns(raw, expression.span)?)
            }
        },
        SqlExprKind::Column { name, .. } => PlanExpressionKind::Column { name: name.clone() },
        SqlExprKind::Binary { left, op, right } => {
            let mut calls = calls;
            PlanExpressionKind::Binary {
                left: Box::new(map_scalar_expression(left, context, calls.as_deref_mut())?),
                op: scalar_binary(*op),
                right: Box::new(map_scalar_expression(right, context, calls)?),
            }
        }
        SqlExprKind::Unary { op, expr } => PlanExpressionKind::Unary {
            op: scalar_unary(*op),
            expression: Box::new(map_scalar_expression(expr, context, calls)?),
        },
        SqlExprKind::Function(SqlFunction::Now) => {
            PlanExpressionKind::Timestamp(context.evaluation_time())
        }
        SqlExprKind::Function(SqlFunction::TimeSeries { function, .. }) => match function {
            TSFunction::TimeBucket { interval, column } => PlanExpressionKind::TimeBucket {
                interval_ns: duration_ns(*interval, expression.span)?,
                column: column.clone(),
            },
            TSFunction::Rate { column } => aggregate_result(
                calls,
                AggregateKind::Rate,
                AggregateInput::Expression(column_expression(column)),
                None,
                Vec::new(),
                false,
                expression.span,
            )?,
            TSFunction::Delta { column } => aggregate_result(
                calls,
                AggregateKind::Delta,
                AggregateInput::Expression(column_expression(column)),
                None,
                Vec::new(),
                false,
                expression.span,
            )?,
            TSFunction::Derivative { column } => aggregate_result(
                calls,
                AggregateKind::Derivative,
                AggregateInput::Expression(column_expression(column)),
                None,
                Vec::new(),
                false,
                expression.span,
            )?,
            TSFunction::First {
                value_column,
                time_column,
            } => aggregate_result(
                calls,
                AggregateKind::First,
                AggregateInput::Expression(column_expression(value_column)),
                None,
                vec![column_expression(time_column)],
                false,
                expression.span,
            )?,
            TSFunction::Last {
                value_column,
                time_column,
            } => aggregate_result(
                calls,
                AggregateKind::Last,
                AggregateInput::Expression(column_expression(value_column)),
                None,
                vec![column_expression(time_column)],
                false,
                expression.span,
            )?,
            TSFunction::HistogramQuantile { quantile, column } => aggregate_result(
                calls,
                AggregateKind::HistogramQuantile,
                AggregateInput::Expression(column_expression(column)),
                Some(Box::new(PlanNode::Scalar(*quantile))),
                Vec::new(),
                false,
                expression.span,
            )?,
        },
        SqlExprKind::Function(SqlFunction::Aggregate {
            function,
            argument,
            distinct,
        }) => {
            let argument = match argument {
                AggregateArgument::Wildcard => AggregateInput::Wildcard,
                AggregateArgument::Expr(argument) => {
                    AggregateInput::Expression(map_scalar_expression(argument, context, None)?)
                }
            };
            aggregate_result(
                calls,
                standard_aggregate(*function),
                argument,
                None,
                Vec::new(),
                *distinct,
                expression.span,
            )?
        }
        SqlExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let mut calls = calls;
            PlanExpressionKind::Between {
                expression: Box::new(map_scalar_expression(expr, context, calls.as_deref_mut())?),
                low: Box::new(map_scalar_expression(low, context, calls.as_deref_mut())?),
                high: Box::new(map_scalar_expression(high, context, calls)?),
                negated: *negated,
            }
        }
        SqlExprKind::Pattern {
            expr,
            pattern,
            escape,
            negated,
            kind,
        } => {
            let mut calls = calls;
            PlanExpressionKind::Pattern {
                expression: Box::new(map_scalar_expression(expr, context, calls.as_deref_mut())?),
                pattern: Box::new(map_scalar_expression(
                    pattern,
                    context,
                    calls.as_deref_mut(),
                )?),
                escape: escape
                    .as_deref()
                    .map(|escape| map_scalar_expression(escape, context, calls).map(Box::new))
                    .transpose()?,
                negated: *negated,
                kind: pattern_kind(*kind),
            }
        }
        SqlExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let mut calls = calls;
            PlanExpressionKind::InList {
                expression: Box::new(map_scalar_expression(expr, context, calls.as_deref_mut())?),
                list: list
                    .iter()
                    .map(|item| map_scalar_expression(item, context, calls.as_deref_mut()))
                    .collect::<Result<Vec<_>>>()?,
                negated: *negated,
            }
        }
        SqlExprKind::IsNull { expr, negated } => PlanExpressionKind::IsNull {
            expression: Box::new(map_scalar_expression(expr, context, calls)?),
            negated: *negated,
        },
    };
    Ok(PlanExpression { kind })
}

#[allow(clippy::too_many_arguments)]
fn aggregate_result(
    calls: Option<&mut Vec<AggregateCall>>,
    kind: AggregateKind,
    argument: AggregateInput,
    parameter: Option<Box<PlanNode>>,
    auxiliary: Vec<PlanExpression>,
    distinct: bool,
    span: SqlSpan,
) -> Result<PlanExpressionKind> {
    let calls = calls.ok_or_else(|| {
        plan_error(
            span,
            "aggregate or time-series function is not valid in this clause".to_string(),
        )
    })?;
    let index = calls.len();
    calls.push(AggregateCall {
        kind,
        argument,
        parameter,
        auxiliary,
        distinct,
    });
    Ok(PlanExpressionKind::AggregateResult { index })
}

fn time_predicate_range(
    expression: &SqlExpr,
    context: PlanContext,
) -> Result<Option<PlanTimeRange>> {
    match &expression.kind {
        SqlExprKind::Binary { left, op, right } => {
            if is_time_column(left) {
                let Some(value) = timestamp_value(right, context)? else {
                    return Ok(None);
                };
                comparison_range(*op, value, expression.span)
            } else if is_time_column(right) {
                let Some(value) = timestamp_value(left, context)? else {
                    return Ok(None);
                };
                comparison_range(reverse_comparison(*op), value, expression.span)
            } else {
                Ok(None)
            }
        }
        SqlExprKind::Between {
            expr,
            low,
            high,
            negated: false,
        } if is_time_column(expr) => {
            let (Some(low), Some(high)) = (
                timestamp_value(low, context)?,
                timestamp_value(high, context)?,
            ) else {
                return Ok(None);
            };
            PlanTimeRange::new(
                Some(TimeBound {
                    value: low,
                    inclusive: true,
                }),
                Some(TimeBound {
                    value: high,
                    inclusive: true,
                }),
            )
            .map(Some)
            .map_err(|error| plan_error(expression.span, error.to_string()))
        }
        _ => Ok(None),
    }
}

fn comparison_range(op: SqlBinaryOp, value: i64, span: SqlSpan) -> Result<Option<PlanTimeRange>> {
    let range = match op {
        SqlBinaryOp::Eq => PlanTimeRange::new(
            Some(TimeBound {
                value,
                inclusive: true,
            }),
            Some(TimeBound {
                value,
                inclusive: true,
            }),
        ),
        SqlBinaryOp::Gt => PlanTimeRange::new(
            Some(TimeBound {
                value,
                inclusive: false,
            }),
            None,
        ),
        SqlBinaryOp::GtEq => PlanTimeRange::new(
            Some(TimeBound {
                value,
                inclusive: true,
            }),
            None,
        ),
        SqlBinaryOp::Lt => PlanTimeRange::new(
            None,
            Some(TimeBound {
                value,
                inclusive: false,
            }),
        ),
        SqlBinaryOp::LtEq => PlanTimeRange::new(
            None,
            Some(TimeBound {
                value,
                inclusive: true,
            }),
        ),
        _ => return Ok(None),
    };
    range
        .map(Some)
        .map_err(|error| plan_error(span, error.to_string()))
}

fn timestamp_value(expression: &SqlExpr, context: PlanContext) -> Result<Option<i64>> {
    match &expression.kind {
        SqlExprKind::Function(SqlFunction::Now) => Ok(Some(context.evaluation_time())),
        SqlExprKind::Binary { left, op, right } => match op {
            SqlBinaryOp::Add => {
                if let Some(timestamp) = timestamp_value(left, context)? {
                    let Some(interval) = interval_value(right)? else {
                        return Ok(None);
                    };
                    return timestamp.checked_add(interval).map(Some).ok_or_else(|| {
                        plan_error(expression.span, "timestamp addition overflows".to_string())
                    });
                }
                if let Some(timestamp) = timestamp_value(right, context)? {
                    let Some(interval) = interval_value(left)? else {
                        return Ok(None);
                    };
                    return timestamp.checked_add(interval).map(Some).ok_or_else(|| {
                        plan_error(expression.span, "timestamp addition overflows".to_string())
                    });
                }
                Ok(None)
            }
            SqlBinaryOp::Sub => {
                let Some(timestamp) = timestamp_value(left, context)? else {
                    return Ok(None);
                };
                let Some(interval) = interval_value(right)? else {
                    return Ok(None);
                };
                timestamp.checked_sub(interval).map(Some).ok_or_else(|| {
                    plan_error(
                        expression.span,
                        "timestamp subtraction overflows".to_string(),
                    )
                })
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn interval_value(expression: &SqlExpr) -> Result<Option<i64>> {
    match &expression.kind {
        SqlExprKind::Literal(SqlLiteral::Interval(raw)) => {
            duration_literal_ns(raw, expression.span).map(Some)
        }
        _ => Ok(None),
    }
}

fn intersect_ranges(
    left: PlanTimeRange,
    right: PlanTimeRange,
    span: SqlSpan,
) -> Result<PlanTimeRange> {
    let start = match (left.start, right.start) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) => Some(if left.value > right.value {
            left
        } else if right.value > left.value {
            right
        } else {
            TimeBound {
                value: left.value,
                inclusive: left.inclusive && right.inclusive,
            }
        }),
    };
    let end = match (left.end, right.end) {
        (None, bound) | (bound, None) => bound,
        (Some(left), Some(right)) => Some(if left.value < right.value {
            left
        } else if right.value < left.value {
            right
        } else {
            TimeBound {
                value: left.value,
                inclusive: left.inclusive && right.inclusive,
            }
        }),
    };
    PlanTimeRange::new(start, end).map_err(|error| plan_error(span, error.to_string()))
}

fn tag_equality(expression: &SqlExpr) -> Option<LabelMatcher> {
    let SqlExprKind::Binary {
        left,
        op: SqlBinaryOp::Eq,
        right,
    } = &expression.kind
    else {
        return None;
    };
    if let (Some(name), Some(value)) = (column_name(left), string_literal(right)) {
        Some(LabelMatcher::new(name, MatchOp::Equal, value))
    } else if let (Some(name), Some(value)) = (column_name(right), string_literal(left)) {
        Some(LabelMatcher::new(name, MatchOp::Equal, value))
    } else {
        None
    }
}

fn collect_field_projection(query: &crate::query::sqlts::SqlTsQuery) -> Option<BTreeSet<String>> {
    if query
        .projections
        .iter()
        .any(|projection| matches!(projection, SqlProjection::Wildcard { .. }))
    {
        return None;
    }
    let mut columns = BTreeSet::new();
    for projection in &query.projections {
        if let SqlProjection::Expr { expr, .. } = projection {
            collect_columns(expr, &mut columns);
        }
    }
    if let Some(selection) = &query.selection {
        collect_columns(selection, &mut columns);
    }
    for group in &query.group_by {
        if let SqlGroupBy::Expression(expression) = group {
            collect_columns(expression, &mut columns);
        }
    }
    for order in &query.order_by {
        if let SqlOrderKey::Expression(expression) = &order.key {
            collect_columns(expression, &mut columns);
        }
    }
    columns.remove("time");
    Some(columns)
}

fn collect_columns(expression: &SqlExpr, output: &mut BTreeSet<String>) {
    match &expression.kind {
        SqlExprKind::Column { name, .. } => {
            output.insert(name.clone());
        }
        SqlExprKind::Binary { left, right, .. } => {
            collect_columns(left, output);
            collect_columns(right, output);
        }
        SqlExprKind::Unary { expr, .. } | SqlExprKind::IsNull { expr, .. } => {
            collect_columns(expr, output);
        }
        SqlExprKind::Between {
            expr, low, high, ..
        } => {
            collect_columns(expr, output);
            collect_columns(low, output);
            collect_columns(high, output);
        }
        SqlExprKind::Pattern {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_columns(expr, output);
            collect_columns(pattern, output);
            if let Some(escape) = escape {
                collect_columns(escape, output);
            }
        }
        SqlExprKind::InList { expr, list, .. } => {
            collect_columns(expr, output);
            for item in list {
                collect_columns(item, output);
            }
        }
        SqlExprKind::Function(SqlFunction::TimeSeries { columns, .. }) => {
            output.extend(columns.iter().map(|column| column.name.clone()));
        }
        SqlExprKind::Function(SqlFunction::Aggregate {
            argument: AggregateArgument::Expr(argument),
            ..
        }) => collect_columns(argument, output),
        SqlExprKind::Literal(_)
        | SqlExprKind::Function(SqlFunction::Aggregate {
            argument: AggregateArgument::Wildcard,
            ..
        })
        | SqlExprKind::Function(SqlFunction::Now) => {}
    }
}

fn contains_aggregate_result(expression: &PlanExpression) -> bool {
    match &expression.kind {
        PlanExpressionKind::AggregateResult { .. } => true,
        PlanExpressionKind::Binary { left, right, .. } => {
            contains_aggregate_result(left) || contains_aggregate_result(right)
        }
        PlanExpressionKind::Unary { expression, .. }
        | PlanExpressionKind::IsNull { expression, .. } => contains_aggregate_result(expression),
        PlanExpressionKind::Between {
            expression,
            low,
            high,
            ..
        } => {
            contains_aggregate_result(expression)
                || contains_aggregate_result(low)
                || contains_aggregate_result(high)
        }
        PlanExpressionKind::Pattern {
            expression,
            pattern,
            escape,
            ..
        } => {
            contains_aggregate_result(expression)
                || contains_aggregate_result(pattern)
                || escape.as_deref().is_some_and(contains_aggregate_result)
        }
        PlanExpressionKind::InList {
            expression, list, ..
        } => contains_aggregate_result(expression) || list.iter().any(contains_aggregate_result),
        _ => false,
    }
}

fn contains_column(expression: &PlanExpression) -> bool {
    match &expression.kind {
        PlanExpressionKind::Column { .. } | PlanExpressionKind::TimeBucket { .. } => true,
        PlanExpressionKind::Binary { left, right, .. } => {
            contains_column(left) || contains_column(right)
        }
        PlanExpressionKind::Unary { expression, .. }
        | PlanExpressionKind::IsNull { expression, .. } => contains_column(expression),
        PlanExpressionKind::Between {
            expression,
            low,
            high,
            ..
        } => contains_column(expression) || contains_column(low) || contains_column(high),
        PlanExpressionKind::Pattern {
            expression,
            pattern,
            escape,
            ..
        } => {
            contains_column(expression)
                || contains_column(pattern)
                || escape.as_deref().is_some_and(contains_column)
        }
        PlanExpressionKind::InList {
            expression, list, ..
        } => contains_column(expression) || list.iter().any(contains_column),
        _ => false,
    }
}

fn is_time_column(expression: &SqlExpr) -> bool {
    matches!(
        &expression.kind,
        SqlExprKind::Column { name, .. } if name.eq_ignore_ascii_case("time")
    )
}

fn column_name(expression: &SqlExpr) -> Option<&str> {
    match &expression.kind {
        SqlExprKind::Column { name, .. } => Some(name),
        _ => None,
    }
}

fn string_literal(expression: &SqlExpr) -> Option<&str> {
    match &expression.kind {
        SqlExprKind::Literal(SqlLiteral::String(value)) => Some(value),
        _ => None,
    }
}

fn column_expression(name: &str) -> PlanExpression {
    PlanExpression {
        kind: PlanExpressionKind::Column {
            name: name.to_string(),
        },
    }
}

fn duration_literal_ns(raw: &str, span: SqlSpan) -> Result<i64> {
    parse_duration(raw)
        .map_err(|message| plan_error(span, message))
        .and_then(|duration| duration_ns(duration, span))
}

fn duration_ns(duration: Duration, span: SqlSpan) -> Result<i64> {
    i64::try_from(duration.as_nanos()).map_err(|_| {
        plan_error(
            span,
            "duration exceeds the logical planner nanosecond range".to_string(),
        )
    })
}

const fn standard_aggregate(function: AggregateFunction) -> AggregateKind {
    match function {
        AggregateFunction::Avg => AggregateKind::Avg,
        AggregateFunction::Sum => AggregateKind::Sum,
        AggregateFunction::Min => AggregateKind::Min,
        AggregateFunction::Max => AggregateKind::Max,
        AggregateFunction::Count => AggregateKind::Count,
    }
}

const fn scalar_binary(op: SqlBinaryOp) -> ScalarBinaryKind {
    match op {
        SqlBinaryOp::Add => ScalarBinaryKind::Add,
        SqlBinaryOp::Sub => ScalarBinaryKind::Sub,
        SqlBinaryOp::Mul => ScalarBinaryKind::Mul,
        SqlBinaryOp::Div => ScalarBinaryKind::Div,
        SqlBinaryOp::Mod => ScalarBinaryKind::Mod,
        SqlBinaryOp::Eq => ScalarBinaryKind::Eq,
        SqlBinaryOp::NotEq => ScalarBinaryKind::NotEq,
        SqlBinaryOp::Lt => ScalarBinaryKind::Lt,
        SqlBinaryOp::Gt => ScalarBinaryKind::Gt,
        SqlBinaryOp::LtEq => ScalarBinaryKind::LtEq,
        SqlBinaryOp::GtEq => ScalarBinaryKind::GtEq,
        SqlBinaryOp::And => ScalarBinaryKind::And,
        SqlBinaryOp::Or => ScalarBinaryKind::Or,
        SqlBinaryOp::StringConcat => ScalarBinaryKind::StringConcat,
    }
}

const fn reverse_comparison(op: SqlBinaryOp) -> SqlBinaryOp {
    match op {
        SqlBinaryOp::Lt => SqlBinaryOp::Gt,
        SqlBinaryOp::Gt => SqlBinaryOp::Lt,
        SqlBinaryOp::LtEq => SqlBinaryOp::GtEq,
        SqlBinaryOp::GtEq => SqlBinaryOp::LtEq,
        other => other,
    }
}

const fn scalar_unary(op: SqlUnaryOp) -> ScalarUnaryKind {
    match op {
        SqlUnaryOp::Not => ScalarUnaryKind::Not,
        SqlUnaryOp::Minus => ScalarUnaryKind::Minus,
    }
}

const fn pattern_kind(kind: PatternKind) -> PatternMatchKind {
    match kind {
        PatternKind::Like => PatternMatchKind::Like,
        PatternKind::ILike => PatternMatchKind::ILike,
        PatternKind::Glob => PatternMatchKind::Glob,
        PatternKind::SimilarTo => PatternMatchKind::SimilarTo,
    }
}

fn plan_error(span: SqlSpan, message: String) -> TsmError {
    TsmError::Plan {
        message,
        line: span.start.line,
        column: span.start.column,
        offset: 0,
    }
}
