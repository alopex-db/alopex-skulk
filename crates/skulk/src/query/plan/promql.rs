//! PromQL to language-independent logical plan conversion.

use super::{
    AggregateCall, AggregateInput, AggregateKind, AggregateNode, AggregationStage, ArithmeticKind,
    BinaryNode, FilterNode, LogicalPlan, MeasurementSelection, PlanContext, PlanNode,
    PlanPredicate, PlanTimeRange, PlanValueType, RangeFunctionKind, RangeFunctionNode, ScanNode,
    ScanResolution, SeriesGroupKind, SeriesGroupNode, SeriesGrouping, SeriesWindow,
    SeriesWindowKind,
};
use crate::query::promql::{
    AggregationOp, BinaryOp, PromDuration, PromExpr, PromExprKind, PromFunction, PromLabelMatcher,
    PromSpan, PromValueType, UnaryOp,
};
use crate::query::MatchOp;
use crate::{Result, TsmError};
use std::collections::BTreeSet;

const NANOS_PER_MILLISECOND: i64 = 1_000_000;
const NAME_LABEL: &str = "__name__";
const FIELD_LABEL: &str = "__field__";
const DEFAULT_FIELD: &str = "value";

/// Converts one validated PromQL AST into a logical operator tree.
pub fn plan_promql(expression: &PromExpr, context: PlanContext) -> Result<LogicalPlan> {
    Ok(LogicalPlan {
        root: plan_expression(expression, context)?,
        output_type: value_type(expression.value_type()),
    })
}

fn plan_expression(expression: &PromExpr, context: PlanContext) -> Result<PlanNode> {
    match &expression.kind {
        PromExprKind::VectorSelector {
            metric,
            matchers,
            offset,
        } => plan_selector(
            metric,
            matchers,
            offset.as_ref(),
            None,
            SeriesWindowKind::Instant,
            context.lookback_ns(),
            expression.span,
            context,
        ),
        PromExprKind::MatrixSelector {
            selector,
            range,
            offset,
        } => {
            let PromExprKind::VectorSelector {
                metric,
                matchers,
                offset: selector_offset,
            } = &selector.kind
            else {
                return Err(plan_error(
                    selector.span,
                    "range selector does not contain a vector selector".to_string(),
                ));
            };
            plan_selector(
                metric,
                matchers,
                selector_offset.as_ref(),
                offset.as_ref(),
                SeriesWindowKind::Range,
                duration_ns(range, expression.span)?,
                expression.span,
                context,
            )
        }
        PromExprKind::NumberLiteral { value, .. } => Ok(PlanNode::Scalar(*value)),
        PromExprKind::StringLiteral { value } => Ok(PlanNode::String(value.clone())),
        PromExprKind::FunctionCall { function, args } => {
            plan_function(*function, args, expression.span, context)
        }
        PromExprKind::Aggregate {
            op,
            expr,
            grouping,
            without,
        } => Ok(PlanNode::Aggregate(AggregateNode {
            input: Box::new(plan_expression(expr, context)?),
            calls: vec![AggregateCall {
                kind: aggregate_kind(*op),
                argument: AggregateInput::CurrentValue,
                parameter: None,
                auxiliary: Vec::new(),
                distinct: false,
            }],
            grouping: grouping.as_ref().map(|labels| SeriesGrouping {
                labels: labels.clone(),
                without: *without,
            }),
            stage: AggregationStage::Single,
        })),
        PromExprKind::Binary { left, op, right } => Ok(PlanNode::Binary(BinaryNode {
            left: Box::new(plan_expression(left, context)?),
            op: arithmetic_kind(*op),
            right: Box::new(plan_expression(right, context)?),
        })),
        PromExprKind::Unary { op, expr } => {
            let inner = plan_expression(expr, context)?;
            if *op == UnaryOp::Plus {
                Ok(inner)
            } else {
                Ok(PlanNode::Binary(BinaryNode {
                    left: Box::new(PlanNode::Scalar(-1.0)),
                    op: ArithmeticKind::Mul,
                    right: Box::new(inner),
                }))
            }
        }
        PromExprKind::Paren { expr } => plan_expression(expr, context),
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_selector(
    metric: &Option<String>,
    matchers: &[PromLabelMatcher],
    selector_offset: Option<&PromDuration>,
    outer_offset: Option<&PromDuration>,
    window_kind: SeriesWindowKind,
    window_ns: i64,
    span: PromSpan,
    context: PlanContext,
) -> Result<PlanNode> {
    let offset_ns = checked_offset(selector_offset, outer_offset, span)?;
    let evaluation_time = context
        .evaluation_time()
        .checked_sub(offset_ns)
        .ok_or_else(|| {
            plan_error(
                span,
                "offset shifts evaluation time outside i64".to_string(),
            )
        })?;
    let start_exclusive = evaluation_time.checked_sub(window_ns).ok_or_else(|| {
        plan_error(
            span,
            "selector window starts outside the supported timestamp range".to_string(),
        )
    })?;
    let time_range = PlanTimeRange::prometheus_window(start_exclusive, evaluation_time)
        .map_err(|error| plan_error(span, error.to_string()))?;

    let mut measurement = MeasurementSelection {
        exact: metric.clone(),
        matchers: Vec::new(),
    };
    let mut field = None;
    let mut tag_equalities = Vec::new();
    let mut residual = Vec::new();

    for matcher in matchers {
        let matcher = matcher.matcher.clone();
        match matcher.name.as_str() {
            NAME_LABEL => {
                if measurement.exact.is_none() && matcher.op == MatchOp::Equal {
                    measurement.exact = Some(matcher.value.clone());
                }
                measurement.matchers.push(matcher);
            }
            FIELD_LABEL => {
                if matcher.op != MatchOp::Equal || matcher.value.is_empty() {
                    return Err(plan_error(
                        span,
                        "`__field__` requires one non-empty equality matcher".to_string(),
                    ));
                }
                match &field {
                    Some(existing) if existing != &matcher.value => {
                        return Err(plan_error(
                            span,
                            "conflicting `__field__` equality matchers".to_string(),
                        ));
                    }
                    Some(_) => {}
                    None => field = Some(matcher.value),
                }
            }
            _ if matcher.op == MatchOp::Equal => tag_equalities.push(matcher),
            _ => residual.push(PlanPredicate::Label(matcher)),
        }
    }

    let scan = PlanNode::Scan(ScanNode {
        measurement,
        time_range,
        tag_equalities,
        field_projection: Some(BTreeSet::from([
            field.unwrap_or_else(|| DEFAULT_FIELD.to_string())
        ])),
        resolution: ScanResolution::raw(),
    });
    let input = if residual.is_empty() {
        scan
    } else {
        PlanNode::Filter(FilterNode {
            input: Box::new(scan),
            predicates: residual,
        })
    };
    Ok(PlanNode::SeriesGroup(SeriesGroupNode {
        input: Box::new(input),
        kind: SeriesGroupKind::Window(SeriesWindow {
            kind: window_kind,
            evaluation_time,
            duration_ns: window_ns,
        }),
    }))
}

fn plan_function(
    function: PromFunction,
    arguments: &[PromExpr],
    span: PromSpan,
    context: PlanContext,
) -> Result<PlanNode> {
    if function == PromFunction::HistogramQuantile {
        let [parameter, input] = arguments else {
            return Err(plan_error(
                span,
                "histogram_quantile planning requires two arguments".to_string(),
            ));
        };
        return Ok(PlanNode::Aggregate(AggregateNode {
            input: Box::new(plan_expression(input, context)?),
            calls: vec![AggregateCall {
                kind: AggregateKind::HistogramQuantile,
                argument: AggregateInput::CurrentValue,
                parameter: Some(Box::new(plan_expression(parameter, context)?)),
                auxiliary: Vec::new(),
                distinct: false,
            }],
            grouping: None,
            stage: AggregationStage::Single,
        }));
    }

    let [input] = arguments else {
        return Err(plan_error(
            span,
            "range function planning requires one argument".to_string(),
        ));
    };
    Ok(PlanNode::RangeFunction(RangeFunctionNode {
        input: Box::new(plan_expression(input, context)?),
        function: range_function_kind(function).ok_or_else(|| {
            plan_error(
                span,
                format!("unsupported range function in logical planner: {function:?}"),
            )
        })?,
    }))
}

fn checked_offset(
    selector_offset: Option<&PromDuration>,
    outer_offset: Option<&PromDuration>,
    span: PromSpan,
) -> Result<i64> {
    [selector_offset, outer_offset]
        .into_iter()
        .flatten()
        .try_fold(0_i64, |total, duration| {
            total
                .checked_add(duration_ns(duration, span)?)
                .ok_or_else(|| plan_error(span, "combined selector offset overflows".to_string()))
        })
}

fn duration_ns(duration: &PromDuration, span: PromSpan) -> Result<i64> {
    duration
        .milliseconds
        .checked_mul(NANOS_PER_MILLISECOND)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            plan_error(
                span,
                format!(
                    "duration `{}` cannot be represented as positive nanoseconds",
                    duration.raw
                ),
            )
        })
}

const fn value_type(value: PromValueType) -> PlanValueType {
    match value {
        PromValueType::Scalar => PlanValueType::Scalar,
        PromValueType::InstantVector => PlanValueType::InstantVector,
        PromValueType::RangeVector => PlanValueType::RangeVector,
        PromValueType::String => PlanValueType::String,
    }
}

const fn aggregate_kind(value: AggregationOp) -> AggregateKind {
    match value {
        AggregationOp::Sum => AggregateKind::Sum,
        AggregationOp::Avg => AggregateKind::Avg,
        AggregationOp::Max => AggregateKind::Max,
        AggregationOp::Min => AggregateKind::Min,
        AggregationOp::Count => AggregateKind::Count,
    }
}

const fn arithmetic_kind(value: BinaryOp) -> ArithmeticKind {
    match value {
        BinaryOp::Add => ArithmeticKind::Add,
        BinaryOp::Sub => ArithmeticKind::Sub,
        BinaryOp::Mul => ArithmeticKind::Mul,
        BinaryOp::Div => ArithmeticKind::Div,
        BinaryOp::Mod => ArithmeticKind::Mod,
        BinaryOp::Pow => ArithmeticKind::Pow,
    }
}

const fn range_function_kind(value: PromFunction) -> Option<RangeFunctionKind> {
    match value {
        PromFunction::Rate => Some(RangeFunctionKind::Rate),
        PromFunction::IRate => Some(RangeFunctionKind::IRate),
        PromFunction::Increase => Some(RangeFunctionKind::Increase),
        PromFunction::AvgOverTime => Some(RangeFunctionKind::AvgOverTime),
        PromFunction::MinOverTime => Some(RangeFunctionKind::MinOverTime),
        PromFunction::MaxOverTime => Some(RangeFunctionKind::MaxOverTime),
        PromFunction::SumOverTime => Some(RangeFunctionKind::SumOverTime),
        PromFunction::CountOverTime => Some(RangeFunctionKind::CountOverTime),
        PromFunction::HistogramQuantile => None,
    }
}

fn plan_error(span: PromSpan, message: String) -> TsmError {
    TsmError::Plan {
        message,
        line: span.start.line,
        column: span.start.column,
        offset: span.start.offset,
    }
}
