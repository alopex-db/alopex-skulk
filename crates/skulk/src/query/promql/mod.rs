//! PromQL wire-AST mapping, validation, and public expression types.

use super::nimffi::{self, ParserLanguage};
use crate::query::exec::limits::ParsingLimits;
use crate::query::{LabelMatcher, MatchOp};
use crate::{Result, TsmError};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;

/// Maximum UTF-8 byte length accepted before calling the Nim parser.
pub const MAX_PROMQL_INPUT_BYTES: usize = ParsingLimits::DEFAULT.max_input_bytes();

/// Maximum UTF-8 byte length of one regular-expression matcher.
pub const MAX_PROMQL_REGEX_BYTES: usize = ParsingLimits::DEFAULT.max_regex_bytes();

/// A one-based line/column and zero-based UTF-8 byte offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PromPosition {
    /// One-based source line.
    pub line: usize,
    /// One-based source column.
    pub column: usize,
    /// Zero-based UTF-8 byte offset.
    pub offset: usize,
}

/// An exclusive-end source span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PromSpan {
    /// First position covered by the expression.
    pub start: PromPosition,
    /// Exclusive position after the expression.
    pub end: PromPosition,
}

/// A Prometheus duration together with its normalized millisecond value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PromDuration {
    /// Original duration spelling.
    pub raw: String,
    /// Normalized signed duration in milliseconds.
    pub milliseconds: i64,
}

/// The value category inferred for a validated PromQL expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PromValueType {
    /// A numeric scalar.
    Scalar,
    /// One sample per series at an evaluation time.
    InstantVector,
    /// A time window of samples per series.
    RangeVector,
    /// A string literal accepted by the grammar.
    String,
}

/// A supported PromQL function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PromFunction {
    /// Per-second counter rate over a range vector.
    Rate,
    /// Instantaneous counter rate over the last two samples.
    IRate,
    /// Counter increase over a range vector.
    Increase,
    /// Average of samples in each range.
    AvgOverTime,
    /// Minimum sample in each range.
    MinOverTime,
    /// Maximum sample in each range.
    MaxOverTime,
    /// Sum of samples in each range.
    SumOverTime,
    /// Number of samples in each range.
    CountOverTime,
    /// Quantile interpolation over classic histogram buckets.
    HistogramQuantile,
}

impl PromFunction {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "rate" => Some(Self::Rate),
            "irate" => Some(Self::IRate),
            "increase" => Some(Self::Increase),
            "avg_over_time" => Some(Self::AvgOverTime),
            "min_over_time" => Some(Self::MinOverTime),
            "max_over_time" => Some(Self::MaxOverTime),
            "sum_over_time" => Some(Self::SumOverTime),
            "count_over_time" => Some(Self::CountOverTime),
            "histogram_quantile" => Some(Self::HistogramQuantile),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Rate => "rate",
            Self::IRate => "irate",
            Self::Increase => "increase",
            Self::AvgOverTime => "avg_over_time",
            Self::MinOverTime => "min_over_time",
            Self::MaxOverTime => "max_over_time",
            Self::SumOverTime => "sum_over_time",
            Self::CountOverTime => "count_over_time",
            Self::HistogramQuantile => "histogram_quantile",
        }
    }

    fn expected_arguments(self) -> &'static [PromValueType] {
        match self {
            Self::Rate
            | Self::IRate
            | Self::Increase
            | Self::AvgOverTime
            | Self::MinOverTime
            | Self::MaxOverTime
            | Self::SumOverTime
            | Self::CountOverTime => &[PromValueType::RangeVector],
            Self::HistogramQuantile => &[PromValueType::Scalar, PromValueType::InstantVector],
        }
    }
}

/// A supported PromQL aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationOp {
    /// Sum values in each output group.
    Sum,
    /// Average values in each output group.
    Avg,
    /// Select the maximum value in each output group.
    Max,
    /// Select the minimum value in each output group.
    Min,
    /// Count values in each output group.
    Count,
}

impl AggregationOp {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "max" => Some(Self::Max),
            "min" => Some(Self::Min),
            "count" => Some(Self::Count),
            _ => None,
        }
    }
}

/// A supported arithmetic binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    /// Addition.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// Division.
    Div,
    /// Remainder.
    Mod,
    /// Exponentiation.
    Pow,
}

/// A unary arithmetic operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    /// Unary plus.
    Plus,
    /// Unary minus.
    Minus,
}

/// A source-spanned label matcher with an eagerly compiled regex when needed.
#[derive(Debug, Clone)]
pub struct PromLabelMatcher {
    /// Language-independent matcher data used by the planner and reader.
    pub matcher: LabelMatcher,
    /// Source span of the complete matcher.
    pub span: PromSpan,
    compiled_regex: Option<Regex>,
}

impl PromLabelMatcher {
    /// Evaluates this matcher against one label value.
    ///
    /// Callers use an empty string for a missing label, matching Prometheus
    /// semantics.
    pub fn matches_label_value(&self, candidate: &str) -> bool {
        match self.matcher.op {
            MatchOp::Equal => candidate == self.matcher.value,
            MatchOp::NotEqual => candidate != self.matcher.value,
            MatchOp::Regex => self
                .compiled_regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(candidate)),
            MatchOp::NotRegex => self
                .compiled_regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(candidate)),
        }
    }

    /// Returns the fully anchored compiled expression for regex matchers.
    pub fn compiled_regex(&self) -> Option<&Regex> {
        self.compiled_regex.as_ref()
    }
}

/// A validated PromQL expression and its inferred value type.
#[derive(Debug, Clone)]
pub struct PromExpr {
    /// Expression-specific data.
    pub kind: PromExprKind,
    /// Source span of the complete expression.
    pub span: PromSpan,
    value_type: PromValueType,
}

impl PromExpr {
    /// Returns the value category inferred during AST mapping.
    pub const fn value_type(&self) -> PromValueType {
        self.value_type
    }
}

/// Validated PromQL expression variants supported by Skulk v0.4.
#[derive(Debug, Clone)]
pub enum PromExprKind {
    /// An instant-vector selector.
    VectorSelector {
        /// Optional measurement name.
        metric: Option<String>,
        /// Label predicates attached to the selector.
        matchers: Vec<PromLabelMatcher>,
        /// Optional evaluation-time shift.
        offset: Option<PromDuration>,
    },
    /// A range-vector selector.
    MatrixSelector {
        /// Underlying instant-vector selector.
        selector: Box<PromExpr>,
        /// Range window.
        range: PromDuration,
        /// Optional evaluation-time shift.
        offset: Option<PromDuration>,
    },
    /// A numeric scalar literal.
    NumberLiteral {
        /// Original source spelling.
        raw: String,
        /// Parsed IEEE-754 value.
        value: f64,
    },
    /// A string literal.
    StringLiteral {
        /// Decoded string value.
        value: String,
    },
    /// A supported function call with validated arguments.
    FunctionCall {
        /// Resolved function identity.
        function: PromFunction,
        /// Validated function arguments.
        args: Vec<PromExpr>,
    },
    /// A vector aggregation.
    Aggregate {
        /// Aggregation operation.
        op: AggregationOp,
        /// Input instant vector.
        expr: Box<PromExpr>,
        /// `by` or `without` labels, or `None` when ungrouped.
        grouping: Option<Vec<String>>,
        /// Whether grouping uses `without` rather than `by`.
        without: bool,
    },
    /// An arithmetic binary expression.
    Binary {
        /// Left operand.
        left: Box<PromExpr>,
        /// Arithmetic operation.
        op: BinaryOp,
        /// Right operand.
        right: Box<PromExpr>,
    },
    /// A unary arithmetic expression.
    Unary {
        /// Unary operation.
        op: UnaryOp,
        /// Operand.
        expr: Box<PromExpr>,
    },
    /// An explicitly parenthesized expression.
    Paren {
        /// Expression inside the parentheses.
        expr: Box<PromExpr>,
    },
}

/// Parses and validates one PromQL expression through the vendored Nim parser.
pub fn parse(input: &str) -> Result<PromExpr> {
    parse_with_limits(input, ParsingLimits::DEFAULT)
}

/// Parses with one query-wide set of input, AST, and regex limits.
pub fn parse_with_limits(input: &str, limits: ParsingLimits) -> Result<PromExpr> {
    let wire: WireExpr = nimffi::parse_with_limits(ParserLanguage::PromQl, input, limits)?;
    let mut nodes = 0;
    map_expression(wire, 1, &mut nodes, limits)
}

/// Returns the checked runtime MessagePack contract version.
pub fn parser_contract_version() -> Result<&'static str> {
    nimffi::checked_contract_version()
}

fn map_expression(
    wire: WireExpr,
    depth: usize,
    nodes: &mut usize,
    limits: ParsingLimits,
) -> Result<PromExpr> {
    if depth > limits.max_ast_depth() {
        return Err(TsmError::ResourceLimit(format!(
            "PromQL AST depth exceeds {}",
            limits.max_ast_depth()
        )));
    }
    *nodes += 1;
    if *nodes > limits.max_ast_nodes() {
        return Err(TsmError::ResourceLimit(format!(
            "PromQL AST node count exceeds {}",
            limits.max_ast_nodes()
        )));
    }

    let span = map_span(wire.span)?;
    let (kind, value_type) = match wire.kind {
        WireExprKind::VectorSelector {
            metric,
            matchers,
            offset,
        } => {
            let offset = map_offset(offset, span)?;
            let matchers = matchers
                .into_iter()
                .map(|matcher| map_matcher(matcher, limits))
                .collect::<Result<Vec<_>>>()?;
            (
                PromExprKind::VectorSelector {
                    metric,
                    matchers,
                    offset,
                },
                PromValueType::InstantVector,
            )
        }
        WireExprKind::MatrixSelector {
            selector,
            range,
            offset,
        } => {
            let selector = map_expression(*selector, depth + 1, nodes, limits)?;
            if !matches!(selector.kind, PromExprKind::VectorSelector { .. }) {
                return Err(TsmError::FfiContract(
                    "MatrixSelector must contain a VectorSelector".to_string(),
                ));
            }
            let range = map_range(range)?;
            let offset = map_offset(offset, span)?;
            (
                PromExprKind::MatrixSelector {
                    selector: Box::new(selector),
                    range,
                    offset,
                },
                PromValueType::RangeVector,
            )
        }
        WireExprKind::NumberLiteral { value } => {
            let parsed = value.parse::<f64>().map_err(|error| {
                parse_error(span, format!("invalid numeric literal `{value}`: {error}"))
            })?;
            if !parsed.is_finite() {
                return Err(parse_error(
                    span,
                    format!("non-finite numeric literal `{value}` is unsupported"),
                ));
            }
            (
                PromExprKind::NumberLiteral {
                    raw: value,
                    value: parsed,
                },
                PromValueType::Scalar,
            )
        }
        WireExprKind::StringLiteral { value } => {
            (PromExprKind::StringLiteral { value }, PromValueType::String)
        }
        WireExprKind::FunctionCall { name, args } => {
            let Some(function) = PromFunction::from_name(&name) else {
                return Err(unsupported_error(span, format!("PromQL function `{name}`")));
            };
            let args = args
                .into_iter()
                .map(|argument| map_expression(argument, depth + 1, nodes, limits))
                .collect::<Result<Vec<_>>>()?;
            validate_function_arguments(function, &args, span)?;
            (
                PromExprKind::FunctionCall { function, args },
                PromValueType::InstantVector,
            )
        }
        WireExprKind::Aggregate {
            op,
            expr,
            grouping,
            without,
        } => {
            let Some(op) = AggregationOp::from_name(&op) else {
                return Err(unsupported_error(
                    span,
                    format!("PromQL aggregation `{op}`"),
                ));
            };
            if without && grouping.is_none() {
                return Err(TsmError::FfiContract(
                    "aggregate has `without=true` but no grouping labels".to_string(),
                ));
            }
            let expr = map_expression(*expr, depth + 1, nodes, limits)?;
            if expr.value_type != PromValueType::InstantVector {
                return Err(type_error(
                    span,
                    format!(
                        "aggregation expects instant vector, got {:?}",
                        expr.value_type
                    ),
                ));
            }
            (
                PromExprKind::Aggregate {
                    op,
                    expr: Box::new(expr),
                    grouping,
                    without,
                },
                PromValueType::InstantVector,
            )
        }
        WireExprKind::BinaryOp { left, op, right } => {
            let left = map_expression(*left, depth + 1, nodes, limits)?;
            let right = map_expression(*right, depth + 1, nodes, limits)?;
            let value_type = binary_value_type(left.value_type, right.value_type, span)?;
            (
                PromExprKind::Binary {
                    left: Box::new(left),
                    op: op.into(),
                    right: Box::new(right),
                },
                value_type,
            )
        }
        WireExprKind::UnaryOp { op, expr } => {
            let expr = map_expression(*expr, depth + 1, nodes, limits)?;
            let value_type = match expr.value_type {
                PromValueType::Scalar | PromValueType::InstantVector => expr.value_type,
                other => {
                    return Err(type_error(
                        span,
                        format!("unary arithmetic expects scalar or instant vector, got {other:?}"),
                    ));
                }
            };
            (
                PromExprKind::Unary {
                    op: op.into(),
                    expr: Box::new(expr),
                },
                value_type,
            )
        }
        WireExprKind::Paren { expr } => {
            let expr = map_expression(*expr, depth + 1, nodes, limits)?;
            let value_type = expr.value_type;
            (
                PromExprKind::Paren {
                    expr: Box::new(expr),
                },
                value_type,
            )
        }
    };

    Ok(PromExpr {
        kind,
        span,
        value_type,
    })
}

fn map_matcher(wire: WireLabelMatcher, limits: ParsingLimits) -> Result<PromLabelMatcher> {
    let span = map_span(wire.span)?;
    let op: MatchOp = wire.op.into();
    let compiled_regex = match op {
        MatchOp::Regex | MatchOp::NotRegex => Some(compile_regex(&wire.value, span, limits)?),
        MatchOp::Equal | MatchOp::NotEqual => None,
    };
    Ok(PromLabelMatcher {
        matcher: LabelMatcher::new(wire.name, op, wire.value),
        span,
        compiled_regex,
    })
}

fn compile_regex(pattern: &str, span: PromSpan, limits: ParsingLimits) -> Result<Regex> {
    if pattern.len() > limits.max_regex_bytes() {
        return Err(TsmError::ResourceLimit(format!(
            "PromQL regex is {} bytes; limit is {} bytes",
            pattern.len(),
            limits.max_regex_bytes()
        )));
    }

    let anchored = format!(r"\A(?s:{pattern})\z");
    RegexBuilder::new(&anchored)
        .size_limit(limits.max_regex_automaton_bytes())
        .dfa_size_limit(limits.max_regex_automaton_bytes())
        .build()
        .map_err(|error| match error {
            regex::Error::CompiledTooBig(limit) => TsmError::ResourceLimit(format!(
                "PromQL regex exceeded the {limit}-byte compiled-size limit"
            )),
            other => parse_error(span, format!("invalid regular expression: {other}")),
        })
}

fn validate_function_arguments(
    function: PromFunction,
    arguments: &[PromExpr],
    span: PromSpan,
) -> Result<()> {
    let expected = function.expected_arguments();
    if arguments.len() != expected.len() {
        return Err(type_error(
            span,
            format!(
                "{} expects {} argument(s), got {}",
                function.name(),
                expected.len(),
                arguments.len()
            ),
        ));
    }
    for (index, (argument, expected_type)) in arguments.iter().zip(expected).enumerate() {
        if argument.value_type != *expected_type {
            return Err(type_error(
                argument.span,
                format!(
                    "{} argument {} expects {expected_type:?}, got {:?}",
                    function.name(),
                    index + 1,
                    argument.value_type
                ),
            ));
        }
    }
    Ok(())
}

fn binary_value_type(
    left: PromValueType,
    right: PromValueType,
    span: PromSpan,
) -> Result<PromValueType> {
    match (left, right) {
        (PromValueType::Scalar, PromValueType::Scalar) => Ok(PromValueType::Scalar),
        (PromValueType::Scalar, PromValueType::InstantVector)
        | (PromValueType::InstantVector, PromValueType::Scalar) => Ok(PromValueType::InstantVector),
        (PromValueType::InstantVector, PromValueType::InstantVector) => Err(unsupported_error(
            span,
            "vector-to-vector binary matching".to_string(),
        )),
        _ => Err(type_error(
            span,
            format!(
                "binary arithmetic expects scalar↔scalar or scalar↔instant-vector, got \
                 {left:?} and {right:?}"
            ),
        )),
    }
}

fn map_range(wire: WireDuration) -> Result<PromDuration> {
    if wire.raw.is_empty() || wire.milliseconds <= 0 {
        return Err(TsmError::FfiContract(
            "range duration must be non-empty and positive".to_string(),
        ));
    }
    Ok(PromDuration {
        raw: wire.raw,
        milliseconds: wire.milliseconds,
    })
}

fn map_offset(wire: Option<WireDuration>, span: PromSpan) -> Result<Option<PromDuration>> {
    let Some(wire) = wire else {
        return Ok(None);
    };
    if wire.milliseconds < 0 {
        return Err(unsupported_error(
            span,
            "negative PromQL offset".to_string(),
        ));
    }
    if wire.raw.is_empty() {
        return Err(TsmError::FfiContract(
            "offset duration has an empty source spelling".to_string(),
        ));
    }
    Ok(Some(PromDuration {
        raw: wire.raw,
        milliseconds: wire.milliseconds,
    }))
}

fn map_span(wire: WireSpan) -> Result<PromSpan> {
    let start = map_position(wire.start)?;
    let end = map_position(wire.end)?;
    if end.offset < start.offset
        || end.line < start.line
        || (end.line == start.line && end.column < start.column)
    {
        return Err(TsmError::FfiContract(
            "PromQL span ends before it starts".to_string(),
        ));
    }
    Ok(PromSpan { start, end })
}

fn map_position(wire: WirePosition) -> Result<PromPosition> {
    let line = usize::try_from(wire.line)
        .map_err(|_| TsmError::FfiContract("PromQL source line does not fit usize".to_string()))?;
    let column = usize::try_from(wire.column).map_err(|_| {
        TsmError::FfiContract("PromQL source column does not fit usize".to_string())
    })?;
    if line == 0 || column == 0 {
        return Err(TsmError::FfiContract(
            "PromQL source lines and columns must be one-based".to_string(),
        ));
    }
    Ok(PromPosition {
        line,
        column,
        offset: usize::try_from(wire.offset).map_err(|_| {
            TsmError::FfiContract("PromQL source offset does not fit usize".to_string())
        })?,
    })
}

fn parse_error(span: PromSpan, message: String) -> TsmError {
    TsmError::Parse {
        language: "PromQL".to_string(),
        message,
        line: span.start.line,
        column: span.start.column,
        offset: span.start.offset,
    }
}

fn type_error(span: PromSpan, message: String) -> TsmError {
    TsmError::Type {
        message,
        line: span.start.line,
        column: span.start.column,
        offset: span.start.offset,
    }
}

fn unsupported_error(span: PromSpan, feature: String) -> TsmError {
    TsmError::Unsupported {
        feature,
        line: span.start.line,
        column: span.start.column,
        offset: span.start.offset,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireExpr {
    kind: WireExprKind,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "variant", deny_unknown_fields)]
enum WireExprKind {
    VectorSelector {
        metric: Option<String>,
        matchers: Vec<WireLabelMatcher>,
        offset: Option<WireDuration>,
    },
    MatrixSelector {
        selector: Box<WireExpr>,
        range: WireDuration,
        offset: Option<WireDuration>,
    },
    NumberLiteral {
        value: String,
    },
    StringLiteral {
        value: String,
    },
    FunctionCall {
        name: String,
        args: Vec<WireExpr>,
    },
    Aggregate {
        op: String,
        expr: Box<WireExpr>,
        grouping: Option<Vec<String>>,
        without: bool,
    },
    BinaryOp {
        left: Box<WireExpr>,
        op: WireBinaryOp,
        right: Box<WireExpr>,
    },
    UnaryOp {
        op: WireUnaryOp,
        expr: Box<WireExpr>,
    },
    Paren {
        expr: Box<WireExpr>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePosition {
    line: u64,
    column: u64,
    offset: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSpan {
    start: WirePosition,
    end: WirePosition,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDuration {
    raw: String,
    milliseconds: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireLabelMatcher {
    name: String,
    op: WireMatchOp,
    value: String,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
enum WireMatchOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

impl From<WireMatchOp> for MatchOp {
    fn from(value: WireMatchOp) -> Self {
        match value {
            WireMatchOp::Equal => Self::Equal,
            WireMatchOp::NotEqual => Self::NotEqual,
            WireMatchOp::Regex => Self::Regex,
            WireMatchOp::NotRegex => Self::NotRegex,
        }
    }
}

#[derive(Debug, Deserialize)]
enum WireBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

impl From<WireBinaryOp> for BinaryOp {
    fn from(value: WireBinaryOp) -> Self {
        match value {
            WireBinaryOp::Add => Self::Add,
            WireBinaryOp::Sub => Self::Sub,
            WireBinaryOp::Mul => Self::Mul,
            WireBinaryOp::Div => Self::Div,
            WireBinaryOp::Mod => Self::Mod,
            WireBinaryOp::Pow => Self::Pow,
        }
    }
}

#[derive(Debug, Deserialize)]
enum WireUnaryOp {
    Plus,
    Minus,
}

impl From<WireUnaryOp> for UnaryOp {
    fn from(value: WireUnaryOp) -> Self {
        match value {
            WireUnaryOp::Plus => Self::Plus,
            WireUnaryOp::Minus => Self::Minus,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire_position(line: u64, column: u64, offset: u64) -> WirePosition {
        WirePosition {
            line,
            column,
            offset,
        }
    }

    #[test]
    fn malformed_wire_positions_and_spans_are_contract_errors() {
        assert!(matches!(
            map_position(wire_position(0, 1, 0)),
            Err(TsmError::FfiContract(_))
        ));
        assert!(matches!(
            map_span(WireSpan {
                start: wire_position(1, 2, 1),
                end: wire_position(1, 1, 0),
            }),
            Err(TsmError::FfiContract(_))
        ));
    }

    #[test]
    fn malformed_wire_durations_are_contract_errors() {
        assert!(matches!(
            map_range(WireDuration {
                raw: "0s".to_string(),
                milliseconds: 0,
            }),
            Err(TsmError::FfiContract(_))
        ));
    }
}
