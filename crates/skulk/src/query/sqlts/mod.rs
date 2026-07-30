//! SQL-TS wire-AST mapping and schema-independent semantic expressions.

pub mod typecheck;

use super::nimffi::{self, ParserLanguage};
use super::TSFunction;
use crate::{Result, TsmError};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

const MAX_SQL_AST_DEPTH: usize = 64;
const MAX_SQL_AST_NODES: usize = 65_536;

/// A one-based SQL source location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlPosition {
    /// One-based source line, or zero when the wire AST has no location.
    pub line: usize,
    /// One-based source column, or zero when the wire AST has no location.
    pub column: usize,
}

/// An inclusive-end SQL source span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlSpan {
    /// First position covered by the node.
    pub start: SqlPosition,
    /// Last position covered by the node.
    pub end: SqlPosition,
}

/// A schema-independent SQL-TS SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlTsQuery {
    /// Single measurement selected by the query.
    pub measurement: String,
    /// Optional alias of the selected measurement.
    pub measurement_alias: Option<String>,
    /// Whether SELECT DISTINCT was requested.
    pub distinct: bool,
    /// Result expressions in source order.
    pub projections: Vec<SqlProjection>,
    /// Original WHERE expression.
    pub selection: Option<SqlExpr>,
    /// Top-level conjunction leaves classified for planning.
    pub predicates: Vec<ClassifiedPredicate>,
    /// GROUP BY expressions with projection aliases and ordinals resolved.
    pub group_by: Vec<SqlGroupBy>,
    /// ORDER BY expressions with projection aliases and ordinals resolved.
    pub order_by: Vec<SqlOrderBy>,
    /// Optional non-negative row limit.
    pub limit: Option<u64>,
    /// Source span of the statement.
    pub span: SqlSpan,
}

/// One SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlProjection {
    /// `*`.
    Wildcard {
        /// Source span of the wildcard.
        span: SqlSpan,
    },
    /// An expression with an optional result-column alias.
    Expr {
        /// Projected expression.
        expr: SqlExpr,
        /// Optional `AS` alias.
        alias: Option<String>,
        /// Source span of the select item.
        span: SqlSpan,
    },
}

/// A source-spanned SQL expression.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlExpr {
    /// Expression-specific data.
    pub kind: SqlExprKind,
    /// Source span.
    pub span: SqlSpan,
}

/// SQL expression variants retained by the SQL-TS semantic layer.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlExprKind {
    /// A scalar or interval literal.
    Literal(SqlLiteral),
    /// A possibly qualified column reference.
    Column {
        /// Optional table or measurement alias.
        qualifier: Option<String>,
        /// Column name.
        name: String,
    },
    /// A binary expression.
    Binary {
        /// Left operand.
        left: Box<SqlExpr>,
        /// Binary operation.
        op: SqlBinaryOp,
        /// Right operand.
        right: Box<SqlExpr>,
    },
    /// A unary expression.
    Unary {
        /// Unary operation.
        op: SqlUnaryOp,
        /// Operand.
        expr: Box<SqlExpr>,
    },
    /// A resolved function call.
    Function(SqlFunction),
    /// A BETWEEN predicate.
    Between {
        /// Tested expression.
        expr: Box<SqlExpr>,
        /// Inclusive lower bound.
        low: Box<SqlExpr>,
        /// Inclusive upper bound.
        high: Box<SqlExpr>,
        /// Whether this is NOT BETWEEN.
        negated: bool,
    },
    /// A pattern predicate.
    Pattern {
        /// Tested expression.
        expr: Box<SqlExpr>,
        /// Pattern expression.
        pattern: Box<SqlExpr>,
        /// Optional escape expression.
        escape: Option<Box<SqlExpr>>,
        /// Whether this is the negated form.
        negated: bool,
        /// Pattern operation.
        kind: PatternKind,
    },
    /// An IN-list predicate.
    InList {
        /// Tested expression.
        expr: Box<SqlExpr>,
        /// Literal or expression candidates.
        list: Vec<SqlExpr>,
        /// Whether this is NOT IN.
        negated: bool,
    },
    /// An IS NULL predicate.
    IsNull {
        /// Tested expression.
        expr: Box<SqlExpr>,
        /// Whether this is IS NOT NULL.
        negated: bool,
    },
}

/// A SQL scalar or interval literal.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlLiteral {
    /// Numeric source spelling.
    Number(String),
    /// Decoded text.
    String(String),
    /// Boolean value.
    Boolean(bool),
    /// NULL.
    Null,
    /// SQL INTERVAL text without the keyword or quotes.
    Interval(String),
}

/// A SQL binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlBinaryOp {
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
    /// Equality.
    Eq,
    /// Inequality.
    NotEq,
    /// Less than.
    Lt,
    /// Greater than.
    Gt,
    /// Less than or equal.
    LtEq,
    /// Greater than or equal.
    GtEq,
    /// Boolean conjunction.
    And,
    /// Boolean disjunction.
    Or,
    /// String concatenation.
    StringConcat,
}

/// A SQL unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlUnaryOp {
    /// Boolean negation.
    Not,
    /// Numeric negation.
    Minus,
}

/// A SQL pattern matching operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternKind {
    /// LIKE.
    Like,
    /// Case-insensitive LIKE.
    ILike,
    /// Glob matching.
    Glob,
    /// SQL SIMILAR TO.
    SimilarTo,
}

/// A resolved SQL function.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlFunction {
    /// A time-series transformation shared with query-common.
    TimeSeries {
        /// Query-common function identity and normalized bare column names.
        function: TSFunction,
        /// Original qualified column references retained for schema validation.
        columns: Vec<SqlColumnReference>,
    },
    /// A standard aggregate.
    Aggregate {
        /// Aggregate identity.
        function: AggregateFunction,
        /// Aggregate input.
        argument: AggregateArgument,
        /// Whether DISTINCT applies to the aggregate input.
        distinct: bool,
    },
    /// Current evaluation time.
    Now,
}

/// One source-spanned column argument retained by a resolved function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumnReference {
    /// Optional measurement or measurement-alias qualifier.
    pub qualifier: Option<String>,
    /// Bare column name.
    pub name: String,
    /// Source span of the argument.
    pub span: SqlSpan,
}

/// A standard SQL aggregate supported by SQL-TS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    /// Average.
    Avg,
    /// Sum.
    Sum,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
    /// Count.
    Count,
}

/// Input to a standard aggregate.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateArgument {
    /// `COUNT(*)`.
    Wildcard,
    /// One expression argument.
    Expr(Box<SqlExpr>),
}

/// A GROUP BY item.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlGroupBy {
    /// A general expression.
    Expression(SqlExpr),
    /// A reference to an `AS` projection alias.
    ProjectionAlias {
        /// Alias text.
        alias: String,
        /// Zero-based projection index.
        projection_index: usize,
        /// Source span of the alias reference.
        span: SqlSpan,
    },
    /// A one-based projection ordinal such as `GROUP BY 1`.
    ProjectionOrdinal {
        /// One-based ordinal from the query.
        ordinal: usize,
        /// Zero-based projection index.
        projection_index: usize,
        /// Source span of the numeric reference.
        span: SqlSpan,
    },
}

/// One ORDER BY item.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlOrderBy {
    /// Resolved ordering key.
    pub key: SqlOrderKey,
    /// True for ascending order; omitted ASC/DESC defaults to ascending.
    pub ascending: bool,
    /// Optional NULLS FIRST/LAST request.
    pub nulls_first: Option<bool>,
    /// Source span.
    pub span: SqlSpan,
}

/// A resolved ORDER BY key.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlOrderKey {
    /// A general expression.
    Expression(SqlExpr),
    /// A projection alias.
    ProjectionAlias {
        /// Alias text.
        alias: String,
        /// Zero-based projection index.
        projection_index: usize,
        /// Source span of the alias reference.
        span: SqlSpan,
    },
    /// A one-based projection ordinal.
    ProjectionOrdinal {
        /// One-based ordinal from the query.
        ordinal: usize,
        /// Zero-based projection index.
        projection_index: usize,
        /// Source span of the numeric reference.
        span: SqlSpan,
    },
}

/// Schema-independent predicate class used by the planner and later typecheck.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredicateClass {
    /// A predicate over the reserved `time` column.
    Time,
    /// A syntactic tag candidate: string equality/inequality or string IN-list.
    Tag,
    /// A field or otherwise residual predicate.
    Field,
}

/// A top-level WHERE conjunction leaf with pushdown metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassifiedPredicate {
    /// Syntactic predicate class.
    pub class: PredicateClass,
    /// Whether its current shape can be pushed into a scan after typecheck.
    pub pushdown_eligible: bool,
    /// Preserved predicate expression.
    pub expr: SqlExpr,
}

/// Parses one SQL-TS SELECT through the shared Nim FFI bridge.
pub fn parse(input: &str) -> Result<SqlTsQuery> {
    let statements: Vec<WireStatement> = nimffi::parse(ParserLanguage::SqlTs, input)?;
    if statements.len() != 1 {
        return Err(unsupported(
            SqlSpan::unknown(),
            format!(
                "SQL-TS requires exactly one SELECT statement, got {}",
                statements.len()
            ),
        ));
    }
    map_statement(statements.into_iter().next().ok_or_else(|| {
        TsmError::FfiContract("single SQL statement disappeared during mapping".to_string())
    })?)
}

impl SqlSpan {
    const fn unknown() -> Self {
        Self {
            start: SqlPosition { line: 0, column: 0 },
            end: SqlPosition { line: 0, column: 0 },
        }
    }
}

fn map_statement(statement: WireStatement) -> Result<SqlTsQuery> {
    let span = map_span(statement.span)?;
    let variant = tagged_variant(&statement.kind)?.to_string();
    if variant != "Select" {
        return Err(unsupported(
            span,
            format!("SQL statement `{variant}` is outside SQL-TS v0.4"),
        ));
    }
    let select: WireSelect = decode_tagged(statement.kind, "Select statement")?;
    if select.variant != "Select" {
        return Err(TsmError::FfiContract(format!(
            "expected Select payload, got {}",
            select.variant
        )));
    }
    if select.projection.is_empty() {
        return Err(parse_error(span, "SELECT projection is empty".to_string()));
    }
    if select.having.is_some() {
        return Err(unsupported(span, "HAVING clause".to_string()));
    }
    if select.offset.is_some() {
        return Err(unsupported(span, "LIMIT/OFFSET".to_string()));
    }
    if select.from.len() != 1 {
        return Err(unsupported(
            span,
            format!(
                "SQL-TS requires one measurement in FROM, got {}",
                select.from.len()
            ),
        ));
    }

    let (measurement, measurement_alias) = map_from_item(
        select
            .from
            .into_iter()
            .next()
            .ok_or_else(|| TsmError::FfiContract("missing FROM item".to_string()))?,
    )?;
    let mut nodes = 0;
    let projections = select
        .projection
        .into_iter()
        .map(|item| map_projection(item, &mut nodes))
        .collect::<Result<Vec<_>>>()?;
    let selection = select
        .selection
        .map(|expr| map_expr(expr, 1, &mut nodes))
        .transpose()?;
    let predicates = selection
        .as_ref()
        .map(classify_predicates)
        .unwrap_or_default();
    let aliases = projection_aliases(&projections)?;
    let group_by = select
        .group_by
        .unwrap_or_default()
        .into_iter()
        .map(|expr| {
            let expr = map_expr(expr, 1, &mut nodes)?;
            resolve_group_by(expr, &aliases, projections.len())
        })
        .collect::<Result<Vec<_>>>()?;
    let order_by = select
        .order_by
        .into_iter()
        .map(|order| map_order_by(order, &aliases, projections.len(), &mut nodes))
        .collect::<Result<Vec<_>>>()?;
    let limit = select.limit.map(map_limit).transpose()?;

    Ok(SqlTsQuery {
        measurement,
        measurement_alias,
        distinct: select.distinct,
        projections,
        selection,
        predicates,
        group_by,
        order_by,
        limit,
        span,
    })
}

fn map_from_item(value: Value) -> Result<(String, Option<String>)> {
    let variant = tagged_variant(&value)?.to_string();
    if variant != "Table" {
        let payload: WireSpannedVariant = decode_tagged(value, "FROM item")?;
        return Err(unsupported(
            map_span(payload.span)?,
            format!("FROM item `{variant}`"),
        ));
    }
    let table: WireTableFrom = decode_tagged(value, "table FROM item")?;
    let span = map_span(table.span)?;
    if table.name.is_empty() {
        return Err(parse_error(
            span,
            "FROM measurement name is empty".to_string(),
        ));
    }
    Ok((table.name, table.alias))
}

fn map_projection(value: Value, nodes: &mut usize) -> Result<SqlProjection> {
    match tagged_variant(&value)? {
        "Wildcard" => {
            let wildcard: WireWildcard = decode_tagged(value, "wildcard projection")?;
            Ok(SqlProjection::Wildcard {
                span: map_span(wildcard.span)?,
            })
        }
        "Expr" => {
            let projection: WireExprProjection = decode_tagged(value, "expression projection")?;
            Ok(SqlProjection::Expr {
                expr: map_expr(projection.expr, 1, nodes)?,
                alias: projection.alias,
                span: map_span(projection.span)?,
            })
        }
        other => Err(TsmError::FfiContract(format!(
            "unknown SELECT projection variant `{other}`"
        ))),
    }
}

fn map_expr(wire: WireExpr, depth: usize, nodes: &mut usize) -> Result<SqlExpr> {
    if depth > MAX_SQL_AST_DEPTH {
        return Err(TsmError::ResourceLimit(format!(
            "SQL-TS AST depth exceeds {MAX_SQL_AST_DEPTH}"
        )));
    }
    *nodes += 1;
    if *nodes > MAX_SQL_AST_NODES {
        return Err(TsmError::ResourceLimit(format!(
            "SQL-TS AST node count exceeds {MAX_SQL_AST_NODES}"
        )));
    }

    let span = map_span(wire.span)?;
    let variant = tagged_variant(&wire.kind)?.to_string();
    let kind = match variant.as_str() {
        "Literal" => {
            let payload: WireLiteralExpr = decode_tagged(wire.kind, "literal expression")?;
            SqlExprKind::Literal(map_literal(payload.literal)?)
        }
        "ColumnRef" => {
            let payload: WireColumnExpr = decode_tagged(wire.kind, "column expression")?;
            SqlExprKind::Column {
                qualifier: payload.table,
                name: payload.column,
            }
        }
        "BinaryOp" => {
            let payload: WireBinaryExpr = decode_tagged(wire.kind, "binary expression")?;
            SqlExprKind::Binary {
                left: Box::new(map_expr(payload.left, depth + 1, nodes)?),
                op: payload.op.into(),
                right: Box::new(map_expr(payload.right, depth + 1, nodes)?),
            }
        }
        "UnaryOp" => {
            let payload: WireUnaryExpr = decode_tagged(wire.kind, "unary expression")?;
            SqlExprKind::Unary {
                op: payload.op.into(),
                expr: Box::new(map_expr(payload.operand, depth + 1, nodes)?),
            }
        }
        "FunctionCall" => {
            let payload: WireFunctionExpr = decode_tagged(wire.kind, "function expression")?;
            let arguments = payload
                .args
                .into_iter()
                .map(|argument| map_expr(argument, depth + 1, nodes))
                .collect::<Result<Vec<_>>>()?;
            SqlExprKind::Function(map_function(
                &payload.name,
                arguments,
                payload.distinct,
                payload.star,
                span,
            )?)
        }
        "Between" => {
            let payload: WireBetweenExpr = decode_tagged(wire.kind, "BETWEEN expression")?;
            SqlExprKind::Between {
                expr: Box::new(map_expr(payload.expr, depth + 1, nodes)?),
                low: Box::new(map_expr(payload.low, depth + 1, nodes)?),
                high: Box::new(map_expr(payload.high, depth + 1, nodes)?),
                negated: payload.negated,
            }
        }
        "Like" => {
            let payload: WirePatternExpr = decode_tagged(wire.kind, "pattern expression")?;
            SqlExprKind::Pattern {
                expr: Box::new(map_expr(payload.expr, depth + 1, nodes)?),
                pattern: Box::new(map_expr(payload.pattern, depth + 1, nodes)?),
                escape: payload
                    .escape
                    .map(|escape| map_expr(escape, depth + 1, nodes).map(Box::new))
                    .transpose()?,
                negated: payload.negated,
                kind: payload.kind.into(),
            }
        }
        "InList" => {
            let payload: WireInListExpr = decode_tagged(wire.kind, "IN-list expression")?;
            SqlExprKind::InList {
                expr: Box::new(map_expr(payload.expr, depth + 1, nodes)?),
                list: payload
                    .list
                    .into_iter()
                    .map(|item| map_expr(item, depth + 1, nodes))
                    .collect::<Result<Vec<_>>>()?,
                negated: payload.negated,
            }
        }
        "IsNull" => {
            let payload: WireIsNullExpr = decode_tagged(wire.kind, "IS NULL expression")?;
            SqlExprKind::IsNull {
                expr: Box::new(map_expr(payload.expr, depth + 1, nodes)?),
                negated: payload.negated,
            }
        }
        "VectorLiteral" | "ScalarSubquery" | "InSubquery" | "Exists" | "Quantified" => {
            return Err(unsupported(span, format!("SQL expression `{variant}`")));
        }
        other => {
            return Err(TsmError::FfiContract(format!(
                "unknown SQL expression variant `{other}`"
            )));
        }
    };
    Ok(SqlExpr { kind, span })
}

fn map_literal(value: Value) -> Result<SqlLiteral> {
    let literal: WireLiteral = decode_tagged(value, "literal")?;
    match literal.variant.as_str() {
        "Number" => literal
            .value
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .map(SqlLiteral::Number)
            .ok_or_else(|| literal_contract_error("Number", "string")),
        "String" => literal
            .value
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .map(SqlLiteral::String)
            .ok_or_else(|| literal_contract_error("String", "string")),
        "Interval" => literal
            .value
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .map(SqlLiteral::Interval)
            .ok_or_else(|| literal_contract_error("Interval", "string")),
        "Boolean" => literal
            .value
            .and_then(|value| value.as_bool())
            .map(SqlLiteral::Boolean)
            .ok_or_else(|| literal_contract_error("Boolean", "boolean")),
        "Null" if literal.value.is_none() => Ok(SqlLiteral::Null),
        "Null" => Err(TsmError::FfiContract(
            "Null literal unexpectedly contains a value".to_string(),
        )),
        other => Err(TsmError::FfiContract(format!(
            "unknown SQL literal variant `{other}`"
        ))),
    }
}

fn literal_contract_error(variant: &str, expected: &str) -> TsmError {
    TsmError::FfiContract(format!("{variant} literal is missing its {expected} value"))
}

fn map_function(
    name: &str,
    arguments: Vec<SqlExpr>,
    distinct: bool,
    star: bool,
    span: SqlSpan,
) -> Result<SqlFunction> {
    let normalized = name.to_ascii_uppercase();
    let time_series = match normalized.as_str() {
        "TIME_BUCKET" => {
            reject_function_flags(name, distinct, star, span)?;
            expect_argument_count(name, &arguments, 2, span)?;
            let column = column_argument(name, &arguments[1])?;
            Some((
                TSFunction::TimeBucket {
                    interval: duration_argument(&arguments[0])?,
                    column: column.name.clone(),
                },
                vec![column],
            ))
        }
        "RATE" => {
            reject_function_flags(name, distinct, star, span)?;
            expect_argument_count(name, &arguments, 1, span)?;
            let column = column_argument(name, &arguments[0])?;
            Some((
                TSFunction::Rate {
                    column: column.name.clone(),
                },
                vec![column],
            ))
        }
        "DELTA" => {
            reject_function_flags(name, distinct, star, span)?;
            expect_argument_count(name, &arguments, 1, span)?;
            let column = column_argument(name, &arguments[0])?;
            Some((
                TSFunction::Delta {
                    column: column.name.clone(),
                },
                vec![column],
            ))
        }
        "DERIVATIVE" => {
            reject_function_flags(name, distinct, star, span)?;
            expect_argument_count(name, &arguments, 1, span)?;
            let column = column_argument(name, &arguments[0])?;
            Some((
                TSFunction::Derivative {
                    column: column.name.clone(),
                },
                vec![column],
            ))
        }
        "FIRST" | "LAST" => {
            reject_function_flags(name, distinct, star, span)?;
            expect_argument_count(name, &arguments, 2, span)?;
            let value_column = column_argument(name, &arguments[0])?;
            let time_column = column_argument(name, &arguments[1])?;
            let function = if normalized == "FIRST" {
                TSFunction::First {
                    value_column: value_column.name.clone(),
                    time_column: time_column.name.clone(),
                }
            } else {
                TSFunction::Last {
                    value_column: value_column.name.clone(),
                    time_column: time_column.name.clone(),
                }
            };
            Some((function, vec![value_column, time_column]))
        }
        "HISTOGRAM_QUANTILE" => {
            reject_function_flags(name, distinct, star, span)?;
            expect_argument_count(name, &arguments, 2, span)?;
            let quantile = numeric_argument(name, &arguments[0])?;
            if !(0.0..=1.0).contains(&quantile) {
                return Err(type_error(
                    arguments[0].span,
                    format!("{name} quantile must be between 0 and 1"),
                ));
            }
            let column = column_argument(name, &arguments[1])?;
            Some((
                TSFunction::HistogramQuantile {
                    quantile,
                    column: column.name.clone(),
                },
                vec![column],
            ))
        }
        _ => None,
    };
    if let Some((function, columns)) = time_series {
        return Ok(SqlFunction::TimeSeries { function, columns });
    }

    if normalized == "NOW" {
        reject_function_flags(name, distinct, star, span)?;
        expect_argument_count(name, &arguments, 0, span)?;
        return Ok(SqlFunction::Now);
    }

    let aggregate = match normalized.as_str() {
        "AVG" => Some(AggregateFunction::Avg),
        "SUM" => Some(AggregateFunction::Sum),
        "MIN" => Some(AggregateFunction::Min),
        "MAX" => Some(AggregateFunction::Max),
        "COUNT" => Some(AggregateFunction::Count),
        _ => None,
    };
    let Some(function) = aggregate else {
        return Err(unsupported(span, format!("SQL function `{name}`")));
    };
    let argument = if star {
        if function != AggregateFunction::Count || !arguments.is_empty() || distinct {
            return Err(type_error(
                span,
                "only COUNT(*) is supported for wildcard aggregates".to_string(),
            ));
        }
        AggregateArgument::Wildcard
    } else {
        expect_argument_count(name, &arguments, 1, span)?;
        AggregateArgument::Expr(Box::new(arguments.into_iter().next().ok_or_else(|| {
            TsmError::FfiContract("aggregate argument vanished".to_string())
        })?))
    };
    Ok(SqlFunction::Aggregate {
        function,
        argument,
        distinct,
    })
}

fn reject_function_flags(name: &str, distinct: bool, star: bool, span: SqlSpan) -> Result<()> {
    if distinct || star {
        return Err(type_error(
            span,
            format!("{name} does not accept DISTINCT or *"),
        ));
    }
    Ok(())
}

fn expect_argument_count(
    name: &str,
    arguments: &[SqlExpr],
    expected: usize,
    span: SqlSpan,
) -> Result<()> {
    if arguments.len() != expected {
        return Err(type_error(
            span,
            format!(
                "{name} expects {expected} argument(s), got {}",
                arguments.len()
            ),
        ));
    }
    Ok(())
}

fn column_argument(function: &str, expression: &SqlExpr) -> Result<SqlColumnReference> {
    match &expression.kind {
        SqlExprKind::Column { qualifier, name } if name != "*" => Ok(SqlColumnReference {
            qualifier: qualifier.clone(),
            name: name.clone(),
            span: expression.span,
        }),
        _ => Err(type_error(
            expression.span,
            format!("{function} expects a column argument"),
        )),
    }
}

fn numeric_argument(function: &str, expression: &SqlExpr) -> Result<f64> {
    let SqlExprKind::Literal(SqlLiteral::Number(raw)) = &expression.kind else {
        return Err(type_error(
            expression.span,
            format!("{function} expects a numeric scalar argument"),
        ));
    };
    raw.parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            type_error(
                expression.span,
                format!("{function} received invalid number `{raw}`"),
            )
        })
}

fn duration_argument(expression: &SqlExpr) -> Result<Duration> {
    let raw = match &expression.kind {
        SqlExprKind::Literal(SqlLiteral::String(raw) | SqlLiteral::Interval(raw)) => raw,
        _ => {
            return Err(type_error(
                expression.span,
                "TIME_BUCKET expects a string or INTERVAL duration".to_string(),
            ));
        }
    };
    parse_duration(raw).map_err(|message| type_error(expression.span, message))
}

fn parse_duration(raw: &str) -> std::result::Result<Duration, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err("duration must not be empty".to_string());
    }

    let (number, unit) = if let Some(split) = normalized.find(char::is_whitespace) {
        let (number, unit) = normalized.split_at(split);
        (number.trim(), unit.trim())
    } else {
        let split = normalized
            .find(|character: char| !character.is_ascii_digit())
            .ok_or_else(|| format!("duration `{raw}` is missing a unit"))?;
        normalized.split_at(split)
    };
    let value = number
        .parse::<u64>()
        .map_err(|_| format!("duration `{raw}` has an invalid number"))?;
    if value == 0 {
        return Err("duration must be greater than zero".to_string());
    }

    let milliseconds_per_unit = match unit {
        "ms" | "millisecond" | "milliseconds" => 1,
        "s" | "second" | "seconds" => 1_000,
        "m" | "minute" | "minutes" => 60_000,
        "h" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        "w" | "week" | "weeks" => 604_800_000,
        _ => return Err(format!("duration `{raw}` has unsupported unit `{unit}`")),
    };
    value
        .checked_mul(milliseconds_per_unit)
        .map(Duration::from_millis)
        .ok_or_else(|| format!("duration `{raw}` overflows"))
}

fn projection_aliases(projections: &[SqlProjection]) -> Result<Vec<(String, usize)>> {
    let mut aliases: Vec<(String, usize)> = Vec::new();
    for (index, projection) in projections.iter().enumerate() {
        let SqlProjection::Expr {
            alias: Some(alias),
            span,
            ..
        } = projection
        else {
            continue;
        };
        if aliases
            .iter()
            .any(|(existing, _)| alias.eq_ignore_ascii_case(existing))
        {
            return Err(type_error(
                *span,
                format!("duplicate projection alias `{alias}`"),
            ));
        }
        aliases.push((alias.clone(), index));
    }
    Ok(aliases)
}

fn resolve_group_by(
    expression: SqlExpr,
    aliases: &[(String, usize)],
    projection_count: usize,
) -> Result<SqlGroupBy> {
    if let Some((alias, projection_index)) = alias_reference(&expression, aliases) {
        return Ok(SqlGroupBy::ProjectionAlias {
            alias,
            projection_index,
            span: expression.span,
        });
    }
    if let Some((ordinal, projection_index)) = projection_ordinal(&expression, projection_count)? {
        return Ok(SqlGroupBy::ProjectionOrdinal {
            ordinal,
            projection_index,
            span: expression.span,
        });
    }
    Ok(SqlGroupBy::Expression(expression))
}

fn map_order_by(
    wire: WireOrderBy,
    aliases: &[(String, usize)],
    projection_count: usize,
    nodes: &mut usize,
) -> Result<SqlOrderBy> {
    let span = map_span(wire.span)?;
    let expression = map_expr(wire.expr, 1, nodes)?;
    let key = if let Some((alias, projection_index)) = alias_reference(&expression, aliases) {
        SqlOrderKey::ProjectionAlias {
            alias,
            projection_index,
            span: expression.span,
        }
    } else if let Some((ordinal, projection_index)) =
        projection_ordinal(&expression, projection_count)?
    {
        SqlOrderKey::ProjectionOrdinal {
            ordinal,
            projection_index,
            span: expression.span,
        }
    } else {
        SqlOrderKey::Expression(expression)
    };
    Ok(SqlOrderBy {
        key,
        ascending: wire.asc.unwrap_or(true),
        nulls_first: wire.nulls_first,
        span,
    })
}

fn alias_reference(expression: &SqlExpr, aliases: &[(String, usize)]) -> Option<(String, usize)> {
    let SqlExprKind::Column {
        qualifier: None,
        name,
    } = &expression.kind
    else {
        return None;
    };
    aliases
        .iter()
        .find(|(alias, _)| name.eq_ignore_ascii_case(alias))
        .map(|(alias, index)| (alias.clone(), *index))
}

fn projection_ordinal(
    expression: &SqlExpr,
    projection_count: usize,
) -> Result<Option<(usize, usize)>> {
    let SqlExprKind::Literal(SqlLiteral::Number(raw)) = &expression.kind else {
        return Ok(None);
    };
    if raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
        return Ok(None);
    }
    let ordinal = raw.parse::<usize>().map_err(|_| {
        type_error(
            expression.span,
            format!("invalid projection ordinal `{raw}`"),
        )
    })?;
    if ordinal == 0 || ordinal > projection_count {
        return Err(type_error(
            expression.span,
            format!("projection ordinal {ordinal} is outside 1..={projection_count}"),
        ));
    }
    Ok(Some((ordinal, ordinal - 1)))
}

fn map_limit(wire: WireExpr) -> Result<u64> {
    let mut nodes = 0;
    let expression = map_expr(wire, 1, &mut nodes)?;
    let SqlExprKind::Literal(SqlLiteral::Number(raw)) = &expression.kind else {
        return Err(type_error(
            expression.span,
            "LIMIT must be a non-negative integer literal".to_string(),
        ));
    };
    raw.parse::<u64>().map_err(|_| {
        type_error(
            expression.span,
            format!("LIMIT `{raw}` is not a non-negative integer"),
        )
    })
}

fn classify_predicates(expression: &SqlExpr) -> Vec<ClassifiedPredicate> {
    let mut predicates = Vec::new();
    collect_predicates(expression, &mut predicates);
    predicates
}

fn collect_predicates(expression: &SqlExpr, output: &mut Vec<ClassifiedPredicate>) {
    if let SqlExprKind::Binary {
        left,
        op: SqlBinaryOp::And,
        right,
    } = &expression.kind
    {
        collect_predicates(left, output);
        collect_predicates(right, output);
        return;
    }

    let class = if contains_time_column(expression) {
        PredicateClass::Time
    } else if is_tag_candidate(expression) {
        PredicateClass::Tag
    } else {
        PredicateClass::Field
    };
    let pushdown_eligible = match class {
        PredicateClass::Time => is_pushdown_time_predicate(expression),
        PredicateClass::Tag => true,
        PredicateClass::Field => false,
    };
    output.push(ClassifiedPredicate {
        class,
        pushdown_eligible,
        expr: expression.clone(),
    });
}

fn contains_time_column(expression: &SqlExpr) -> bool {
    match &expression.kind {
        SqlExprKind::Column { name, .. } => name.eq_ignore_ascii_case("time"),
        SqlExprKind::Binary { left, right, .. } => {
            contains_time_column(left) || contains_time_column(right)
        }
        SqlExprKind::Unary { expr, .. } | SqlExprKind::IsNull { expr, .. } => {
            contains_time_column(expr)
        }
        SqlExprKind::Between {
            expr, low, high, ..
        } => contains_time_column(expr) || contains_time_column(low) || contains_time_column(high),
        SqlExprKind::Pattern {
            expr,
            pattern,
            escape,
            ..
        } => {
            contains_time_column(expr)
                || contains_time_column(pattern)
                || escape.as_deref().is_some_and(contains_time_column)
        }
        SqlExprKind::InList { expr, list, .. } => {
            contains_time_column(expr) || list.iter().any(contains_time_column)
        }
        SqlExprKind::Function(SqlFunction::TimeSeries {
            function: TSFunction::TimeBucket { column, .. },
            ..
        }) => column.eq_ignore_ascii_case("time"),
        SqlExprKind::Literal(_) | SqlExprKind::Function(_) => false,
    }
}

fn is_pushdown_time_predicate(expression: &SqlExpr) -> bool {
    match &expression.kind {
        SqlExprKind::Binary {
            left,
            op:
                SqlBinaryOp::Eq
                | SqlBinaryOp::Lt
                | SqlBinaryOp::Gt
                | SqlBinaryOp::LtEq
                | SqlBinaryOp::GtEq,
            right,
        } => is_time_column(left) ^ is_time_column(right),
        SqlExprKind::Between { expr, .. } => is_time_column(expr),
        _ => false,
    }
}

fn is_tag_candidate(expression: &SqlExpr) -> bool {
    match &expression.kind {
        SqlExprKind::Binary {
            left,
            op: SqlBinaryOp::Eq | SqlBinaryOp::NotEq,
            right,
        } => {
            (is_column(left) && is_string_literal(right))
                || (is_column(right) && is_string_literal(left))
        }
        SqlExprKind::InList { expr, list, .. } => {
            is_column(expr) && !list.is_empty() && list.iter().all(is_string_literal)
        }
        _ => false,
    }
}

fn is_time_column(expression: &SqlExpr) -> bool {
    matches!(
        &expression.kind,
        SqlExprKind::Column { name, .. } if name.eq_ignore_ascii_case("time")
    )
}

fn is_column(expression: &SqlExpr) -> bool {
    matches!(&expression.kind, SqlExprKind::Column { .. })
}

fn is_string_literal(expression: &SqlExpr) -> bool {
    matches!(
        &expression.kind,
        SqlExprKind::Literal(SqlLiteral::String(_))
    )
}

fn tagged_variant(value: &Value) -> Result<&str> {
    value
        .as_object()
        .and_then(|object| object.get("variant"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            TsmError::FfiContract("wire AST tagged value is missing string `variant`".to_string())
        })
}

fn decode_tagged<T>(value: Value, context: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| {
        TsmError::FfiContract(format!("invalid {context} in SQL wire AST: {error}"))
    })
}

fn map_span(wire: WireSpan) -> Result<SqlSpan> {
    let start = map_position(wire.start)?;
    let end = map_position(wire.end)?;
    if start.line != 0
        && end.line != 0
        && (end.line < start.line || (end.line == start.line && end.column < start.column))
    {
        return Err(TsmError::FfiContract(
            "SQL span ends before it starts".to_string(),
        ));
    }
    Ok(SqlSpan { start, end })
}

fn map_position(wire: WirePosition) -> Result<SqlPosition> {
    Ok(SqlPosition {
        line: usize::try_from(wire.line)
            .map_err(|_| TsmError::FfiContract("SQL source line overflows usize".to_string()))?,
        column: usize::try_from(wire.column)
            .map_err(|_| TsmError::FfiContract("SQL source column overflows usize".to_string()))?,
    })
}

fn parse_error(span: SqlSpan, message: String) -> TsmError {
    TsmError::Parse {
        language: "SQL-TS".to_string(),
        message,
        line: span.start.line,
        column: span.start.column,
        offset: 0,
    }
}

fn type_error(span: SqlSpan, message: String) -> TsmError {
    TsmError::Type {
        message,
        line: span.start.line,
        column: span.start.column,
        offset: 0,
    }
}

fn unsupported(span: SqlSpan, feature: String) -> TsmError {
    TsmError::Unsupported {
        feature,
        line: span.start.line,
        column: span.start.column,
        offset: 0,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireStatement {
    kind: Value,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSelect {
    variant: String,
    distinct: bool,
    projection: Vec<Value>,
    #[serde(rename = "from")]
    from: Vec<Value>,
    selection: Option<WireExpr>,
    group_by: Option<Vec<WireExpr>>,
    having: Option<WireExpr>,
    order_by: Vec<WireOrderBy>,
    limit: Option<WireExpr>,
    offset: Option<WireExpr>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTableFrom {
    #[serde(rename = "variant")]
    _variant: String,
    name: String,
    alias: Option<String>,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
struct WireSpannedVariant {
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireWildcard {
    #[serde(rename = "variant")]
    _variant: String,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireExprProjection {
    #[serde(rename = "variant")]
    _variant: String,
    expr: WireExpr,
    alias: Option<String>,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOrderBy {
    expr: WireExpr,
    asc: Option<bool>,
    nulls_first: Option<bool>,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireExpr {
    kind: Value,
    span: WireSpan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireLiteralExpr {
    #[serde(rename = "variant")]
    _variant: String,
    literal: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireColumnExpr {
    #[serde(rename = "variant")]
    _variant: String,
    table: Option<String>,
    column: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireBinaryExpr {
    #[serde(rename = "variant")]
    _variant: String,
    left: WireExpr,
    op: WireBinaryOp,
    right: WireExpr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireUnaryExpr {
    #[serde(rename = "variant")]
    _variant: String,
    op: WireUnaryOp,
    operand: WireExpr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFunctionExpr {
    #[serde(rename = "variant")]
    _variant: String,
    name: String,
    args: Vec<WireExpr>,
    distinct: bool,
    star: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireBetweenExpr {
    #[serde(rename = "variant")]
    _variant: String,
    expr: WireExpr,
    low: WireExpr,
    high: WireExpr,
    negated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePatternExpr {
    #[serde(rename = "variant")]
    _variant: String,
    expr: WireExpr,
    pattern: WireExpr,
    escape: Option<WireExpr>,
    negated: bool,
    kind: WirePatternKind,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireInListExpr {
    #[serde(rename = "variant")]
    _variant: String,
    expr: WireExpr,
    list: Vec<WireExpr>,
    negated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireIsNullExpr {
    #[serde(rename = "variant")]
    _variant: String,
    expr: WireExpr,
    negated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireLiteral {
    variant: String,
    #[serde(default)]
    value: Option<Value>,
}

#[derive(Debug, Deserialize)]
enum WireBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    StringConcat,
}

impl From<WireBinaryOp> for SqlBinaryOp {
    fn from(value: WireBinaryOp) -> Self {
        match value {
            WireBinaryOp::Add => Self::Add,
            WireBinaryOp::Sub => Self::Sub,
            WireBinaryOp::Mul => Self::Mul,
            WireBinaryOp::Div => Self::Div,
            WireBinaryOp::Mod => Self::Mod,
            WireBinaryOp::Eq => Self::Eq,
            WireBinaryOp::Neq => Self::NotEq,
            WireBinaryOp::Lt => Self::Lt,
            WireBinaryOp::Gt => Self::Gt,
            WireBinaryOp::LtEq => Self::LtEq,
            WireBinaryOp::GtEq => Self::GtEq,
            WireBinaryOp::And => Self::And,
            WireBinaryOp::Or => Self::Or,
            WireBinaryOp::StringConcat => Self::StringConcat,
        }
    }
}

#[derive(Debug, Deserialize)]
enum WireUnaryOp {
    Not,
    Minus,
}

impl From<WireUnaryOp> for SqlUnaryOp {
    fn from(value: WireUnaryOp) -> Self {
        match value {
            WireUnaryOp::Not => Self::Not,
            WireUnaryOp::Minus => Self::Minus,
        }
    }
}

#[derive(Debug, Deserialize)]
enum WirePatternKind {
    Like,
    ILike,
    Glob,
    SimilarTo,
}

impl From<WirePatternKind> for PatternKind {
    fn from(value: WirePatternKind) -> Self {
        match value {
            WirePatternKind::Like => Self::Like,
            WirePatternKind::ILike => Self::ILike,
            WirePatternKind::Glob => Self::Glob,
            WirePatternKind::SimilarTo => Self::SimilarTo,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePosition {
    line: u64,
    column: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSpan {
    start: WirePosition,
    end: WirePosition,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span() -> SqlSpan {
        SqlSpan {
            start: SqlPosition { line: 1, column: 1 },
            end: SqlPosition { line: 1, column: 2 },
        }
    }

    fn column(name: &str) -> SqlExpr {
        SqlExpr {
            kind: SqlExprKind::Column {
                qualifier: None,
                name: name.to_string(),
            },
            span: span(),
        }
    }

    fn string(value: &str) -> SqlExpr {
        SqlExpr {
            kind: SqlExprKind::Literal(SqlLiteral::String(value.to_string())),
            span: span(),
        }
    }

    #[test]
    fn duration_parser_is_bounded_and_accepts_short_and_sql_units() {
        assert_eq!(parse_duration("1ms").unwrap(), Duration::from_millis(1));
        assert_eq!(parse_duration("2 seconds").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("3m").unwrap(), Duration::from_secs(180));
        assert_eq!(
            parse_duration("4 hours").unwrap(),
            Duration::from_secs(14_400)
        );
        assert_eq!(
            parse_duration("1 day").unwrap(),
            Duration::from_secs(86_400)
        );
        assert_eq!(
            parse_duration("1 week").unwrap(),
            Duration::from_secs(604_800)
        );
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("18446744073709551615w").is_err());
        assert!(parse_duration("1 month").is_err());
    }

    #[test]
    fn compound_or_is_classified_but_never_marked_for_pushdown() {
        let time = SqlExpr {
            kind: SqlExprKind::Binary {
                left: Box::new(column("time")),
                op: SqlBinaryOp::Gt,
                right: Box::new(SqlExpr {
                    kind: SqlExprKind::Literal(SqlLiteral::Number("0".to_string())),
                    span: span(),
                }),
            },
            span: span(),
        };
        let tag = SqlExpr {
            kind: SqlExprKind::Binary {
                left: Box::new(column("host")),
                op: SqlBinaryOp::Eq,
                right: Box::new(string("api")),
            },
            span: span(),
        };
        let predicate = SqlExpr {
            kind: SqlExprKind::Binary {
                left: Box::new(time),
                op: SqlBinaryOp::Or,
                right: Box::new(tag),
            },
            span: span(),
        };

        let classified = classify_predicates(&predicate);
        assert_eq!(classified.len(), 1);
        assert_eq!(classified[0].class, PredicateClass::Time);
        assert!(!classified[0].pushdown_eligible);
    }
}
