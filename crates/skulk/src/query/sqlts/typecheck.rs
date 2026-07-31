//! SQL-TS expression type inference against a composed measurement schema.

use super::{
    AggregateArgument, AggregateFunction, PredicateClass, SqlBinaryOp, SqlColumnReference, SqlExpr,
    SqlExprKind, SqlFunction, SqlGroupBy, SqlLiteral, SqlOrderKey, SqlProjection, SqlSpan,
    SqlTsQuery, SqlUnaryOp,
};
use crate::query::TSFunction;
use crate::store::schema::{ColumnDataType, ColumnKind, ColumnSchema, MeasurementSchema};
use crate::{Result, TsmError};

/// Query-visible scalar types after SQL-TS inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlValueType {
    /// Nanoseconds since the Unix epoch.
    TimestampNanosecond,
    /// IEEE-754 double precision.
    Float64,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 64-bit integer.
    UInt64,
    /// Boolean.
    Boolean,
    /// UTF-8 string.
    Utf8,
}

impl SqlValueType {
    const fn is_numeric(self) -> bool {
        matches!(self, Self::Float64 | Self::Int64 | Self::UInt64)
    }

    const fn name(self) -> &'static str {
        match self {
            Self::TimestampNanosecond => "timestamp",
            Self::Float64 => "float64",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
            Self::Boolean => "boolean",
            Self::Utf8 => "string",
        }
    }
}

impl From<ColumnDataType> for SqlValueType {
    fn from(value: ColumnDataType) -> Self {
        match value {
            ColumnDataType::TimestampNanosecond => Self::TimestampNanosecond,
            ColumnDataType::Float64 => Self::Float64,
            ColumnDataType::Int64 => Self::Int64,
            ColumnDataType::UInt64 => Self::UInt64,
            ColumnDataType::Boolean => Self::Boolean,
            ColumnDataType::Utf8 => Self::Utf8,
        }
    }
}

/// One wildcard-expanded output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedColumn {
    /// SQL-visible column name (`time` denotes the physical `_time` column).
    pub name: String,
    /// Tag, field, or system-time role.
    pub kind: ColumnKind,
    /// Logical scalar type.
    pub data_type: SqlValueType,
}

/// Inferred output shape for one SELECT item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedProjection {
    /// `*`, expanded from the composed schema in deterministic column order.
    Wildcard {
        /// Columns selected by the wildcard.
        columns: Vec<TypedColumn>,
    },
    /// One expression result.
    Expr {
        /// Optional output alias retained from the parsed query.
        alias: Option<String>,
        /// Inferred result type.
        data_type: SqlValueType,
    },
}

/// A SQL-TS query whose references and expression types are validated.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedSqlTsQuery {
    /// Validated query AST. Predicate classes are refined using schema roles.
    pub query: SqlTsQuery,
    /// Output types aligned one-to-one with `query.projections`.
    pub projections: Vec<TypedProjection>,
}

/// Resolves every SQL-TS reference and expression against one composed schema.
///
/// Numeric literals are context-sensitive, but two concrete column types are
/// never promoted or coerced. Schema conflicts are returned by
/// [`crate::store::schema::SchemaResolver`] before this function is called.
pub fn typecheck(mut query: SqlTsQuery, schema: &MeasurementSchema) -> Result<TypedSqlTsQuery> {
    if query.measurement != schema.measurement() {
        return Err(type_error(
            query.span,
            format!(
                "query measurement `{}` does not match composed schema `{}`",
                query.measurement,
                schema.measurement()
            ),
        ));
    }

    let checker = TypeChecker {
        schema,
        measurement: &query.measurement,
        measurement_alias: query.measurement_alias.as_deref(),
    };

    let projections = query
        .projections
        .iter()
        .map(|projection| checker.projection(projection))
        .collect::<Result<Vec<_>>>()?;

    if let Some(selection) = &query.selection {
        checker.require_type(
            checker.infer(selection)?,
            SqlValueType::Boolean,
            selection.span,
            "WHERE expression",
        )?;
    }

    for group in &query.group_by {
        match group {
            SqlGroupBy::Expression(expression) => {
                checker.finalize(checker.infer(expression)?, expression.span)?;
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
            } => validate_projection_reference(*projection_index, &projections, *span)?,
        }
    }
    for order in &query.order_by {
        match &order.key {
            SqlOrderKey::Expression(expression) => {
                checker.finalize(checker.infer(expression)?, expression.span)?;
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
            } => validate_projection_reference(*projection_index, &projections, *span)?,
        }
    }

    for predicate in &mut query.predicates {
        let mut kinds = Vec::new();
        checker.collect_column_kinds(&predicate.expr, &mut kinds)?;
        predicate.class = if kinds.contains(&ColumnKind::Time) {
            PredicateClass::Time
        } else if !kinds.is_empty() && kinds.iter().all(|kind| *kind == ColumnKind::Tag) {
            PredicateClass::Tag
        } else {
            PredicateClass::Field
        };
        if predicate.class == PredicateClass::Field {
            predicate.pushdown_eligible = false;
        }
    }

    Ok(TypedSqlTsQuery { query, projections })
}

fn validate_projection_reference(
    projection_index: usize,
    projections: &[TypedProjection],
    span: SqlSpan,
) -> Result<()> {
    let Some(projection) = projections.get(projection_index) else {
        return Err(type_error(
            span,
            format!("projection index {projection_index} is outside the SELECT list"),
        ));
    };
    if matches!(projection, TypedProjection::Wildcard { .. }) {
        return Err(type_error(
            span,
            "GROUP BY or ORDER BY cannot reference a wildcard projection".to_string(),
        ));
    }
    Ok(())
}

struct TypeChecker<'a> {
    schema: &'a MeasurementSchema,
    measurement: &'a str,
    measurement_alias: Option<&'a str>,
}

impl TypeChecker<'_> {
    fn projection(&self, projection: &SqlProjection) -> Result<TypedProjection> {
        match projection {
            SqlProjection::Wildcard { .. } => Ok(TypedProjection::Wildcard {
                columns: self
                    .schema
                    .columns()
                    .iter()
                    .map(|(name, column)| TypedColumn {
                        name: if column.kind() == ColumnKind::Time {
                            "time".to_string()
                        } else {
                            name.clone()
                        },
                        kind: column.kind(),
                        data_type: column.data_type().into(),
                    })
                    .collect(),
            }),
            SqlProjection::Expr {
                expr, alias, span, ..
            } => Ok(TypedProjection::Expr {
                alias: alias.clone(),
                data_type: self.finalize(self.infer(expr)?, *span)?,
            }),
        }
    }

    fn infer(&self, expression: &SqlExpr) -> Result<ExprType> {
        match &expression.kind {
            SqlExprKind::Literal(literal) => self.literal(literal, expression.span),
            SqlExprKind::Column { qualifier, name } => {
                self.validate_qualifier(qualifier.as_deref(), expression.span)?;
                Ok(ExprType::Scalar(
                    self.resolve_column(name, expression.span)?
                        .data_type()
                        .into(),
                ))
            }
            SqlExprKind::Binary { left, op, right } => {
                self.binary(left, *op, right, expression.span)
            }
            SqlExprKind::Unary { op, expr } => self.unary(*op, expr, expression.span),
            SqlExprKind::Function(function) => self.function(function, expression.span),
            SqlExprKind::Between {
                expr, low, high, ..
            } => {
                let value_type = self.infer(expr)?;
                self.require_compatible(
                    value_type,
                    self.infer(low)?,
                    low.span,
                    "BETWEEN lower bound",
                )?;
                self.require_compatible(
                    value_type,
                    self.infer(high)?,
                    high.span,
                    "BETWEEN upper bound",
                )?;
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlExprKind::Pattern {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.require_type(
                    self.infer(expr)?,
                    SqlValueType::Utf8,
                    expr.span,
                    "pattern input",
                )?;
                self.require_type(
                    self.infer(pattern)?,
                    SqlValueType::Utf8,
                    pattern.span,
                    "pattern",
                )?;
                if let Some(escape) = escape {
                    self.require_type(
                        self.infer(escape)?,
                        SqlValueType::Utf8,
                        escape.span,
                        "pattern escape",
                    )?;
                }
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlExprKind::InList { expr, list, .. } => {
                let value_type = self.infer(expr)?;
                for item in list {
                    self.require_compatible(
                        value_type,
                        self.infer(item)?,
                        item.span,
                        "IN-list item",
                    )?;
                }
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlExprKind::IsNull { expr, .. } => {
                self.infer(expr)?;
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
        }
    }

    fn literal(&self, literal: &SqlLiteral, span: SqlSpan) -> Result<ExprType> {
        match literal {
            SqlLiteral::Number(raw) => Ok(ExprType::Number(NumberLiteral::parse(raw, span)?)),
            SqlLiteral::String(_) => Ok(ExprType::Scalar(SqlValueType::Utf8)),
            SqlLiteral::Boolean(_) => Ok(ExprType::Scalar(SqlValueType::Boolean)),
            SqlLiteral::Null => Ok(ExprType::Null),
            SqlLiteral::Interval(raw) => {
                super::parse_duration(raw).map_err(|message| type_error(span, message))?;
                Ok(ExprType::Interval)
            }
        }
    }

    fn binary(
        &self,
        left: &SqlExpr,
        op: SqlBinaryOp,
        right: &SqlExpr,
        span: SqlSpan,
    ) -> Result<ExprType> {
        let left_type = self.infer(left)?;
        let right_type = self.infer(right)?;
        match op {
            SqlBinaryOp::And | SqlBinaryOp::Or => {
                self.require_type(left_type, SqlValueType::Boolean, left.span, "left operand")?;
                self.require_type(
                    right_type,
                    SqlValueType::Boolean,
                    right.span,
                    "right operand",
                )?;
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlBinaryOp::Eq | SqlBinaryOp::NotEq => {
                self.require_compatible(left_type, right_type, span, "comparison")?;
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlBinaryOp::Lt | SqlBinaryOp::Gt | SqlBinaryOp::LtEq | SqlBinaryOp::GtEq => {
                self.require_ordered_compatible(left_type, right_type, span)?;
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlBinaryOp::StringConcat => {
                self.require_type(left_type, SqlValueType::Utf8, left.span, "left operand")?;
                self.require_type(right_type, SqlValueType::Utf8, right.span, "right operand")?;
                Ok(ExprType::Scalar(SqlValueType::Utf8))
            }
            SqlBinaryOp::Add | SqlBinaryOp::Sub => {
                if matches!(
                    (left_type, right_type),
                    (
                        ExprType::Scalar(SqlValueType::TimestampNanosecond),
                        ExprType::Interval
                    )
                ) || (op == SqlBinaryOp::Add
                    && matches!(
                        (left_type, right_type),
                        (
                            ExprType::Interval,
                            ExprType::Scalar(SqlValueType::TimestampNanosecond)
                        )
                    ))
                {
                    return Ok(ExprType::Scalar(SqlValueType::TimestampNanosecond));
                }
                self.numeric_result(left_type, right_type, span)
            }
            SqlBinaryOp::Mul | SqlBinaryOp::Div | SqlBinaryOp::Mod => {
                self.numeric_result(left_type, right_type, span)
            }
        }
    }

    fn unary(&self, op: SqlUnaryOp, expression: &SqlExpr, span: SqlSpan) -> Result<ExprType> {
        let value = self.infer(expression)?;
        match op {
            SqlUnaryOp::Not => {
                self.require_type(value, SqlValueType::Boolean, expression.span, "NOT operand")?;
                Ok(ExprType::Scalar(SqlValueType::Boolean))
            }
            SqlUnaryOp::Minus => match value {
                ExprType::Scalar(SqlValueType::Float64 | SqlValueType::Int64)
                | ExprType::Number(NumberLiteral {
                    preferred: SqlValueType::Float64 | SqlValueType::Int64,
                    ..
                }) => Ok(value),
                _ => Err(type_error(
                    span,
                    format!(
                        "unary minus requires float64 or int64, got {}",
                        value.name()
                    ),
                )),
            },
        }
    }

    fn function(&self, function: &SqlFunction, span: SqlSpan) -> Result<ExprType> {
        match function {
            SqlFunction::TimeSeries { function, columns } => {
                self.time_series_function(function, columns)
            }
            SqlFunction::Aggregate {
                function, argument, ..
            } => {
                let argument_type = match argument {
                    AggregateArgument::Wildcard => {
                        if *function != AggregateFunction::Count {
                            return Err(type_error(
                                span,
                                "only COUNT accepts a wildcard argument".to_string(),
                            ));
                        }
                        return Ok(ExprType::Scalar(SqlValueType::UInt64));
                    }
                    AggregateArgument::Expr(expression) => self.infer(expression)?,
                };
                if *function == AggregateFunction::Count {
                    self.finalize(argument_type, span)?;
                    return Ok(ExprType::Scalar(SqlValueType::UInt64));
                }
                let scalar = self.finalize(argument_type, span)?;
                if !scalar.is_numeric() {
                    return Err(type_error(
                        span,
                        format!(
                            "{function:?} requires a numeric expression, got {}",
                            scalar.name()
                        ),
                    ));
                }
                Ok(ExprType::Scalar(if *function == AggregateFunction::Avg {
                    SqlValueType::Float64
                } else {
                    scalar
                }))
            }
            SqlFunction::Now => Ok(ExprType::Scalar(SqlValueType::TimestampNanosecond)),
        }
    }

    fn time_series_function(
        &self,
        function: &TSFunction,
        columns: &[SqlColumnReference],
    ) -> Result<ExprType> {
        match function {
            TSFunction::TimeBucket { column, .. } => {
                let column = self.function_column(columns, 0, column, 1)?;
                self.require_column_kind(column, ColumnKind::Time, "TIME_BUCKET")?;
                Ok(ExprType::Scalar(SqlValueType::TimestampNanosecond))
            }
            TSFunction::Rate { column } => {
                let column = self.function_column(columns, 0, column, 1)?;
                self.require_numeric_field(column, "RATE")?;
                Ok(ExprType::Scalar(SqlValueType::Float64))
            }
            TSFunction::Delta { column } => {
                let column = self.function_column(columns, 0, column, 1)?;
                let data_type = self.require_numeric_field(column, "DELTA")?;
                Ok(ExprType::Scalar(data_type))
            }
            TSFunction::Derivative { column } => {
                let column = self.function_column(columns, 0, column, 1)?;
                self.require_numeric_field(column, "DERIVATIVE")?;
                Ok(ExprType::Scalar(SqlValueType::Float64))
            }
            TSFunction::First {
                value_column,
                time_column,
            }
            | TSFunction::Last {
                value_column,
                time_column,
            } => {
                let function_name = if matches!(function, TSFunction::First { .. }) {
                    "FIRST"
                } else {
                    "LAST"
                };
                let value_column = self.function_column(columns, 0, value_column, 2)?;
                let time_column = self.function_column(columns, 1, time_column, 2)?;
                let value =
                    self.require_column_kind(value_column, ColumnKind::Field, function_name)?;
                self.require_column_kind(time_column, ColumnKind::Time, function_name)?;
                Ok(ExprType::Scalar(value.data_type().into()))
            }
            TSFunction::HistogramQuantile { column, .. } => {
                let column = self.function_column(columns, 0, column, 1)?;
                self.require_numeric_field(column, "HISTOGRAM_QUANTILE")?;
                Ok(ExprType::Scalar(SqlValueType::Float64))
            }
        }
    }

    fn require_numeric_field(
        &self,
        reference: &SqlColumnReference,
        function: &str,
    ) -> Result<SqlValueType> {
        let column = self.require_column_kind(reference, ColumnKind::Field, function)?;
        let data_type = SqlValueType::from(column.data_type());
        if !data_type.is_numeric() {
            return Err(type_error(
                reference.span,
                format!(
                    "{function} requires a numeric field, `{}` is {}",
                    reference.name,
                    data_type.name()
                ),
            ));
        }
        Ok(data_type)
    }

    fn require_column_kind(
        &self,
        reference: &SqlColumnReference,
        expected: ColumnKind,
        function: &str,
    ) -> Result<ColumnSchema> {
        let column = self.resolve_column(&reference.name, reference.span)?;
        if column.kind() != expected {
            return Err(type_error(
                reference.span,
                format!(
                    "{function} requires a {} column, `{}` is {}",
                    kind_name(expected),
                    reference.name,
                    kind_name(column.kind())
                ),
            ));
        }
        Ok(column)
    }

    fn function_column<'a>(
        &self,
        columns: &'a [SqlColumnReference],
        index: usize,
        expected_name: &str,
        expected_count: usize,
    ) -> Result<&'a SqlColumnReference> {
        if columns.len() != expected_count {
            return Err(TsmError::FfiContract(format!(
                "resolved time-series function contains {} column reference(s), expected {expected_count}",
                columns.len()
            )));
        }
        let reference = columns.get(index).ok_or_else(|| {
            TsmError::FfiContract(
                "resolved time-series function column reference disappeared".to_string(),
            )
        })?;
        if reference.name != expected_name {
            return Err(TsmError::FfiContract(format!(
                "resolved time-series function column `{expected_name}` disagrees with retained reference `{}`",
                reference.name
            )));
        }
        self.validate_qualifier(reference.qualifier.as_deref(), reference.span)?;
        self.resolve_column(&reference.name, reference.span)?;
        Ok(reference)
    }

    fn resolve_column(&self, name: &str, span: SqlSpan) -> Result<ColumnSchema> {
        if name.eq_ignore_ascii_case("time") {
            return self.schema.column("_time").copied().ok_or_else(|| {
                type_error(span, "composed schema has no system time column".into())
            });
        }
        if name == "_time" || name == "__name__" {
            return Err(type_error(
                span,
                format!("`{name}` is a reserved query identifier"),
            ));
        }
        self.schema.column(name).copied().ok_or_else(|| {
            type_error(
                span,
                format!(
                    "column `{name}` does not exist in measurement `{}`",
                    self.measurement
                ),
            )
        })
    }

    fn validate_qualifier(&self, qualifier: Option<&str>, span: SqlSpan) -> Result<()> {
        let Some(qualifier) = qualifier else {
            return Ok(());
        };
        let matches_measurement = qualifier.eq_ignore_ascii_case(self.measurement);
        let matches_alias = self
            .measurement_alias
            .is_some_and(|alias| qualifier.eq_ignore_ascii_case(alias));
        if matches_measurement || matches_alias {
            Ok(())
        } else {
            Err(type_error(
                span,
                format!(
                    "unknown measurement qualifier `{qualifier}` for `{}`",
                    self.measurement
                ),
            ))
        }
    }

    fn numeric_result(&self, left: ExprType, right: ExprType, span: SqlSpan) -> Result<ExprType> {
        let result = match (left, right) {
            (ExprType::Scalar(left), ExprType::Scalar(right))
                if left == right && left.is_numeric() =>
            {
                left
            }
            (ExprType::Scalar(scalar), ExprType::Number(literal))
            | (ExprType::Number(literal), ExprType::Scalar(scalar))
                if scalar.is_numeric() && literal.fits(scalar) =>
            {
                scalar
            }
            (ExprType::Number(left), ExprType::Number(right)) => left.common_type(right),
            _ => {
                return Err(type_error(
                    span,
                    format!(
                        "numeric operands must have one exact type, got {} and {}",
                        left.name(),
                        right.name()
                    ),
                ));
            }
        };
        Ok(ExprType::Scalar(result))
    }

    fn require_ordered_compatible(
        &self,
        left: ExprType,
        right: ExprType,
        span: SqlSpan,
    ) -> Result<()> {
        self.require_compatible(left, right, span, "ordered comparison")?;
        let data_type = compatible_scalar(left, right).ok_or_else(|| {
            type_error(
                span,
                format!(
                    "ordered comparison has incompatible {} and {} operands",
                    left.name(),
                    right.name()
                ),
            )
        })?;
        if data_type == SqlValueType::Boolean {
            return Err(type_error(
                span,
                "boolean values do not support ordered comparison".to_string(),
            ));
        }
        Ok(())
    }

    fn require_compatible(
        &self,
        left: ExprType,
        right: ExprType,
        span: SqlSpan,
        purpose: &str,
    ) -> Result<()> {
        if compatible_scalar(left, right).is_some() {
            Ok(())
        } else {
            Err(type_error(
                span,
                format!(
                    "{purpose} requires compatible types, got {} and {}",
                    left.name(),
                    right.name()
                ),
            ))
        }
    }

    fn require_type(
        &self,
        actual: ExprType,
        expected: SqlValueType,
        span: SqlSpan,
        purpose: &str,
    ) -> Result<()> {
        if matches!(actual, ExprType::Scalar(value) if value == expected)
            || matches!(actual, ExprType::Number(literal) if literal.fits(expected))
        {
            Ok(())
        } else {
            Err(type_error(
                span,
                format!(
                    "{purpose} requires {}, got {}",
                    expected.name(),
                    actual.name()
                ),
            ))
        }
    }

    fn finalize(&self, value: ExprType, span: SqlSpan) -> Result<SqlValueType> {
        match value {
            ExprType::Scalar(value) => Ok(value),
            ExprType::Number(value) => Ok(value.preferred),
            ExprType::Interval => Err(type_error(
                span,
                "an INTERVAL has no standalone result type".to_string(),
            )),
            ExprType::Null => Err(type_error(
                span,
                "NULL requires an explicit typed context".to_string(),
            )),
        }
    }

    fn collect_column_kinds(
        &self,
        expression: &SqlExpr,
        output: &mut Vec<ColumnKind>,
    ) -> Result<()> {
        match &expression.kind {
            SqlExprKind::Column { qualifier, name } => {
                self.validate_qualifier(qualifier.as_deref(), expression.span)?;
                output.push(self.resolve_column(name, expression.span)?.kind());
            }
            SqlExprKind::Binary { left, right, .. } => {
                self.collect_column_kinds(left, output)?;
                self.collect_column_kinds(right, output)?;
            }
            SqlExprKind::Unary { expr, .. } | SqlExprKind::IsNull { expr, .. } => {
                self.collect_column_kinds(expr, output)?;
            }
            SqlExprKind::Between {
                expr, low, high, ..
            } => {
                self.collect_column_kinds(expr, output)?;
                self.collect_column_kinds(low, output)?;
                self.collect_column_kinds(high, output)?;
            }
            SqlExprKind::Pattern {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.collect_column_kinds(expr, output)?;
                self.collect_column_kinds(pattern, output)?;
                if let Some(escape) = escape {
                    self.collect_column_kinds(escape, output)?;
                }
            }
            SqlExprKind::InList { expr, list, .. } => {
                self.collect_column_kinds(expr, output)?;
                for item in list {
                    self.collect_column_kinds(item, output)?;
                }
            }
            SqlExprKind::Function(function) => {
                self.collect_function_column_kinds(function, output)?;
            }
            SqlExprKind::Literal(_) => {}
        }
        Ok(())
    }

    fn collect_function_column_kinds(
        &self,
        function: &SqlFunction,
        output: &mut Vec<ColumnKind>,
    ) -> Result<()> {
        match function {
            SqlFunction::TimeSeries { columns, .. } => {
                for reference in columns {
                    self.validate_qualifier(reference.qualifier.as_deref(), reference.span)?;
                    output.push(self.resolve_column(&reference.name, reference.span)?.kind());
                }
            }
            SqlFunction::Aggregate {
                argument: AggregateArgument::Expr(expression),
                ..
            } => self.collect_column_kinds(expression, output)?,
            SqlFunction::Aggregate {
                argument: AggregateArgument::Wildcard,
                ..
            }
            | SqlFunction::Now => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprType {
    Scalar(SqlValueType),
    Number(NumberLiteral),
    Interval,
    Null,
}

impl ExprType {
    fn name(self) -> &'static str {
        match self {
            Self::Scalar(value) => value.name(),
            Self::Number(value) => value.preferred.name(),
            Self::Interval => "interval",
            Self::Null => "null",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NumberLiteral {
    preferred: SqlValueType,
    fits_i64: bool,
    fits_u64: bool,
    fits_f64: bool,
}

impl NumberLiteral {
    fn parse(raw: &str, span: SqlSpan) -> Result<Self> {
        let is_float = raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E'));
        let fits_i64 = !is_float && raw.parse::<i64>().is_ok();
        let fits_u64 = !is_float && raw.parse::<u64>().is_ok();
        let fits_f64 = raw.parse::<f64>().is_ok_and(f64::is_finite);
        if !fits_i64 && !fits_u64 && !fits_f64 {
            return Err(type_error(
                span,
                format!("numeric literal `{raw}` is outside supported scalar ranges"),
            ));
        }
        Ok(Self {
            preferred: if is_float {
                SqlValueType::Float64
            } else if fits_i64 {
                SqlValueType::Int64
            } else {
                SqlValueType::UInt64
            },
            fits_i64,
            fits_u64,
            fits_f64,
        })
    }

    const fn fits(self, data_type: SqlValueType) -> bool {
        match data_type {
            SqlValueType::Float64 => self.fits_f64,
            SqlValueType::Int64 => self.fits_i64,
            SqlValueType::UInt64 => self.fits_u64,
            SqlValueType::TimestampNanosecond | SqlValueType::Boolean | SqlValueType::Utf8 => false,
        }
    }

    fn common_type(self, other: Self) -> SqlValueType {
        if self.preferred == SqlValueType::Float64 || other.preferred == SqlValueType::Float64 {
            SqlValueType::Float64
        } else if self.fits_i64 && other.fits_i64 {
            SqlValueType::Int64
        } else {
            SqlValueType::UInt64
        }
    }
}

fn compatible_scalar(left: ExprType, right: ExprType) -> Option<SqlValueType> {
    match (left, right) {
        (ExprType::Scalar(left), ExprType::Scalar(right)) if left == right => Some(left),
        (ExprType::Scalar(scalar), ExprType::Number(literal))
        | (ExprType::Number(literal), ExprType::Scalar(scalar))
            if scalar.is_numeric() && literal.fits(scalar) =>
        {
            Some(scalar)
        }
        (ExprType::Number(left), ExprType::Number(right)) => Some(left.common_type(right)),
        _ => None,
    }
}

const fn kind_name(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Time => "time",
        ColumnKind::Tag => "tag",
        ColumnKind::Field => "field",
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

#[cfg(test)]
mod tests {
    use super::{typecheck, SqlValueType, TypedProjection};
    use crate::error::TsmError;
    use crate::query::sqlts::{parse, PredicateClass};
    use crate::store::schema::{ColumnDataType, ColumnKind, ColumnSchema, MeasurementSchema};

    fn schema() -> MeasurementSchema {
        let mut schema = MeasurementSchema::new("metrics");
        for (name, data_type) in [
            ("float_value", ColumnDataType::Float64),
            ("int_value", ColumnDataType::Int64),
            ("uint_value", ColumnDataType::UInt64),
            ("bool_value", ColumnDataType::Boolean),
            ("string_value", ColumnDataType::Utf8),
        ] {
            schema
                .merge(name, ColumnSchema::new(ColumnKind::Field, data_type))
                .expect("unique field");
        }
        schema
            .merge(
                "host",
                ColumnSchema::new(ColumnKind::Tag, ColumnDataType::Utf8),
            )
            .expect("unique tag");
        schema
    }

    fn check(sql: &str) -> super::TypedSqlTsQuery {
        typecheck(parse(sql).expect("parse"), &schema()).expect("typecheck")
    }

    fn expression_type(projection: &TypedProjection) -> SqlValueType {
        match projection {
            TypedProjection::Expr { data_type, .. } => *data_type,
            TypedProjection::Wildcard { .. } => panic!("expected expression projection"),
        }
    }

    #[test]
    fn resolves_all_five_field_types_and_wildcard_columns() {
        let typed = check(
            "SELECT float_value, int_value, uint_value, bool_value, string_value, * \
             FROM metrics",
        );
        assert_eq!(
            typed.projections[..5]
                .iter()
                .map(expression_type)
                .collect::<Vec<_>>(),
            vec![
                SqlValueType::Float64,
                SqlValueType::Int64,
                SqlValueType::UInt64,
                SqlValueType::Boolean,
                SqlValueType::Utf8,
            ]
        );
        let TypedProjection::Wildcard { columns } = &typed.projections[5] else {
            panic!("expected wildcard");
        };
        assert!(columns
            .iter()
            .any(|column| column.name == "host" && column.kind == ColumnKind::Tag));
        assert!(columns.iter().any(|column| {
            column.name == "float_value" && column.data_type == SqlValueType::Float64
        }));
    }

    #[test]
    fn validates_numeric_and_value_function_signatures_for_all_field_types() {
        for column in ["float_value", "int_value", "uint_value"] {
            let typed = check(&format!(
                "SELECT RATE({column}), DELTA({column}), DERIVATIVE({column}), \
                 HISTOGRAM_QUANTILE(0.9, {column}), AVG({column}), SUM({column}), \
                 MIN({column}), MAX({column}) FROM metrics"
            ));
            assert_eq!(
                typed
                    .projections
                    .iter()
                    .map(expression_type)
                    .collect::<Vec<_>>(),
                vec![
                    SqlValueType::Float64,
                    typed_scalar(column),
                    SqlValueType::Float64,
                    SqlValueType::Float64,
                    SqlValueType::Float64,
                    typed_scalar(column),
                    typed_scalar(column),
                    typed_scalar(column),
                ]
            );
        }

        for column in [
            "float_value",
            "int_value",
            "uint_value",
            "bool_value",
            "string_value",
        ] {
            let typed = check(&format!(
                "SELECT FIRST({column}, time), LAST({column}, time), COUNT({column}) FROM metrics"
            ));
            assert_eq!(expression_type(&typed.projections[0]), typed_scalar(column));
            assert_eq!(expression_type(&typed.projections[1]), typed_scalar(column));
            assert_eq!(expression_type(&typed.projections[2]), SqlValueType::UInt64);
        }
    }

    fn typed_scalar(column: &str) -> SqlValueType {
        match column {
            "float_value" => SqlValueType::Float64,
            "int_value" => SqlValueType::Int64,
            "uint_value" => SqlValueType::UInt64,
            "bool_value" => SqlValueType::Boolean,
            "string_value" => SqlValueType::Utf8,
            _ => panic!("unknown test column"),
        }
    }

    #[test]
    fn rejects_non_numeric_fields_and_tag_arguments_before_execution() {
        for column in ["bool_value", "string_value", "host"] {
            for function in ["RATE", "DELTA", "DERIVATIVE", "AVG", "SUM", "MIN", "MAX"] {
                let sql = format!("SELECT {function}({column}) FROM metrics");
                assert!(
                    matches!(
                        typecheck(parse(&sql).expect("parse"), &schema()),
                        Err(TsmError::Type {
                            line,
                            column,
                            ..
                        }) if line > 0 && column > 0
                    ),
                    "{sql}"
                );
            }
        }
        assert!(matches!(
            typecheck(
                parse("SELECT FIRST(host, time) FROM metrics").expect("parse"),
                &schema()
            ),
            Err(TsmError::Type { .. })
        ));
    }

    #[test]
    fn validates_aliases_qualifiers_and_measurement_identity() {
        let typed = check(
            "SELECT AVG(m.float_value) AS average, RATE(m.float_value) AS rate \
             FROM metrics AS m \
             WHERE m.host = 'edge' GROUP BY average ORDER BY average",
        );
        assert_eq!(
            expression_type(&typed.projections[0]),
            SqlValueType::Float64
        );
        assert_eq!(
            expression_type(&typed.projections[1]),
            SqlValueType::Float64
        );
        assert_eq!(typed.query.predicates[0].class, PredicateClass::Tag);
        assert!(typed.query.predicates[0].pushdown_eligible);

        for sql in [
            "SELECT missing FROM metrics",
            "SELECT other.float_value FROM metrics AS m",
            "SELECT RATE(other.float_value) FROM metrics AS m",
        ] {
            assert!(matches!(
                typecheck(parse(sql).expect("parse"), &schema()),
                Err(TsmError::Type {
                    line,
                    column,
                    ..
                }) if line > 0 && column > 0
            ));
        }

        let query = parse("SELECT float_value FROM other").expect("parse");
        assert!(matches!(
            typecheck(query, &schema()),
            Err(TsmError::Type { message, .. }) if message.contains("measurement")
        ));
    }

    #[test]
    fn validates_operators_without_implicit_column_coercion() {
        check(
            "SELECT float_value + 1.5, int_value + 1, uint_value + 1, \
             string_value || 'x', NOT bool_value FROM metrics \
             WHERE time > NOW() - INTERVAL '1 hour' \
               AND float_value BETWEEN 0.0 AND 2.0 \
               AND host IN ('a', 'b')",
        );

        for sql in [
            "SELECT float_value + int_value FROM metrics",
            "SELECT bool_value + 1 FROM metrics",
            "SELECT string_value > 1 FROM metrics",
            "SELECT -uint_value FROM metrics",
            "SELECT TIME_BUCKET('1 hour', float_value) FROM metrics",
        ] {
            assert!(
                matches!(
                    typecheck(parse(sql).expect("parse"), &schema()),
                    Err(TsmError::Type { .. })
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn refines_syntactic_predicates_with_schema_roles() {
        let typed = check(
            "SELECT float_value FROM metrics \
             WHERE host = 'edge' AND string_value = 'ready' AND time > NOW()",
        );
        assert_eq!(
            typed
                .query
                .predicates
                .iter()
                .map(|predicate| (predicate.class, predicate.pushdown_eligible))
                .collect::<Vec<_>>(),
            vec![
                (PredicateClass::Tag, true),
                (PredicateClass::Field, false),
                (PredicateClass::Time, true),
            ]
        );
    }

    #[test]
    fn schema_conflicts_remain_machine_readable() {
        let mut schema = schema();
        assert!(matches!(
            schema.merge(
                "host",
                ColumnSchema::new(ColumnKind::Field, ColumnDataType::Utf8)
            ),
            Err(TsmError::SchemaConflict {
                measurement,
                column,
                ..
            }) if measurement == "metrics" && column == "host"
        ));
    }
}
