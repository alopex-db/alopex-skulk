//! PromQL aggregation, scalar arithmetic, and typed SQL table execution.

use super::{execution_error, InstantSample, OperatorValue};
use crate::model::{FieldValue, SeriesKey, Tags, Timestamp};
use crate::query::plan::{
    AggregateCall, AggregateInput, AggregateKind, AggregateNode, AggregationStage, ArithmeticKind,
    PatternMatchKind, PlanDataType, PlanExpression, PlanExpressionKind, ProjectNode,
    ProjectionExpression, ProjectionItem, ScalarBinaryKind, ScalarUnaryKind, SeriesGrouping,
    SortKey,
};
use crate::store::seq::SequencedRow;
use crate::{Result, TsmError};
use regex::RegexBuilder;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const SMALL_DELTA_TOLERANCE: f64 = 1e-12;
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
const MAX_PATTERN_BYTES: usize = 64 * 1024;

/// One typed scalar in a SQL result table.
#[derive(Debug, Clone, PartialEq)]
pub enum TableValue {
    /// SQL NULL.
    Null,
    /// Nanoseconds since the Unix epoch.
    TimestampNanosecond(i64),
    /// IEEE-754 double precision.
    Float64(f64),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// Boolean.
    Boolean(bool),
    /// UTF-8 string.
    Utf8(String),
    /// Internal interval value used while evaluating timestamp expressions.
    IntervalNanoseconds(i64),
}

impl TableValue {
    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// One typed SQL result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumn {
    name: String,
    data_type: PlanDataType,
}

impl TableColumn {
    /// Returns the SQL-visible column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type retained by SQL type inference.
    pub const fn data_type(&self) -> PlanDataType {
        self.data_type
    }
}

/// Schema-preserving SQL rows before Arrow materialization.
#[derive(Debug, Clone, PartialEq)]
pub struct TableResult {
    columns: Vec<TableColumn>,
    rows: Vec<Vec<TableValue>>,
}

impl TableResult {
    /// Returns columns in SELECT-list order.
    pub fn columns(&self) -> &[TableColumn] {
        &self.columns
    }

    /// Returns rows after filtering, grouping, ordering, and limiting.
    pub fn rows(&self) -> &[Vec<TableValue>] {
        &self.rows
    }
}

pub(super) struct GroupSet {
    keys: Vec<PlanExpression>,
    groups: Vec<GroupRows>,
}

struct GroupRows {
    key_values: Vec<TableValue>,
    rows: Vec<SequencedRow>,
}

pub(super) struct AggregateSet {
    keys: Vec<PlanExpression>,
    rows: Vec<AggregateRow>,
}

struct AggregateRow {
    key_values: Vec<TableValue>,
    values: Vec<TableValue>,
}

enum EvalRow<'a> {
    Raw(&'a SequencedRow),
    Aggregate {
        keys: &'a [PlanExpression],
        row: &'a AggregateRow,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ValueKey {
    Null,
    Timestamp(i64),
    Float(u64),
    Int64(i64),
    UInt64(u64),
    Boolean(bool),
    Utf8(String),
    Interval(i64),
}

impl From<&TableValue> for ValueKey {
    fn from(value: &TableValue) -> Self {
        match value {
            TableValue::Null => Self::Null,
            TableValue::TimestampNanosecond(value) => Self::Timestamp(*value),
            TableValue::Float64(value) => Self::Float(ordered_float_bits(*value)),
            TableValue::Int64(value) => Self::Int64(*value),
            TableValue::UInt64(value) => Self::UInt64(*value),
            TableValue::Boolean(value) => Self::Boolean(*value),
            TableValue::Utf8(value) => Self::Utf8(value.clone()),
            TableValue::IntervalNanoseconds(value) => Self::Interval(*value),
        }
    }
}

pub(super) fn group_rows(input: OperatorValue, keys: &[PlanExpression]) -> Result<OperatorValue> {
    let OperatorValue::Rows(rows) = input else {
        return Err(execution_error("SQL grouping requires storage rows"));
    };
    let mut grouped: BTreeMap<Vec<ValueKey>, GroupRows> = BTreeMap::new();
    for row in rows.rows {
        let key_values = keys
            .iter()
            .map(|expression| evaluate_expression(expression, EvalRow::Raw(&row)))
            .collect::<Result<Vec<_>>>()?;
        let key = key_values.iter().map(ValueKey::from).collect::<Vec<_>>();
        grouped
            .entry(key)
            .or_insert_with(|| GroupRows {
                key_values,
                rows: Vec::new(),
            })
            .rows
            .push(row);
    }
    if keys.is_empty() && grouped.is_empty() {
        grouped.insert(
            Vec::new(),
            GroupRows {
                key_values: Vec::new(),
                rows: Vec::new(),
            },
        );
    }
    Ok(OperatorValue::Groups(GroupSet {
        keys: keys.to_vec(),
        groups: grouped.into_values().collect(),
    }))
}

pub(super) fn expression_is_true(expression: &PlanExpression, row: &SequencedRow) -> Result<bool> {
    Ok(matches!(
        evaluate_expression(expression, EvalRow::Raw(row))?,
        TableValue::Boolean(true)
    ))
}

fn evaluate_expression(expression: &PlanExpression, row: EvalRow<'_>) -> Result<TableValue> {
    match &expression.kind {
        PlanExpressionKind::Column { name } => column_value(row, name),
        PlanExpressionKind::Number(raw) => number_value(raw),
        PlanExpressionKind::String(value) => Ok(TableValue::Utf8(value.clone())),
        PlanExpressionKind::Boolean(value) => Ok(TableValue::Boolean(*value)),
        PlanExpressionKind::Null => Ok(TableValue::Null),
        PlanExpressionKind::Timestamp(value) => Ok(TableValue::TimestampNanosecond(*value)),
        PlanExpressionKind::Interval(value) => Ok(TableValue::IntervalNanoseconds(*value)),
        PlanExpressionKind::Binary { left, op, right } => {
            let left = evaluate_expression(left, row.reborrow())?;
            let right = evaluate_expression(right, row)?;
            scalar_binary(left, *op, right)
        }
        PlanExpressionKind::Unary { op, expression } => {
            let value = evaluate_expression(expression, row)?;
            scalar_unary(*op, value)
        }
        PlanExpressionKind::Between {
            expression,
            low,
            high,
            negated,
        } => {
            let value = evaluate_expression(expression, row.reborrow())?;
            let low = evaluate_expression(low, row.reborrow())?;
            let high = evaluate_expression(high, row)?;
            if value.is_null() || low.is_null() || high.is_null() {
                return Ok(TableValue::Null);
            }
            let result = compare_values(&value, &low)? != Ordering::Less
                && compare_values(&value, &high)? != Ordering::Greater;
            Ok(TableValue::Boolean(if *negated { !result } else { result }))
        }
        PlanExpressionKind::Pattern {
            expression,
            pattern,
            escape,
            negated,
            kind,
        } => {
            let value = evaluate_expression(expression, row.reborrow())?;
            let pattern = evaluate_expression(pattern, row.reborrow())?;
            let escape = escape
                .as_deref()
                .map(|escape| evaluate_expression(escape, row.reborrow()))
                .transpose()?;
            pattern_value(value, pattern, escape, *kind, *negated)
        }
        PlanExpressionKind::InList {
            expression,
            list,
            negated,
        } => {
            let value = evaluate_expression(expression, row.reborrow())?;
            if value.is_null() {
                return Ok(TableValue::Null);
            }
            let mut saw_null = false;
            for candidate in list {
                let candidate = evaluate_expression(candidate, row.reborrow())?;
                if candidate.is_null() {
                    saw_null = true;
                } else if values_equal(&value, &candidate)? {
                    return Ok(TableValue::Boolean(!*negated));
                }
            }
            if saw_null {
                Ok(TableValue::Null)
            } else {
                Ok(TableValue::Boolean(*negated))
            }
        }
        PlanExpressionKind::IsNull {
            expression,
            negated,
        } => {
            let is_null = evaluate_expression(expression, row)?.is_null();
            Ok(TableValue::Boolean(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        PlanExpressionKind::TimeBucket {
            interval_ns,
            column,
        } => {
            if *interval_ns <= 0 {
                return Err(execution_error("TIME_BUCKET interval must be positive"));
            }
            match row {
                EvalRow::Aggregate { keys, row } => aggregate_key_value(
                    keys,
                    row,
                    expression,
                    "TIME_BUCKET expression is not one of the group keys",
                ),
                EvalRow::Raw(row) => match raw_column_value(row, column)? {
                    TableValue::TimestampNanosecond(timestamp) => {
                        let bucket = floor_time_bucket(timestamp, *interval_ns)?;
                        Ok(TableValue::TimestampNanosecond(bucket))
                    }
                    TableValue::Null => Ok(TableValue::Null),
                    _ => Err(execution_error("TIME_BUCKET requires a timestamp column")),
                },
            }
        }
        PlanExpressionKind::AggregateResult { index } => match row {
            EvalRow::Aggregate { row, .. } => row.values.get(*index).cloned().ok_or_else(|| {
                execution_error(format!("aggregate result index {index} is out of bounds"))
            }),
            EvalRow::Raw(_) => Err(execution_error(
                "aggregate result cannot be evaluated on a storage row",
            )),
        },
    }
}

fn floor_time_bucket(timestamp: i64, interval_ns: i64) -> Result<i64> {
    timestamp
        .div_euclid(interval_ns)
        .checked_mul(interval_ns)
        .ok_or_else(|| execution_error("TIME_BUCKET result overflows"))
}

impl<'a> EvalRow<'a> {
    fn reborrow(&self) -> EvalRow<'_> {
        match self {
            Self::Raw(row) => EvalRow::Raw(row),
            Self::Aggregate { keys, row } => EvalRow::Aggregate { keys, row },
        }
    }
}

fn column_value(row: EvalRow<'_>, name: &str) -> Result<TableValue> {
    match row {
        EvalRow::Raw(row) => raw_column_value(row, name),
        EvalRow::Aggregate { keys, row } => {
            let expression = PlanExpression {
                kind: PlanExpressionKind::Column {
                    name: name.to_string(),
                },
            };
            aggregate_key_value(
                keys,
                row,
                &expression,
                format!("column `{name}` is not one of the group keys"),
            )
        }
    }
}

fn aggregate_key_value(
    keys: &[PlanExpression],
    row: &AggregateRow,
    expression: &PlanExpression,
    error: impl Into<String>,
) -> Result<TableValue> {
    keys.iter()
        .position(|key| key == expression)
        .and_then(|index| row.key_values.get(index))
        .cloned()
        .ok_or_else(|| execution_error(error))
}

fn raw_column_value(row: &SequencedRow, name: &str) -> Result<TableValue> {
    if name.eq_ignore_ascii_case("time") {
        return Ok(TableValue::TimestampNanosecond(row.row().timestamp()));
    }
    if let Some(value) = row.row().series().tags().get(name) {
        return Ok(TableValue::Utf8(value.clone()));
    }
    Ok(row.row().field(name).map_or(TableValue::Null, field_value))
}

fn field_value(value: &FieldValue) -> TableValue {
    match value {
        FieldValue::Float(value) => TableValue::Float64(*value),
        FieldValue::Integer(value) => TableValue::Int64(*value),
        FieldValue::Unsigned(value) => TableValue::UInt64(*value),
        FieldValue::Boolean(value) => TableValue::Boolean(*value),
        FieldValue::String(value) => TableValue::Utf8(value.clone()),
    }
}

fn number_value(raw: &str) -> Result<TableValue> {
    if raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
        return raw
            .parse::<f64>()
            .map(TableValue::Float64)
            .map_err(|_| execution_error(format!("invalid float literal `{raw}`")));
    }
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(TableValue::Int64(value));
    }
    raw.parse::<u64>()
        .map(TableValue::UInt64)
        .map_err(|_| execution_error(format!("invalid integer literal `{raw}`")))
}

fn scalar_unary(operation: ScalarUnaryKind, value: TableValue) -> Result<TableValue> {
    if value.is_null() {
        return Ok(TableValue::Null);
    }
    match (operation, value) {
        (ScalarUnaryKind::Not, TableValue::Boolean(value)) => Ok(TableValue::Boolean(!value)),
        (ScalarUnaryKind::Minus, TableValue::Float64(value)) => Ok(TableValue::Float64(-value)),
        (ScalarUnaryKind::Minus, TableValue::Int64(value)) => value
            .checked_neg()
            .map(TableValue::Int64)
            .ok_or_else(|| execution_error("signed negation overflows")),
        _ => Err(execution_error("invalid SQL unary operand type")),
    }
}

fn scalar_binary(
    left: TableValue,
    operation: ScalarBinaryKind,
    right: TableValue,
) -> Result<TableValue> {
    use ScalarBinaryKind as Op;
    if matches!(operation, Op::And | Op::Or) {
        return boolean_binary(left, operation, right);
    }
    if left.is_null() || right.is_null() {
        return Ok(TableValue::Null);
    }
    match operation {
        Op::Eq => Ok(TableValue::Boolean(values_equal(&left, &right)?)),
        Op::NotEq => Ok(TableValue::Boolean(!values_equal(&left, &right)?)),
        Op::Lt | Op::Gt | Op::LtEq | Op::GtEq => {
            let ordering = compare_values(&left, &right)?;
            let result = match operation {
                Op::Lt => ordering == Ordering::Less,
                Op::Gt => ordering == Ordering::Greater,
                Op::LtEq => ordering != Ordering::Greater,
                Op::GtEq => ordering != Ordering::Less,
                _ => false,
            };
            Ok(TableValue::Boolean(result))
        }
        Op::StringConcat => match (left, right) {
            (TableValue::Utf8(mut left), TableValue::Utf8(right)) => {
                left.push_str(&right);
                Ok(TableValue::Utf8(left))
            }
            _ => Err(execution_error("SQL concatenation requires UTF-8 values")),
        },
        Op::Add | Op::Sub => match (&left, &right) {
            (
                TableValue::TimestampNanosecond(timestamp),
                TableValue::IntervalNanoseconds(interval),
            ) => checked_timestamp(*timestamp, *interval, operation),
            (
                TableValue::IntervalNanoseconds(interval),
                TableValue::TimestampNanosecond(timestamp),
            ) if operation == Op::Add => checked_timestamp(*timestamp, *interval, operation),
            _ => numeric_binary(left, operation, right),
        },
        Op::Mul | Op::Div | Op::Mod => numeric_binary(left, operation, right),
        Op::And | Op::Or => Err(execution_error("invalid duplicate boolean dispatch")),
    }
}

fn boolean_binary(
    left: TableValue,
    operation: ScalarBinaryKind,
    right: TableValue,
) -> Result<TableValue> {
    let left = match left {
        TableValue::Boolean(value) => Some(value),
        TableValue::Null => None,
        _ => return Err(execution_error("boolean operation requires booleans")),
    };
    let right = match right {
        TableValue::Boolean(value) => Some(value),
        TableValue::Null => None,
        _ => return Err(execution_error("boolean operation requires booleans")),
    };
    let value = match operation {
        ScalarBinaryKind::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        ScalarBinaryKind::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        _ => return Err(execution_error("invalid boolean operation")),
    };
    Ok(value.map_or(TableValue::Null, TableValue::Boolean))
}

fn checked_timestamp(
    timestamp: i64,
    interval: i64,
    operation: ScalarBinaryKind,
) -> Result<TableValue> {
    let value = match operation {
        ScalarBinaryKind::Add => timestamp.checked_add(interval),
        ScalarBinaryKind::Sub => timestamp.checked_sub(interval),
        _ => None,
    };
    value
        .map(TableValue::TimestampNanosecond)
        .ok_or_else(|| execution_error("timestamp arithmetic overflows"))
}

fn numeric_binary(
    left: TableValue,
    operation: ScalarBinaryKind,
    right: TableValue,
) -> Result<TableValue> {
    if matches!(left, TableValue::Float64(_)) || matches!(right, TableValue::Float64(_)) {
        let left = numeric_f64(&left)?;
        let right = numeric_f64(&right)?;
        if matches!(operation, ScalarBinaryKind::Div | ScalarBinaryKind::Mod) && right == 0.0 {
            return Err(execution_error("floating-point arithmetic divides by zero"));
        }
        let value = match operation {
            ScalarBinaryKind::Add => left + right,
            ScalarBinaryKind::Sub => left - right,
            ScalarBinaryKind::Mul => left * right,
            ScalarBinaryKind::Div => left / right,
            ScalarBinaryKind::Mod => left % right,
            _ => return Err(execution_error("invalid floating-point operation")),
        };
        return Ok(TableValue::Float64(value));
    }
    if matches!(left, TableValue::UInt64(_)) || matches!(right, TableValue::UInt64(_)) {
        let left = numeric_u64(&left)?;
        let right = numeric_u64(&right)?;
        let value = match operation {
            ScalarBinaryKind::Add => left.checked_add(right),
            ScalarBinaryKind::Sub => left.checked_sub(right),
            ScalarBinaryKind::Mul => left.checked_mul(right),
            ScalarBinaryKind::Div => (right != 0).then(|| left / right),
            ScalarBinaryKind::Mod => (right != 0).then(|| left % right),
            _ => None,
        };
        return value
            .map(TableValue::UInt64)
            .ok_or_else(|| execution_error("unsigned arithmetic overflows or divides by zero"));
    }
    let (TableValue::Int64(left), TableValue::Int64(right)) = (left, right) else {
        return Err(execution_error("numeric operation requires numeric values"));
    };
    let value = match operation {
        ScalarBinaryKind::Add => left.checked_add(right),
        ScalarBinaryKind::Sub => left.checked_sub(right),
        ScalarBinaryKind::Mul => left.checked_mul(right),
        ScalarBinaryKind::Div => left.checked_div(right),
        ScalarBinaryKind::Mod => left.checked_rem(right),
        _ => None,
    };
    value
        .map(TableValue::Int64)
        .ok_or_else(|| execution_error("signed arithmetic overflows or divides by zero"))
}

fn numeric_f64(value: &TableValue) -> Result<f64> {
    match value {
        TableValue::Float64(value) => Ok(*value),
        TableValue::Int64(value) => Ok(*value as f64),
        TableValue::UInt64(value) => Ok(*value as f64),
        _ => Err(execution_error("expected a numeric value")),
    }
}

fn numeric_u64(value: &TableValue) -> Result<u64> {
    match value {
        TableValue::UInt64(value) => Ok(*value),
        TableValue::Int64(value) => u64::try_from(*value)
            .map_err(|_| execution_error("negative literal cannot be used with uint64")),
        _ => Err(execution_error("expected an integer value")),
    }
}

fn values_equal(left: &TableValue, right: &TableValue) -> Result<bool> {
    if left.is_null() || right.is_null() {
        return Ok(false);
    }
    Ok(compare_values(left, right)? == Ordering::Equal)
}

fn compare_values(left: &TableValue, right: &TableValue) -> Result<Ordering> {
    match (left, right) {
        (TableValue::TimestampNanosecond(left), TableValue::TimestampNanosecond(right))
        | (TableValue::IntervalNanoseconds(left), TableValue::IntervalNanoseconds(right))
        | (TableValue::Int64(left), TableValue::Int64(right)) => Ok(left.cmp(right)),
        (TableValue::UInt64(left), TableValue::UInt64(right)) => Ok(left.cmp(right)),
        (TableValue::Int64(left), TableValue::UInt64(right)) => {
            if *left < 0 {
                Ok(Ordering::Less)
            } else {
                Ok((*left as u64).cmp(right))
            }
        }
        (TableValue::UInt64(left), TableValue::Int64(right)) => {
            if *right < 0 {
                Ok(Ordering::Greater)
            } else {
                Ok(left.cmp(&(*right as u64)))
            }
        }
        (TableValue::Float64(left), TableValue::Float64(right)) => Ok(left.total_cmp(right)),
        (TableValue::Boolean(left), TableValue::Boolean(right)) => Ok(left.cmp(right)),
        (TableValue::Utf8(left), TableValue::Utf8(right)) => Ok(left.cmp(right)),
        (
            TableValue::Float64(_) | TableValue::Int64(_) | TableValue::UInt64(_),
            TableValue::Float64(_) | TableValue::Int64(_) | TableValue::UInt64(_),
        ) => Ok(numeric_f64(left)?.total_cmp(&numeric_f64(right)?)),
        _ => Err(execution_error(
            "SQL values have incompatible comparison types",
        )),
    }
}

fn pattern_value(
    value: TableValue,
    pattern: TableValue,
    escape: Option<TableValue>,
    kind: PatternMatchKind,
    negated: bool,
) -> Result<TableValue> {
    if value.is_null() || pattern.is_null() || escape.as_ref().is_some_and(TableValue::is_null) {
        return Ok(TableValue::Null);
    }
    let (TableValue::Utf8(value), TableValue::Utf8(pattern)) = (value, pattern) else {
        return Err(execution_error(
            "SQL pattern matching requires UTF-8 values",
        ));
    };
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(TsmError::ResourceLimit(format!(
            "SQL pattern length {} exceeds limit {MAX_PATTERN_BYTES}",
            pattern.len()
        )));
    }
    let escape = match escape {
        None => None,
        Some(TableValue::Utf8(value)) => {
            let mut chars = value.chars();
            let character = chars
                .next()
                .ok_or_else(|| execution_error("SQL pattern escape must contain one character"))?;
            if chars.next().is_some() {
                return Err(execution_error(
                    "SQL pattern escape must contain one character",
                ));
            }
            Some(character)
        }
        Some(_) => return Err(execution_error("SQL pattern escape must be UTF-8")),
    };
    let source = pattern_regex(&pattern, escape, kind);
    let regex = RegexBuilder::new(&source)
        .case_insensitive(kind == PatternMatchKind::ILike)
        .size_limit(1 << 20)
        .dfa_size_limit(1 << 20)
        .build()
        .map_err(|error| execution_error(format!("invalid SQL pattern: {error}")))?;
    let matches = regex.is_match(&value);
    Ok(TableValue::Boolean(if negated {
        !matches
    } else {
        matches
    }))
}

fn pattern_regex(pattern: &str, escape: Option<char>, kind: PatternMatchKind) -> String {
    let mut output = String::from("^(?:");
    let mut escaped = false;
    for character in pattern.chars() {
        if !escaped && escape == Some(character) {
            escaped = true;
            continue;
        }
        if escaped {
            output.push_str(&regex::escape(&character.to_string()));
            escaped = false;
            continue;
        }
        let replacement = match kind {
            PatternMatchKind::Like | PatternMatchKind::ILike | PatternMatchKind::SimilarTo => {
                match character {
                    '%' => Some(".*"),
                    '_' => Some("."),
                    _ => None,
                }
            }
            PatternMatchKind::Glob => match character {
                '*' => Some(".*"),
                '?' => Some("."),
                _ => None,
            },
        };
        if let Some(replacement) = replacement {
            output.push_str(replacement);
        } else if kind == PatternMatchKind::SimilarTo
            && matches!(
                character,
                '|' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}'
            )
        {
            output.push(character);
        } else {
            output.push_str(&regex::escape(&character.to_string()));
        }
    }
    output.push_str(")$");
    output
}

pub(super) fn sort(input: OperatorValue, keys: &[SortKey]) -> Result<OperatorValue> {
    match input {
        OperatorValue::Rows(mut rows) => {
            let mut keyed = rows
                .rows
                .into_iter()
                .map(|row| {
                    let values = keys
                        .iter()
                        .map(|key| evaluate_expression(&key.expression, EvalRow::Raw(&row)))
                        .collect::<Result<Vec<_>>>()?;
                    Ok((values, row))
                })
                .collect::<Result<Vec<_>>>()?;
            keyed.sort_by(|left, right| compare_sort_keys(&left.0, &right.0, keys));
            rows.rows = keyed.into_iter().map(|(_, row)| row).collect();
            Ok(OperatorValue::Rows(rows))
        }
        OperatorValue::AggregateRows(mut aggregate) => {
            let mut keyed = aggregate
                .rows
                .into_iter()
                .map(|row| {
                    let values = keys
                        .iter()
                        .map(|key| {
                            evaluate_expression(
                                &key.expression,
                                EvalRow::Aggregate {
                                    keys: &aggregate.keys,
                                    row: &row,
                                },
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok((values, row))
                })
                .collect::<Result<Vec<_>>>()?;
            keyed.sort_by(|left, right| compare_sort_keys(&left.0, &right.0, keys));
            aggregate.rows = keyed.into_iter().map(|(_, row)| row).collect();
            Ok(OperatorValue::AggregateRows(aggregate))
        }
        _ => Err(execution_error(
            "Sort requires raw or aggregated SQL rows before projection",
        )),
    }
}

fn compare_sort_keys(left: &[TableValue], right: &[TableValue], keys: &[SortKey]) -> Ordering {
    for ((left, right), key) in left.iter().zip(right).zip(keys) {
        let nulls_first = key.nulls_first.map_or(key.descending, |value| value);
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => match compare_values(left, right) {
                Ok(ordering) => ordering,
                Err(_) => Ordering::Equal,
            },
        };
        let ordering = if key.descending && !left.is_null() && !right.is_null() {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(super) fn limit(input: OperatorValue, rows: u64) -> Result<OperatorValue> {
    let maximum = match usize::try_from(rows) {
        Ok(value) => value,
        Err(_) => usize::MAX,
    };
    match input {
        OperatorValue::Rows(mut input) => {
            input.rows.truncate(maximum);
            Ok(OperatorValue::Rows(input))
        }
        OperatorValue::AggregateRows(mut input) => {
            input.rows.truncate(maximum);
            Ok(OperatorValue::AggregateRows(input))
        }
        OperatorValue::Table(mut input) => {
            input.rows.truncate(maximum);
            Ok(OperatorValue::Table(input))
        }
        _ => Err(execution_error("LIMIT requires SQL rows")),
    }
}

pub(super) fn project(input: OperatorValue, project: &ProjectNode) -> Result<OperatorValue> {
    let mut columns = Vec::new();
    for item in &project.items {
        match *item {
            ProjectionItem::Wildcard => {
                columns.extend(project.wildcard_columns.iter().map(|column| TableColumn {
                    name: column.name.clone(),
                    data_type: column.data_type,
                }));
            }
            ProjectionItem::Expression(index) => {
                let expression = explicit_projection(project, index)?;
                columns.push(TableColumn {
                    name: expression.output_name.clone(),
                    data_type: expression.data_type,
                });
            }
        }
    }

    let rows = match input {
        OperatorValue::Rows(input) => input
            .rows
            .iter()
            .map(|row| {
                let mut output = Vec::with_capacity(columns.len());
                for item in &project.items {
                    match *item {
                        ProjectionItem::Wildcard => {
                            for column in &project.wildcard_columns {
                                output.push(coerce_value(
                                    raw_column_value(row, &column.name)?,
                                    column.data_type,
                                )?);
                            }
                        }
                        ProjectionItem::Expression(index) => {
                            let expression = explicit_projection(project, index)?;
                            output.push(coerce_value(
                                evaluate_expression(&expression.expression, EvalRow::Raw(row))?,
                                expression.data_type,
                            )?);
                        }
                    }
                }
                Ok(output)
            })
            .collect::<Result<Vec<_>>>()?,
        OperatorValue::AggregateRows(input) => {
            if project.wildcard || !project.wildcard_columns.is_empty() {
                return Err(execution_error(
                    "wildcard projection cannot consume aggregate rows",
                ));
            }
            input
                .rows
                .iter()
                .map(|row| {
                    let mut output = Vec::with_capacity(columns.len());
                    for item in &project.items {
                        let ProjectionItem::Expression(index) = *item else {
                            return Err(execution_error(
                                "wildcard projection cannot consume aggregate rows",
                            ));
                        };
                        let expression = explicit_projection(project, index)?;
                        output.push(coerce_value(
                            evaluate_expression(
                                &expression.expression,
                                EvalRow::Aggregate {
                                    keys: &input.keys,
                                    row,
                                },
                            )?,
                            expression.data_type,
                        )?);
                    }
                    Ok(output)
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => return Err(execution_error("Project requires SQL rows")),
    };
    Ok(OperatorValue::Table(TableResult { columns, rows }))
}

fn explicit_projection(project: &ProjectNode, index: usize) -> Result<&ProjectionExpression> {
    project.expressions.get(index).ok_or_else(|| {
        execution_error(format!(
            "projection expression index {index} is out of bounds"
        ))
    })
}

fn coerce_value(value: TableValue, expected: PlanDataType) -> Result<TableValue> {
    if value.is_null() {
        return Ok(TableValue::Null);
    }
    match (expected, value) {
        (PlanDataType::TimestampNanosecond, value @ TableValue::TimestampNanosecond(_))
        | (PlanDataType::Float64, value @ TableValue::Float64(_))
        | (PlanDataType::Int64, value @ TableValue::Int64(_))
        | (PlanDataType::UInt64, value @ TableValue::UInt64(_))
        | (PlanDataType::Boolean, value @ TableValue::Boolean(_))
        | (PlanDataType::Utf8, value @ TableValue::Utf8(_)) => Ok(value),
        (PlanDataType::Float64, TableValue::Int64(value)) => Ok(TableValue::Float64(value as f64)),
        (PlanDataType::Float64, TableValue::UInt64(value)) => Ok(TableValue::Float64(value as f64)),
        (PlanDataType::Int64, TableValue::UInt64(value)) => i64::try_from(value)
            .map(TableValue::Int64)
            .map_err(|_| execution_error("uint64 result does not fit int64 projection")),
        (PlanDataType::UInt64, TableValue::Int64(value)) => u64::try_from(value)
            .map(TableValue::UInt64)
            .map_err(|_| execution_error("negative result does not fit uint64 projection")),
        (_, TableValue::IntervalNanoseconds(_)) => {
            Err(execution_error("INTERVAL cannot be projected directly"))
        }
        _ => Err(execution_error(
            "SQL result does not match its inferred type",
        )),
    }
}

pub(super) fn binary(
    left: OperatorValue,
    operation: ArithmeticKind,
    right: OperatorValue,
) -> Result<OperatorValue> {
    match (left, right) {
        (OperatorValue::Scalar(left), OperatorValue::Scalar(right)) => Ok(OperatorValue::Scalar(
            float_arithmetic(left, operation, right),
        )),
        (OperatorValue::InstantVector(mut vector), OperatorValue::Scalar(scalar)) => {
            for sample in &mut vector {
                sample.value = float_arithmetic(sample.value, operation, scalar);
                sample.drop_metric_name = true;
            }
            Ok(OperatorValue::InstantVector(vector))
        }
        (OperatorValue::Scalar(scalar), OperatorValue::InstantVector(mut vector)) => {
            for sample in &mut vector {
                sample.value = float_arithmetic(scalar, operation, sample.value);
                sample.drop_metric_name = true;
            }
            Ok(OperatorValue::InstantVector(vector))
        }
        (OperatorValue::InstantVector(_), OperatorValue::InstantVector(_)) => {
            Err(TsmError::Unsupported {
                feature: "vector-to-vector arithmetic label matching is outside v0.4".to_string(),
                line: 0,
                column: 0,
                offset: 0,
            })
        }
        _ => Err(execution_error(
            "PromQL arithmetic requires scalar/scalar or scalar/vector operands",
        )),
    }
}

fn float_arithmetic(left: f64, operation: ArithmeticKind, right: f64) -> f64 {
    match operation {
        ArithmeticKind::Add => left + right,
        ArithmeticKind::Sub => left - right,
        ArithmeticKind::Mul => left * right,
        ArithmeticKind::Div => left / right,
        ArithmeticKind::Mod => left % right,
        ArithmeticKind::Pow => left.powf(right),
    }
}

pub(super) fn evaluate_aggregate(
    input: OperatorValue,
    node: &AggregateNode,
    parameters: &[Option<f64>],
    output_timestamp: Timestamp,
) -> Result<OperatorValue> {
    if node.stage != AggregationStage::Single {
        return Err(TsmError::Unsupported {
            feature: "partial/final aggregation is reserved for distributed query execution"
                .to_string(),
            line: 0,
            column: 0,
            offset: 0,
        });
    }
    match input {
        OperatorValue::InstantVector(vector) => {
            prometheus_aggregate(vector, node, parameters, output_timestamp)
                .map(OperatorValue::InstantVector)
        }
        OperatorValue::Groups(groups) => {
            sql_aggregate(groups, &node.calls, parameters).map(OperatorValue::AggregateRows)
        }
        _ => Err(execution_error(
            "Aggregate requires an InstantVector or SQL group input",
        )),
    }
}

fn prometheus_aggregate(
    vector: Vec<InstantSample>,
    node: &AggregateNode,
    parameters: &[Option<f64>],
    output_timestamp: Timestamp,
) -> Result<Vec<InstantSample>> {
    let [call] = node.calls.as_slice() else {
        return Err(execution_error(
            "PromQL Aggregate requires exactly one aggregate call",
        ));
    };
    if call.argument != AggregateInput::CurrentValue || call.distinct || !call.auxiliary.is_empty()
    {
        return Err(execution_error(
            "PromQL Aggregate contains SQL-only call metadata",
        ));
    }
    if call.kind == AggregateKind::HistogramQuantile {
        let quantile = parameters
            .first()
            .copied()
            .flatten()
            .ok_or_else(|| execution_error("histogram_quantile requires a scalar parameter"))?;
        return histogram_quantile(vector, quantile, output_timestamp);
    }
    if !matches!(
        call.kind,
        AggregateKind::Sum
            | AggregateKind::Avg
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::Count
    ) {
        return Err(execution_error(
            "PromQL Aggregate received a SQL-only aggregate kind",
        ));
    }

    struct Group {
        series: SeriesKey,
        values: Vec<f64>,
        drop_metric_name: bool,
    }
    let mut groups: BTreeMap<SeriesKey, Group> = BTreeMap::new();
    for sample in vector {
        let series = grouped_series(&sample, node.grouping.as_ref());
        let output_name_is_absent = series.measurement().is_empty();
        let group = groups.entry(series.clone()).or_insert_with(|| Group {
            series,
            values: Vec::new(),
            drop_metric_name: output_name_is_absent,
        });
        group.drop_metric_name |= sample.drop_metric_name;
        group.values.push(sample.value);
    }

    groups
        .into_values()
        .map(|group| {
            let value = aggregate_floats(call.kind, &group.values)?;
            Ok(InstantSample {
                series: group.series,
                evaluation_timestamp: output_timestamp,
                source_timestamp: output_timestamp,
                value,
                drop_metric_name: group.drop_metric_name,
            })
        })
        .collect()
}

fn grouped_series(sample: &InstantSample, grouping: Option<&SeriesGrouping>) -> SeriesKey {
    let Some(grouping) = grouping else {
        return SeriesKey::new("", Tags::new());
    };
    let labels = grouping
        .labels
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if grouping.without {
        let tags = sample
            .series
            .tags()
            .iter()
            .filter(|(name, _)| !labels.contains(name.as_str()))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        SeriesKey::new("", tags)
    } else {
        let measurement = if labels.contains("__name__") {
            sample.series.measurement()
        } else {
            ""
        };
        let tags = sample
            .series
            .tags()
            .iter()
            .filter(|(name, _)| labels.contains(name.as_str()))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        SeriesKey::new(measurement, tags)
    }
}

fn aggregate_floats(kind: AggregateKind, values: &[f64]) -> Result<f64> {
    let Some(first) = values.first().copied() else {
        return Err(execution_error("PromQL aggregate group is empty"));
    };
    match kind {
        AggregateKind::Sum => {
            let (sum, compensation) = values
                .iter()
                .fold((0.0, 0.0), |(sum, compensation), value| {
                    compensated_add(*value, sum, compensation)
                });
            Ok(if sum.is_infinite() {
                sum
            } else {
                sum + compensation
            })
        }
        AggregateKind::Avg => {
            average_floats(values).ok_or_else(|| execution_error("PromQL average group is empty"))
        }
        AggregateKind::Min => Ok(values.iter().skip(1).fold(first, |minimum, value| {
            if *value < minimum || minimum.is_nan() {
                *value
            } else {
                minimum
            }
        })),
        AggregateKind::Max => Ok(values.iter().skip(1).fold(first, |maximum, value| {
            if *value > maximum || maximum.is_nan() {
                *value
            } else {
                maximum
            }
        })),
        AggregateKind::Count => Ok(values.len() as f64),
        _ => Err(execution_error("unsupported PromQL aggregate kind")),
    }
}

fn average_floats(values: &[f64]) -> Option<f64> {
    let first = *values.first()?;
    let mut sum = first;
    let mut compensation = 0.0;
    let mut mean = 0.0;
    let mut incremental = false;
    let mut count = 1.0;
    for (index, value) in values.iter().enumerate().skip(1) {
        count = (index + 1) as f64;
        if !incremental {
            let (new_sum, new_compensation) = compensated_add(*value, sum, compensation);
            if !new_sum.is_infinite() {
                sum = new_sum;
                compensation = new_compensation;
                continue;
            }
            incremental = true;
            mean = sum / (count - 1.0);
            compensation /= count - 1.0;
        }
        let weight = (count - 1.0) / count;
        (mean, compensation) =
            compensated_add(*value / count, weight * mean, weight * compensation);
    }
    Some(if incremental {
        mean + compensation
    } else {
        sum / count + compensation / count
    })
}

fn compensated_add(increment: f64, sum: f64, mut compensation: f64) -> (f64, f64) {
    let total = sum + increment;
    if total.is_infinite() {
        compensation = 0.0;
    } else if sum.abs() >= increment.abs() {
        compensation += (sum - total) + increment;
    } else {
        compensation += (increment - total) + sum;
    }
    (total, compensation)
}

#[derive(Clone, Copy)]
struct Bucket {
    upper_bound: f64,
    count: f64,
}

fn histogram_quantile(
    vector: Vec<InstantSample>,
    quantile: f64,
    output_timestamp: Timestamp,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<SeriesKey, Vec<Bucket>> = BTreeMap::new();
    for sample in vector {
        let Some(boundary) = sample.series.tags().get("le") else {
            continue;
        };
        let upper_bound = parse_bucket_boundary(boundary)?;
        let tags = sample
            .series
            .tags()
            .iter()
            .filter(|(name, _)| name.as_str() != "le")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        groups
            .entry(SeriesKey::new("", tags))
            .or_default()
            .push(Bucket {
                upper_bound,
                count: sample.value,
            });
    }
    Ok(groups
        .into_iter()
        .map(|(series, buckets)| InstantSample {
            series,
            evaluation_timestamp: output_timestamp,
            source_timestamp: output_timestamp,
            value: bucket_quantile(quantile, buckets),
            drop_metric_name: true,
        })
        .collect())
}

fn parse_bucket_boundary(value: &str) -> Result<f64> {
    let boundary = match value {
        "+Inf" | "Inf" | "+inf" | "inf" => f64::INFINITY,
        "-Inf" | "-inf" => f64::NEG_INFINITY,
        _ => value.parse::<f64>().map_err(|_| {
            execution_error(format!(
                "histogram bucket boundary `{value}` is not numeric"
            ))
        })?,
    };
    if boundary.is_nan() {
        return Err(execution_error("histogram bucket boundary must not be NaN"));
    }
    Ok(boundary)
}

fn bucket_quantile(quantile: f64, mut buckets: Vec<Bucket>) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if buckets.is_empty() {
        return f64::NAN;
    }
    buckets.sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
    if !buckets
        .last()
        .is_some_and(|bucket| bucket.upper_bound == f64::INFINITY)
    {
        return f64::NAN;
    }
    let mut coalesced: Vec<Bucket> = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        if let Some(last) = coalesced.last_mut() {
            if last.upper_bound == bucket.upper_bound {
                last.count += bucket.count;
                continue;
            }
        }
        coalesced.push(bucket);
    }
    ensure_monotonic(&mut coalesced);
    if coalesced.len() < 2 {
        return f64::NAN;
    }
    let observations = coalesced.last().map_or(0.0, |bucket| bucket.count);
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = quantile * observations;
    let last_index = coalesced.len() - 1;
    let bucket_index = match coalesced
        .iter()
        .take(last_index)
        .position(|bucket| bucket.count >= rank)
    {
        Some(index) => index,
        None => last_index,
    };
    if bucket_index == last_index {
        return coalesced[last_index - 1].upper_bound;
    }
    if bucket_index == 0 && coalesced[0].upper_bound <= 0.0 {
        return coalesced[0].upper_bound;
    }
    let mut bucket_start = 0.0;
    let bucket_end = coalesced[bucket_index].upper_bound;
    let mut count = coalesced[bucket_index].count;
    if bucket_index > 0 {
        bucket_start = coalesced[bucket_index - 1].upper_bound;
        count -= coalesced[bucket_index - 1].count;
        rank -= coalesced[bucket_index - 1].count;
    }
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

fn ensure_monotonic(buckets: &mut [Bucket]) {
    let Some(first) = buckets.first() else {
        return;
    };
    let mut previous = first.count;
    for bucket in buckets.iter_mut().skip(1) {
        if bucket.count == previous {
            continue;
        }
        if almost_equal(previous, bucket.count, SMALL_DELTA_TOLERANCE) || bucket.count < previous {
            bucket.count = previous;
        } else {
            previous = bucket.count;
        }
    }
}

fn almost_equal(left: f64, right: f64, epsilon: f64) -> bool {
    if left.is_nan() && right.is_nan() {
        return true;
    }
    if left == right {
        return true;
    }
    let min_normal = f64::from_bits(0x0010_0000_0000_0000);
    let absolute_sum = left.abs() + right.abs();
    let difference = (left - right).abs();
    if left == 0.0 || right == 0.0 || absolute_sum < min_normal {
        difference < epsilon * min_normal
    } else {
        difference / absolute_sum.min(f64::MAX) < epsilon
    }
}

fn sql_aggregate(
    groups: GroupSet,
    calls: &[AggregateCall],
    parameters: &[Option<f64>],
) -> Result<AggregateSet> {
    let rows = groups
        .groups
        .into_iter()
        .map(|group| {
            let values = calls
                .iter()
                .enumerate()
                .map(|(index, call)| {
                    sql_aggregate_call(call, parameters.get(index).copied().flatten(), &group.rows)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(AggregateRow {
                key_values: group.key_values,
                values,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(AggregateSet {
        keys: groups.keys,
        rows,
    })
}

fn sql_aggregate_call(
    call: &AggregateCall,
    parameter: Option<f64>,
    rows: &[SequencedRow],
) -> Result<TableValue> {
    if call.kind == AggregateKind::HistogramQuantile {
        let _ = parameter
            .ok_or_else(|| execution_error("SQL HISTOGRAM_QUANTILE requires a scalar parameter"))?;
        return Err(TsmError::Unsupported {
            feature:
                "SQL HISTOGRAM_QUANTILE requires a typed bucket representation not present in v0.3 storage"
                    .to_string(),
            line: 0,
            column: 0,
            offset: 0,
        });
    }
    match call.kind {
        AggregateKind::First
        | AggregateKind::Last
        | AggregateKind::Rate
        | AggregateKind::Delta
        | AggregateKind::Derivative => sql_time_series_aggregate(call, rows),
        AggregateKind::Count => {
            if call.argument == AggregateInput::Wildcard {
                return Ok(TableValue::UInt64(rows.len() as u64));
            }
            let mut values = sql_argument_values(call, rows)?;
            if call.distinct {
                let mut seen = BTreeSet::new();
                values.retain(|value| seen.insert(ValueKey::from(value)));
            }
            Ok(TableValue::UInt64(
                values.iter().filter(|value| !value.is_null()).count() as u64,
            ))
        }
        AggregateKind::Sum | AggregateKind::Avg | AggregateKind::Min | AggregateKind::Max => {
            let mut values = sql_argument_values(call, rows)?
                .into_iter()
                .filter(|value| !value.is_null())
                .collect::<Vec<_>>();
            if call.distinct {
                let mut seen = BTreeSet::new();
                values.retain(|value| seen.insert(ValueKey::from(value)));
            }
            sql_standard_aggregate(call.kind, &values)
        }
        AggregateKind::HistogramQuantile => Err(execution_error(
            "histogram quantile dispatch was not handled",
        )),
    }
}

fn sql_argument_values(call: &AggregateCall, rows: &[SequencedRow]) -> Result<Vec<TableValue>> {
    let AggregateInput::Expression(expression) = &call.argument else {
        return Err(execution_error(
            "SQL aggregate requires an expression argument",
        ));
    };
    rows.iter()
        .map(|row| evaluate_expression(expression, EvalRow::Raw(row)))
        .collect()
}

fn sql_standard_aggregate(kind: AggregateKind, values: &[TableValue]) -> Result<TableValue> {
    let Some(first) = values.first() else {
        return Ok(TableValue::Null);
    };
    match kind {
        AggregateKind::Avg => {
            let floats = values.iter().map(numeric_f64).collect::<Result<Vec<_>>>()?;
            Ok(TableValue::Float64(
                average_floats(&floats).unwrap_or(f64::NAN),
            ))
        }
        AggregateKind::Min | AggregateKind::Max => {
            let mut selected = first.clone();
            for value in values.iter().skip(1) {
                let ordering = compare_values(value, &selected)?;
                if (kind == AggregateKind::Min && ordering == Ordering::Less)
                    || (kind == AggregateKind::Max && ordering == Ordering::Greater)
                {
                    selected = value.clone();
                }
            }
            Ok(selected)
        }
        AggregateKind::Sum => sum_sql_values(values),
        _ => Err(execution_error("invalid SQL standard aggregate kind")),
    }
}

fn sum_sql_values(values: &[TableValue]) -> Result<TableValue> {
    match values.first() {
        None => Ok(TableValue::Null),
        Some(TableValue::Float64(_)) => {
            let (sum, compensation) =
                values
                    .iter()
                    .try_fold((0.0, 0.0), |(sum, compensation), value| {
                        let value = numeric_f64(value)?;
                        Ok::<_, TsmError>(compensated_add(value, sum, compensation))
                    })?;
            Ok(TableValue::Float64(if sum.is_infinite() {
                sum
            } else {
                sum + compensation
            }))
        }
        Some(TableValue::Int64(_)) => values
            .iter()
            .try_fold(0_i64, |sum, value| {
                let TableValue::Int64(value) = value else {
                    return Err(execution_error("SUM input types changed within a group"));
                };
                sum.checked_add(*value)
                    .ok_or_else(|| execution_error("SUM(int64) overflows"))
            })
            .map(TableValue::Int64),
        Some(TableValue::UInt64(_)) => values
            .iter()
            .try_fold(0_u64, |sum, value| {
                let TableValue::UInt64(value) = value else {
                    return Err(execution_error("SUM input types changed within a group"));
                };
                sum.checked_add(*value)
                    .ok_or_else(|| execution_error("SUM(uint64) overflows"))
            })
            .map(TableValue::UInt64),
        _ => Err(execution_error("SUM requires numeric values")),
    }
}

fn sql_time_series_aggregate(call: &AggregateCall, rows: &[SequencedRow]) -> Result<TableValue> {
    let AggregateInput::Expression(argument) = &call.argument else {
        return Err(execution_error(
            "SQL time-series aggregate requires a value expression",
        ));
    };
    let time_expression = match call.auxiliary.first() {
        Some(expression) => expression.clone(),
        None => PlanExpression {
            kind: PlanExpressionKind::Column {
                name: "time".to_string(),
            },
        },
    };
    if call.auxiliary.len() > 1 {
        return Err(execution_error(
            "SQL time-series aggregate has too many auxiliary expressions",
        ));
    }
    let mut samples = rows
        .iter()
        .map(|row| {
            let value = evaluate_expression(argument, EvalRow::Raw(row))?;
            let timestamp = match evaluate_expression(&time_expression, EvalRow::Raw(row))? {
                TableValue::TimestampNanosecond(value) => value,
                _ => {
                    return Err(execution_error(
                        "SQL time-series aggregate ordering value must be a timestamp",
                    ));
                }
            };
            Ok((timestamp, row.ingest_seq().get(), value))
        })
        .collect::<Result<Vec<_>>>()?;
    samples.sort_by_key(|(timestamp, sequence, _)| (*timestamp, *sequence));
    match call.kind {
        AggregateKind::First => Ok(samples
            .first()
            .map_or(TableValue::Null, |(_, _, value)| value.clone())),
        AggregateKind::Last => Ok(samples
            .last()
            .map_or(TableValue::Null, |(_, _, value)| value.clone())),
        AggregateKind::Delta => {
            let (Some((_, _, first)), Some((_, _, last))) = (samples.first(), samples.last())
            else {
                return Ok(TableValue::Null);
            };
            if samples.len() < 2 || first.is_null() || last.is_null() {
                return Ok(TableValue::Null);
            }
            numeric_difference(last, first)
        }
        AggregateKind::Derivative | AggregateKind::Rate => {
            let numeric = samples
                .iter()
                .filter(|(_, _, value)| !value.is_null())
                .map(|(timestamp, _, value)| Ok((*timestamp, numeric_f64(value)?)))
                .collect::<Result<Vec<_>>>()?;
            let (Some((first_timestamp, first)), Some((last_timestamp, last))) =
                (numeric.first(), numeric.last())
            else {
                return Ok(TableValue::Null);
            };
            if numeric.len() < 2 {
                return Ok(TableValue::Null);
            }
            let elapsed = (i128::from(*last_timestamp) - i128::from(*first_timestamp)) as f64
                / NANOS_PER_SECOND;
            if elapsed == 0.0 {
                return Ok(TableValue::Null);
            }
            let difference = if call.kind == AggregateKind::Rate {
                let mut increase = last - first;
                for pair in numeric.windows(2) {
                    if pair[1].1 < pair[0].1 {
                        increase += pair[0].1;
                    }
                }
                increase
            } else {
                last - first
            };
            Ok(TableValue::Float64(difference / elapsed))
        }
        _ => Err(execution_error("invalid SQL time-series aggregate kind")),
    }
}

fn numeric_difference(left: &TableValue, right: &TableValue) -> Result<TableValue> {
    match (left, right) {
        (TableValue::Float64(left), TableValue::Float64(right)) => {
            Ok(TableValue::Float64(left - right))
        }
        (TableValue::Int64(left), TableValue::Int64(right)) => left
            .checked_sub(*right)
            .map(TableValue::Int64)
            .ok_or_else(|| execution_error("DELTA(int64) overflows")),
        (TableValue::UInt64(left), TableValue::UInt64(right)) => left
            .checked_sub(*right)
            .map(TableValue::UInt64)
            .ok_or_else(|| execution_error("DELTA(uint64) would be negative")),
        _ => Err(execution_error("DELTA input types changed within a group")),
    }
}

fn ordered_float_bits(value: f64) -> u64 {
    let value = if value == 0.0 {
        0.0
    } else if value.is_nan() {
        f64::NAN
    } else {
        value
    };
    let bits = value.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}

#[cfg(test)]
mod tests {
    use super::{compare_values, floor_time_bucket, Ordering, TableValue};

    #[test]
    fn time_bucket_floors_negative_timestamps_and_reports_boundary_overflow() {
        assert_eq!(floor_time_bucket(-1, 10).expect("negative bucket"), -10);
        assert!(floor_time_bucket(i64::MIN, 3).is_err());
    }

    #[test]
    fn mixed_signed_unsigned_comparisons_preserve_integer_precision() {
        let above_f64_precision = 9_007_199_254_740_993_u64;
        assert_eq!(
            compare_values(
                &TableValue::UInt64(above_f64_precision),
                &TableValue::Int64(9_007_199_254_740_992),
            )
            .expect("mixed comparison"),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&TableValue::Int64(-1), &TableValue::UInt64(0))
                .expect("negative comparison"),
            Ordering::Less
        );
    }
}
