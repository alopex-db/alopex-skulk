//! HTTP-independent query facade and metadata enumeration.

use super::exec::limits::{
    CancellationToken, QueryExecutionContext, QueryLimits, DEFAULT_QUERY_TIMEOUT,
};
use super::exec::{
    EvaluationRange, ExecutionValue, Executor, ExecutorConfig, MatrixSeries, RangeSeries,
    TableResult, TableValue,
};
use super::plan::{LogicalPlan, PlanDataType, PlanValueType};
use super::{LabelMatcher, MatchOp, QueryResult};
use crate::model::{SeriesKey, Tags, Timestamp};
use crate::store::reader::{
    ScanDecodeLimits, ScanRequest, ScanTimeRange, StorageReader, TagPredicate, TagPredicateOp,
};
use crate::{Result, TsmError};
use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::UInt32Type;
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

/// Conjunctive label matchers and an inclusive time range for metadata calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequest {
    matchers: Vec<LabelMatcher>,
    time_range: ScanTimeRange,
}

impl MetadataRequest {
    /// Creates a metadata request over one inclusive storage time range.
    pub fn new(matchers: Vec<LabelMatcher>, time_range: ScanTimeRange) -> Self {
        Self {
            matchers,
            time_range,
        }
    }

    /// Returns the conjunctive label matchers.
    pub fn matchers(&self) -> &[LabelMatcher] {
        &self.matchers
    }

    /// Returns the inclusive metadata scan range.
    pub const fn time_range(&self) -> ScanTimeRange {
        self.time_range
    }
}

/// HTTP-independent facade over planning, execution, Arrow materialization,
/// and metadata enumeration.
pub struct QueryEngine<'a, R: StorageReader> {
    reader: &'a R,
    limits: QueryLimits,
    default_timeout: Duration,
}

impl<'a, R: StorageReader> QueryEngine<'a, R> {
    /// Creates an engine with bounded embedded executor defaults.
    pub fn new(reader: &'a R) -> Self {
        Self {
            reader,
            limits: QueryLimits::DEFAULT,
            default_timeout: DEFAULT_QUERY_TIMEOUT,
        }
    }

    /// Creates an engine with explicit executor limits.
    pub const fn with_executor_config(reader: &'a R, executor_config: ExecutorConfig) -> Self {
        Self {
            reader,
            limits: QueryLimits::DEFAULT.with_execution(executor_config),
            default_timeout: DEFAULT_QUERY_TIMEOUT,
        }
    }

    /// Creates an engine with one query-wide limit set and non-zero default timeout.
    pub fn with_governance(
        reader: &'a R,
        limits: QueryLimits,
        default_timeout: Duration,
    ) -> Result<Self> {
        QueryExecutionContext::with_timeout(limits, default_timeout, CancellationToken::new())?;
        Ok(Self {
            reader,
            limits,
            default_timeout,
        })
    }

    /// Returns the query-wide defaults injected into every stage.
    pub const fn limits(&self) -> QueryLimits {
        self.limits
    }

    /// Returns the default per-query wall-clock budget.
    pub const fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    fn default_context(&self) -> Result<QueryExecutionContext> {
        QueryExecutionContext::with_timeout(
            self.limits,
            self.default_timeout,
            CancellationToken::new(),
        )
    }

    /// Executes a pre-built logical plan without requiring a text-query feature.
    pub fn execute(&self, plan: &LogicalPlan, evaluation_time: Timestamp) -> Result<QueryResult> {
        self.execute_with_context(plan, evaluation_time, &self.default_context()?)
    }

    /// Executes a pre-built plan with caller-visible deadline and cancellation.
    pub fn execute_with_context(
        &self,
        plan: &LogicalPlan,
        evaluation_time: Timestamp,
        context: &QueryExecutionContext,
    ) -> Result<QueryResult> {
        context.check()?;
        let value =
            Executor::with_context(self.reader, context.clone()).evaluate(plan, evaluation_time)?;
        context.check()?;
        let result = materialize_execution(value)?;
        context.check()?;
        Ok(result)
    }

    /// Executes an instant-vector or scalar plan at every inclusive range step.
    pub fn execute_range(
        &self,
        plan_at_start: &LogicalPlan,
        range: EvaluationRange,
    ) -> Result<QueryResult> {
        self.execute_range_with_context(plan_at_start, range, &self.default_context()?)
    }

    /// Executes a range schedule with caller-visible deadline and cancellation.
    pub fn execute_range_with_context(
        &self,
        plan_at_start: &LogicalPlan,
        range: EvaluationRange,
        context: &QueryExecutionContext,
    ) -> Result<QueryResult> {
        context.check()?;
        if plan_at_start.output_type == PlanValueType::Scalar {
            return self.execute_scalar_range(plan_at_start, range, context);
        }
        let matrix = Executor::with_context(self.reader, context.clone())
            .execute_range(plan_at_start, range)?;
        context.check()?;
        let result = materialize_matrix(matrix)?;
        context.check()?;
        Ok(result)
    }

    fn execute_scalar_range(
        &self,
        plan: &LogicalPlan,
        range: EvaluationRange,
        context: &QueryExecutionContext,
    ) -> Result<QueryResult> {
        let executor = Executor::with_context(self.reader, context.clone());
        let execution_limits = context.limits().execution();
        let mut points = Vec::new();
        let mut timestamp = range.start();
        loop {
            context.check()?;
            if points.len() >= execution_limits.max_range_steps()
                || points.len() >= execution_limits.max_output_samples()
            {
                return Err(TsmError::ResourceLimit(
                    "scalar range output exceeds executor limits".to_string(),
                ));
            }
            let ExecutionValue::Scalar(sample) = executor.evaluate(plan, timestamp)? else {
                return Err(TsmError::Plan {
                    message: "scalar range plan produced a non-scalar value".to_string(),
                    line: 0,
                    column: 0,
                    offset: 0,
                });
            };
            points.push(LongPoint {
                series: SeriesKey::new("", Tags::new()),
                timestamp,
                value: sample.value(),
            });
            let Some(next) = timestamp.checked_add(range.step_ns()) else {
                break;
            };
            if next > range.end() {
                break;
            }
            timestamp = next;
        }
        context.check()?;
        Ok(QueryResult::Matrix(vec![long_batch(points)?]))
    }

    /// Parses, plans, and executes one PromQL instant query.
    #[cfg(feature = "promql")]
    pub fn query_promql(&self, expression: &str, at: Timestamp) -> Result<QueryResult> {
        self.query_promql_with_context(expression, at, &self.default_context()?)
    }

    /// Executes PromQL instant text with explicit query governance.
    #[cfg(feature = "promql")]
    pub fn query_promql_with_context(
        &self,
        expression: &str,
        at: Timestamp,
        context: &QueryExecutionContext,
    ) -> Result<QueryResult> {
        context.check()?;
        let expression = super::promql::parse_with_limits(expression, context.limits().parsing())?;
        context.check()?;
        let plan = super::plan::plan_promql(&expression, super::plan::PlanContext::instant(at))?;
        context.check()?;
        self.execute_with_context(&plan, at, context)
    }

    /// Parses, plans, and executes one PromQL range query.
    #[cfg(feature = "promql")]
    pub fn query_promql_range(
        &self,
        expression: &str,
        start: Timestamp,
        end: Timestamp,
        step_ns: i64,
    ) -> Result<QueryResult> {
        self.query_promql_range_with_context(
            expression,
            start,
            end,
            step_ns,
            &self.default_context()?,
        )
    }

    /// Executes PromQL range text with explicit query governance.
    #[cfg(feature = "promql")]
    pub fn query_promql_range_with_context(
        &self,
        expression: &str,
        start: Timestamp,
        end: Timestamp,
        step_ns: i64,
        context: &QueryExecutionContext,
    ) -> Result<QueryResult> {
        context.check()?;
        let range = EvaluationRange::new(start, end, step_ns)?;
        let expression = super::promql::parse_with_limits(expression, context.limits().parsing())?;
        context.check()?;
        let plan = super::plan::plan_promql(&expression, super::plan::PlanContext::instant(start))?;
        context.check()?;
        self.execute_range_with_context(&plan, range, context)
    }

    /// Lists canonical series matching all labels and the inclusive time range.
    pub fn series(&self, request: &MetadataRequest) -> Result<Vec<SeriesKey>> {
        self.series_with_context(request, &self.default_context()?)
    }

    /// Lists canonical series with explicit query governance.
    pub fn series_with_context(
        &self,
        request: &MetadataRequest,
        context: &QueryExecutionContext,
    ) -> Result<Vec<SeriesKey>> {
        collect_series(self.reader, request, context).map(|series| series.into_iter().collect())
    }

    /// Lists label names present on matching series, including `__name__`.
    pub fn label_names(&self, request: &MetadataRequest) -> Result<Vec<String>> {
        self.label_names_with_context(request, &self.default_context()?)
    }

    /// Lists label names with explicit query governance.
    pub fn label_names_with_context(
        &self,
        request: &MetadataRequest,
        context: &QueryExecutionContext,
    ) -> Result<Vec<String>> {
        let series = collect_series(self.reader, request, context)?;
        let mut names = BTreeSet::new();
        if !series.is_empty() {
            names.insert("__name__".to_string());
        }
        for series in series {
            names.extend(series.tags().keys().cloned());
        }
        Ok(names.into_iter().collect())
    }

    /// Lists values present for one label on matching series.
    pub fn label_values(&self, label_name: &str, request: &MetadataRequest) -> Result<Vec<String>> {
        self.label_values_with_context(label_name, request, &self.default_context()?)
    }

    /// Lists values for one label with explicit query governance.
    pub fn label_values_with_context(
        &self,
        label_name: &str,
        request: &MetadataRequest,
        context: &QueryExecutionContext,
    ) -> Result<Vec<String>> {
        if label_name.is_empty() {
            return Err(TsmError::InvalidInput(
                "metadata label name must be non-empty".to_string(),
            ));
        }
        let series = collect_series(self.reader, request, context)?;
        let values: BTreeSet<String> = if label_name == "__name__" {
            series
                .iter()
                .map(|series| series.measurement().to_string())
                .collect()
        } else {
            series
                .iter()
                .filter_map(|series| series.tags().get(label_name).cloned())
                .collect()
        };
        Ok(values.into_iter().collect())
    }

    /// Parses, typechecks, plans, and executes one SQL-TS query.
    #[cfg(feature = "sql-ts")]
    pub fn query_sql(&self, text: &str, at: Timestamp) -> Result<QueryResult> {
        self.query_sql_with_context(text, at, &self.default_context()?)
    }

    /// Executes SQL-TS text with explicit query governance.
    #[cfg(feature = "sql-ts")]
    pub fn query_sql_with_context(
        &self,
        text: &str,
        at: Timestamp,
        context: &QueryExecutionContext,
    ) -> Result<QueryResult> {
        context.check()?;
        let query = super::sqlts::parse_with_limits(text, context.limits().parsing())?;
        context.check()?;
        let schema = StorageReader::measurement_schema(self.reader, &query.measurement)?;
        context.check()?;
        let typed = super::sqlts::typecheck::typecheck(query, &schema)?;
        context.check()?;
        let plan = super::plan::plan_sql(&typed, super::plan::PlanContext::instant(at))?;
        context.check()?;
        self.execute_with_context(&plan, at, context)
    }
}

fn collect_series<R: StorageReader>(
    reader: &R,
    request: &MetadataRequest,
    context: &QueryExecutionContext,
) -> Result<BTreeSet<SeriesKey>> {
    context.check()?;
    let parsing_limits = context.limits().parsing();
    let mut measurement_predicates = Vec::new();
    let mut tag_predicates = Vec::new();
    for matcher in request.matchers() {
        let predicate = TagPredicate::new(
            matcher.name.clone(),
            tag_predicate_op(matcher.op),
            matcher.value.clone(),
        );
        if matcher.name == "__name__" {
            measurement_predicates.push(predicate.prepare_with_limits(
                parsing_limits.max_regex_bytes(),
                parsing_limits.max_regex_automaton_bytes(),
            )?);
        } else {
            predicate.prepare_with_limits(
                parsing_limits.max_regex_bytes(),
                parsing_limits.max_regex_automaton_bytes(),
            )?;
            tag_predicates.push(predicate);
        }
    }

    let mut series = BTreeSet::new();
    let measurements = StorageReader::measurement_names(reader)?;
    let scan_limits = context.limits().scan();
    if measurements.len() > scan_limits.max_series_expansion() {
        return Err(TsmError::ResourceLimit(format!(
            "metadata measurement expansion {} exceeds limit {}",
            measurements.len(),
            scan_limits.max_series_expansion()
        )));
    }
    let mut decoded_rows = 0usize;
    let mut decoded_bytes = 0usize;
    for measurement in measurements {
        context.check()?;
        if !measurement_predicates
            .iter()
            .all(|predicate| predicate.matches_value(&measurement))
        {
            continue;
        }
        let decode_limits = ScanDecodeLimits::new(
            scan_limits.max_decoded_rows().saturating_sub(decoded_rows),
            scan_limits
                .max_decoded_bytes()
                .saturating_sub(decoded_bytes),
        );
        let scan = ScanRequest::new(measurement, request.time_range())
            .with_field_projection(std::iter::empty::<String>())
            .with_tag_predicates(tag_predicates.clone())
            .with_decode_limits(decode_limits);
        let result = reader.scan(&scan)?;
        context.check()?;
        decoded_rows = decoded_rows
            .checked_add(result.stats().decoded_rows())
            .ok_or_else(|| {
                TsmError::ResourceLimit("metadata decoded row count overflows".to_string())
            })?;
        decoded_bytes = decoded_bytes
            .checked_add(result.stats().decoded_bytes())
            .ok_or_else(|| {
                TsmError::ResourceLimit("metadata decoded byte count overflows".to_string())
            })?;
        if decoded_rows > scan_limits.max_decoded_rows()
            || decoded_bytes > scan_limits.max_decoded_bytes()
        {
            return Err(TsmError::ResourceLimit(format!(
                "metadata decode used {decoded_rows} rows/{decoded_bytes} bytes; limits are {}/{}",
                scan_limits.max_decoded_rows(),
                scan_limits.max_decoded_bytes()
            )));
        }
        for row in result.into_rows() {
            series.insert(row.row().series().clone());
            if series.len() > scan_limits.max_series_expansion() {
                return Err(TsmError::ResourceLimit(format!(
                    "metadata series expansion {} exceeds limit {}",
                    series.len(),
                    scan_limits.max_series_expansion()
                )));
            }
        }
    }
    context.check()?;
    Ok(series)
}

const fn tag_predicate_op(operation: MatchOp) -> TagPredicateOp {
    match operation {
        MatchOp::Equal => TagPredicateOp::Equal,
        MatchOp::NotEqual => TagPredicateOp::NotEqual,
        MatchOp::Regex => TagPredicateOp::Regex,
        MatchOp::NotRegex => TagPredicateOp::NotRegex,
    }
}

fn materialize_execution(value: ExecutionValue) -> Result<QueryResult> {
    match value {
        ExecutionValue::Scalar(sample) => {
            let point = LongPoint {
                series: SeriesKey::new("", Tags::new()),
                timestamp: sample.timestamp(),
                value: sample.value(),
            };
            Ok(QueryResult::Scalar(long_batch(vec![point])?))
        }
        ExecutionValue::InstantVector(vector) => {
            let points = vector
                .into_iter()
                .map(|sample| LongPoint {
                    series: result_series(sample.series(), sample.metric_name_is_dropped()),
                    timestamp: sample.evaluation_timestamp(),
                    value: sample.value(),
                })
                .collect();
            Ok(QueryResult::Vector(vec![long_batch(points)?]))
        }
        ExecutionValue::RangeVector(matrix) => {
            let points = range_points(matrix);
            Ok(QueryResult::Matrix(vec![long_batch(points)?]))
        }
        ExecutionValue::Table(table) => Ok(QueryResult::Table(vec![materialize_table(&table)?])),
    }
}

fn materialize_matrix(matrix: Vec<MatrixSeries>) -> Result<QueryResult> {
    let mut points = Vec::new();
    for series in matrix {
        let result_series = result_series(series.series(), series.metric_name_is_dropped());
        points.extend(series.samples().iter().map(|sample| LongPoint {
            series: result_series.clone(),
            timestamp: sample.timestamp(),
            value: sample.value(),
        }));
    }
    Ok(QueryResult::Matrix(vec![long_batch(points)?]))
}

fn range_points(matrix: Vec<RangeSeries>) -> Vec<LongPoint> {
    let mut points = Vec::new();
    for series in matrix {
        points.extend(series.samples().iter().map(|sample| LongPoint {
            series: series.series().clone(),
            timestamp: sample.timestamp(),
            value: sample.value(),
        }));
    }
    points
}

fn result_series(series: &SeriesKey, drop_metric_name: bool) -> SeriesKey {
    SeriesKey::new(
        if drop_metric_name {
            ""
        } else {
            series.measurement()
        },
        series.tags().clone(),
    )
}

struct LongPoint {
    series: SeriesKey,
    timestamp: Timestamp,
    value: f64,
}

fn long_batch(mut points: Vec<LongPoint>) -> Result<RecordBatch> {
    points.sort_by(|left, right| {
        left.series
            .cmp(&right.series)
            .then_with(|| left.timestamp.cmp(&right.timestamp))
    });
    let mut label_names = BTreeSet::new();
    if points
        .iter()
        .any(|point| !point.series.measurement().is_empty())
    {
        label_names.insert("__name__".to_string());
    }
    for point in &points {
        label_names.extend(point.series.tags().keys().cloned());
    }

    let mut fields = vec![
        Field::new(
            "_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("_value", DataType::Float64, false),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(
            points
                .iter()
                .map(|point| point.timestamp)
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            points.iter().map(|point| point.value).collect::<Vec<_>>(),
        )),
    ];
    for label_name in label_names {
        fields.push(Field::new(
            &label_name,
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        ));
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for point in &points {
            let value = if label_name == "__name__" {
                (!point.series.measurement().is_empty()).then(|| point.series.measurement())
            } else {
                point.series.tags().get(&label_name).map(String::as_str)
            };
            if let Some(value) = value {
                builder
                    .append(value)
                    .map_err(|error| arrow_error("dictionary label", error))?;
            } else {
                builder.append_null();
            }
        }
        arrays.push(Arc::new(builder.finish()));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| arrow_error("long-form result", error))
}

fn materialize_table(table: &TableResult) -> Result<RecordBatch> {
    for row in table.rows() {
        if row.len() != table.columns().len() {
            return Err(TsmError::Serialization(format!(
                "SQL result row has {} values for {} columns",
                row.len(),
                table.columns().len()
            )));
        }
    }
    let fields = table
        .columns()
        .iter()
        .map(|column| Field::new(column.name(), arrow_data_type(column.data_type()), true))
        .collect::<Vec<_>>();
    let arrays = table
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| table_array(table, index, column.data_type()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| arrow_error("SQL table result", error))
}

fn table_array(table: &TableResult, index: usize, data_type: PlanDataType) -> Result<ArrayRef> {
    match data_type {
        PlanDataType::TimestampNanosecond => {
            let values = table_values(table, index, |value| match value {
                TableValue::TimestampNanosecond(value) => Some(*value),
                _ => None,
            })?;
            Ok(Arc::new(TimestampNanosecondArray::from(values)))
        }
        PlanDataType::Float64 => {
            let values = table_values(table, index, |value| match value {
                TableValue::Float64(value) => Some(*value),
                _ => None,
            })?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        PlanDataType::Int64 => {
            let values = table_values(table, index, |value| match value {
                TableValue::Int64(value) => Some(*value),
                _ => None,
            })?;
            Ok(Arc::new(Int64Array::from(values)))
        }
        PlanDataType::UInt64 => {
            let values = table_values(table, index, |value| match value {
                TableValue::UInt64(value) => Some(*value),
                _ => None,
            })?;
            Ok(Arc::new(UInt64Array::from(values)))
        }
        PlanDataType::Boolean => {
            let values = table_values(table, index, |value| match value {
                TableValue::Boolean(value) => Some(*value),
                _ => None,
            })?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
        PlanDataType::Utf8 => {
            let values = table_values(table, index, |value| match value {
                TableValue::Utf8(value) => Some(value.as_str()),
                _ => None,
            })?;
            Ok(Arc::new(StringArray::from(values)))
        }
    }
}

fn table_values<'a, T>(
    table: &'a TableResult,
    index: usize,
    convert: impl Fn(&'a TableValue) -> Option<T>,
) -> Result<Vec<Option<T>>> {
    table
        .rows()
        .iter()
        .map(|row| match row.get(index) {
            Some(TableValue::Null) => Ok(None),
            Some(value) => convert(value)
                .map(Some)
                .ok_or_else(|| TsmError::Serialization("SQL value/type mismatch".to_string())),
            None => Err(TsmError::Serialization(
                "SQL result row is shorter than its schema".to_string(),
            )),
        })
        .collect()
}

const fn arrow_data_type(data_type: PlanDataType) -> DataType {
    match data_type {
        PlanDataType::TimestampNanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),
        PlanDataType::Float64 => DataType::Float64,
        PlanDataType::Int64 => DataType::Int64,
        PlanDataType::UInt64 => DataType::UInt64,
        PlanDataType::Boolean => DataType::Boolean,
        PlanDataType::Utf8 => DataType::Utf8,
    }
}

fn arrow_error(context: &str, error: impl std::fmt::Display) -> TsmError {
    TsmError::Serialization(format!("{context}: {error}"))
}
