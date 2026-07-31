//! Storage-independent logical-plan execution core.

mod aggregate;
mod functions;
pub mod limits;

pub use aggregate::{TableColumn, TableResult, TableValue};
pub use limits::ExecutionLimits as ExecutorConfig;

use crate::model::{FieldValue, SeriesKey, Timestamp};
use crate::query::plan::{
    LogicalPlan, PlanNode, PlanPredicate, PlanTimeRange, PlanValueType, ScanNode, SeriesGroupKind,
    SeriesWindow, SeriesWindowKind, TimeBound,
};
use crate::query::{LabelMatcher, MatchOp};
use crate::store::reader::{
    FloatSeriesScanResult, PreparedTagPredicate, ScanDecodeLimits, ScanRequest, ScanTimeRange,
    StorageReader, TagPredicate, TagPredicateOp,
};
use crate::store::seq::SequencedRow;
use crate::{Result, TsmError};
use limits::{ParsingLimits, QueryExecutionContext, QueryLimits};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Prometheus's exact IEEE-754 staleness marker bit pattern.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Returns whether a floating-point value is Prometheus's staleness marker.
pub fn is_stale_nan(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}

/// Validated inclusive range-query evaluation schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationRange {
    start: Timestamp,
    end: Timestamp,
    step_ns: i64,
}

impl EvaluationRange {
    /// Creates an inclusive schedule with a positive step.
    pub fn new(start: Timestamp, end: Timestamp, step_ns: i64) -> Result<Self> {
        if start > end {
            return Err(TsmError::InvalidInput(
                "range query start must not exceed end".to_string(),
            ));
        }
        if step_ns <= 0 {
            return Err(TsmError::InvalidInput(
                "range query step must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            start,
            end,
            step_ns,
        })
    }

    /// Returns the first evaluation timestamp.
    pub const fn start(self) -> Timestamp {
        self.start
    }

    /// Returns the inclusive final timestamp boundary.
    pub const fn end(self) -> Timestamp {
        self.end
    }

    /// Returns the positive step in nanoseconds.
    pub const fn step_ns(self) -> i64 {
        self.step_ns
    }

    fn step_count(self) -> Result<usize> {
        let span = i128::from(self.end) - i128::from(self.start);
        let count = span
            .checked_div(i128::from(self.step_ns))
            .and_then(|steps| steps.checked_add(1))
            .ok_or_else(|| TsmError::ResourceLimit("range step count overflows".to_string()))?;
        usize::try_from(count)
            .map_err(|_| TsmError::ResourceLimit("range step count exceeds usize".to_string()))
    }
}

/// One timestamped floating-point sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FloatSample {
    timestamp: Timestamp,
    value: f64,
}

impl FloatSample {
    fn new(timestamp: Timestamp, value: f64) -> Self {
        Self { timestamp, value }
    }

    /// Returns the nanosecond timestamp.
    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }

    /// Returns the exact stored floating-point value.
    pub const fn value(self) -> f64 {
        self.value
    }
}

/// One series sample returned by an instant evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    series: SeriesKey,
    evaluation_timestamp: Timestamp,
    source_timestamp: Timestamp,
    value: f64,
    drop_metric_name: bool,
}

impl InstantSample {
    /// Returns the exact measurement and canonical tags.
    pub const fn series(&self) -> &SeriesKey {
        &self.series
    }

    /// Returns the query evaluation timestamp written to the result.
    pub const fn evaluation_timestamp(&self) -> Timestamp {
        self.evaluation_timestamp
    }

    /// Returns the selected stored timestamp, or the evaluation timestamp for
    /// a computed function result.
    pub const fn source_timestamp(&self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns the selected floating-point value.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Returns whether PromQL result materialization must omit `__name__`.
    pub const fn metric_name_is_dropped(&self) -> bool {
        self.drop_metric_name
    }
}

/// Raw samples for one series in a range-vector window.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSeries {
    series: SeriesKey,
    samples: Vec<FloatSample>,
}

impl RangeSeries {
    /// Returns the exact measurement and canonical tags.
    pub const fn series(&self) -> &SeriesKey {
        &self.series
    }

    /// Returns source-timestamped samples after staleness filtering.
    pub fn samples(&self) -> &[FloatSample] {
        &self.samples
    }
}

/// Evaluated samples for one series across a range query.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSeries {
    series: SeriesKey,
    samples: Vec<FloatSample>,
    drop_metric_name: bool,
}

impl MatrixSeries {
    /// Returns the exact measurement and canonical tags.
    pub const fn series(&self) -> &SeriesKey {
        &self.series
    }

    /// Returns samples timestamped at range-query evaluation steps.
    pub fn samples(&self) -> &[FloatSample] {
        &self.samples
    }

    /// Returns whether PromQL result materialization must omit `__name__`.
    pub const fn metric_name_is_dropped(&self) -> bool {
        self.drop_metric_name
    }
}

/// One numeric scalar at a PromQL evaluation timestamp.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScalarSample {
    timestamp: Timestamp,
    value: f64,
}

impl ScalarSample {
    /// Returns the query evaluation timestamp.
    pub const fn timestamp(self) -> Timestamp {
        self.timestamp
    }

    /// Returns the scalar value.
    pub const fn value(self) -> f64 {
        self.value
    }
}

/// Public execution values produced by the v0.4 operator pipeline.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionValue {
    /// One numeric scalar at the evaluation timestamp.
    Scalar(ScalarSample),
    /// One selected value per series.
    InstantVector(Vec<InstantSample>),
    /// Raw window samples per series for range functions.
    RangeVector(Vec<RangeSeries>),
    /// One typed SQL table.
    Table(TableResult),
}

/// Logical-plan executor depending only on the read-only StorageReader contract.
pub struct Executor<'a> {
    reader: &'a dyn StorageReader,
    context: QueryExecutionContext,
}

impl<'a> Executor<'a> {
    /// Creates an executor with bounded embedded defaults.
    pub fn new(reader: &'a dyn StorageReader) -> Self {
        Self {
            reader,
            context: QueryExecutionContext::with_default_timeout(QueryLimits::DEFAULT),
        }
    }

    /// Creates an executor with explicit non-zero limits.
    pub fn with_config(reader: &'a dyn StorageReader, config: ExecutorConfig) -> Self {
        Self {
            reader,
            context: QueryExecutionContext::with_default_timeout(
                QueryLimits::DEFAULT.with_execution(config),
            ),
        }
    }

    /// Creates an executor sharing one query-wide limit/deadline/cancellation context.
    pub fn with_context(reader: &'a dyn StorageReader, context: QueryExecutionContext) -> Self {
        Self { reader, context }
    }

    /// Evaluates the currently implemented operator subset.
    pub fn evaluate(
        &self,
        plan: &LogicalPlan,
        output_timestamp: Timestamp,
    ) -> Result<ExecutionValue> {
        self.context.check()?;
        let result = match self.evaluate_node(&plan.root, output_timestamp)? {
            OperatorValue::Scalar(value) => Ok(ExecutionValue::Scalar(ScalarSample {
                timestamp: output_timestamp,
                value,
            })),
            OperatorValue::InstantVector(vector) => Ok(ExecutionValue::InstantVector(vector)),
            OperatorValue::RangeVector(range) => Ok(ExecutionValue::RangeVector(range.series)),
            OperatorValue::Table(table) => Ok(ExecutionValue::Table(table)),
            OperatorValue::Rows(_) | OperatorValue::Groups(_) | OperatorValue::AggregateRows(_) => {
                Err(execution_error(
                    "logical plan did not produce its public result type",
                ))
            }
        };
        self.context.check()?;
        result
    }

    /// Executes a schema-preserving SQL table plan.
    pub fn execute_table(&self, plan: &LogicalPlan) -> Result<TableResult> {
        if plan.output_type != PlanValueType::Table {
            return Err(execution_error(
                "table execution requires a Table logical plan",
            ));
        }
        match self.evaluate(plan, 0)? {
            ExecutionValue::Table(table) => Ok(table),
            _ => Err(execution_error(
                "table execution produced a non-table value",
            )),
        }
    }

    /// Executes one instant-vector plan at its externally visible timestamp.
    pub fn execute_instant(
        &self,
        plan: &LogicalPlan,
        evaluation_timestamp: Timestamp,
    ) -> Result<Vec<InstantSample>> {
        if plan.output_type != PlanValueType::InstantVector {
            return Err(execution_error(
                "instant execution requires an InstantVector logical plan",
            ));
        }
        match self.evaluate(plan, evaluation_timestamp)? {
            ExecutionValue::InstantVector(vector) => Ok(vector),
            ExecutionValue::Scalar(_)
            | ExecutionValue::RangeVector(_)
            | ExecutionValue::Table(_) => Err(execution_error(
                "instant execution produced a non-vector value",
            )),
        }
    }

    /// Executes a plan created at `range.start()` at each inclusive step.
    ///
    /// Offset-adjusted selector windows and their scan ranges are rebased by
    /// the same checked timestamp delta for every step.
    pub fn execute_range(
        &self,
        plan_at_start: &LogicalPlan,
        range: EvaluationRange,
    ) -> Result<Vec<MatrixSeries>> {
        if plan_at_start.output_type != PlanValueType::InstantVector {
            return Err(execution_error(
                "range execution requires an InstantVector logical plan",
            ));
        }
        let step_count = range.step_count()?;
        check_limit(
            step_count,
            self.context.limits().execution().max_range_steps(),
            "range evaluation steps",
        )?;

        let mut matrix: BTreeMap<(SeriesKey, bool), Vec<FloatSample>> = BTreeMap::new();
        let mut output_samples = 0usize;
        let mut timestamp = range.start;
        loop {
            self.context.check()?;
            let mut step_plan = plan_at_start.clone();
            rebase_node(&mut step_plan.root, range.start, timestamp)?;
            let vector = self.execute_instant(&step_plan, timestamp)?;
            output_samples = output_samples.checked_add(vector.len()).ok_or_else(|| {
                TsmError::ResourceLimit("range output sample count overflows".to_string())
            })?;
            check_limit(
                output_samples,
                self.context.limits().execution().max_output_samples(),
                "range output samples",
            )?;
            for sample in vector {
                matrix
                    .entry((sample.series, sample.drop_metric_name))
                    .or_default()
                    .push(FloatSample::new(timestamp, sample.value));
            }

            let Some(next) = timestamp.checked_add(range.step_ns) else {
                break;
            };
            if next > range.end {
                break;
            }
            timestamp = next;
        }
        self.context.check()?;
        Ok(matrix
            .into_iter()
            .map(|((series, drop_metric_name), samples)| MatrixSeries {
                series,
                samples,
                drop_metric_name,
            })
            .collect())
    }

    fn evaluate_node(&self, node: &PlanNode, output_timestamp: Timestamp) -> Result<OperatorValue> {
        self.context.check()?;
        let result = match node {
            PlanNode::Scan(scan) => self.scan(scan),
            PlanNode::Filter(filter) => {
                let input = self.evaluate_node(&filter.input, output_timestamp)?;
                self.filter(input, &filter.predicates)
            }
            PlanNode::SeriesGroup(group) => {
                let input = self.evaluate_node(&group.input, output_timestamp)?;
                match &group.kind {
                    SeriesGroupKind::Window(window) => {
                        self.group_window(input, *window, output_timestamp)
                    }
                    SeriesGroupKind::Keys(keys) => aggregate::group_rows(input, keys),
                }
            }
            PlanNode::RangeFunction(function) => {
                let input = self.evaluate_node(&function.input, output_timestamp)?;
                let OperatorValue::RangeVector(range) = input else {
                    return Err(execution_error(
                        "range-vector functions require a RangeVector input",
                    ));
                };
                let vector = functions::evaluate(function.function, range, output_timestamp);
                check_limit(
                    vector.len(),
                    self.context.limits().execution().max_output_samples(),
                    "range-function output samples",
                )?;
                Ok(OperatorValue::InstantVector(vector))
            }
            PlanNode::Aggregate(node) => {
                if let Some(output) = self.try_float_time_bucket_aggregate(node)? {
                    Ok(output)
                } else {
                    let input = self.evaluate_node(&node.input, output_timestamp)?;
                    let mut parameters = Vec::with_capacity(node.calls.len());
                    for call in &node.calls {
                        let value = if let Some(parameter) = &call.parameter {
                            match self.evaluate_node(parameter, output_timestamp)? {
                                OperatorValue::Scalar(value) => Some(value),
                                _ => {
                                    return Err(execution_error(
                                        "aggregate parameters must evaluate to scalars",
                                    ));
                                }
                            }
                        } else {
                            None
                        };
                        parameters.push(value);
                    }
                    let output =
                        aggregate::evaluate_aggregate(input, node, &parameters, output_timestamp)?;
                    if let OperatorValue::InstantVector(vector) = &output {
                        check_limit(
                            vector.len(),
                            self.context.limits().execution().max_output_samples(),
                            "aggregate output samples",
                        )?;
                    }
                    Ok(output)
                }
            }
            PlanNode::Binary(node) => {
                let left = self.evaluate_node(&node.left, output_timestamp)?;
                let right = self.evaluate_node(&node.right, output_timestamp)?;
                aggregate::binary(left, node.op, right)
            }
            PlanNode::Project(node) => {
                let input = self.evaluate_node(&node.input, output_timestamp)?;
                aggregate::project(input, node)
            }
            PlanNode::Sort(node) => {
                let input = self.evaluate_node(&node.input, output_timestamp)?;
                aggregate::sort(input, &node.keys)
            }
            PlanNode::Limit(node) => {
                let input = self.evaluate_node(&node.input, output_timestamp)?;
                aggregate::limit(input, node.rows)
            }
            PlanNode::Scalar(value) => Ok(OperatorValue::Scalar(*value)),
            PlanNode::String(_) => Err(unsupported_operator(
                "standalone string plan nodes are not executable values",
            )),
        };
        self.context.check()?;
        result
    }

    fn try_float_time_bucket_aggregate(
        &self,
        node: &crate::query::plan::AggregateNode,
    ) -> Result<Option<OperatorValue>> {
        let Some((scan, field, interval_ns)) = aggregate::float_time_bucket_scan(node) else {
            return Ok(None);
        };
        let Some(time_range) = storage_time_range(scan.time_range)? else {
            let empty = FloatSeriesScanResult::new(Vec::new(), Default::default());
            return aggregate::aggregate_float_time_buckets(&empty, node, interval_ns).map(Some);
        };
        let measurement = scan
            .measurement
            .exact
            .as_deref()
            .ok_or_else(|| execution_error("float scan requires one exact measurement"))?;
        let parsing_limits = self.context.limits().parsing();
        let predicates = scan
            .tag_equalities
            .iter()
            .map(to_tag_predicate)
            .collect::<Result<Vec<_>>>()?;
        for predicate in &predicates {
            predicate.prepare_with_limits(
                parsing_limits.max_regex_bytes(),
                parsing_limits.max_regex_automaton_bytes(),
            )?;
        }
        let scan_limits = self.context.limits().scan();
        let request = ScanRequest::new(measurement, time_range)
            .with_field_projection([field])
            .with_tag_predicates(predicates)
            .with_decode_limits(ScanDecodeLimits::new(
                scan_limits.max_decoded_rows(),
                scan_limits.max_decoded_bytes(),
            ));
        self.context.check()?;
        let Some(result) = self.reader.try_scan_float_series(&request, field)? else {
            return Ok(None);
        };
        self.context.check()?;
        check_limit(
            result.stats().decoded_rows(),
            scan_limits.max_decoded_rows(),
            "query decoded rows",
        )?;
        check_limit(
            result.stats().decoded_bytes(),
            scan_limits.max_decoded_bytes(),
            "query decoded bytes",
        )?;
        check_limit(
            result.series().len(),
            scan_limits.max_series_expansion(),
            "series expansion",
        )?;
        let point_count = result.series().iter().try_fold(0_usize, |total, series| {
            total.checked_add(series.points().len()).ok_or_else(|| {
                TsmError::ResourceLimit("float scan point count overflows".to_string())
            })
        })?;
        check_limit(
            point_count,
            self.context.limits().execution().max_intermediate_rows(),
            "float scan result rows",
        )?;
        aggregate::aggregate_float_time_buckets(result.as_ref(), node, interval_ns).map(Some)
    }

    fn scan(&self, scan: &ScanNode) -> Result<OperatorValue> {
        self.context.check()?;
        if !scan.resolution.is_raw() {
            return Err(unsupported_operator(format!(
                "StorageReader has no v0.4 adapter for resolution `{}`",
                scan.resolution.name()
            )));
        }
        let Some(time_range) = storage_time_range(scan.time_range)? else {
            return Ok(OperatorValue::Rows(RowSet {
                rows: Vec::new(),
                field_projection: scan.field_projection.clone(),
            }));
        };
        let mut measurements = match scan.measurement.exact.as_deref() {
            Some(measurement) => vec![measurement.to_string()],
            None => self.reader.measurement_names()?,
        };
        measurements.sort();
        measurements.dedup();
        let scan_limits = self.context.limits().scan();
        check_limit(
            measurements.len(),
            scan_limits.max_series_expansion(),
            "measurement expansion",
        )?;
        let measurement_predicates = scan
            .measurement
            .matchers
            .iter()
            .map(|matcher| {
                let limits = self.context.limits().parsing();
                to_tag_predicate(matcher)?.prepare_with_limits(
                    limits.max_regex_bytes(),
                    limits.max_regex_automaton_bytes(),
                )
            })
            .collect::<Result<Vec<PreparedTagPredicate>>>()?;
        let predicates = scan
            .tag_equalities
            .iter()
            .map(to_tag_predicate)
            .collect::<Result<Vec<_>>>()?;
        let parsing_limits = self.context.limits().parsing();
        for predicate in &predicates {
            predicate.prepare_with_limits(
                parsing_limits.max_regex_bytes(),
                parsing_limits.max_regex_automaton_bytes(),
            )?;
        }

        let mut rows = Vec::new();
        let mut expanded_series = BTreeSet::new();
        let mut decoded_rows = 0usize;
        let mut decoded_bytes = 0usize;
        for measurement in measurements {
            self.context.check()?;
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
            let mut request =
                ScanRequest::new(measurement, time_range).with_decode_limits(decode_limits);
            if let Some(fields) = &scan.field_projection {
                request = request.with_field_projection(fields.iter().cloned());
            }
            request = request.with_tag_predicates(predicates.clone());
            let result = self.reader.scan(&request)?;
            self.context.check()?;
            decoded_rows = decoded_rows
                .checked_add(result.stats().decoded_rows())
                .ok_or_else(|| {
                    TsmError::ResourceLimit("query decoded row count overflows".to_string())
                })?;
            decoded_bytes = decoded_bytes
                .checked_add(result.stats().decoded_bytes())
                .ok_or_else(|| {
                    TsmError::ResourceLimit("query decoded byte count overflows".to_string())
                })?;
            check_limit(
                decoded_rows,
                scan_limits.max_decoded_rows(),
                "query decoded rows",
            )?;
            check_limit(
                decoded_bytes,
                scan_limits.max_decoded_bytes(),
                "query decoded bytes",
            )?;
            let mut scanned = result.into_rows();
            for row in &scanned {
                expanded_series.insert(row.row().shared_series());
                check_limit(
                    expanded_series.len(),
                    scan_limits.max_series_expansion(),
                    "series expansion",
                )?;
            }
            let row_count = rows.len().checked_add(scanned.len()).ok_or_else(|| {
                TsmError::ResourceLimit("scan result row count overflows".to_string())
            })?;
            check_limit(
                row_count,
                self.context.limits().execution().max_intermediate_rows(),
                "scan result rows",
            )?;
            rows.append(&mut scanned);
        }
        Ok(OperatorValue::Rows(RowSet {
            rows,
            field_projection: scan.field_projection.clone(),
        }))
    }

    fn filter(&self, input: OperatorValue, predicates: &[PlanPredicate]) -> Result<OperatorValue> {
        let OperatorValue::Rows(mut rows) = input else {
            return Err(execution_error(
                "label Filter currently requires storage rows",
            ));
        };
        let prepared = predicates
            .iter()
            .filter_map(|predicate| match predicate {
                PlanPredicate::Label(matcher) => Some(prepare_label_matcher(
                    matcher,
                    self.context.limits().parsing(),
                )),
                PlanPredicate::Expression(_) => None,
            })
            .collect::<Result<Vec<_>>>()?;
        let mut filtered = Vec::with_capacity(rows.rows.len());
        for row in rows.rows {
            if !prepared
                .iter()
                .all(|matcher| matcher.matches(row.row().series()))
            {
                continue;
            }
            let mut keep = true;
            for predicate in predicates {
                if let PlanPredicate::Expression(expression) = predicate {
                    keep &= aggregate::expression_is_true(expression, &row)?;
                }
            }
            if keep {
                filtered.push(row);
            }
        }
        rows.rows = filtered;
        check_limit(
            rows.rows.len(),
            self.context.limits().execution().max_intermediate_rows(),
            "filtered rows",
        )?;
        Ok(OperatorValue::Rows(rows))
    }

    fn group_window(
        &self,
        input: OperatorValue,
        window: SeriesWindow,
        output_timestamp: Timestamp,
    ) -> Result<OperatorValue> {
        if window.duration_ns <= 0 {
            return Err(execution_error(
                "series window duration must be greater than zero",
            ));
        }
        let OperatorValue::Rows(rows) = input else {
            return Err(execution_error(
                "SeriesGroup window currently requires storage rows",
            ));
        };
        let field = single_float_field(&rows.field_projection)?;
        match window.kind {
            SeriesWindowKind::Instant => {
                self.group_instant(rows.rows, field, window, output_timestamp)
            }
            SeriesWindowKind::Range => self.group_range(rows.rows, field, window),
        }
    }

    fn group_instant(
        &self,
        rows: Vec<SequencedRow>,
        field: &str,
        window: SeriesWindow,
        output_timestamp: Timestamp,
    ) -> Result<OperatorValue> {
        let lower = window
            .evaluation_time
            .checked_sub(window.duration_ns)
            .ok_or_else(|| execution_error("instant lookback lower bound overflows"))?;
        let mut latest: BTreeMap<Arc<SeriesKey>, (Timestamp, u64, f64)> = BTreeMap::new();
        for row in rows {
            let timestamp = row.row().timestamp();
            if timestamp <= lower || timestamp > window.evaluation_time {
                continue;
            }
            let Some(value) = float_field(row.row().field(field), field)? else {
                continue;
            };
            let candidate = (timestamp, row.ingest_seq().get(), value);
            let entry = latest.entry(row.row().shared_series()).or_insert(candidate);
            if (candidate.0, candidate.1) > (entry.0, entry.1) {
                *entry = candidate;
            }
        }
        check_limit(
            latest.len(),
            self.context.limits().execution().max_intermediate_rows(),
            "instant grouped series",
        )?;
        let vector = latest
            .into_iter()
            .filter_map(|(series, (source_timestamp, _, value))| {
                (!is_stale_nan(value)).then_some(InstantSample {
                    series: (*series).clone(),
                    evaluation_timestamp: output_timestamp,
                    source_timestamp,
                    value,
                    drop_metric_name: false,
                })
            })
            .collect::<Vec<_>>();
        check_limit(
            vector.len(),
            self.context.limits().execution().max_output_samples(),
            "instant output samples",
        )?;
        Ok(OperatorValue::InstantVector(vector))
    }

    fn group_range(
        &self,
        rows: Vec<SequencedRow>,
        field: &str,
        window: SeriesWindow,
    ) -> Result<OperatorValue> {
        let lower = window
            .evaluation_time
            .checked_sub(window.duration_ns)
            .ok_or_else(|| execution_error("range-vector lower bound overflows"))?;
        let mut grouped: BTreeMap<Arc<SeriesKey>, Vec<FloatSample>> = BTreeMap::new();
        let mut sample_count = 0usize;
        for row in rows {
            let timestamp = row.row().timestamp();
            if timestamp <= lower || timestamp > window.evaluation_time {
                continue;
            }
            let Some(value) = float_field(row.row().field(field), field)? else {
                continue;
            };
            if is_stale_nan(value) {
                continue;
            }
            sample_count = sample_count.checked_add(1).ok_or_else(|| {
                TsmError::ResourceLimit("range-vector sample count overflows".to_string())
            })?;
            check_limit(
                sample_count,
                self.context.limits().execution().max_intermediate_rows(),
                "range-vector samples",
            )?;
            grouped
                .entry(row.row().shared_series())
                .or_default()
                .push(FloatSample::new(timestamp, value));
        }
        Ok(OperatorValue::RangeVector(RangeVectorValue {
            series: grouped
                .into_iter()
                .map(|(series, samples)| RangeSeries {
                    series: (*series).clone(),
                    samples,
                })
                .collect(),
            window,
        }))
    }
}

enum OperatorValue {
    Rows(RowSet),
    Groups(aggregate::GroupSet),
    AggregateRows(aggregate::AggregateSet),
    Scalar(f64),
    InstantVector(Vec<InstantSample>),
    RangeVector(RangeVectorValue),
    Table(TableResult),
}

struct RangeVectorValue {
    series: Vec<RangeSeries>,
    window: SeriesWindow,
}

struct RowSet {
    rows: Vec<SequencedRow>,
    field_projection: Option<BTreeSet<String>>,
}

struct PreparedSeriesMatcher {
    name: String,
    predicate: PreparedTagPredicate,
}

impl PreparedSeriesMatcher {
    fn matches(&self, series: &SeriesKey) -> bool {
        if self.name == "__name__" {
            self.predicate.matches_value(series.measurement())
        } else {
            self.predicate.matches(series.tags())
        }
    }
}

/// Evaluates one label matcher, treating a missing label as the empty string.
pub fn label_matches(matcher: &LabelMatcher, series: &SeriesKey) -> Result<bool> {
    Ok(prepare_label_matcher(matcher, ParsingLimits::DEFAULT)?.matches(series))
}

fn prepare_label_matcher(
    matcher: &LabelMatcher,
    limits: ParsingLimits,
) -> Result<PreparedSeriesMatcher> {
    Ok(PreparedSeriesMatcher {
        name: matcher.name.clone(),
        predicate: to_tag_predicate(matcher)?
            .prepare_with_limits(limits.max_regex_bytes(), limits.max_regex_automaton_bytes())?,
    })
}

fn to_tag_predicate(matcher: &LabelMatcher) -> Result<TagPredicate> {
    if matcher.name.is_empty() {
        return Err(TsmError::InvalidInput(
            "label matcher name must be non-empty".to_string(),
        ));
    }
    let operation = match matcher.op {
        MatchOp::Equal => TagPredicateOp::Equal,
        MatchOp::NotEqual => TagPredicateOp::NotEqual,
        MatchOp::Regex => TagPredicateOp::Regex,
        MatchOp::NotRegex => TagPredicateOp::NotRegex,
    };
    Ok(TagPredicate::new(
        matcher.name.clone(),
        operation,
        matcher.value.clone(),
    ))
}

fn single_float_field(projection: &Option<BTreeSet<String>>) -> Result<&str> {
    let Some(fields) = projection else {
        return Err(execution_error(
            "Prometheus window execution requires one projected field",
        ));
    };
    if fields.len() != 1 {
        return Err(execution_error(
            "Prometheus window execution requires exactly one projected field",
        ));
    }
    fields
        .first()
        .map(String::as_str)
        .ok_or_else(|| execution_error("Prometheus field projection is empty"))
}

fn float_field(value: Option<&FieldValue>, field: &str) -> Result<Option<f64>> {
    match value {
        None => Ok(None),
        Some(FieldValue::Float(value)) => Ok(Some(*value)),
        Some(other) => Err(TsmError::Type {
            message: format!(
                "Prometheus field `{field}` must be float, found {:?}",
                other.field_type()
            ),
            line: 0,
            column: 0,
            offset: 0,
        }),
    }
}

fn storage_time_range(range: PlanTimeRange) -> Result<Option<ScanTimeRange>> {
    if matches!(
        range.start,
        Some(TimeBound {
            value: Timestamp::MAX,
            inclusive: false
        })
    ) || matches!(
        range.end,
        Some(TimeBound {
            value: Timestamp::MIN,
            inclusive: false
        })
    ) {
        return Ok(None);
    }
    let start = discrete_lower_bound(range.start)?;
    let end = discrete_upper_bound(range.end)?;
    if matches!((start, end), (Some(start), Some(end)) if start > end) {
        return Ok(None);
    }
    ScanTimeRange::new(start, end).map(Some)
}

fn discrete_lower_bound(bound: Option<TimeBound>) -> Result<Option<Timestamp>> {
    match bound {
        None => Ok(None),
        Some(bound) if bound.inclusive => Ok(Some(bound.value)),
        Some(bound) => bound.value.checked_add(1).map(Some).ok_or_else(|| {
            execution_error("exclusive scan lower bound has no representable timestamp")
        }),
    }
}

fn discrete_upper_bound(bound: Option<TimeBound>) -> Result<Option<Timestamp>> {
    match bound {
        None => Ok(None),
        Some(bound) if bound.inclusive => Ok(Some(bound.value)),
        Some(bound) => bound.value.checked_sub(1).map(Some).ok_or_else(|| {
            execution_error("exclusive scan upper bound has no representable timestamp")
        }),
    }
}

fn rebase_node(node: &mut PlanNode, from: Timestamp, to: Timestamp) -> Result<()> {
    match node {
        PlanNode::Scan(scan) => {
            scan.time_range = PlanTimeRange::new(
                rebase_bound(scan.time_range.start, from, to)?,
                rebase_bound(scan.time_range.end, from, to)?,
            )?;
        }
        PlanNode::Filter(filter) => rebase_node(&mut filter.input, from, to)?,
        PlanNode::SeriesGroup(group) => {
            if let SeriesGroupKind::Window(window) = &mut group.kind {
                window.evaluation_time = rebase_timestamp(window.evaluation_time, from, to)?;
            }
            rebase_node(&mut group.input, from, to)?;
        }
        PlanNode::RangeFunction(function) => rebase_node(&mut function.input, from, to)?,
        PlanNode::Aggregate(aggregate) => {
            rebase_node(&mut aggregate.input, from, to)?;
            for call in &mut aggregate.calls {
                if let Some(parameter) = &mut call.parameter {
                    rebase_node(parameter, from, to)?;
                }
            }
        }
        PlanNode::Binary(binary) => {
            rebase_node(&mut binary.left, from, to)?;
            rebase_node(&mut binary.right, from, to)?;
        }
        PlanNode::Project(project) => rebase_node(&mut project.input, from, to)?,
        PlanNode::Sort(sort) => rebase_node(&mut sort.input, from, to)?,
        PlanNode::Limit(limit) => rebase_node(&mut limit.input, from, to)?,
        PlanNode::Scalar(_) | PlanNode::String(_) => {}
    }
    Ok(())
}

fn rebase_bound(
    bound: Option<TimeBound>,
    from: Timestamp,
    to: Timestamp,
) -> Result<Option<TimeBound>> {
    bound
        .map(|bound| {
            Ok(TimeBound {
                value: rebase_timestamp(bound.value, from, to)?,
                inclusive: bound.inclusive,
            })
        })
        .transpose()
}

fn rebase_timestamp(value: Timestamp, from: Timestamp, to: Timestamp) -> Result<Timestamp> {
    let rebased = i128::from(value) + i128::from(to) - i128::from(from);
    Timestamp::try_from(rebased)
        .map_err(|_| execution_error("range-query timestamp rebasing overflows"))
}

fn check_limit(actual: usize, maximum: usize, resource: &str) -> Result<()> {
    if actual > maximum {
        return Err(TsmError::ResourceLimit(format!(
            "{resource} count {actual} exceeds limit {maximum}"
        )));
    }
    Ok(())
}

fn execution_error(message: impl Into<String>) -> TsmError {
    TsmError::Plan {
        message: message.into(),
        line: 0,
        column: 0,
        offset: 0,
    }
}

fn unsupported_operator(feature: impl Into<String>) -> TsmError {
    TsmError::Unsupported {
        feature: feature.into(),
        line: 0,
        column: 0,
        offset: 0,
    }
}
