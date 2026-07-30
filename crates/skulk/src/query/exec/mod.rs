//! Storage-independent logical-plan execution core.

mod functions;

use crate::model::{FieldValue, SeriesKey, Timestamp};
use crate::query::plan::{
    LogicalPlan, PlanNode, PlanPredicate, PlanTimeRange, PlanValueType, ScanNode, SeriesGroupKind,
    SeriesWindow, SeriesWindowKind, TimeBound,
};
use crate::query::{LabelMatcher, MatchOp};
use crate::store::reader::{
    PreparedTagPredicate, ScanRequest, ScanTimeRange, StorageReader, TagPredicate, TagPredicateOp,
};
use crate::store::seq::SequencedRow;
use crate::{Result, TsmError};
use std::collections::{BTreeMap, BTreeSet};

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

/// Allocation limits applied at executor operator boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutorConfig {
    max_intermediate_rows: usize,
    max_output_samples: usize,
    max_range_steps: usize,
}

impl ExecutorConfig {
    /// Embedded defaults, bounded independently from StorageReader decode limits.
    pub const DEFAULT: Self = Self {
        max_intermediate_rows: 1_000_000,
        max_output_samples: 1_000_000,
        max_range_steps: 100_000,
    };

    /// Creates a configuration whose limits must all be non-zero.
    pub fn new(
        max_intermediate_rows: usize,
        max_output_samples: usize,
        max_range_steps: usize,
    ) -> Result<Self> {
        if max_intermediate_rows == 0 || max_output_samples == 0 || max_range_steps == 0 {
            return Err(TsmError::InvalidInput(
                "executor row, output, and range-step limits must be non-zero".to_string(),
            ));
        }
        Ok(Self {
            max_intermediate_rows,
            max_output_samples,
            max_range_steps,
        })
    }

    /// Returns the per-operator input row/sample limit.
    pub const fn max_intermediate_rows(self) -> usize {
        self.max_intermediate_rows
    }

    /// Returns the complete query output sample limit.
    pub const fn max_output_samples(self) -> usize {
        self.max_output_samples
    }

    /// Returns the maximum number of inclusive range evaluations.
    pub const fn max_range_steps(self) -> usize {
        self.max_range_steps
    }
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self::DEFAULT
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

/// Public execution values produced by the v0.4 operator pipeline.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionValue {
    /// One selected value per series.
    InstantVector(Vec<InstantSample>),
    /// Raw window samples per series for range functions.
    RangeVector(Vec<RangeSeries>),
}

/// Logical-plan executor depending only on the read-only StorageReader contract.
pub struct Executor<'a> {
    reader: &'a dyn StorageReader,
    config: ExecutorConfig,
}

impl<'a> Executor<'a> {
    /// Creates an executor with bounded embedded defaults.
    pub fn new(reader: &'a dyn StorageReader) -> Self {
        Self {
            reader,
            config: ExecutorConfig::default(),
        }
    }

    /// Creates an executor with explicit non-zero limits.
    pub const fn with_config(reader: &'a dyn StorageReader, config: ExecutorConfig) -> Self {
        Self { reader, config }
    }

    /// Evaluates the currently implemented operator subset.
    pub fn evaluate(
        &self,
        plan: &LogicalPlan,
        output_timestamp: Timestamp,
    ) -> Result<ExecutionValue> {
        match self.evaluate_node(&plan.root, output_timestamp)? {
            OperatorValue::InstantVector(vector) => Ok(ExecutionValue::InstantVector(vector)),
            OperatorValue::RangeVector(range) => Ok(ExecutionValue::RangeVector(range.series)),
            OperatorValue::Rows(_) => Err(execution_error(
                "a Scan or Filter cannot be a final query result",
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
            ExecutionValue::RangeVector(_) => {
                Err(execution_error("instant execution produced a range vector"))
            }
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
            self.config.max_range_steps,
            "range evaluation steps",
        )?;

        let mut matrix: BTreeMap<(SeriesKey, bool), Vec<FloatSample>> = BTreeMap::new();
        let mut output_samples = 0usize;
        let mut timestamp = range.start;
        loop {
            let mut step_plan = plan_at_start.clone();
            rebase_node(&mut step_plan.root, range.start, timestamp)?;
            let vector = self.execute_instant(&step_plan, timestamp)?;
            output_samples = output_samples.checked_add(vector.len()).ok_or_else(|| {
                TsmError::ResourceLimit("range output sample count overflows".to_string())
            })?;
            check_limit(
                output_samples,
                self.config.max_output_samples,
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
        match node {
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
                    SeriesGroupKind::Keys(_) => Err(unsupported_operator(
                        "SQL expression grouping is implemented in Task 11",
                    )),
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
                    self.config.max_output_samples,
                    "range-function output samples",
                )?;
                Ok(OperatorValue::InstantVector(vector))
            }
            PlanNode::Aggregate(_)
            | PlanNode::Binary(_)
            | PlanNode::Project(_)
            | PlanNode::Sort(_)
            | PlanNode::Limit(_)
            | PlanNode::Scalar(_)
            | PlanNode::String(_) => Err(unsupported_operator(
                "aggregate, arithmetic, scalar, and SQL operators are implemented in Task 11",
            )),
        }
    }

    fn scan(&self, scan: &ScanNode) -> Result<OperatorValue> {
        if !scan.resolution.is_raw() {
            return Err(unsupported_operator(format!(
                "StorageReader has no v0.4 adapter for resolution `{}`",
                scan.resolution.name()
            )));
        }
        let Some(measurement) = scan.measurement.exact.as_deref() else {
            return Err(unsupported_operator(
                "matcher-only measurement enumeration is provided by the unified query engine",
            ));
        };
        for matcher in &scan.measurement.matchers {
            if !label_matches_value(matcher, measurement)? {
                return Ok(OperatorValue::Rows(RowSet {
                    rows: Vec::new(),
                    field_projection: scan.field_projection.clone(),
                }));
            }
        }
        let Some(time_range) = storage_time_range(scan.time_range)? else {
            return Ok(OperatorValue::Rows(RowSet {
                rows: Vec::new(),
                field_projection: scan.field_projection.clone(),
            }));
        };
        let mut request = ScanRequest::new(measurement, time_range);
        if let Some(fields) = &scan.field_projection {
            request = request.with_field_projection(fields.iter().cloned());
        }
        let predicates = scan
            .tag_equalities
            .iter()
            .map(to_tag_predicate)
            .collect::<Result<Vec<_>>>()?;
        request = request.with_tag_predicates(predicates);

        let rows = self.reader.scan(&request)?.into_rows();
        check_limit(
            rows.len(),
            self.config.max_intermediate_rows,
            "scan result rows",
        )?;
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
            .map(|predicate| match predicate {
                PlanPredicate::Label(matcher) => prepare_label_matcher(matcher),
                PlanPredicate::Expression(_) => Err(unsupported_operator(
                    "SQL scalar filters are implemented in Task 11",
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        rows.rows.retain(|row| {
            prepared
                .iter()
                .all(|matcher| matcher.matches(row.row().series()))
        });
        check_limit(
            rows.rows.len(),
            self.config.max_intermediate_rows,
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
        let mut latest: BTreeMap<SeriesKey, (Timestamp, u64, f64)> = BTreeMap::new();
        for row in rows {
            let timestamp = row.row().timestamp();
            if timestamp <= lower || timestamp > window.evaluation_time {
                continue;
            }
            let Some(value) = float_field(row.row().field(field), field)? else {
                continue;
            };
            let candidate = (timestamp, row.ingest_seq().get(), value);
            let entry = latest
                .entry(row.row().series().clone())
                .or_insert(candidate);
            if (candidate.0, candidate.1) > (entry.0, entry.1) {
                *entry = candidate;
            }
        }
        check_limit(
            latest.len(),
            self.config.max_intermediate_rows,
            "instant grouped series",
        )?;
        let vector = latest
            .into_iter()
            .filter_map(|(series, (source_timestamp, _, value))| {
                (!is_stale_nan(value)).then_some(InstantSample {
                    series,
                    evaluation_timestamp: output_timestamp,
                    source_timestamp,
                    value,
                    drop_metric_name: false,
                })
            })
            .collect::<Vec<_>>();
        check_limit(
            vector.len(),
            self.config.max_output_samples,
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
        let mut grouped: BTreeMap<SeriesKey, Vec<FloatSample>> = BTreeMap::new();
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
                self.config.max_intermediate_rows,
                "range-vector samples",
            )?;
            grouped
                .entry(row.row().series().clone())
                .or_default()
                .push(FloatSample::new(timestamp, value));
        }
        Ok(OperatorValue::RangeVector(RangeVectorValue {
            series: grouped
                .into_iter()
                .map(|(series, samples)| RangeSeries { series, samples })
                .collect(),
            window,
        }))
    }
}

enum OperatorValue {
    Rows(RowSet),
    InstantVector(Vec<InstantSample>),
    RangeVector(RangeVectorValue),
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
    Ok(prepare_label_matcher(matcher)?.matches(series))
}

fn label_matches_value(matcher: &LabelMatcher, value: &str) -> Result<bool> {
    Ok(to_tag_predicate(matcher)?.prepare()?.matches_value(value))
}

fn prepare_label_matcher(matcher: &LabelMatcher) -> Result<PreparedSeriesMatcher> {
    Ok(PreparedSeriesMatcher {
        name: matcher.name.clone(),
        predicate: to_tag_predicate(matcher)?.prepare()?,
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
