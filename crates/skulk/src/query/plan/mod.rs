//! Language-independent logical query plan vocabulary.

use crate::query::LabelMatcher;
use crate::{Result, TsmError};
use std::collections::BTreeSet;

#[cfg(feature = "promql")]
mod promql;

#[cfg(feature = "promql")]
pub use promql::plan_promql;

/// Default instant-selector lookback window in nanoseconds.
pub const DEFAULT_LOOKBACK_NS: i64 = 300_000_000_000;

/// Planning-time evaluation settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanContext {
    evaluation_time: i64,
    lookback_ns: i64,
}

impl PlanContext {
    /// Creates an instant evaluation context with the five-minute default lookback.
    pub const fn instant(evaluation_time: i64) -> Self {
        Self {
            evaluation_time,
            lookback_ns: DEFAULT_LOOKBACK_NS,
        }
    }

    /// Creates an instant context with an explicit positive lookback.
    pub fn with_lookback(evaluation_time: i64, lookback_ns: i64) -> Result<Self> {
        if lookback_ns <= 0 {
            return Err(TsmError::InvalidInput(
                "query lookback must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            evaluation_time,
            lookback_ns,
        })
    }

    /// Returns the unshifted evaluation timestamp.
    pub const fn evaluation_time(self) -> i64 {
        self.evaluation_time
    }

    /// Returns the instant-selector lookback in nanoseconds.
    pub const fn lookback_ns(self) -> i64 {
        self.lookback_ns
    }
}

/// The logical value category produced by a plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlanValueType {
    /// One floating-point scalar.
    Scalar,
    /// One sample per series at an evaluation time.
    InstantVector,
    /// A time window of samples per series.
    RangeVector,
    /// A string scalar retained for complete frontend representation.
    String,
    /// A schema-preserving SQL table.
    Table,
}

/// A complete logical query plan.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    /// Root operator.
    pub root: PlanNode,
    /// Logical result category.
    pub output_type: PlanValueType,
}

/// Language-independent logical operator tree.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    /// Storage scan with early pruning inputs.
    Scan(ScanNode),
    /// Residual predicate evaluation.
    Filter(FilterNode),
    /// Deterministic grouping into per-series evaluation windows.
    SeriesGroup(SeriesGroupNode),
    /// One range-vector transformation.
    RangeFunction(RangeFunctionNode),
    /// Group or histogram aggregation.
    Aggregate(AggregateNode),
    /// Scalar/vector arithmetic.
    Binary(BinaryNode),
    /// Numeric scalar literal.
    Scalar(f64),
    /// String scalar literal.
    String(String),
    /// Stable result ordering.
    Sort(SortNode),
    /// Result row bound.
    Limit(LimitNode),
}

/// Measurement constraints extracted from a source selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasurementSelection {
    /// Exact measurement from a metric name or equality matcher.
    pub exact: Option<String>,
    /// Additional `__name__` predicates, evaluated conjunctively.
    pub matchers: Vec<LabelMatcher>,
}

/// An open-lower, closed-upper scan range matching Prometheus windows `(start, end]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanTimeRange {
    start_exclusive: i64,
    end_inclusive: i64,
}

impl PlanTimeRange {
    /// Creates a non-empty `(start, end]` range.
    pub fn new(start_exclusive: i64, end_inclusive: i64) -> Result<Self> {
        if start_exclusive >= end_inclusive {
            return Err(TsmError::InvalidInput(
                "logical scan time range must have start < end".to_string(),
            ));
        }
        Ok(Self {
            start_exclusive,
            end_inclusive,
        })
    }

    /// Returns the excluded lower timestamp.
    pub const fn start_exclusive(self) -> i64 {
        self.start_exclusive
    }

    /// Returns the included upper timestamp.
    pub const fn end_inclusive(self) -> i64 {
        self.end_inclusive
    }
}

/// One storage scan and its pushdown candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanNode {
    /// Exact or matcher-based measurement selection.
    pub measurement: MeasurementSelection,
    /// Offset-adjusted evaluation window.
    pub time_range: PlanTimeRange,
    /// Tag equality predicates eligible for storage pruning.
    pub tag_equalities: Vec<LabelMatcher>,
    /// Exact field columns to decode.
    pub field_projection: BTreeSet<String>,
}

/// A language-independent residual predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanPredicate {
    /// A label predicate evaluated with missing labels treated as empty strings.
    Label(LabelMatcher),
}

/// Residual filter operator.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    /// Input operator.
    pub input: Box<PlanNode>,
    /// Conjunctive predicates not admitted to scan pushdown.
    pub predicates: Vec<PlanPredicate>,
}

/// Per-series sample selection semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SeriesWindowKind {
    /// Latest non-stale sample in the lookback window.
    Instant,
    /// All non-stale samples in a range-vector window.
    Range,
}

/// One offset-adjusted per-series evaluation window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeriesWindow {
    /// Instant or range semantics.
    pub kind: SeriesWindowKind,
    /// Evaluation timestamp after applying `offset`.
    pub evaluation_time: i64,
    /// Lookback or explicit range duration.
    pub duration_ns: i64,
}

/// Series grouping operator.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesGroupNode {
    /// Scan or filtered scan input.
    pub input: Box<PlanNode>,
    /// Per-series window semantics.
    pub window: SeriesWindow,
}

/// Supported range-vector transformations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeFunctionKind {
    /// Counter rate with reset correction and extrapolation.
    Rate,
    /// Instant rate over the last two samples.
    IRate,
    /// Counter increase with reset correction and extrapolation.
    Increase,
    /// Average over a range.
    AvgOverTime,
    /// Minimum over a range.
    MinOverTime,
    /// Maximum over a range.
    MaxOverTime,
    /// Sum over a range.
    SumOverTime,
    /// Sample count over a range.
    CountOverTime,
}

/// Range function operator.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeFunctionNode {
    /// Per-series range input.
    pub input: Box<PlanNode>,
    /// Function identity.
    pub function: RangeFunctionKind,
}

/// Shared aggregation identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateKind {
    /// Sum.
    Sum,
    /// Average.
    Avg,
    /// Maximum.
    Max,
    /// Minimum.
    Min,
    /// Count.
    Count,
    /// Prometheus classic-bucket histogram quantile.
    HistogramQuantile,
}

/// Aggregation execution stage, ready for future partial/final splitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationStage {
    /// One-node aggregation used by v0.4.
    Single,
    /// Future shard-local partial aggregation.
    Partial,
    /// Future cross-shard reduction.
    Final,
}

/// Prometheus label grouping semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesGrouping {
    /// Grouping label names in frontend order.
    pub labels: Vec<String>,
    /// `true` for `without`, `false` for `by`.
    pub without: bool,
}

/// Aggregate operator.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateNode {
    /// Instant-vector input.
    pub input: Box<PlanNode>,
    /// Aggregate identity.
    pub kind: AggregateKind,
    /// Optional `by`/`without` grouping.
    pub grouping: Option<SeriesGrouping>,
    /// Single, partial, or final stage.
    pub stage: AggregationStage,
    /// Optional scalar parameter, used by histogram quantile.
    pub parameter: Option<Box<PlanNode>>,
}

/// Shared arithmetic identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithmeticKind {
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

/// Binary arithmetic operator.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryNode {
    /// Left operand.
    pub left: Box<PlanNode>,
    /// Operation.
    pub op: ArithmeticKind,
    /// Right operand.
    pub right: Box<PlanNode>,
}

/// One language-independent sort key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    /// Result column or alias.
    pub name: String,
    /// Descending rather than ascending.
    pub descending: bool,
    /// Optional explicit null placement.
    pub nulls_first: Option<bool>,
}

/// Stable sort operator.
#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    /// Input operator.
    pub input: Box<PlanNode>,
    /// Keys in comparison priority order.
    pub keys: Vec<SortKey>,
}

/// Row limit operator.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    /// Input operator.
    pub input: Box<PlanNode>,
    /// Maximum output rows.
    pub rows: u64,
}
