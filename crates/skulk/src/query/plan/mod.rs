//! Language-independent logical query plan vocabulary.

use crate::query::LabelMatcher;
use crate::{Result, TsmError};
use std::collections::BTreeSet;

mod resolution;

pub use resolution::{
    select_plan_resolutions, Resolution, ResolutionCapability, ResolutionCatalog,
    ResolutionRequirements, ScanResolution,
};

#[cfg(feature = "promql")]
mod promql;

#[cfg(feature = "promql")]
pub use promql::plan_promql;

#[cfg(feature = "sql-ts")]
mod sql;

#[cfg(feature = "sql-ts")]
pub use sql::plan_sql;

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
    /// Result expression evaluation and aliasing.
    Project(ProjectNode),
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

/// One timestamp bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeBound {
    /// Nanosecond timestamp.
    pub value: i64,
    /// Whether the timestamp itself is included.
    pub inclusive: bool,
}

/// Optional lower and upper scan bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanTimeRange {
    /// Lower timestamp bound.
    pub start: Option<TimeBound>,
    /// Upper timestamp bound.
    pub end: Option<TimeBound>,
}

impl PlanTimeRange {
    /// Creates validated optional bounds.
    pub fn new(start: Option<TimeBound>, end: Option<TimeBound>) -> Result<Self> {
        if matches!(
            (start, end),
            (Some(start), Some(end))
                if start.value > end.value
                    || (start.value == end.value && (!start.inclusive || !end.inclusive))
        ) {
            return Err(TsmError::InvalidInput(
                "logical scan time range is empty or inverted".to_string(),
            ));
        }
        Ok(Self { start, end })
    }

    /// Creates an open-lower, closed-upper Prometheus window `(start, end]`.
    pub fn prometheus_window(start_exclusive: i64, end_inclusive: i64) -> Result<Self> {
        Self::new(
            Some(TimeBound {
                value: start_exclusive,
                inclusive: false,
            }),
            Some(TimeBound {
                value: end_inclusive,
                inclusive: true,
            }),
        )
    }

    /// Creates an unbounded range.
    pub const fn all() -> Self {
        Self {
            start: None,
            end: None,
        }
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
    /// Exact field columns to decode, or `None` for every field.
    pub field_projection: Option<BTreeSet<String>>,
    /// Physical data resolution selected for this scan.
    pub resolution: ScanResolution,
}

/// A language-independent residual predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanPredicate {
    /// A label predicate evaluated with missing labels treated as empty strings.
    Label(LabelMatcher),
    /// A typed language-independent scalar predicate.
    Expression(PlanExpression),
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
    /// Window or explicit expression grouping.
    pub kind: SeriesGroupKind,
}

/// Shared series grouping strategies.
#[derive(Debug, Clone, PartialEq)]
pub enum SeriesGroupKind {
    /// Prometheus per-series evaluation window.
    Window(SeriesWindow),
    /// SQL grouping key expressions.
    Keys(Vec<PlanExpression>),
}

/// Supported range-vector transformations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    /// SQL first value by time.
    First,
    /// SQL last value by time.
    Last,
    /// SQL counter rate.
    Rate,
    /// SQL successive-value delta.
    Delta,
    /// SQL time derivative.
    Derivative,
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
    /// Aggregate calls evaluated over each input group.
    pub calls: Vec<AggregateCall>,
    /// Optional `by`/`without` grouping.
    pub grouping: Option<SeriesGrouping>,
    /// Single, partial, or final stage.
    pub stage: AggregationStage,
}

/// One aggregate call within an aggregate operator.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateCall {
    /// Aggregate identity.
    pub kind: AggregateKind,
    /// Current vector values, a SQL expression, or wildcard rows.
    pub argument: AggregateInput,
    /// Optional scalar parameter such as histogram quantile.
    pub parameter: Option<Box<PlanNode>>,
    /// Additional scalar inputs, such as the ordering timestamp for FIRST/LAST.
    pub auxiliary: Vec<PlanExpression>,
    /// SQL DISTINCT modifier.
    pub distinct: bool,
}

/// Input consumed by one aggregate call.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateInput {
    /// Current PromQL vector value.
    CurrentValue,
    /// All rows, as in `COUNT(*)`.
    Wildcard,
    /// One SQL scalar expression.
    Expression(PlanExpression),
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

/// Language-independent scalar expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanExpression {
    /// Expression-specific data.
    pub kind: PlanExpressionKind,
}

/// Scalar expression variants shared by SQL planning and execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanExpressionKind {
    /// Named input column.
    Column {
        /// Bare logical name (`time` denotes the system timestamp).
        name: String,
    },
    /// Number spelling retained from SQL.
    Number(String),
    /// UTF-8 literal.
    String(String),
    /// Boolean literal.
    Boolean(bool),
    /// SQL NULL.
    Null,
    /// Query-start timestamp substituted for `NOW()`.
    Timestamp(i64),
    /// Checked SQL interval in nanoseconds.
    Interval(i64),
    /// Binary scalar operation.
    Binary {
        /// Left operand.
        left: Box<PlanExpression>,
        /// Operation.
        op: ScalarBinaryKind,
        /// Right operand.
        right: Box<PlanExpression>,
    },
    /// Unary scalar operation.
    Unary {
        /// Operation.
        op: ScalarUnaryKind,
        /// Operand.
        expression: Box<PlanExpression>,
    },
    /// BETWEEN predicate.
    Between {
        /// Tested expression.
        expression: Box<PlanExpression>,
        /// Lower bound.
        low: Box<PlanExpression>,
        /// Upper bound.
        high: Box<PlanExpression>,
        /// Negated form.
        negated: bool,
    },
    /// Pattern predicate.
    Pattern {
        /// Tested expression.
        expression: Box<PlanExpression>,
        /// Pattern expression.
        pattern: Box<PlanExpression>,
        /// Optional escape expression.
        escape: Option<Box<PlanExpression>>,
        /// Negated form.
        negated: bool,
        /// Pattern operation.
        kind: PatternMatchKind,
    },
    /// IN-list predicate.
    InList {
        /// Tested expression.
        expression: Box<PlanExpression>,
        /// Candidate values.
        list: Vec<PlanExpression>,
        /// Negated form.
        negated: bool,
    },
    /// IS NULL predicate.
    IsNull {
        /// Tested expression.
        expression: Box<PlanExpression>,
        /// IS NOT NULL form.
        negated: bool,
    },
    /// Fixed-width timestamp bucketing.
    TimeBucket {
        /// Bucket width in nanoseconds.
        interval_ns: i64,
        /// Timestamp column.
        column: String,
    },
    /// Reference to one output of the preceding Aggregate node.
    AggregateResult {
        /// Zero-based aggregate call index.
        index: usize,
    },
}

/// Shared scalar binary operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarBinaryKind {
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
    /// UTF-8 concatenation.
    StringConcat,
}

/// Shared scalar unary operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarUnaryKind {
    /// Boolean negation.
    Not,
    /// Numeric negation.
    Minus,
}

/// Shared string pattern operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternMatchKind {
    /// SQL LIKE.
    Like,
    /// Case-insensitive LIKE.
    ILike,
    /// Glob matching.
    Glob,
    /// SQL SIMILAR TO.
    SimilarTo,
}

/// One projected result expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionExpression {
    /// Expression after aggregate calls are replaced with result references.
    pub expression: PlanExpression,
    /// Optional result alias.
    pub alias: Option<String>,
}

/// Result projection operator.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectNode {
    /// Input operator.
    pub input: Box<PlanNode>,
    /// Explicit projection expressions in source order; wildcard is tracked separately.
    pub expressions: Vec<ProjectionExpression>,
    /// Whether a wildcard is present.
    pub wildcard: bool,
}

/// One language-independent sort key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    /// Resolved sort expression.
    pub expression: PlanExpression,
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
