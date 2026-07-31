//! Conservative physical resolution catalog and logical-plan selection.

use super::{
    AggregateInput, AggregateKind, LogicalPlan, PlanExpression, PlanExpressionKind, PlanNode,
    PlanPredicate, PlanTimeRange, RangeFunctionKind, SeriesGroupKind, TimeBound,
};
use crate::{Result, TsmError};
use std::collections::{BTreeMap, BTreeSet};

const RAW_NAME: &str = "raw";

/// The physical data source selected for one scan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScanResolution {
    /// Exact stored samples.
    Raw,
    /// A registered downsampled source.
    Rollup {
        /// Stable catalog name used by the storage implementation.
        name: String,
        /// Fixed epoch-aligned bucket width in nanoseconds.
        interval_ns: i64,
    },
}

impl ScanResolution {
    /// Returns the exact raw source.
    pub const fn raw() -> Self {
        Self::Raw
    }

    /// Returns whether this selection reads exact raw samples.
    pub const fn is_raw(&self) -> bool {
        matches!(self, Self::Raw)
    }

    /// Returns the stable catalog name.
    pub fn name(&self) -> &str {
        match self {
            Self::Raw => RAW_NAME,
            Self::Rollup { name, .. } => name,
        }
    }

    /// Returns the rollup bucket width, or `None` for raw.
    pub const fn interval_ns(&self) -> Option<i64> {
        match self {
            Self::Raw => None,
            Self::Rollup { interval_ns, .. } => Some(*interval_ns),
        }
    }
}

/// An operation whose semantics a rollup explicitly promises to preserve.
///
/// Rollup schemas are required to expose the same logical measurement, tag,
/// and field names as raw. Capabilities cover operations where pre-aggregation
/// can otherwise change results. Raw implicitly supports every capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResolutionCapability {
    /// A residual scalar predicate can be evaluated exactly.
    Filter,
    /// Fixed-width SQL time bucketing can be reconstructed exactly.
    TimeBucket,
    /// One PromQL range-vector function.
    RangeFunction(RangeFunctionKind),
    /// One PromQL or SQL aggregate.
    Aggregate(AggregateKind),
}

/// One registered physical resolution and its exactness contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    selection: ScanResolution,
    coverage: PlanTimeRange,
    capabilities: BTreeSet<ResolutionCapability>,
}

impl Resolution {
    fn raw() -> Self {
        Self {
            selection: ScanResolution::raw(),
            coverage: PlanTimeRange::all(),
            capabilities: BTreeSet::new(),
        }
    }

    /// Creates a future rollup registration.
    ///
    /// `coverage` is the exact available timestamp extent. The interval is
    /// epoch-aligned and must be positive. A rollup name must be a trimmed,
    /// non-empty identifier other than the reserved `raw` name.
    pub fn rollup<I>(
        name: impl Into<String>,
        interval_ns: i64,
        coverage: PlanTimeRange,
        capabilities: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = ResolutionCapability>,
    {
        let name = name.into();
        if name.is_empty() || name.trim() != name {
            return Err(TsmError::InvalidInput(
                "resolution name must be non-empty and have no surrounding whitespace".to_string(),
            ));
        }
        if name == RAW_NAME {
            return Err(TsmError::InvalidInput(
                "`raw` is a reserved resolution name".to_string(),
            ));
        }
        if interval_ns <= 0 {
            return Err(TsmError::InvalidInput(
                "rollup interval must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            selection: ScanResolution::Rollup { name, interval_ns },
            coverage,
            capabilities: capabilities.into_iter().collect(),
        })
    }

    /// Returns the stable catalog name.
    pub fn name(&self) -> &str {
        self.selection.name()
    }

    /// Returns the bucket width, or `None` for raw.
    pub const fn interval_ns(&self) -> Option<i64> {
        self.selection.interval_ns()
    }

    /// Returns the registered coverage.
    pub const fn coverage(&self) -> PlanTimeRange {
        self.coverage
    }

    /// Returns the explicitly safe rollup operations.
    pub fn capabilities(&self) -> &BTreeSet<ResolutionCapability> {
        &self.capabilities
    }
}

/// Validated correctness constraints for one scan.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolutionRequirements {
    alignment_periods_ns: BTreeSet<i64>,
    capabilities: BTreeSet<ResolutionCapability>,
}

impl ResolutionRequirements {
    /// Creates requirements for an explicit range-query step.
    pub fn for_step(step_ns: i64) -> Result<Self> {
        Self::default().with_alignment_period(step_ns)
    }

    /// Requires a rollup interval to divide one epoch-aligned period exactly.
    pub fn with_alignment_period(mut self, period_ns: i64) -> Result<Self> {
        if period_ns <= 0 {
            return Err(TsmError::InvalidInput(
                "resolution alignment period must be greater than zero".to_string(),
            ));
        }
        self.alignment_periods_ns.insert(period_ns);
        Ok(self)
    }

    /// Requires one operation to be explicitly supported by a rollup.
    pub fn with_capability(mut self, capability: ResolutionCapability) -> Self {
        self.capabilities.insert(capability);
        self
    }

    /// Returns all exact alignment constraints.
    pub fn alignment_periods_ns(&self) -> &BTreeSet<i64> {
        &self.alignment_periods_ns
    }

    /// Returns all semantic capability constraints.
    pub fn capabilities(&self) -> &BTreeSet<ResolutionCapability> {
        &self.capabilities
    }

    fn add_alignment_period(&mut self, period_ns: i64) -> Result<()> {
        if period_ns <= 0 {
            return Err(TsmError::InvalidInput(
                "resolution alignment period must be greater than zero".to_string(),
            ));
        }
        self.alignment_periods_ns.insert(period_ns);
        Ok(())
    }

    fn add_capability(&mut self, capability: ResolutionCapability) {
        self.capabilities.insert(capability);
    }
}

/// Registered raw and future rollup resolutions.
#[derive(Debug, Clone)]
pub struct ResolutionCatalog {
    raw: Resolution,
    rollups: BTreeMap<String, Resolution>,
}

impl Default for ResolutionCatalog {
    fn default() -> Self {
        Self {
            raw: Resolution::raw(),
            rollups: BTreeMap::new(),
        }
    }
}

impl ResolutionCatalog {
    /// Registers one rollup under a unique stable name.
    pub fn register(&mut self, resolution: Resolution) -> Result<()> {
        if resolution.selection.is_raw() {
            return Err(TsmError::InvalidInput(
                "raw resolution is registered automatically".to_string(),
            ));
        }
        let name = resolution.name().to_string();
        if self.rollups.contains_key(&name) {
            return Err(TsmError::InvalidInput(format!(
                "resolution `{name}` is already registered"
            )));
        }
        self.rollups.insert(name, resolution);
        Ok(())
    }

    /// Lists raw followed by rollups in stable name order.
    pub fn resolutions(&self) -> impl Iterator<Item = &Resolution> {
        std::iter::once(&self.raw).chain(self.rollups.values())
    }

    /// Selects the unique coarsest safe resolution, falling back to raw.
    ///
    /// A rollup is eligible only when its coverage contains the full scan,
    /// every requested period is an exact multiple of its interval, and every
    /// required operation is registered as safe. No alignment period means
    /// there is not enough evidence to downsample, so raw wins. Two equally
    /// coarse eligible rollups are deliberately treated as ambiguous.
    pub fn select(
        &self,
        scan_range: &PlanTimeRange,
        requirements: &ResolutionRequirements,
    ) -> ScanResolution {
        if requirements.alignment_periods_ns.is_empty() {
            return ScanResolution::raw();
        }

        let mut best: Option<&Resolution> = None;
        let mut ambiguous = false;
        for candidate in self
            .rollups
            .values()
            .filter(|candidate| candidate_is_eligible(candidate, scan_range, requirements))
        {
            let Some(interval) = candidate.interval_ns() else {
                continue;
            };
            match best {
                None => {
                    best = Some(candidate);
                    ambiguous = false;
                }
                Some(current) => {
                    let Some(current_interval) = current.interval_ns() else {
                        best = Some(candidate);
                        ambiguous = false;
                        continue;
                    };
                    if interval > current_interval {
                        best = Some(candidate);
                        ambiguous = false;
                    } else if interval == current_interval {
                        ambiguous = true;
                    }
                }
            }
        }
        if ambiguous {
            ScanResolution::raw()
        } else {
            best.map_or_else(ScanResolution::raw, |resolution| {
                resolution.selection.clone()
            })
        }
    }
}

/// Applies conservative resolution selection to every scan in a logical plan.
///
/// `step_ns` is the range-query evaluation step. SQL `TIME_BUCKET` expressions
/// add their own alignment periods, so a bucketed SQL plan can select a rollup
/// without an external step. Returned selections follow left-to-right scan
/// order and are also written into each [`super::ScanNode`].
pub fn select_plan_resolutions(
    plan: &mut LogicalPlan,
    catalog: &ResolutionCatalog,
    step_ns: Option<i64>,
) -> Result<Vec<ScanResolution>> {
    let mut requirements = ResolutionRequirements::default();
    if let Some(step_ns) = step_ns {
        requirements.add_alignment_period(step_ns)?;
    }
    let mut candidate = plan.clone();
    let mut selected = Vec::new();
    select_node(&mut candidate.root, catalog, &requirements, &mut selected)?;
    *plan = candidate;
    Ok(selected)
}

fn select_node(
    node: &mut PlanNode,
    catalog: &ResolutionCatalog,
    inherited: &ResolutionRequirements,
    selected: &mut Vec<ScanResolution>,
) -> Result<()> {
    match node {
        PlanNode::Scan(scan) => {
            scan.resolution = catalog.select(&scan.time_range, inherited);
            selected.push(scan.resolution.clone());
        }
        PlanNode::Filter(filter) => {
            let mut requirements = inherited.clone();
            requirements.add_capability(ResolutionCapability::Filter);
            for predicate in &filter.predicates {
                if let PlanPredicate::Expression(expression) = predicate {
                    add_expression_requirements(expression, &mut requirements)?;
                }
            }
            select_node(&mut filter.input, catalog, &requirements, selected)?;
        }
        PlanNode::SeriesGroup(group) => {
            let mut requirements = inherited.clone();
            if let SeriesGroupKind::Keys(keys) = &group.kind {
                for key in keys {
                    add_expression_requirements(key, &mut requirements)?;
                }
            }
            select_node(&mut group.input, catalog, &requirements, selected)?;
        }
        PlanNode::RangeFunction(function) => {
            let mut requirements = inherited.clone();
            requirements.add_capability(ResolutionCapability::RangeFunction(function.function));
            select_node(&mut function.input, catalog, &requirements, selected)?;
        }
        PlanNode::Aggregate(aggregate) => {
            let mut requirements = inherited.clone();
            for call in &aggregate.calls {
                requirements.add_capability(ResolutionCapability::Aggregate(call.kind));
                if let AggregateInput::Expression(expression) = &call.argument {
                    add_expression_requirements(expression, &mut requirements)?;
                }
                for expression in &call.auxiliary {
                    add_expression_requirements(expression, &mut requirements)?;
                }
            }
            select_node(&mut aggregate.input, catalog, &requirements, selected)?;
            for call in &mut aggregate.calls {
                if let Some(parameter) = &mut call.parameter {
                    select_node(parameter, catalog, inherited, selected)?;
                }
            }
        }
        PlanNode::Binary(binary) => {
            select_node(&mut binary.left, catalog, inherited, selected)?;
            select_node(&mut binary.right, catalog, inherited, selected)?;
        }
        PlanNode::Project(project) => {
            let mut requirements = inherited.clone();
            for projection in &project.expressions {
                add_expression_requirements(&projection.expression, &mut requirements)?;
            }
            select_node(&mut project.input, catalog, &requirements, selected)?;
        }
        PlanNode::Sort(sort) => {
            let mut requirements = inherited.clone();
            for key in &sort.keys {
                add_expression_requirements(&key.expression, &mut requirements)?;
            }
            select_node(&mut sort.input, catalog, &requirements, selected)?;
        }
        PlanNode::Limit(limit) => {
            select_node(&mut limit.input, catalog, inherited, selected)?;
        }
        PlanNode::Scalar(_) | PlanNode::String(_) => {}
    }
    Ok(())
}

fn candidate_is_eligible(
    candidate: &Resolution,
    scan_range: &PlanTimeRange,
    requirements: &ResolutionRequirements,
) -> bool {
    let Some(interval_ns) = candidate.interval_ns() else {
        return false;
    };
    range_contains(&candidate.coverage, scan_range)
        && requirements
            .alignment_periods_ns
            .iter()
            .all(|period| interval_ns <= *period && period % interval_ns == 0)
        && requirements.capabilities.is_subset(&candidate.capabilities)
}

fn range_contains(coverage: &PlanTimeRange, requested: &PlanTimeRange) -> bool {
    lower_contains(coverage.start, requested.start) && upper_contains(coverage.end, requested.end)
}

fn lower_contains(coverage: Option<TimeBound>, requested: Option<TimeBound>) -> bool {
    match (coverage, requested) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(coverage), Some(requested)) => {
            coverage.value < requested.value
                || (coverage.value == requested.value
                    && (coverage.inclusive || !requested.inclusive))
        }
    }
}

fn upper_contains(coverage: Option<TimeBound>, requested: Option<TimeBound>) -> bool {
    match (coverage, requested) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(coverage), Some(requested)) => {
            coverage.value > requested.value
                || (coverage.value == requested.value
                    && (coverage.inclusive || !requested.inclusive))
        }
    }
}

fn add_expression_requirements(
    expression: &PlanExpression,
    requirements: &mut ResolutionRequirements,
) -> Result<()> {
    match &expression.kind {
        PlanExpressionKind::Binary { left, right, .. } => {
            add_expression_requirements(left, requirements)?;
            add_expression_requirements(right, requirements)?;
        }
        PlanExpressionKind::Unary { expression, .. }
        | PlanExpressionKind::IsNull { expression, .. } => {
            add_expression_requirements(expression, requirements)?;
        }
        PlanExpressionKind::Between {
            expression,
            low,
            high,
            ..
        } => {
            add_expression_requirements(expression, requirements)?;
            add_expression_requirements(low, requirements)?;
            add_expression_requirements(high, requirements)?;
        }
        PlanExpressionKind::Pattern {
            expression,
            pattern,
            escape,
            ..
        } => {
            add_expression_requirements(expression, requirements)?;
            add_expression_requirements(pattern, requirements)?;
            if let Some(escape) = escape {
                add_expression_requirements(escape, requirements)?;
            }
        }
        PlanExpressionKind::InList {
            expression, list, ..
        } => {
            add_expression_requirements(expression, requirements)?;
            for candidate in list {
                add_expression_requirements(candidate, requirements)?;
            }
        }
        PlanExpressionKind::TimeBucket { interval_ns, .. } => {
            requirements.add_alignment_period(*interval_ns)?;
            requirements.add_capability(ResolutionCapability::TimeBucket);
        }
        PlanExpressionKind::Column { .. }
        | PlanExpressionKind::Number(_)
        | PlanExpressionKind::String(_)
        | PlanExpressionKind::Boolean(_)
        | PlanExpressionKind::Null
        | PlanExpressionKind::Timestamp(_)
        | PlanExpressionKind::Interval(_)
        | PlanExpressionKind::AggregateResult { .. } => {}
    }
    Ok(())
}
