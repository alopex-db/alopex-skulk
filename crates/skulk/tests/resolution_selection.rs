use alopex_skulk::query::plan::{
    select_plan_resolutions, AggregateCall, AggregateInput, AggregateKind, AggregateNode,
    AggregationStage, ArithmeticKind, BinaryNode, LogicalPlan, MeasurementSelection,
    PlanExpression, PlanExpressionKind, PlanNode, PlanTimeRange, PlanValueType, RangeFunctionKind,
    RangeFunctionNode, Resolution, ResolutionCapability, ResolutionCatalog, ResolutionRequirements,
    ScanNode, ScanResolution, SeriesGroupKind, SeriesGroupNode, SeriesWindow, SeriesWindowKind,
    TimeBound,
};
use std::collections::BTreeSet;

const SECOND: i64 = 1_000_000_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;

fn bounded(start: i64, end: i64) -> PlanTimeRange {
    PlanTimeRange::new(
        Some(TimeBound {
            value: start,
            inclusive: true,
        }),
        Some(TimeBound {
            value: end,
            inclusive: true,
        }),
    )
    .expect("bounded range")
}

fn scan(range: PlanTimeRange) -> PlanNode {
    PlanNode::Scan(ScanNode {
        measurement: MeasurementSelection {
            exact: Some("cpu".to_string()),
            matchers: Vec::new(),
        },
        time_range: range,
        tag_equalities: Vec::new(),
        field_projection: Some(BTreeSet::from(["value".to_string()])),
        resolution: ScanResolution::raw(),
    })
}

fn rollup(
    name: &str,
    interval_ns: i64,
    coverage: PlanTimeRange,
    capabilities: impl IntoIterator<Item = ResolutionCapability>,
) -> Resolution {
    Resolution::rollup(name, interval_ns, coverage, capabilities).expect("valid rollup")
}

#[test]
fn default_catalog_is_raw_and_applying_it_is_a_plan_noop() {
    let catalog = ResolutionCatalog::default();
    assert_eq!(catalog.resolutions().count(), 1);
    assert!(catalog
        .select(
            &bounded(0, 10 * HOUR),
            &ResolutionRequirements::for_step(5 * MINUTE).expect("requirements"),
        )
        .is_raw());

    let mut plan = LogicalPlan {
        root: scan(bounded(0, HOUR)),
        output_type: PlanValueType::InstantVector,
    };
    let baseline = plan.clone();
    let selected =
        select_plan_resolutions(&mut plan, &catalog, Some(MINUTE)).expect("resolution selection");
    assert_eq!(selected, [ScanResolution::raw()]);
    assert_eq!(plan, baseline);
}

#[test]
fn selection_requires_coverage_step_alignment_and_unique_coarsest_candidate() {
    let mut catalog = ResolutionCatalog::default();
    catalog
        .register(rollup("one-minute", MINUTE, bounded(0, 24 * HOUR), []))
        .expect("register one minute");
    catalog
        .register(rollup("five-minute", 5 * MINUTE, bounded(0, 24 * HOUR), []))
        .expect("register five minute");
    catalog
        .register(rollup("one-hour", HOUR, bounded(0, 24 * HOUR), []))
        .expect("register one hour");

    let selection = catalog.select(
        &bounded(HOUR, 12 * HOUR),
        &ResolutionRequirements::for_step(15 * MINUTE).expect("requirements"),
    );
    assert_eq!(selection.name(), "five-minute");
    assert_eq!(selection.interval_ns(), Some(5 * MINUTE));

    let misaligned = catalog.select(
        &bounded(HOUR, 12 * HOUR),
        &ResolutionRequirements::for_step(14 * MINUTE).expect("requirements"),
    );
    assert_eq!(misaligned.name(), "one-minute");
    assert!(catalog
        .select(
            &bounded(HOUR, 12 * HOUR),
            &ResolutionRequirements::for_step(150 * SECOND).expect("requirements"),
        )
        .is_raw());

    let uncovered = catalog.select(
        &bounded(-HOUR, HOUR),
        &ResolutionRequirements::for_step(15 * MINUTE).expect("requirements"),
    );
    assert!(uncovered.is_raw());

    let mut ambiguous = ResolutionCatalog::default();
    for name in ["five-minute-a", "five-minute-b"] {
        ambiguous
            .register(rollup(name, 5 * MINUTE, PlanTimeRange::all(), []))
            .expect("register ambiguous rollup");
    }
    assert!(ambiguous
        .select(
            &bounded(0, HOUR),
            &ResolutionRequirements::for_step(15 * MINUTE).expect("requirements"),
        )
        .is_raw());
}

#[test]
fn required_function_capabilities_prevent_semantically_unsafe_rollups() {
    let range = bounded(0, HOUR);
    let requirements = ResolutionRequirements::for_step(5 * MINUTE)
        .expect("requirements")
        .with_capability(ResolutionCapability::RangeFunction(RangeFunctionKind::Rate));

    let mut unsafe_catalog = ResolutionCatalog::default();
    unsafe_catalog
        .register(rollup("one-minute", MINUTE, PlanTimeRange::all(), []))
        .expect("register");
    assert!(unsafe_catalog.select(&range, &requirements).is_raw());

    let mut safe_catalog = ResolutionCatalog::default();
    safe_catalog
        .register(rollup(
            "rate-one-minute",
            MINUTE,
            PlanTimeRange::all(),
            [ResolutionCapability::RangeFunction(RangeFunctionKind::Rate)],
        ))
        .expect("register");
    assert_eq!(
        safe_catalog.select(&range, &requirements).name(),
        "rate-one-minute"
    );
}

#[test]
fn plan_selection_propagates_range_function_requirements_to_scan() {
    let mut catalog = ResolutionCatalog::default();
    catalog
        .register(rollup(
            "rate-one-minute",
            MINUTE,
            PlanTimeRange::all(),
            [ResolutionCapability::RangeFunction(RangeFunctionKind::Rate)],
        ))
        .expect("register");

    let mut plan = LogicalPlan {
        root: PlanNode::RangeFunction(RangeFunctionNode {
            input: Box::new(PlanNode::SeriesGroup(SeriesGroupNode {
                input: Box::new(scan(bounded(0, HOUR))),
                kind: SeriesGroupKind::Window(SeriesWindow {
                    kind: SeriesWindowKind::Range,
                    evaluation_time: HOUR,
                    duration_ns: 5 * MINUTE,
                }),
            })),
            function: RangeFunctionKind::Rate,
        }),
        output_type: PlanValueType::InstantVector,
    };

    let selected =
        select_plan_resolutions(&mut plan, &catalog, Some(5 * MINUTE)).expect("select plan");
    assert_eq!(selected[0].name(), "rate-one-minute");
    let PlanNode::RangeFunction(function) = &plan.root else {
        panic!("range function");
    };
    let PlanNode::SeriesGroup(group) = function.input.as_ref() else {
        panic!("series group");
    };
    let PlanNode::Scan(scan) = group.input.as_ref() else {
        panic!("scan");
    };
    assert_eq!(scan.resolution, selected[0]);
}

#[test]
fn sql_time_bucket_supplies_alignment_and_aggregate_capabilities() {
    let capabilities = [
        ResolutionCapability::TimeBucket,
        ResolutionCapability::Aggregate(AggregateKind::Avg),
    ];
    let mut catalog = ResolutionCatalog::default();
    catalog
        .register(rollup(
            "five-minute-avg",
            5 * MINUTE,
            PlanTimeRange::all(),
            capabilities,
        ))
        .expect("register");

    let mut plan = LogicalPlan {
        root: PlanNode::Aggregate(AggregateNode {
            input: Box::new(PlanNode::SeriesGroup(SeriesGroupNode {
                input: Box::new(scan(bounded(0, 2 * HOUR))),
                kind: SeriesGroupKind::Keys(vec![PlanExpression {
                    kind: PlanExpressionKind::TimeBucket {
                        interval_ns: HOUR,
                        column: "time".to_string(),
                    },
                }]),
            })),
            calls: vec![AggregateCall {
                kind: AggregateKind::Avg,
                argument: AggregateInput::CurrentValue,
                parameter: None,
                auxiliary: Vec::new(),
                distinct: false,
            }],
            grouping: None,
            stage: AggregationStage::Single,
        }),
        output_type: PlanValueType::Table,
    };

    let selected =
        select_plan_resolutions(&mut plan, &catalog, None).expect("select from time bucket");
    assert_eq!(selected[0].name(), "five-minute-avg");
}

#[test]
fn invalid_catalog_and_step_inputs_are_rejected_without_panics() {
    assert!(Resolution::rollup("invalid", 0, PlanTimeRange::all(), std::iter::empty(),).is_err());
    assert!(Resolution::rollup("raw", MINUTE, PlanTimeRange::all(), std::iter::empty(),).is_err());
    assert!(ResolutionRequirements::for_step(0).is_err());

    let mut catalog = ResolutionCatalog::default();
    let resolution = rollup("one-minute", MINUTE, PlanTimeRange::all(), []);
    catalog
        .register(resolution.clone())
        .expect("first registration");
    assert!(catalog.register(resolution).is_err());
}

#[test]
fn failed_plan_selection_does_not_partially_mutate_earlier_scans() {
    let mut catalog = ResolutionCatalog::default();
    catalog
        .register(rollup("one-minute", MINUTE, PlanTimeRange::all(), []))
        .expect("register");
    let invalid_bucket = PlanNode::SeriesGroup(SeriesGroupNode {
        input: Box::new(scan(bounded(0, HOUR))),
        kind: SeriesGroupKind::Keys(vec![PlanExpression {
            kind: PlanExpressionKind::TimeBucket {
                interval_ns: 0,
                column: "time".to_string(),
            },
        }]),
    });
    let mut plan = LogicalPlan {
        root: PlanNode::Binary(BinaryNode {
            left: Box::new(scan(bounded(0, HOUR))),
            op: ArithmeticKind::Add,
            right: Box::new(invalid_bucket),
        }),
        output_type: PlanValueType::InstantVector,
    };
    let baseline = plan.clone();

    assert!(select_plan_resolutions(&mut plan, &catalog, Some(5 * MINUTE)).is_err());
    assert_eq!(plan, baseline);
}
