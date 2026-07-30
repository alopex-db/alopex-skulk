#![cfg(feature = "sql-ts")]

use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::query::sqlts::parse;
use alopex_skulk::query::sqlts::typecheck::{typecheck, SqlValueType, TypedProjection};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::TsmError;

#[test]
fn typechecks_against_the_current_durable_plus_pending_schema() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .ingest(WideRow::new(
            SeriesKey::new("metrics", Tags::from([("host".into(), "edge-a".into())])),
            1,
            Fields::from([
                ("float_value".into(), FieldValue::Float(1.0)),
                ("int_value".into(), FieldValue::Integer(2)),
                ("uint_value".into(), FieldValue::Unsigned(3)),
                ("bool_value".into(), FieldValue::Boolean(true)),
                ("string_value".into(), FieldValue::String("ready".into())),
            ]),
        ))
        .expect("ingest");
    let schema = store.measurement_schema("metrics").expect("schema");

    let query = parse(
        "SELECT AVG(float_value) AS average, FIRST(string_value, time) AS first_state, \
         COUNT(*) AS samples FROM metrics WHERE host = 'edge-a' AND bool_value = true \
         GROUP BY host ORDER BY average",
    )
    .expect("parse");
    let typed = typecheck(query, &schema).expect("typecheck");

    assert!(matches!(
        typed.projections.as_slice(),
        [
            TypedProjection::Expr {
                alias: Some(average),
                data_type: SqlValueType::Float64,
            },
            TypedProjection::Expr {
                alias: Some(first_state),
                data_type: SqlValueType::Utf8,
            },
            TypedProjection::Expr {
                alias: Some(samples),
                data_type: SqlValueType::UInt64,
            },
        ] if average == "average" && first_state == "first_state" && samples == "samples"
    ));

    let invalid = parse("SELECT AVG(string_value) FROM metrics").expect("parse");
    assert!(matches!(
        typecheck(invalid, &schema),
        Err(TsmError::Type {
            line,
            column,
            ..
        }) if line > 0 && column > 0
    ));
}

#[test]
fn rejects_unknown_measurements_before_execution() {
    let root = tempfile::tempdir().expect("tempdir");
    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    assert!(matches!(
        store.measurement_schema("missing"),
        Err(TsmError::InvalidInput(message)) if message.contains("does not exist")
    ));
}
