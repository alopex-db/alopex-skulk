use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::retention::HOUR_NANOS;

fn row(host: &str, timestamp: i64, fields: Fields) -> WideRow {
    WideRow::new(
        SeriesKey::new("cpu", Tags::from([("host".into(), host.into())])),
        timestamp,
        fields,
    )
}

#[test]
fn compaction_replaces_small_files_and_keeps_the_latest_exact_series_timestamp() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .ingest(row(
            "edge-a",
            10,
            Fields::from([
                ("value".into(), FieldValue::Integer(1)),
                ("left".into(), FieldValue::String("old".into())),
            ]),
        ))
        .expect("old duplicate");
    store
        .ingest(row(
            "edge-b",
            20,
            Fields::from([("left".into(), FieldValue::String("kept".into()))]),
        ))
        .expect("left-only row");
    store.flush_all().expect("first flush");

    store
        .ingest(row(
            "edge-a",
            10,
            Fields::from([
                ("value".into(), FieldValue::Integer(2)),
                ("right".into(), FieldValue::Boolean(true)),
            ]),
        ))
        .expect("new duplicate");
    store
        .ingest(row(
            "edge-c",
            30,
            Fields::from([("right".into(), FieldValue::Boolean(false))]),
        ))
        .expect("right-only row");
    store.flush_all().expect("second flush");

    let before = store.manifest_state().expect("state before");
    let old_names = before.active_files().keys().cloned().collect::<Vec<_>>();
    assert_eq!(old_names.len(), 2);

    let result = store
        .compact_measurement("cpu")
        .expect("compact")
        .expect("two files compact");

    assert_eq!(result.input_file_count(), 2);
    assert_eq!(result.input_row_count(), 4);
    assert_eq!(result.output_row_count(), 3);
    let after = store.manifest_state().expect("state after");
    assert_eq!(after.active_files().len(), 1);
    assert!(after.active_file(result.output_file()).is_some());
    for name in old_names {
        assert!(!root.path().join("segments").join(name).exists());
    }

    let rows = store.read_measurement("cpu").expect("read compacted");
    assert_eq!(rows.len(), 3);
    let winner = rows
        .iter()
        .find(|row| row.row().series().tags().get("host").map(String::as_str) == Some("edge-a"))
        .expect("winner");
    assert_eq!(winner.row().field("value"), Some(&FieldValue::Integer(2)));
    assert_eq!(
        winner.row().field("right"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(winner.row().field("left"), None);
    assert!(rows.iter().any(|row| row.row().field("left").is_some()));
    assert!(rows.iter().any(|row| row.row().field("right").is_some()));
}

#[test]
fn a_single_active_file_is_an_idempotent_noop() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .ingest(row(
            "edge-a",
            10,
            Fields::from([("value".into(), FieldValue::Integer(1))]),
        ))
        .expect("row");
    store.flush_all().expect("flush");
    let before = store.manifest_state().expect("before");

    assert!(store
        .compact_measurement("cpu")
        .expect("compact noop")
        .is_none());

    assert_eq!(store.manifest_state().expect("after"), before);
}

#[test]
fn compaction_never_merges_across_time_partition_boundaries() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    for offset in [1, 2] {
        store
            .ingest(row(
                "edge-a",
                offset,
                Fields::from([("value".into(), FieldValue::Integer(offset))]),
            ))
            .expect("first partition");
        store
            .ingest(row(
                "edge-b",
                HOUR_NANOS + offset,
                Fields::from([("value".into(), FieldValue::Integer(offset))]),
            ))
            .expect("second partition");
        store.flush_all().expect("flush both partitions");
    }
    assert_eq!(
        store.manifest_state().expect("before").active_files().len(),
        4
    );

    let first = store
        .compact_measurement("cpu")
        .expect("first compact")
        .expect("eligible partition");
    assert_eq!(first.input_file_count(), 2);
    assert_eq!(
        store.manifest_state().expect("middle").active_files().len(),
        3
    );
    assert_eq!(store.list_partitions("cpu").expect("partitions").len(), 2);

    store
        .compact_measurement("cpu")
        .expect("second compact")
        .expect("second eligible partition");
    assert_eq!(
        store.manifest_state().expect("after").active_files().len(),
        2
    );
    assert_eq!(store.read_measurement("cpu").expect("all rows").len(), 4);
}
