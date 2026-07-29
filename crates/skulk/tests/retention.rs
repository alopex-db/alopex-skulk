use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::retention::{LateWritePolicy, RetentionPolicy, HOUR_NANOS};

fn row(timestamp: i64) -> WideRow {
    WideRow::new(
        SeriesKey::new("cpu", Tags::from([("host".into(), "edge-a".into())])),
        timestamp,
        Fields::from([("value".into(), FieldValue::Integer(timestamp))]),
    )
}

#[test]
fn policy_changes_online_and_survives_reopen() {
    let root = tempfile::tempdir().expect("tempdir");
    let first =
        RetentionPolicy::new(2 * HOUR_NANOS as u64, LateWritePolicy::Reject).expect("first policy");
    let changed =
        RetentionPolicy::new(HOUR_NANOS as u64, LateWritePolicy::Accept).expect("changed policy");
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store.set_retention_policy("cpu", first).expect("set first");
        assert_eq!(store.retention_policy("cpu").expect("policy"), Some(first));
        store
            .set_retention_policy("cpu", changed)
            .expect("change online");
        assert_eq!(
            store.retention_policy("cpu").expect("changed"),
            Some(changed)
        );
    }
    let interrupted = root.path().join(".retention-v3.tmp-dead-0");
    let unrelated = root.path().join("retention-user-data");
    std::fs::write(&interrupted, b"reproducible partial policy").expect("partial");
    std::fs::write(&unrelated, b"keep").expect("unrelated");

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen");
    assert!(!interrupted.exists());
    assert!(unrelated.exists());
    assert_eq!(
        store.retention_policy("cpu").expect("recovered policy"),
        Some(changed)
    );
}

#[test]
fn time_partitions_are_created_listed_and_recovered() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store.ingest(row(HOUR_NANOS - 1)).expect("first hour");
        store.ingest(row(HOUR_NANOS)).expect("second hour");
        store.flush_all().expect("partitioned flush");
        let partitions = store.list_partitions("cpu").expect("list");
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].start(), 0);
        assert_eq!(partitions[0].end_exclusive(), HOUR_NANOS);
        assert_eq!(partitions[1].start(), HOUR_NANOS);
    }

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen");
    assert_eq!(store.list_partitions("cpu").expect("recover").len(), 2);
    assert_eq!(store.read_measurement("cpu").expect("rows").len(), 2);
}

#[test]
fn ttl_removes_expired_partitions_from_the_active_set_idempotently() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store.ingest(row(0)).expect("old row");
    store.ingest(row(2 * HOUR_NANOS)).expect("fresh row");
    store.flush_all().expect("flush");
    store
        .set_retention_policy(
            "cpu",
            RetentionPolicy::new(HOUR_NANOS as u64, LateWritePolicy::Accept).expect("policy"),
        )
        .expect("set policy");

    let first = store
        .expire_retention("cpu", 2 * HOUR_NANOS)
        .expect("expire");
    assert_eq!(first.expired_partition_count(), 1);
    assert_eq!(first.removed_file_count(), 1);
    assert_eq!(store.list_partitions("cpu").expect("remaining").len(), 1);
    assert_eq!(store.read_measurement("cpu").expect("fresh only").len(), 1);
    let generation = store.manifest_state().expect("after first").generation();
    drop(store);

    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reopen");
    let second = store
        .expire_retention("cpu", 2 * HOUR_NANOS)
        .expect("repeat");
    assert_eq!(second.expired_partition_count(), 0);
    assert_eq!(second.removed_file_count(), 0);
    assert_eq!(
        store.manifest_state().expect("after repeat").generation(),
        generation
    );
}

#[test]
fn cutoff_boundary_obeys_the_explicit_late_write_policy() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .set_retention_policy(
            "cpu",
            RetentionPolicy::new(100, LateWritePolicy::Reject).expect("reject policy"),
        )
        .expect("set reject");

    assert!(store.ingest_at(row(i64::MAX), 1_000).is_err());
    let boundary = store
        .ingest_at(row(900), 1_000)
        .expect("cutoff is accepted");
    assert!(store.ingest_at(row(899), 1_000).is_err());
    store
        .set_retention_policy(
            "cpu",
            RetentionPolicy::new(100, LateWritePolicy::Accept).expect("accept policy"),
        )
        .expect("set accept");
    let accepted_old = store.ingest_at(row(899), 1_000).expect("explicit accept");

    assert_eq!(boundary.get(), 1);
    assert_eq!(accepted_old.get(), 2);
}
