use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::reader::{ScanRequest, ScanTimeRange, StorageReader};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};

fn row(host: &str, timestamp: i64, fields: Fields) -> WideRow {
    WideRow::new(
        SeriesKey::new("cpu", Tags::from([("host".into(), host.into())])),
        timestamp,
        fields,
    )
}

#[test]
fn recovery_store_scan_merges_pending_deduplicates_and_is_flush_invariant() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
    store
        .ingest(row(
            "edge-b",
            20,
            Fields::from([("value".into(), FieldValue::Integer(20))]),
        ))
        .expect("edge-b");
    store
        .ingest(row(
            "edge-a",
            30,
            Fields::from([
                ("value".into(), FieldValue::Integer(1)),
                ("left".into(), FieldValue::String("old".into())),
            ]),
        ))
        .expect("old duplicate");
    store.flush_all().expect("first flush");

    let winner_sequence = store
        .ingest(row(
            "edge-a",
            30,
            Fields::from([
                ("value".into(), FieldValue::Integer(2)),
                ("right".into(), FieldValue::Boolean(true)),
            ]),
        ))
        .expect("new duplicate");
    store
        .ingest(row(
            "edge-a",
            10,
            Fields::from([("right".into(), FieldValue::Boolean(false))]),
        ))
        .expect("pending sparse row");

    let request =
        ScanRequest::new("cpu", ScanTimeRange::all()).with_field_projection(["value", "right"]);
    let before = StorageReader::scan(&store, &request).expect("scan before flush");
    assert_eq!(before.stats().pending_rows_considered(), 2);
    assert_eq!(
        before
            .rows()
            .iter()
            .map(|row| {
                (
                    row.row()
                        .series()
                        .tags()
                        .get("host")
                        .expect("host")
                        .as_str(),
                    row.row().timestamp(),
                )
            })
            .collect::<Vec<_>>(),
        [("edge-a", 10), ("edge-a", 30), ("edge-b", 20)]
    );
    let winner = &before.rows()[1];
    assert_eq!(winner.ingest_seq(), winner_sequence);
    assert_eq!(winner.row().field("value"), Some(&FieldValue::Integer(2)));
    assert_eq!(
        winner.row().field("right"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(winner.row().field("left"), None);
    assert_eq!(before.rows()[0].row().field("value"), None);

    let before_rows = before.into_rows();
    store.flush_all().expect("second flush");
    let after = StorageReader::scan(&store, &request).expect("scan after flush");
    assert_eq!(after.stats().pending_rows_considered(), 0);
    assert_eq!(after.rows(), before_rows);
}
