use alopex_skulk::ingest::{
    IngestBatch, IngestLimits, IngestPolicyBasis, Ingestor, O3Config, SourceLocation, TooOldPolicy,
};
use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::retention::{LateWritePolicy, RetentionPolicy};
use alopex_skulk::store::seq::IngestSeq;
use std::time::Duration;

const SECOND: i64 = 1_000_000_000;
const HOUR: i64 = 3_600 * SECOND;
const NOW: i64 = 10 * HOUR;

fn row(timestamp: i64, field: FieldValue) -> WideRow {
    WideRow::new(
        SeriesKey::new("cpu", Tags::new()),
        timestamp,
        Fields::from([("value".to_string(), field)]),
    )
}

fn batch(rows: impl IntoIterator<Item = (usize, WideRow)>) -> IngestBatch {
    let mut batch = IngestBatch::new(1, 1);
    for (index, row) in rows {
        batch.push_row(SourceLocation::Item(index), row);
    }
    batch
}

fn ingestor(config: O3Config) -> (tempfile::TempDir, Ingestor<RecoveryStore>) {
    let root = tempfile::tempdir().expect("tempdir");
    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
    (
        root,
        Ingestor::with_o3_config(store, IngestLimits::default(), config),
    )
}

#[test]
fn default_window_is_one_hour_and_uses_wall_clock_not_series_high_water() {
    let config = O3Config::default();
    assert_eq!(config.allowed_window(), Duration::from_secs(3_600));
    assert_eq!(config.too_old_policy(), TooOldPolicy::Reject);
    assert!(!config.allow_backfill());

    let (_root, mut ingestor) = ingestor(config);
    let future = ingestor
        .ingest(batch([(0, row(NOW + HOUR, FieldValue::Float(1.0)))]), NOW)
        .expect("future row");
    assert_eq!(future.accepted_count(), 1);

    let within_wall_clock_window = ingestor
        .ingest(
            batch([(1, row(NOW - HOUR / 2, FieldValue::Float(2.0)))]),
            NOW,
        )
        .expect("wall-clock window");
    assert_eq!(within_wall_clock_window.accepted_count(), 1);
    assert_eq!(
        within_wall_clock_window.accepted()[0].sequence(),
        IngestSeq::new(2)
    );

    let exact_boundary = ingestor
        .ingest(batch([(2, row(NOW - HOUR, FieldValue::Float(3.0)))]), NOW)
        .expect("inclusive O3 boundary");
    assert_eq!(exact_boundary.accepted_count(), 1);
    assert_eq!(exact_boundary.accepted()[0].sequence(), IngestSeq::new(3));

    assert!(O3Config::new(
        Duration::from_secs(i64::MAX as u64),
        TooOldPolicy::Reject,
        false,
    )
    .is_err());
}

#[test]
fn three_too_old_policies_are_observable_and_never_return_partial_writes() {
    let old = NOW - 2 * HOUR;
    let fresh = NOW;

    let (_root, mut reject) = ingestor(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Reject, false)
            .expect("reject config"),
    );
    let rejected = reject
        .ingest(
            batch([
                (0, row(old, FieldValue::Float(1.0))),
                (1, row(fresh, FieldValue::Float(2.0))),
            ]),
            NOW,
        )
        .expect("row-level reject");
    assert_eq!(rejected.accepted_count(), 1);
    assert_eq!(rejected.rejected_count(), 1);
    assert_eq!(
        rejected.rejections()[0].policy_basis(),
        Some(IngestPolicyBasis::O3)
    );
    assert!(rejected.rejections()[0].reason().contains("O3"));
    assert_eq!(rejected.warning_count(), 0);
    assert_eq!(rejected.dropped_count(), 0);
    assert!(!rejected.backfill_active());

    let (_root, mut warning) = ingestor(
        O3Config::new(
            Duration::from_secs(3_600),
            TooOldPolicy::AcceptWithWarning,
            false,
        )
        .expect("warning config"),
    );
    let warned = warning
        .ingest(batch([(0, row(old, FieldValue::Float(1.0)))]), NOW)
        .expect("warning accept");
    assert_eq!(warned.accepted_count(), 1);
    assert_eq!(warned.warning_count(), 1);
    assert_eq!(warned.warnings()[0].source(), SourceLocation::Item(0));
    assert_eq!(warned.warnings()[0].basis(), IngestPolicyBasis::O3);
    assert_eq!(warned.warnings()[0].timestamp(), old);
    assert_eq!(warned.warnings()[0].cutoff(), NOW - HOUR);

    let (_root, mut drop) = ingestor(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Drop, false).expect("drop config"),
    );
    let dropped = drop
        .ingest(batch([(0, row(old, FieldValue::Float(1.0)))]), NOW)
        .expect("observed drop");
    assert_eq!(dropped.accepted_count(), 0);
    assert_eq!(dropped.rejected_count(), 0);
    assert_eq!(dropped.warning_count(), 0);
    assert_eq!(dropped.dropped_count(), 1);
}

#[test]
fn excluded_rows_do_not_pollute_frozen_batch_qualification() {
    let (_root, mut ingestor) = ingestor(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Drop, false).expect("drop config"),
    );
    let outcome = ingestor
        .ingest(
            batch([
                (
                    0,
                    row(
                        NOW - 2 * HOUR,
                        FieldValue::String("must-not-set-column-role".into()),
                    ),
                ),
                (1, row(NOW, FieldValue::Float(2.0))),
            ]),
            NOW,
        )
        .expect("dropped row must not affect qualification");

    assert_eq!(outcome.accepted_count(), 1);
    assert_eq!(outcome.dropped_count(), 1);
    assert_eq!(
        ingestor
            .sink()
            .measurement_state("cpu")
            .expect("measurement state")
            .row_count(),
        1
    );
}

#[test]
fn retention_basis_wins_hard_reject_and_o3_config_changes_online() {
    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
    store
        .set_retention_policy(
            "cpu",
            RetentionPolicy::new(HOUR as u64, LateWritePolicy::Reject).expect("retention policy"),
        )
        .expect("set retention");
    let mut ingestor = Ingestor::with_o3_config(
        store,
        IngestLimits::default(),
        O3Config::new(
            Duration::from_secs(2 * 3_600),
            TooOldPolicy::AcceptWithWarning,
            false,
        )
        .expect("wide O3"),
    );

    let retention = ingestor
        .ingest(
            batch([(0, row(NOW - HOUR - 1, FieldValue::Float(1.0)))]),
            NOW,
        )
        .expect("retention row rejection");
    assert_eq!(retention.accepted_count(), 0);
    assert_eq!(
        retention.rejections()[0].policy_basis(),
        Some(IngestPolicyBasis::Retention)
    );
    assert!(retention.rejections()[0]
        .reason()
        .contains("retention cutoff"));

    ingestor
        .sink_mut()
        .set_retention_policy(
            "cpu",
            RetentionPolicy::new((2 * HOUR) as u64, LateWritePolicy::Reject)
                .expect("retention policy"),
        )
        .expect("update retention");
    ingestor.set_o3_config(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Reject, false).expect("narrow O3"),
    );
    let o3 = ingestor
        .ingest(
            batch([(1, row(NOW - HOUR - 1, FieldValue::Float(2.0)))]),
            NOW,
        )
        .expect("O3 row rejection");
    assert_eq!(
        o3.rejections()[0].policy_basis(),
        Some(IngestPolicyBasis::O3)
    );
}

#[test]
fn backfill_disables_only_o3_and_is_reported() {
    let (_root, mut ingestor) = ingestor(
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Reject, true)
            .expect("backfill config"),
    );
    let outcome = ingestor
        .ingest(
            batch([(0, row(NOW - 100 * HOUR, FieldValue::Float(1.0)))]),
            NOW,
        )
        .expect("backfill");

    assert_eq!(outcome.accepted_count(), 1);
    assert_eq!(outcome.rejected_count(), 0);
    assert_eq!(outcome.warning_count(), 0);
    assert_eq!(outcome.dropped_count(), 0);
    assert!(outcome.backfill_active());

    let retention_root = tempfile::tempdir().expect("tempdir");
    let mut retention_store =
        RecoveryStore::open(retention_root.path(), RecoveryConfig::default()).expect("store");
    retention_store
        .set_retention_policy(
            "cpu",
            RetentionPolicy::new(HOUR as u64, LateWritePolicy::Reject).expect("retention policy"),
        )
        .expect("set retention");
    let mut retention_backfill = Ingestor::with_o3_config(
        retention_store,
        IngestLimits::default(),
        O3Config::new(Duration::from_secs(3_600), TooOldPolicy::Reject, true)
            .expect("backfill config"),
    );
    let retained = retention_backfill
        .ingest(
            batch([(0, row(NOW - 2 * HOUR, FieldValue::Float(1.0)))]),
            NOW,
        )
        .expect("retention still applies");
    assert_eq!(retained.accepted_count(), 0);
    assert_eq!(
        retained.rejections()[0].policy_basis(),
        Some(IngestPolicyBasis::Retention)
    );
    assert!(retained.backfill_active());

    let root = tempfile::tempdir().expect("tempdir");
    let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("store");
    store
        .ingest(row(NOW - 100 * HOUR, FieldValue::Float(1.0)))
        .expect("direct storage API has no O3 policy");
}
