use alopex_skulk::model::{FieldValue, Fields, SeriesKey, Tags, WideRow};
use alopex_skulk::store::parquet_writer::PublishHook;
use alopex_skulk::store::recovery::{
    RecoveryBoundary, RecoveryConfig, RecoveryHook, RecoveryStore,
};
use alopex_skulk::Result;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

const CRASH_ROOT_ENV: &str = "SKULK_RECOVERY_CRASH_ROOT";
const CRASH_STAGE_ENV: &str = "SKULK_RECOVERY_CRASH_STAGE";
const CRASH_READY_FILE: &str = "crash-boundary-ready";

fn row(value: i64) -> WideRow {
    WideRow::new(
        SeriesKey::new("cpu", Tags::from([("host".into(), "edge-a".into())])),
        value,
        Fields::from([("value".into(), FieldValue::Integer(value))]),
    )
}

#[test]
fn acknowledged_wal_row_is_queryable_after_reopen() {
    let root = tempfile::tempdir().expect("tempdir");
    let sequence = {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store.ingest(row(42)).expect("ack")
    };

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    let rows = store.read_measurement("cpu").expect("read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ingest_seq(), sequence);
    assert_eq!(rows[0].row(), &row(42));
}

#[test]
fn rejected_row_is_not_left_in_the_recovery_log() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store.ingest(row(1)).expect("first row");
        let incompatible = WideRow::new(
            SeriesKey::new("cpu", Tags::from([("host".into(), "edge-a".into())])),
            2,
            Fields::from([("value".into(), FieldValue::Float(2.0))]),
        );
        assert!(store.ingest(incompatible).is_err());
    }

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert_eq!(store.read_measurement("cpu").expect("read").len(), 1);
}

fn force_kill_at(root: &Path, stage: &str) {
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "crash_boundary_child", "--nocapture"])
        .env(CRASH_ROOT_ENV, root)
        .env(CRASH_STAGE_ENV, stage)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn crash child");
    let marker = root.join(CRASH_READY_FILE);
    for _ in 0..500 {
        if marker.exists() {
            break;
        }
        if child.try_wait().expect("query child").is_some() {
            panic!("crash child exited before boundary {stage}");
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(marker.exists(), "child did not reach {stage}");
    child.kill().expect("force kill child");
    child.wait().expect("reap child");
    std::fs::remove_file(marker).expect("remove marker");
}

struct CrashHook {
    root: PathBuf,
    stage: String,
}

impl CrashHook {
    fn stop(&self) -> ! {
        let marker = File::create(self.root.join(CRASH_READY_FILE)).expect("marker");
        marker.sync_all().expect("sync marker");
        loop {
            thread::park();
        }
    }
}

impl RecoveryHook for CrashHook {
    fn reached(&self, boundary: RecoveryBoundary, _path: Option<&Path>) -> Result<()> {
        if self.stage == "manifest" && boundary == RecoveryBoundary::AfterManifestPublish {
            self.stop();
        }
        Ok(())
    }
}

impl PublishHook for CrashHook {
    fn before_publish(&self, _temporary_path: &Path) -> Result<()> {
        if self.stage == "parquet-temp" {
            self.stop();
        }
        Ok(())
    }
}

#[test]
fn crash_boundary_child() {
    let (Some(root), Some(stage)) = (
        std::env::var_os(CRASH_ROOT_ENV),
        std::env::var_os(CRASH_STAGE_ENV),
    ) else {
        return;
    };
    let root = PathBuf::from(root);
    let stage = stage.to_string_lossy().into_owned();
    let hook = CrashHook {
        root: root.clone(),
        stage: stage.clone(),
    };
    let mut store = RecoveryStore::open(&root, RecoveryConfig::default()).expect("child open");
    store.ingest(row(99)).expect("child ack");
    if stage == "ack" {
        hook.stop();
    }
    store
        .flush_all_with_hooks(&hook, &hook)
        .expect("child flush");
}

#[test]
fn i1_force_kill_after_ack_recovers_the_row() {
    let root = tempfile::tempdir().expect("tempdir");
    force_kill_at(root.path(), "ack");

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert_eq!(store.read_measurement("cpu").expect("read").len(), 1);
}

#[test]
fn i2_force_kill_after_manifest_publish_does_not_duplicate_wal_replay() {
    let root = tempfile::tempdir().expect("tempdir");
    force_kill_at(root.path(), "manifest");

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert_eq!(store.replayed_row_count(), 0);
    assert_eq!(store.read_measurement("cpu").expect("read").len(), 1);
}

#[test]
fn i3_force_kill_with_a_complete_temp_never_makes_it_active() {
    let root = tempfile::tempdir().expect("tempdir");
    force_kill_at(root.path(), "parquet-temp");

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert!(store
        .manifest_state()
        .expect("manifest")
        .active_files()
        .is_empty());
    assert_eq!(store.read_measurement("cpu").expect("WAL replay").len(), 1);
    assert_eq!(
        std::fs::read_dir(root.path().join("segments"))
            .expect("segments")
            .count(),
        0
    );
}

struct StopAfterManifest;

impl RecoveryHook for StopAfterManifest {
    fn reached(&self, boundary: RecoveryBoundary, _path: Option<&Path>) -> Result<()> {
        if boundary == RecoveryBoundary::AfterManifestPublish {
            return Err(alopex_skulk::TsmError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "injected stop after manifest",
            )));
        }
        Ok(())
    }
}

#[test]
fn manifest_publish_before_wal_checkpoint_does_not_replay_a_duplicate() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store.ingest(row(7)).expect("ack");
        assert!(store.flush_all_with_hook(&StopAfterManifest).is_err());
    }

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert_eq!(store.replayed_row_count(), 0);
    let rows = store.read_measurement("cpu").expect("read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row(), &row(7));
}

#[test]
fn i4_torn_wal_tail_preserves_the_acknowledged_prefix() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        store.ingest(row(1)).expect("ack");
    }
    let mut wal = OpenOptions::new()
        .append(true)
        .open(root.path().join("wal-v3.log"))
        .expect("open WAL");
    wal.write_all(&[64, 0, 0]).expect("torn length");
    wal.sync_all().expect("sync torn tail");
    drop(wal);

    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    assert_eq!(store.read_measurement("cpu").expect("read").len(), 1);
}

#[test]
fn i5_repeated_startup_does_not_accumulate_unreferenced_files() {
    let root = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(root.path().join("segments")).expect("segments");
    for cycle in 0..3 {
        let checkpoint = root
            .path()
            .join(format!(".wal-v3.log.checkpoint-dead-{cycle}"));
        std::fs::write(
            root.path()
                .join("segments")
                .join(format!("orphan-{cycle}.parquet")),
            b"orphan",
        )
        .expect("orphan");
        std::fs::write(&checkpoint, b"partial checkpoint").expect("checkpoint");
        let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
        assert_eq!(
            std::fs::read_dir(root.path().join("segments"))
                .expect("segments")
                .count(),
            0
        );
        assert!(!checkpoint.exists());
        drop(store);
    }
}

#[test]
fn i6_second_integrated_store_open_fails() {
    let root = tempfile::tempdir().expect("tempdir");
    let first = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("first");
    assert!(RecoveryStore::open(root.path(), RecoveryConfig::default()).is_err());
    drop(first);
    RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("reacquire");
}

#[test]
fn measured_recovery_is_within_rto_and_acknowledged_rpo_is_zero() {
    let root = tempfile::tempdir().expect("tempdir");
    {
        let mut store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("open");
        for value in 0..100 {
            store.ingest(row(value)).expect("ack");
        }
    }
    let started = std::time::Instant::now();
    let store = RecoveryStore::open(root.path(), RecoveryConfig::default()).expect("recover");
    let elapsed = started.elapsed();

    assert!(elapsed < std::time::Duration::from_secs(300));
    assert_eq!(store.read_measurement("cpu").expect("read").len(), 100);
    eprintln!("RTO={elapsed:?}; RPO=0 acknowledged rows");
}
