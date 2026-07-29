use alopex_skulk::store::manifest::{ActiveFile, ManifestStore};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};
use alopex_skulk::store::retention::{LateWritePolicy, RetentionPolicy, TimePartition};
use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn v02_tsm_magic_is_rejected_without_modifying_the_source_file() {
    let root = tempfile::tempdir().expect("tempdir");
    let partition = root.path().join("1970-01-01").join("00");
    std::fs::create_dir_all(&partition).expect("partition");
    let legacy = partition.join("0000000000000001_L0_G001.skulk");
    let bytes = [b"ATSM".as_slice(), &3_u16.to_le_bytes(), &[0; 26]].concat();
    std::fs::write(&legacy, &bytes).expect("legacy TSM");

    let error = RecoveryStore::open(root.path(), RecoveryConfig::default())
        .err()
        .expect("legacy error");

    assert!(error.to_string().contains("v0.2 TSM"));
    assert_eq!(std::fs::read(&legacy).expect("preserved"), bytes);
}

#[test]
fn v02_segmented_wal_magic_is_rejected_without_creating_a_v03_wal() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_dir = root.path().join("wal");
    std::fs::create_dir_all(&wal_dir).expect("WAL directory");
    let legacy = wal_dir.join("segment_0000000000000000.wal");
    let bytes = [b"SWAL".as_slice(), &2_u16.to_le_bytes(), &[0; 16]].concat();
    std::fs::write(&legacy, &bytes).expect("legacy WAL");

    let error = RecoveryStore::open(root.path(), RecoveryConfig::default())
        .err()
        .expect("legacy error");

    assert!(error.to_string().contains("v0.2 WAL"));
    assert_eq!(std::fs::read(&legacy).expect("preserved"), bytes);
    assert!(!root.path().join("wal-v3.log").exists());
}

#[test]
fn malformed_durable_headers_return_errors_without_panicking() {
    let corpus: &[&[u8]] = &[
        b"x",
        b"SKM3",
        b"SKM3\x02\x00\xff\xff\xff\xff",
        b"not-a-manifest",
        &[0xff; 32],
    ];
    for (index, bytes) in corpus.iter().enumerate() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::write(root.path().join("manifest-v3.current"), bytes).expect("manifest corpus");
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            RecoveryStore::open(root.path(), RecoveryConfig::default())
        }));
        assert!(outcome.is_ok(), "manifest corpus {index} panicked");
        assert!(
            outcome.expect("no panic").is_err(),
            "manifest corpus {index} was silently accepted"
        );
    }
}

#[test]
fn invalid_public_storage_inputs_are_result_errors_without_panicking() {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        (
            ActiveFile::new("cpu", "../escape.parquet", 1, 1, 0, 0),
            RetentionPolicy::new(u64::MAX, LateWritePolicy::Reject),
            TimePartition::for_timestamp(i64::MAX),
        )
    }))
    .expect("constructors must not panic");
    assert!(outcome.0.is_err());
    assert!(outcome.1.is_err());
    assert!(outcome.2.is_err());

    let root = tempfile::tempdir().expect("tempdir");
    let store = ManifestStore::open(root.path()).expect("manifest");
    assert!(store.state().is_ok());
}
