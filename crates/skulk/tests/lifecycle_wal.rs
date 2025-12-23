//! Integration tests for WAL truncation with safe LSN constraints.

use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
use alopex_skulk::wal::{SyncMode, Wal, WalConfig, WalEntry};
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_wal_truncate_respects_pending_drop() {
    let temp_dir = TempDir::new().unwrap();

    let wal_config = WalConfig {
        batch_size: 10,
        batch_timeout: Duration::from_millis(10),
        segment_size: 200,
        sync_mode: SyncMode::Fsync,
    };
    let mut wal = Wal::new(temp_dir.path(), wal_config).unwrap();

    let mut boundary_seq = None;
    let mut previous_segment_id = wal.current_segment_id();

    for i in 0..200 {
        let entry = WalEntry::new(1, i as i64, i as f64);
        let seq = wal.append(entry).unwrap();
        let current_segment_id = wal.current_segment_id();

        if current_segment_id != previous_segment_id && boundary_seq.is_none() {
            boundary_seq = Some(seq);
        }

        previous_segment_id = current_segment_id;
        if boundary_seq.is_some() && i > 50 {
            break;
        }
    }

    wal.sync().unwrap();

    let boundary_seq = boundary_seq.expect("WAL segment should rotate during test setup");
    let safe_lsn_tracker = SafeLsnTracker::new();
    safe_lsn_tracker.notify_flush(0, boundary_seq + 5);
    safe_lsn_tracker.notify_drop_recorded(boundary_seq + 1);

    let safe_lsn = safe_lsn_tracker.calculate_safe_lsn().unwrap();
    assert_eq!(safe_lsn, boundary_seq);

    wal.truncate(safe_lsn).unwrap();
    assert_eq!(wal.flushed_sequence(), safe_lsn);

    let recovered = Wal::recover(temp_dir.path()).unwrap();
    assert!(recovered.iter().all(|entry| entry.sequence() > safe_lsn));
}
