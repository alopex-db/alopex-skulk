//! Integration tests for retention, WAL replay, and drop recovery.

use alopex_skulk::lifecycle::partition::{PartitionDuration, PartitionLayout};
use alopex_skulk::lifecycle::retention::{RetentionManager, RetentionPolicy};
use alopex_skulk::lifecycle::safe_lsn::SafeLsnTracker;
use alopex_skulk::tsm::partition::PartitionState;
use alopex_skulk::tsm::{DataPoint, PartitionManager, PartitionManagerConfig, TimePartition};
use alopex_skulk::wal::{SyncMode, Wal, WalConfig, WalEntry};
use std::fs;
use std::time::Duration;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct ImmediateExpiry;

impl RetentionPolicy for ImmediateExpiry {
    fn retention_duration(&self) -> Duration {
        Duration::from_secs(0)
    }
}

#[test]
fn test_retention_drop_recovery_flow() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();

    let layout = PartitionLayout::new(&data_dir, PartitionDuration::Hourly);
    let mut partition_manager = PartitionManager::new(PartitionManagerConfig::default());

    let point = DataPoint::new("cpu.usage", vec![], 0, 1.0);
    partition_manager.insert(&point).unwrap();

    let partition = TimePartition::new(0, Duration::from_secs(3600));
    let partition_dir = layout.partition_dir(&partition);
    fs::create_dir_all(&partition_dir).unwrap();
    fs::write(partition_dir.join("segment.skulk"), b"data").unwrap();

    let wal_config = WalConfig {
        batch_size: 10,
        batch_timeout: Duration::from_millis(10),
        segment_size: 1024 * 1024,
        sync_mode: SyncMode::Fsync,
    };
    let wal = Wal::new(&wal_dir, wal_config).unwrap();

    let safe_lsn_tracker = SafeLsnTracker::new();
    let mut retention = RetentionManager::new(
        ImmediateExpiry,
        partition_manager,
        wal,
        safe_lsn_tracker,
        layout.clone(),
    );

    let dropped = retention.run_retention_check().unwrap();
    assert_eq!(dropped, 1);
    assert!(!partition_dir.exists());

    let entries = Wal::recover(&wal_dir).unwrap();
    assert!(entries
        .iter()
        .any(|entry| matches!(entry, WalEntry::DropPartition { .. })));

    fs::create_dir_all(&partition_dir).unwrap();
    fs::write(partition_dir.join("segment.skulk"), b"data").unwrap();

    let mut recovered_manager = PartitionManager::new(PartitionManagerConfig::default());
    Wal::recover_with_partition_manager(&wal_dir, &mut recovered_manager, &layout).unwrap();
    assert!(!partition_dir.exists());
    assert_eq!(
        recovered_manager.get_state(0),
        Some(PartitionState::Dropped)
    );
}
