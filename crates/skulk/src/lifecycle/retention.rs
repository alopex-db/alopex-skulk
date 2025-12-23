//! Retention policy and manager for TTL-based partition deletion.

use crate::error::{Result, TsmError};
use crate::lifecycle::partition::PartitionLayout;
use crate::lifecycle::safe_lsn::SafeLsnTracker;
use crate::tsm::partition::PartitionState;
use crate::tsm::{PartitionManager, TimePartition, Timestamp};
use crate::wal::{Wal, WalEntry};
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::error;

/// Retention policy for determining expired partitions.
pub trait RetentionPolicy: Send + Sync {
    /// Returns the retention duration.
    fn retention_duration(&self) -> Duration;

    /// Returns true if the partition is expired at the given time.
    fn is_expired(&self, partition: &TimePartition, now: Timestamp) -> bool {
        let retention_nanos = self.retention_duration().as_nanos() as i64;
        partition.end_ts() + retention_nanos < now
    }
}

/// Default retention policy (7 days).
#[derive(Debug, Clone)]
pub struct DefaultRetentionPolicy {
    retention: Duration,
}

impl Default for DefaultRetentionPolicy {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

impl DefaultRetentionPolicy {
    /// Creates a new default retention policy with a custom duration.
    pub fn new(retention: Duration) -> Self {
        Self { retention }
    }
}

impl RetentionPolicy for DefaultRetentionPolicy {
    fn retention_duration(&self) -> Duration {
        self.retention
    }
}

/// Retention manager for TTL-based partition deletion.
pub struct RetentionManager<P: RetentionPolicy> {
    policy: P,
    partition_manager: PartitionManager,
    wal: Wal,
    safe_lsn_tracker: SafeLsnTracker,
    layout: PartitionLayout,
}

impl<P: RetentionPolicy> RetentionManager<P> {
    /// Creates a new retention manager.
    pub fn new(
        policy: P,
        partition_manager: PartitionManager,
        wal: Wal,
        safe_lsn_tracker: SafeLsnTracker,
        layout: PartitionLayout,
    ) -> Self {
        Self {
            policy,
            partition_manager,
            wal,
            safe_lsn_tracker,
            layout,
        }
    }

    /// Runs a retention check and drops expired partitions.
    pub fn run_retention_check(&mut self) -> Result<usize> {
        let now = current_timestamp_nanos()?;
        let start_ts: Vec<i64> = self
            .partition_manager
            .partition_timestamps()
            .copied()
            .collect();
        let mut dropped = 0;

        for start_ts in start_ts {
            let partition =
                TimePartition::new(start_ts, self.partition_manager.config().partition_duration);
            if self.policy.is_expired(&partition, now) {
                match self.drop_partition(&partition) {
                    Ok(()) => dropped += 1,
                    Err(err) => {
                        error!(
                            "Retention drop failed for partition {}: {:?}",
                            partition.start_ts, err
                        );
                    }
                }
            }
        }

        if let Some(safe_lsn) = self.safe_lsn_tracker.calculate_safe_lsn() {
            if let Err(err) = self.wal.truncate(safe_lsn) {
                error!("WAL truncate failed at safe_lsn {}: {:?}", safe_lsn, err);
            }
        }

        Ok(dropped)
    }

    fn drop_partition(&mut self, partition: &TimePartition) -> Result<()> {
        let start_ts = partition.start_ts;
        self.partition_manager
            .set_state(start_ts, PartitionState::Dropping);

        delete_partition_files(&self.layout, partition)?;

        let lsn = self.wal.append(WalEntry::new_drop_partition(
            start_ts,
            partition.duration.as_nanos() as u64,
            current_timestamp_nanos()?,
        ))?;
        self.wal.sync()?;
        self.safe_lsn_tracker.notify_drop_recorded(lsn);

        self.partition_manager
            .set_state(start_ts, PartitionState::Dropped);
        self.safe_lsn_tracker.notify_drop_complete(lsn);

        Ok(())
    }
}

fn current_timestamp_nanos() -> Result<Timestamp> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| TsmError::RetentionError(err.to_string()))?;
    Ok(now.as_nanos() as i64)
}

fn delete_partition_files(layout: &PartitionLayout, partition: &TimePartition) -> Result<()> {
    let dir = layout.partition_dir(partition);
    if !dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }

    fs::remove_dir(&dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::partition::PartitionDuration;
    use crate::tsm::{DataPoint, PartitionManagerConfig};
    use crate::wal::{SyncMode, WalConfig};
    use tempfile::TempDir;

    #[derive(Debug, Clone)]
    struct FixedPolicy(Duration);

    impl RetentionPolicy for FixedPolicy {
        fn retention_duration(&self) -> Duration {
            self.0
        }
    }

    #[test]
    fn test_policy_expiry_boundaries() {
        let policy = FixedPolicy(Duration::from_secs(10));
        let partition = TimePartition::new(0, Duration::from_secs(5));
        let end = partition.end_ts();

        assert!(!policy.is_expired(&partition, end + 10_000_000_000));
        assert!(!policy.is_expired(&partition, end + 9_000_000_000));
        assert!(policy.is_expired(&partition, end + 10_000_000_001));
    }

    #[test]
    fn test_drop_partition_order_success() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();

        let policy = FixedPolicy(Duration::from_secs(0));
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut partition_manager = PartitionManager::new(config);
        partition_manager
            .insert(&DataPoint::new("metric", vec![], 0, 1.0))
            .unwrap();
        let wal = Wal::new(
            &wal_dir,
            WalConfig {
                batch_size: 1,
                batch_timeout: Duration::from_millis(1),
                segment_size: 1024 * 1024,
                sync_mode: SyncMode::None,
            },
        )
        .unwrap();
        let safe_lsn_tracker = SafeLsnTracker::new();
        let layout = PartitionLayout::new(&data_dir, PartitionDuration::Hourly);

        let mut manager = RetentionManager::new(
            policy,
            partition_manager,
            wal,
            safe_lsn_tracker,
            layout.clone(),
        );

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let dir = layout.partition_dir(&partition);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("test.skulk"), b"").unwrap();

        manager.drop_partition(&partition).unwrap();
        assert_eq!(
            manager.partition_manager.get_state(0),
            Some(PartitionState::Dropped)
        );
    }

    #[test]
    fn test_drop_partition_error_keeps_state() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();

        let policy = FixedPolicy(Duration::from_secs(0));
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut pm = PartitionManager::new(config);
        pm.insert(&DataPoint::new("metric", vec![], 0, 1.0))
            .unwrap();

        let wal = Wal::new(
            &wal_dir,
            WalConfig {
                batch_size: 1,
                batch_timeout: Duration::from_millis(1),
                segment_size: 1024 * 1024,
                sync_mode: SyncMode::None,
            },
        )
        .unwrap();
        let safe_lsn_tracker = SafeLsnTracker::new();
        let layout = PartitionLayout::new(&data_dir, PartitionDuration::Hourly);

        let mut manager = RetentionManager::new(policy, pm, wal, safe_lsn_tracker, layout.clone());

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let dir = layout.partition_dir(&partition);
        fs::create_dir_all(&dir).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            // Make deletion fail by removing permissions.
            let mut perms = fs::metadata(&dir).unwrap().permissions();
            perms.set_mode(0o000);
            fs::set_permissions(&dir, perms).unwrap();

            let result = manager.drop_partition(&partition);
            assert!(result.is_err());
            assert_eq!(
                manager.partition_manager.get_state(0),
                Some(PartitionState::Dropping)
            );

            // Restore permissions so tempdir can clean up.
            let mut perms = fs::metadata(&dir).unwrap().permissions();
            perms.set_mode(0o700);
            fs::set_permissions(&dir, perms).unwrap();
        }
    }
}
