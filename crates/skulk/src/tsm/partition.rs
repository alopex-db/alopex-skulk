//! Partition manager for multi-partition time series data routing.
//!
//! This module provides the [`PartitionManager`] which manages multiple
//! [`TimeSeriesMemTable`]s, each covering a specific time partition.
//!
//! # Architecture
//!
//! The partition manager routes incoming data points to the appropriate
//! partition based on their timestamps. It also handles:
//!
//! - Partition creation (on-demand)
//! - Flush policy (oldest-first, size/age thresholds)
//! - Cross-partition queries
//!
//! # Example
//!
//! ```rust,ignore
//! use skulk::tsm::{PartitionManager, PartitionManagerConfig, DataPoint};
//!
//! let config = PartitionManagerConfig::default();
//! let mut manager = PartitionManager::new(config);
//!
//! // Insert data points - they are automatically routed to partitions
//! let point = DataPoint::new("cpu.usage", vec![], 1234567890, 0.75);
//! manager.insert(&point)?;
//! ```

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::Duration;

use crate::error::{Result, TsmError};
use crate::tsm::file::TsmFileHandle;
use crate::tsm::memtable::TimeSeriesMemTable;
use crate::tsm::{DataPoint, TimePartition, TimeRange, Timestamp};

/// Default partition duration: 1 hour.
pub const DEFAULT_PARTITION_DURATION: Duration = Duration::from_secs(3600);

/// Default size threshold for flushing a partition: 64 MB.
pub const DEFAULT_PARTITION_SIZE_THRESHOLD: usize = 64 * 1024 * 1024;

/// Default age threshold for flushing a partition: 15 minutes.
pub const DEFAULT_PARTITION_AGE_THRESHOLD: Duration = Duration::from_secs(15 * 60);

/// Default maximum number of active partitions.
pub const DEFAULT_MAX_ACTIVE_PARTITIONS: usize = 4;

/// Configuration for the partition manager.
#[derive(Debug, Clone)]
pub struct PartitionManagerConfig {
    /// Duration of each partition.
    ///
    /// Data points are grouped into partitions based on this duration.
    /// Default: 1 hour.
    pub partition_duration: Duration,

    /// Size threshold for flushing a partition.
    ///
    /// When a partition's memory usage exceeds this threshold, it becomes
    /// a candidate for flushing to disk. Default: 64 MB.
    pub partition_size_threshold: usize,

    /// Age threshold for flushing a partition.
    ///
    /// When a partition's age exceeds this threshold, it becomes a candidate
    /// for flushing to disk. Default: 15 minutes.
    pub partition_age_threshold: Duration,

    /// Maximum number of active partitions.
    ///
    /// When the number of partitions exceeds this limit, the oldest partitions
    /// are flushed to disk. Default: 4.
    pub max_active_partitions: usize,
}

impl Default for PartitionManagerConfig {
    fn default() -> Self {
        Self {
            partition_duration: DEFAULT_PARTITION_DURATION,
            partition_size_threshold: DEFAULT_PARTITION_SIZE_THRESHOLD,
            partition_age_threshold: DEFAULT_PARTITION_AGE_THRESHOLD,
            max_active_partitions: DEFAULT_MAX_ACTIVE_PARTITIONS,
        }
    }
}

impl PartitionManagerConfig {
    /// Creates a new configuration with custom partition duration.
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.partition_duration = duration;
        self
    }

    /// Creates a new configuration with custom size threshold.
    pub fn with_size_threshold(mut self, threshold: usize) -> Self {
        self.partition_size_threshold = threshold;
        self
    }

    /// Creates a new configuration with custom age threshold.
    pub fn with_age_threshold(mut self, threshold: Duration) -> Self {
        self.partition_age_threshold = threshold;
        self
    }

    /// Creates a new configuration with custom max active partitions.
    pub fn with_max_active_partitions(mut self, max: usize) -> Self {
        self.max_active_partitions = max;
        self
    }
}

/// Priority for flushing partitions.
///
/// Used to determine the order in which partitions should be flushed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FlushPriority {
    /// Size threshold exceeded (highest priority).
    SizeExceeded,
    /// Age threshold exceeded.
    AgeExceeded,
    /// Partition count limit exceeded (lowest priority).
    CountExceeded,
}

/// Statistics for the partition manager.
#[derive(Debug, Default)]
pub struct PartitionManagerStats {
    /// Number of active partitions.
    pub active_partitions: usize,
    /// Total number of data points across all partitions.
    pub total_point_count: u64,
    /// Total memory usage in bytes across all partitions.
    pub total_memory_bytes: usize,
    /// Start timestamp of the oldest partition.
    pub oldest_partition_ts: Option<i64>,
    /// Start timestamp of the newest partition.
    pub newest_partition_ts: Option<i64>,
}

/// Manages multiple time partitions for time series data.
///
/// The partition manager routes incoming data points to the appropriate
/// partition based on their timestamps. It handles partition lifecycle
/// including creation, flushing, and cleanup.
///
/// # Flush Policy (Oldest-First)
///
/// Partitions are flushed based on the following priority:
/// 1. Size threshold exceeded (64 MB by default)
/// 2. Age threshold exceeded (15 minutes by default)
/// 3. Partition count exceeded (4 by default)
///
/// When multiple partitions qualify for flushing, the oldest partition
/// (by start timestamp) is flushed first.
pub struct PartitionManager {
    /// Active partitions (start_ts -> MemTable).
    partitions: BTreeMap<i64, TimeSeriesMemTable>,
    /// Configuration for the manager.
    config: PartitionManagerConfig,
    /// Set of partition start timestamps that are currently being flushed.
    /// Partitions in this set are immutable and will reject writes.
    flushing_partitions: HashSet<i64>,
}

impl PartitionManager {
    /// Creates a new partition manager with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for partition management
    pub fn new(config: PartitionManagerConfig) -> Self {
        Self {
            partitions: BTreeMap::new(),
            config,
            flushing_partitions: HashSet::new(),
        }
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &PartitionManagerConfig {
        &self.config
    }

    /// Calculates the partition start timestamp for a given timestamp.
    ///
    /// The partition start is aligned to the partition duration.
    fn partition_start_ts(&self, timestamp: Timestamp) -> i64 {
        let duration_nanos = self.config.partition_duration.as_nanos() as i64;
        (timestamp / duration_nanos) * duration_nanos
    }

    /// Gets or creates the partition for a given timestamp.
    ///
    /// If the partition doesn't exist, a new one is created.
    pub fn get_or_create_partition(&mut self, timestamp: Timestamp) -> &mut TimeSeriesMemTable {
        let start_ts = self.partition_start_ts(timestamp);

        if !self.partitions.contains_key(&start_ts) {
            let partition = TimePartition::new(start_ts, self.config.partition_duration);
            let memtable = TimeSeriesMemTable::new(partition);
            self.partitions.insert(start_ts, memtable);
        }

        self.partitions.get_mut(&start_ts).unwrap()
    }

    /// Inserts a single data point into the appropriate partition.
    ///
    /// The data point is routed to the partition based on its timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition is currently being flushed (immutable),
    /// or if the insert operation fails.
    pub fn insert(&mut self, point: &DataPoint) -> Result<()> {
        let start_ts = self.partition_start_ts(point.timestamp);

        // Check if partition is currently being flushed (immutable)
        if self.flushing_partitions.contains(&start_ts) {
            return Err(TsmError::PartitionFlushing { start_ts });
        }

        let memtable = self.get_or_create_partition(point.timestamp);
        memtable.insert(point)
    }

    /// Inserts multiple data points into the appropriate partitions.
    ///
    /// Data points are grouped by partition and inserted in batches
    /// to minimize partition lookups.
    ///
    /// # Errors
    ///
    /// Returns an error if any partition is currently being flushed (immutable),
    /// or if any insert operation fails.
    pub fn insert_batch(&mut self, points: &[DataPoint]) -> Result<()> {
        // Group points by partition start timestamp
        let mut grouped: BTreeMap<i64, Vec<&DataPoint>> = BTreeMap::new();
        for point in points {
            let start_ts = self.partition_start_ts(point.timestamp);
            grouped.entry(start_ts).or_default().push(point);
        }

        // Check for flushing partitions before any modifications
        for &start_ts in grouped.keys() {
            if self.flushing_partitions.contains(&start_ts) {
                return Err(TsmError::PartitionFlushing { start_ts });
            }
        }

        // Insert points for each partition
        for (start_ts, partition_points) in grouped {
            // Ensure partition exists
            if !self.partitions.contains_key(&start_ts) {
                let partition = TimePartition::new(start_ts, self.config.partition_duration);
                let memtable = TimeSeriesMemTable::new(partition);
                self.partitions.insert(start_ts, memtable);
            }

            // Insert points
            if let Some(memtable) = self.partitions.get_mut(&start_ts) {
                for point in partition_points {
                    memtable.insert(point)?;
                }
            }
        }

        Ok(())
    }

    /// Returns the partition start timestamps that should be flushed.
    ///
    /// Partitions are returned in oldest-first order, prioritized by:
    /// 1. Size threshold exceeded
    /// 2. Age threshold exceeded
    /// 3. Partition count exceeded
    pub fn partitions_to_flush(&self) -> Vec<i64> {
        let mut candidates: Vec<(FlushPriority, i64)> = Vec::new();

        for (&start_ts, memtable) in &self.partitions {
            // Check size threshold
            if memtable.stats().memory_bytes() >= self.config.partition_size_threshold {
                candidates.push((FlushPriority::SizeExceeded, start_ts));
                continue;
            }

            // Check age threshold
            if memtable.age() >= self.config.partition_age_threshold {
                candidates.push((FlushPriority::AgeExceeded, start_ts));
                continue;
            }
        }

        // Check partition count (oldest partitions first)
        if self.partitions.len() > self.config.max_active_partitions {
            let overflow = self.partitions.len() - self.config.max_active_partitions;
            let mut already_included: std::collections::HashSet<i64> =
                candidates.iter().map(|(_, ts)| *ts).collect();

            for &start_ts in self.partitions.keys().take(overflow) {
                if !already_included.contains(&start_ts) {
                    candidates.push((FlushPriority::CountExceeded, start_ts));
                    already_included.insert(start_ts);
                }
            }
        }

        // Sort by priority (higher first), then by timestamp (older first)
        candidates.sort_by(|a, b| match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => a.1.cmp(&b.1),
            other => other,
        });

        candidates.into_iter().map(|(_, ts)| ts).collect()
    }

    /// Flushes the specified partition to a TSM file using atomic write pattern.
    ///
    /// The partition is removed from the manager **only after successful flush**.
    /// Per requirements.md "Flush Durability Contract":
    /// 1. Write to temp file (.skulk.tmp)
    /// 2. fsync temp file
    /// 3. fsync directory (file entry persistence)
    /// 4. Atomic rename to final path
    /// 5. fsync directory again (rename persistence)
    /// 6. Remove partition from manager
    ///
    /// # Arguments
    ///
    /// * `start_ts` - Start timestamp of the partition to flush
    /// * `dir` - Directory where the TSM file should be written.
    ///   The file will be named `{start_ts}.skulk`.
    ///
    /// # Returns
    ///
    /// A `TsmFileHandle` for the written file, or `None` if the partition
    /// doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush operation fails. On error, the partition
    /// data is preserved and remains in the manager.
    pub fn flush_partition(&mut self, start_ts: i64, dir: &Path) -> Result<Option<TsmFileHandle>> {
        // Check if partition exists without removing it
        if !self.partitions.contains_key(&start_ts) {
            return Ok(None);
        }

        // Mark partition as flushing (immutable) before starting
        self.mark_flushing(start_ts);

        // Wrap the flush operation to ensure cleanup on error
        let result = self.do_flush_partition(start_ts, dir);

        match &result {
            Ok(_) => {
                // Success: remove partition and flushing flag
                self.partitions.remove(&start_ts);
                self.flushing_partitions.remove(&start_ts);
            }
            Err(_) => {
                // Failure: unmark flushing so partition can accept writes again
                self.unmark_flushing(start_ts);
            }
        }

        result
    }

    /// Internal helper for the actual flush operation.
    fn do_flush_partition(&self, start_ts: i64, dir: &Path) -> Result<Option<TsmFileHandle>> {
        // Get a reference to perform flush (don't remove yet)
        let memtable = self.partitions.get(&start_ts).unwrap();

        // Construct file paths
        let final_path = dir.join(format!("{}.skulk", start_ts));
        let tmp_path = dir.join(format!("{}.skulk.tmp", start_ts));

        // 1. Write to temp file
        let handle = memtable.flush(&tmp_path)?;

        // 2. fsync temp file
        {
            let file = std::fs::File::open(&tmp_path)?;
            file.sync_all()?;
        }

        // 3. fsync directory (file entry persistence)
        {
            let dir_file = std::fs::File::open(dir)?;
            dir_file.sync_all()?;
        }

        // 4. Atomic rename
        std::fs::rename(&tmp_path, &final_path)?;

        // 5. fsync directory again (rename persistence)
        {
            let dir_file = std::fs::File::open(dir)?;
            dir_file.sync_all()?;
        }

        // Return handle with the final path
        Ok(Some(TsmFileHandle {
            path: final_path,
            header: handle.header,
            footer: handle.footer,
        }))
    }

    /// Flushes the oldest partition that qualifies for flushing.
    ///
    /// Uses the oldest-first policy to determine which partition to flush.
    /// The flush uses atomic write pattern per requirements.md "Flush Durability Contract".
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory where the TSM file should be written.
    ///   The file will be named `{start_ts}.skulk`.
    ///
    /// # Returns
    ///
    /// A `TsmFileHandle` for the written file, or `None` if no partition
    /// qualifies for flushing.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush operation fails. On error, the partition
    /// data is preserved and remains in the manager.
    pub fn flush_oldest(&mut self, dir: &Path) -> Result<Option<TsmFileHandle>> {
        let to_flush = self.partitions_to_flush();
        if let Some(start_ts) = to_flush.first() {
            self.flush_partition(*start_ts, dir)
        } else {
            Ok(None)
        }
    }

    /// Scans data points within the given time range across all partitions.
    ///
    /// Only partitions that overlap with the time range are scanned.
    ///
    /// # Arguments
    ///
    /// * `range` - The time range to scan
    ///
    /// # Returns
    ///
    /// An iterator over matching data points from all relevant partitions.
    pub fn scan(&self, range: TimeRange) -> impl Iterator<Item = DataPoint> + '_ {
        self.partitions
            .iter()
            .filter(move |(&start_ts, memtable)| {
                // Check if partition overlaps with the range
                let end_ts = memtable.partition().end_ts();
                start_ts < range.end && end_ts > range.start
            })
            .flat_map(move |(_, memtable)| memtable.scan(range))
    }

    /// Returns statistics for the partition manager.
    pub fn stats(&self) -> PartitionManagerStats {
        let active_partitions = self.partitions.len();
        let mut total_point_count = 0u64;
        let mut total_memory_bytes = 0usize;

        for memtable in self.partitions.values() {
            total_point_count += memtable.point_count();
            total_memory_bytes += memtable.stats().memory_bytes();
        }

        let oldest_partition_ts = self.partitions.keys().next().copied();
        let newest_partition_ts = self.partitions.keys().next_back().copied();

        PartitionManagerStats {
            active_partitions,
            total_point_count,
            total_memory_bytes,
            oldest_partition_ts,
            newest_partition_ts,
        }
    }

    /// Returns the number of active partitions.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Returns an iterator over partition start timestamps.
    pub fn partition_timestamps(&self) -> impl Iterator<Item = &i64> {
        self.partitions.keys()
    }

    /// Returns a reference to a specific partition.
    pub fn get_partition(&self, start_ts: i64) -> Option<&TimeSeriesMemTable> {
        self.partitions.get(&start_ts)
    }

    /// Returns a mutable reference to a specific partition.
    pub fn get_partition_mut(&mut self, start_ts: i64) -> Option<&mut TimeSeriesMemTable> {
        self.partitions.get_mut(&start_ts)
    }

    /// Marks a partition as flushing (immutable).
    ///
    /// While a partition is marked as flushing, any attempts to insert data
    /// into that partition will return a `PartitionFlushing` error.
    ///
    /// This is used internally by `flush_partition` but can also be called
    /// directly for custom flush implementations.
    ///
    /// # Arguments
    ///
    /// * `start_ts` - Start timestamp of the partition to mark as flushing
    pub fn mark_flushing(&mut self, start_ts: i64) {
        self.flushing_partitions.insert(start_ts);
    }

    /// Unmarks a partition as flushing (makes it writable again).
    ///
    /// This should be called if a flush operation fails and the partition
    /// needs to remain active, or for cleanup after a successful flush.
    ///
    /// # Arguments
    ///
    /// * `start_ts` - Start timestamp of the partition to unmark
    pub fn unmark_flushing(&mut self, start_ts: i64) {
        self.flushing_partitions.remove(&start_ts);
    }

    /// Returns true if the partition is currently being flushed.
    pub fn is_flushing(&self, start_ts: i64) -> bool {
        self.flushing_partitions.contains(&start_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_point(metric: &str, timestamp: i64, value: f64) -> DataPoint {
        DataPoint::new(metric.to_string(), vec![], timestamp, value)
    }

    #[test]
    fn test_partition_manager_config_default() {
        let config = PartitionManagerConfig::default();
        assert_eq!(config.partition_duration, Duration::from_secs(3600));
        assert_eq!(config.partition_size_threshold, 64 * 1024 * 1024);
        assert_eq!(config.partition_age_threshold, Duration::from_secs(15 * 60));
        assert_eq!(config.max_active_partitions, 4);
    }

    #[test]
    fn test_partition_manager_config_builder() {
        let config = PartitionManagerConfig::default()
            .with_partition_duration(Duration::from_secs(7200))
            .with_size_threshold(128 * 1024 * 1024)
            .with_age_threshold(Duration::from_secs(30 * 60))
            .with_max_active_partitions(8);

        assert_eq!(config.partition_duration, Duration::from_secs(7200));
        assert_eq!(config.partition_size_threshold, 128 * 1024 * 1024);
        assert_eq!(config.partition_age_threshold, Duration::from_secs(30 * 60));
        assert_eq!(config.max_active_partitions, 8);
    }

    #[test]
    fn test_partition_routing() {
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600)); // 1 hour
        let mut manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Points in first hour
        let point1 = make_point("cpu", 0, 1.0);
        let point2 = make_point("cpu", duration_nanos / 2, 2.0);

        // Point in second hour
        let point3 = make_point("cpu", duration_nanos + 1000, 3.0);

        manager.insert(&point1).unwrap();
        manager.insert(&point2).unwrap();
        manager.insert(&point3).unwrap();

        assert_eq!(manager.partition_count(), 2);

        // Verify partition timestamps
        let timestamps: Vec<_> = manager.partition_timestamps().copied().collect();
        assert!(timestamps.contains(&0));
        assert!(timestamps.contains(&duration_nanos));
    }

    #[test]
    fn test_insert_batch_groups_by_partition() {
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Create points spanning multiple partitions
        let points = vec![
            make_point("cpu", 0, 1.0),
            make_point("cpu", 1000, 2.0),
            make_point("cpu", duration_nanos, 3.0),
            make_point("cpu", duration_nanos + 1000, 4.0),
            make_point("cpu", 2 * duration_nanos, 5.0),
        ];

        manager.insert_batch(&points).unwrap();

        assert_eq!(manager.partition_count(), 3);

        // First partition should have 2 points
        let partition = manager.get_partition(0).unwrap();
        assert_eq!(partition.point_count(), 2);

        // Second partition should have 2 points
        let partition = manager.get_partition(duration_nanos).unwrap();
        assert_eq!(partition.point_count(), 2);

        // Third partition should have 1 point
        let partition = manager.get_partition(2 * duration_nanos).unwrap();
        assert_eq!(partition.point_count(), 1);
    }

    #[test]
    fn test_partitions_to_flush_count_exceeded() {
        let config = PartitionManagerConfig::default()
            .with_partition_duration(Duration::from_secs(3600))
            .with_max_active_partitions(2);
        let mut manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Create 4 partitions (exceeds max of 2)
        for i in 0..4 {
            let point = make_point("cpu", i * duration_nanos, i as f64);
            manager.insert(&point).unwrap();
        }

        assert_eq!(manager.partition_count(), 4);

        // Should return 2 oldest partitions (0 and duration_nanos)
        let to_flush = manager.partitions_to_flush();
        assert_eq!(to_flush.len(), 2);
        assert_eq!(to_flush[0], 0);
        assert_eq!(to_flush[1], duration_nanos);
    }

    #[test]
    fn test_cross_partition_scan() {
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Insert points across partitions
        let points = vec![
            make_point("cpu", 1000, 1.0),
            make_point("cpu", 2000, 2.0),
            make_point("cpu", duration_nanos + 1000, 3.0),
            make_point("cpu", duration_nanos + 2000, 4.0),
            make_point("cpu", 2 * duration_nanos + 1000, 5.0),
        ];
        manager.insert_batch(&points).unwrap();

        // Scan first partition only
        let range = TimeRange::new(0, duration_nanos);
        let results: Vec<_> = manager.scan(range).collect();
        assert_eq!(results.len(), 2);

        // Scan across first two partitions
        let range = TimeRange::new(0, 2 * duration_nanos);
        let results: Vec<_> = manager.scan(range).collect();
        assert_eq!(results.len(), 4);

        // Scan all partitions
        let range = TimeRange::new(0, 3 * duration_nanos);
        let results: Vec<_> = manager.scan(range).collect();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_stats() {
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Insert points across partitions
        for i in 0..3 {
            let point = make_point("cpu", i * duration_nanos + 1000, i as f64);
            manager.insert(&point).unwrap();
        }

        let stats = manager.stats();
        assert_eq!(stats.active_partitions, 3);
        assert_eq!(stats.total_point_count, 3);
        assert!(stats.total_memory_bytes > 0);
        assert_eq!(stats.oldest_partition_ts, Some(0));
        assert_eq!(stats.newest_partition_ts, Some(2 * duration_nanos));
    }

    #[test]
    fn test_flush_priority_ordering() {
        // FlushPriority should be ordered: SizeExceeded < AgeExceeded < CountExceeded
        assert!(FlushPriority::SizeExceeded < FlushPriority::AgeExceeded);
        assert!(FlushPriority::AgeExceeded < FlushPriority::CountExceeded);
    }

    #[test]
    fn test_partition_start_ts_calculation() {
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Timestamps within first hour should map to 0
        assert_eq!(manager.partition_start_ts(0), 0);
        assert_eq!(manager.partition_start_ts(1000), 0);
        assert_eq!(manager.partition_start_ts(duration_nanos - 1), 0);

        // Timestamps in second hour should map to duration_nanos
        assert_eq!(manager.partition_start_ts(duration_nanos), duration_nanos);
        assert_eq!(
            manager.partition_start_ts(duration_nanos + 1000),
            duration_nanos
        );

        // Timestamps in third hour should map to 2 * duration_nanos
        assert_eq!(
            manager.partition_start_ts(2 * duration_nanos),
            2 * duration_nanos
        );
    }

    #[test]
    fn test_flush_partition_preserves_on_error() {
        // Test that flush_partition preserves partition data on I/O error
        // per requirements.md "Flush Durability Contract": MemTable drop only after success
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        // Insert data
        let point = make_point("cpu", 1000, 42.0);
        manager.insert(&point).unwrap();

        // Verify we have 1 partition with 1 point
        assert_eq!(manager.partition_count(), 1);
        assert_eq!(manager.stats().total_point_count, 1);

        // Try to flush to non-existent directory (should fail)
        let bad_path = std::path::Path::new("/nonexistent/dir/test.skulk");
        let result = manager.flush_partition(0, bad_path);

        // Flush should fail
        assert!(result.is_err());

        // CRITICAL: Partition and data MUST still exist after failed flush
        assert_eq!(
            manager.partition_count(),
            1,
            "Partition must be preserved after flush failure"
        );
        assert_eq!(
            manager.stats().total_point_count,
            1,
            "Data must be preserved after flush failure"
        );

        // Data should still be queryable
        let range = crate::tsm::TimeRange::new(0, 10000);
        let results: Vec<_> = manager.scan(range).collect();
        assert_eq!(
            results.len(),
            1,
            "Data must be queryable after flush failure"
        );
        assert!((results[0].value - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_flush_partition_atomic_path() {
        // Test that flush uses atomic tmp file + rename pattern
        // per requirements.md "Flush Durability Contract": tmp→fsync→rename→fsync(dir)
        let temp_dir = tempfile::tempdir().unwrap();
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        // Insert data
        let point = make_point("cpu", 1000, 42.0);
        manager.insert(&point).unwrap();

        // Flush to directory (not a specific file path)
        // The implementation should create {start_ts}.skulk atomically
        let result = manager.flush_partition(0, temp_dir.path());

        assert!(result.is_ok(), "Flush should succeed");
        let handle = result.unwrap();
        assert!(handle.is_some(), "Handle should be returned");

        // Final file should exist with correct name
        let expected_file = temp_dir.path().join("0.skulk");
        assert!(
            expected_file.exists(),
            "Final file {expected_file:?} should exist"
        );

        // No temp files should remain
        let tmp_file = temp_dir.path().join("0.skulk.tmp");
        assert!(
            !tmp_file.exists(),
            "Temp file {tmp_file:?} should not remain after successful flush"
        );

        // Partition should be removed after successful flush
        assert_eq!(
            manager.partition_count(),
            0,
            "Partition should be removed after successful flush"
        );
    }

    #[test]
    fn test_insert_blocked_during_flush() {
        // Test that insert to a flushing partition returns PartitionFlushing error
        // per requirements.md "Flush Durability Contract": MemTable immutable during flush
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        // Insert initial data
        let point1 = make_point("cpu", 1000, 42.0);
        manager.insert(&point1).unwrap();
        assert_eq!(manager.partition_count(), 1);

        // Mark partition as flushing
        manager.mark_flushing(0);

        // Try to insert to the same partition - should fail with PartitionFlushing
        let point2 = make_point("cpu", 2000, 43.0);
        let result = manager.insert(&point2);
        assert!(result.is_err(), "Insert to flushing partition should fail");

        // Verify it's the correct error type
        match result.unwrap_err() {
            crate::error::TsmError::PartitionFlushing { start_ts } => {
                assert_eq!(start_ts, 0, "Error should reference the flushing partition");
            }
            other => panic!("Expected PartitionFlushing error, got: {:?}", other),
        }

        // Unmark flushing - insert should work again
        manager.unmark_flushing(0);
        let point3 = make_point("cpu", 3000, 44.0);
        assert!(
            manager.insert(&point3).is_ok(),
            "Insert should succeed after unmark_flushing"
        );
    }

    #[test]
    fn test_insert_redirected_to_new_partition_during_flush() {
        // Test that insert to a different partition works while another is flushing
        let config =
            PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));
        let mut manager = PartitionManager::new(config);

        let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

        // Insert data to partition 0
        let point1 = make_point("cpu", 1000, 42.0);
        manager.insert(&point1).unwrap();
        assert_eq!(manager.partition_count(), 1);

        // Mark partition 0 as flushing
        manager.mark_flushing(0);

        // Insert to a different partition (partition 1) should succeed
        let point2 = make_point("cpu", duration_nanos + 1000, 43.0);
        assert!(
            manager.insert(&point2).is_ok(),
            "Insert to non-flushing partition should succeed"
        );
        assert_eq!(manager.partition_count(), 2);
    }

    #[test]
    fn test_insert_requires_wal_before_memtable() {
        // Test for WAL/Durability Contract (task 4.C completed):
        // "Client → WAL append → batch fsync → MemTable insert → Ack"
        //
        // This test verifies that:
        // 1. WAL write occurs before MemTable mutation
        // 2. MemTable remains unchanged if WAL write fails
        // 3. Data is recoverable from WAL after crash
        use crate::tsm::memtable::TimeSeriesMemTable;
        use crate::tsm::TimePartition;
        use crate::wal::{SyncMode, Wal, WalConfig};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        // Create WAL and MemTable
        let wal_config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(&wal_dir, wal_config).unwrap();

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        // Initial state verification
        assert_eq!(memtable.stats().point_count(), 0);
        assert_eq!(memtable.stats().series_count(), 0);
        assert_eq!(wal.current_sequence(), 1);

        // Insert with WAL - this follows the durability contract:
        // 1. compute_series_id (no mutation)
        // 2. WAL append + fsync
        // 3. MemTable mutation (only after WAL success)
        let point = make_point("cpu.usage", 1000, 0.75);
        let seq = memtable.insert_with_wal(&point, &mut wal).unwrap();

        // Verify WAL recorded the entry
        assert_eq!(seq, 1);
        assert_eq!(wal.current_sequence(), 2);

        // Verify MemTable was updated
        assert_eq!(memtable.stats().point_count(), 1);
        assert_eq!(memtable.stats().series_count(), 1);

        // Simulate crash - drop everything and recover from WAL
        drop(memtable);
        drop(wal);

        // Recover from WAL
        let recovered = Wal::recover(&wal_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].sequence, 1);
        assert_eq!(recovered[0].timestamp, 1000);
        assert!((recovered[0].value - 0.75).abs() < f64::EPSILON);
    }
}
