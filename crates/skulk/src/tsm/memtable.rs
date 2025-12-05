//! TimeSeriesMemTable - Time-partition aware in-memory buffer.
//!
//! This module provides the in-memory buffer for time series data points
//! before they are flushed to TSM files on disk.
//!
//! # WAL Integration
//!
//! For durability, the write path should follow:
//! ```text
//! Client → WAL append → batch fsync → MemTable insert → Ack
//! ```
//!
//! Use the [`TimeSeriesMemTable::insert_with_wal`] and
//! [`TimeSeriesMemTable::insert_batch_with_wal`] methods to ensure data is
//! written to WAL before being inserted into the MemTable.

use crate::error::{Result, TsmError};
use crate::tsm::{DataPoint, SeriesId, SeriesMeta, TimePartition, TimeRange, Timestamp};
use crate::tsm::{TsmFileHandle, TsmWriter};
use crate::wal::{Wal, WalEntry};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Size threshold for triggering flush (64 MB).
pub const FLUSH_SIZE_THRESHOLD: usize = 64 * 1024 * 1024;

/// Age threshold for triggering flush (15 minutes).
pub const FLUSH_AGE_THRESHOLD: Duration = Duration::from_secs(15 * 60);

/// Statistics for a TimeSeriesMemTable.
#[derive(Debug, Default)]
pub struct MemTableStats {
    /// Total number of data points.
    pub point_count: AtomicU64,
    /// Number of unique series.
    pub series_count: AtomicU32,
    /// Estimated memory usage in bytes.
    pub memory_bytes: AtomicUsize,
    /// Minimum timestamp across all points.
    pub min_timestamp: AtomicI64,
    /// Maximum timestamp across all points.
    pub max_timestamp: AtomicI64,
}

impl MemTableStats {
    /// Creates new stats with default values.
    pub fn new() -> Self {
        Self {
            point_count: AtomicU64::new(0),
            series_count: AtomicU32::new(0),
            memory_bytes: AtomicUsize::new(0),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_timestamp: AtomicI64::new(i64::MIN),
        }
    }

    /// Gets the current point count.
    pub fn point_count(&self) -> u64 {
        self.point_count.load(Ordering::Relaxed)
    }

    /// Gets the current series count.
    pub fn series_count(&self) -> u32 {
        self.series_count.load(Ordering::Relaxed)
    }

    /// Gets the current memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Gets the minimum timestamp.
    pub fn min_timestamp(&self) -> i64 {
        self.min_timestamp.load(Ordering::Relaxed)
    }

    /// Gets the maximum timestamp.
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp.load(Ordering::Relaxed)
    }

    /// Updates the timestamp range.
    pub fn update_timestamp(&self, ts: i64) {
        // Update min
        let mut current = self.min_timestamp.load(Ordering::Relaxed);
        while ts < current {
            match self.min_timestamp.compare_exchange_weak(
                current,
                ts,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        // Update max
        let mut current = self.max_timestamp.load(Ordering::Relaxed);
        while ts > current {
            match self.max_timestamp.compare_exchange_weak(
                current,
                ts,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Increments the point count.
    pub fn increment_points(&self, count: u64) {
        self.point_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Increments the series count.
    pub fn increment_series(&self) {
        self.series_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds to the memory usage.
    pub fn add_memory(&self, bytes: usize) {
        self.memory_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Time-partition aware in-memory buffer for time series data.
pub struct TimeSeriesMemTable {
    /// The time partition this MemTable covers.
    partition: TimePartition,
    /// Data points per series, sorted by timestamp.
    series_data: HashMap<SeriesId, BTreeMap<Timestamp, f64>>,
    /// Metadata for each series.
    series_meta: HashMap<SeriesId, SeriesMeta>,
    /// Statistics for this MemTable.
    stats: MemTableStats,
    /// Creation time for age-based flush.
    created_at: Instant,
}

impl TimeSeriesMemTable {
    /// Returns the elapsed time since this MemTable was created.
    /// Useful for monitoring and debugging.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Creates a MemTable with a custom creation time for testing purposes.
    #[cfg(test)]
    pub(crate) fn with_created_at(partition: TimePartition, created_at: Instant) -> Self {
        Self {
            partition,
            series_data: HashMap::new(),
            series_meta: HashMap::new(),
            stats: MemTableStats::new(),
            created_at,
        }
    }
}

impl TimeSeriesMemTable {
    /// Creates a new TimeSeriesMemTable for the given partition.
    ///
    /// # Arguments
    ///
    /// * `partition` - The time partition this MemTable covers
    pub fn new(partition: TimePartition) -> Self {
        Self {
            partition,
            series_data: HashMap::new(),
            series_meta: HashMap::new(),
            stats: MemTableStats::new(),
            created_at: Instant::now(),
        }
    }

    /// Returns the partition this MemTable covers.
    pub fn partition(&self) -> &TimePartition {
        &self.partition
    }

    /// Returns the statistics for this MemTable.
    pub fn stats(&self) -> &MemTableStats {
        &self.stats
    }

    /// Computes the series ID for a given metric and labels without modifying state.
    ///
    /// This is a pure function that can be used to compute the series ID
    /// before committing to WAL, ensuring no MemTable mutation occurs
    /// until the WAL write succeeds.
    pub fn compute_series_id(metric: &str, labels: &[(String, String)]) -> SeriesId {
        // Sort labels for consistent hashing
        let mut sorted_labels = labels.to_vec();
        sorted_labels.sort();

        // Compute hash
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        metric.hash(&mut hasher);
        for (k, v) in &sorted_labels {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Gets or creates a series entry for the given metric and labels.
    ///
    /// Returns the series ID and whether the series was newly created.
    /// Note: This method modifies MemTable state. For WAL-integrated writes,
    /// use [`compute_series_id`] first to get the ID without side effects,
    /// write to WAL, then call this method only after WAL success.
    pub fn get_or_create_series(
        &mut self,
        metric: &str,
        labels: &[(String, String)],
    ) -> (SeriesId, bool) {
        let series_id = Self::compute_series_id(metric, labels);

        // Check if series exists
        let is_new = !self.series_meta.contains_key(&series_id);

        if is_new {
            // Sort labels for storage
            let mut sorted_labels = labels.to_vec();
            sorted_labels.sort();

            // Create new series entry
            self.series_meta.insert(
                series_id,
                SeriesMeta::new(metric.to_string(), sorted_labels),
            );
            self.series_data.insert(series_id, BTreeMap::new());
            self.stats.increment_series();

            // Estimate memory for metadata
            let meta_size = std::mem::size_of::<SeriesMeta>()
                + metric.len()
                + labels.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
            self.stats.add_memory(meta_size);
        }

        (series_id, is_new)
    }

    /// Inserts a single data point into the MemTable.
    ///
    /// # Errors
    ///
    /// Returns `TsmError::PartitionMismatch` if the point's timestamp is outside
    /// this MemTable's partition.
    pub fn insert(&mut self, point: &DataPoint) -> Result<()> {
        // Validate timestamp is within partition
        if !self.partition.contains(point.timestamp) {
            return Err(TsmError::PartitionMismatch {
                point_ts: point.timestamp,
                start: self.partition.start_ts,
                end: self.partition.end_ts(),
            });
        }

        // Get or create series
        let (series_id, _is_new) = self.get_or_create_series(&point.metric, &point.labels);

        // Insert data point
        if let Some(series) = self.series_data.get_mut(&series_id) {
            series.insert(point.timestamp, point.value);
        }

        // Update stats
        self.stats.increment_points(1);
        self.stats.update_timestamp(point.timestamp);
        self.stats.add_memory(std::mem::size_of::<(i64, f64)>());

        Ok(())
    }

    /// Inserts multiple data points into the MemTable.
    ///
    /// # Errors
    ///
    /// Returns `TsmError::PartitionMismatch` if any point's timestamp is outside
    /// this MemTable's partition.
    pub fn insert_batch(&mut self, points: &[DataPoint]) -> Result<()> {
        for point in points {
            self.insert(point)?;
        }
        Ok(())
    }

    /// Inserts a single data point with WAL durability.
    ///
    /// This method follows the write path durability contract:
    /// 1. Generate series ID
    /// 2. Write to WAL and fsync
    /// 3. Insert into MemTable
    ///
    /// Only after WAL write succeeds should the operation be acknowledged.
    ///
    /// # Arguments
    ///
    /// * `point` - The data point to insert
    /// * `wal` - The WAL to write to before inserting
    ///
    /// # Returns
    ///
    /// The WAL sequence number of the written entry.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The point's timestamp is outside this partition
    /// - WAL write fails
    pub fn insert_with_wal(&mut self, point: &DataPoint, wal: &mut Wal) -> Result<u64> {
        // Validate timestamp is within partition
        if !self.partition.contains(point.timestamp) {
            return Err(TsmError::PartitionMismatch {
                point_ts: point.timestamp,
                start: self.partition.start_ts,
                end: self.partition.end_ts(),
            });
        }

        // Compute series_id WITHOUT modifying MemTable state
        // This ensures MemTable remains unchanged if WAL write fails
        let series_id = Self::compute_series_id(&point.metric, &point.labels);

        // Write to WAL first (this includes fsync)
        // If this fails, MemTable is unchanged - durability contract preserved
        let entry = WalEntry::new(series_id, point.timestamp, point.value);
        let seq = wal.append(entry)?;
        wal.sync()?; // Ensure durability before MemTable insert

        // WAL write succeeded - now safe to mutate MemTable
        // get_or_create_series will create series entry if needed
        let (_, _is_new) = self.get_or_create_series(&point.metric, &point.labels);

        // Insert data point
        if let Some(series) = self.series_data.get_mut(&series_id) {
            series.insert(point.timestamp, point.value);
        }

        // Update stats
        self.stats.increment_points(1);
        self.stats.update_timestamp(point.timestamp);
        self.stats.add_memory(std::mem::size_of::<(i64, f64)>());

        Ok(seq)
    }

    /// Inserts multiple data points with WAL durability.
    ///
    /// This method follows the write path durability contract:
    /// 1. Generate series IDs for all points
    /// 2. Write batch to WAL and fsync
    /// 3. Insert all points into MemTable
    ///
    /// # Arguments
    ///
    /// * `points` - The data points to insert
    /// * `wal` - The WAL to write to before inserting
    ///
    /// # Returns
    ///
    /// The WAL sequence numbers of the written entries.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any point's timestamp is outside this partition
    /// - WAL write fails
    pub fn insert_batch_with_wal(
        &mut self,
        points: &[DataPoint],
        wal: &mut Wal,
    ) -> Result<Vec<u64>> {
        // Validate all timestamps first
        for point in points {
            if !self.partition.contains(point.timestamp) {
                return Err(TsmError::PartitionMismatch {
                    point_ts: point.timestamp,
                    start: self.partition.start_ts,
                    end: self.partition.end_ts(),
                });
            }
        }

        // Compute series IDs WITHOUT modifying MemTable state
        // This ensures MemTable remains unchanged if WAL write fails
        let mut wal_entries = Vec::with_capacity(points.len());
        let mut series_ids = Vec::with_capacity(points.len());

        for point in points {
            let series_id = Self::compute_series_id(&point.metric, &point.labels);
            series_ids.push(series_id);
            wal_entries.push(WalEntry::new(series_id, point.timestamp, point.value));
        }

        // Write batch to WAL (includes fsync)
        // If this fails, MemTable is unchanged - durability contract preserved
        let sequences = wal.append_batch(&wal_entries)?;

        // WAL write succeeded - now safe to mutate MemTable
        for (point, &series_id) in points.iter().zip(series_ids.iter()) {
            // Create series entry if needed (now safe to mutate)
            let _ = self.get_or_create_series(&point.metric, &point.labels);

            // Insert data point
            if let Some(series) = self.series_data.get_mut(&series_id) {
                series.insert(point.timestamp, point.value);
            }

            // Update stats
            self.stats.increment_points(1);
            self.stats.update_timestamp(point.timestamp);
            self.stats.add_memory(std::mem::size_of::<(i64, f64)>());
        }

        Ok(sequences)
    }

    /// Returns true if the MemTable should be flushed to disk.
    ///
    /// Flush is triggered when:
    /// - Memory usage exceeds `FLUSH_SIZE_THRESHOLD` (64 MB), or
    /// - Age exceeds `FLUSH_AGE_THRESHOLD` (15 minutes)
    pub fn should_flush(&self) -> bool {
        self.stats.memory_bytes() >= FLUSH_SIZE_THRESHOLD
            || self.created_at.elapsed() >= FLUSH_AGE_THRESHOLD
    }

    /// Scans data points within the given time range.
    ///
    /// Returns an iterator over matching data points across all series.
    pub fn scan(&self, range: TimeRange) -> impl Iterator<Item = DataPoint> + '_ {
        self.series_data.iter().flat_map(move |(&series_id, data)| {
            let meta = self.series_meta.get(&series_id);
            data.range(range.start..range.end)
                .map(move |(&ts, &value)| {
                    let (metric, labels) = meta
                        .map(|m| (m.metric_name.clone(), m.labels.clone()))
                        .unwrap_or_default();
                    DataPoint {
                        metric,
                        labels,
                        timestamp: ts,
                        value,
                    }
                })
        })
    }

    /// Returns a reference to the data for a specific series.
    pub fn get_series(&self, series_id: SeriesId) -> Option<&BTreeMap<Timestamp, f64>> {
        self.series_data.get(&series_id)
    }

    /// Returns the metadata for a specific series.
    pub fn get_series_meta(&self, series_id: SeriesId) -> Option<&SeriesMeta> {
        self.series_meta.get(&series_id)
    }

    /// Returns all series IDs in this MemTable.
    pub fn series_ids(&self) -> impl Iterator<Item = &SeriesId> {
        self.series_data.keys()
    }

    /// Returns the number of unique series.
    pub fn series_count(&self) -> usize {
        self.series_data.len()
    }

    /// Returns the total number of data points.
    pub fn point_count(&self) -> u64 {
        self.stats.point_count()
    }

    /// Flushes the MemTable contents to a TSM file.
    ///
    /// This creates a TSM file at the given path containing all series data
    /// from this MemTable, compressed using Gorilla compression.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the TSM file should be written
    ///
    /// # Returns
    ///
    /// A `TsmFileHandle` containing the header and footer of the written file,
    /// which can be used for subsequent reads.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be created
    /// - Any I/O operation fails during writing
    pub fn flush(&self, path: &Path) -> Result<TsmFileHandle> {
        let mut writer = TsmWriter::new(path)?;

        // Write each series with its metadata and data points
        for (&series_id, data) in &self.series_data {
            if data.is_empty() {
                continue;
            }

            // Get the metadata for this series
            let meta = self
                .series_meta
                .get(&series_id)
                .cloned()
                .unwrap_or_else(|| SeriesMeta::new("unknown", vec![]));

            // Write the series
            writer.write_series(series_id, &meta, data)?;
        }

        // Finalize and return the handle
        writer.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tsm::TsmReader;
    use tempfile::TempDir;

    fn make_partition() -> TimePartition {
        TimePartition::new(0, Duration::from_secs(3600)) // 1 hour
    }

    #[test]
    fn test_insert_single_point() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let point = DataPoint::new(
            "cpu.usage",
            vec![("host".to_string(), "server1".to_string())],
            1000,
            0.75,
        );

        memtable.insert(&point).unwrap();

        assert_eq!(memtable.stats.point_count(), 1);
        assert_eq!(memtable.stats.series_count(), 1);
        assert_eq!(memtable.stats.min_timestamp(), 1000);
        assert_eq!(memtable.stats.max_timestamp(), 1000);
    }

    #[test]
    fn test_insert_batch() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let points = vec![
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server1".to_string())],
                1000,
                0.75,
            ),
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server1".to_string())],
                2000,
                0.80,
            ),
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server2".to_string())],
                1500,
                0.65,
            ),
        ];

        memtable.insert_batch(&points).unwrap();

        assert_eq!(memtable.stats.point_count(), 3);
        assert_eq!(memtable.stats.series_count(), 2); // 2 unique series
        assert_eq!(memtable.stats.min_timestamp(), 1000);
        assert_eq!(memtable.stats.max_timestamp(), 2000);
    }

    #[test]
    fn test_series_id_consistency() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let labels = vec![("host".to_string(), "server1".to_string())];

        let (id1, is_new1) = memtable.get_or_create_series("cpu.usage", &labels);
        let (id2, is_new2) = memtable.get_or_create_series("cpu.usage", &labels);

        assert!(is_new1);
        assert!(!is_new2);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_series_id_label_order_invariance() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let labels1 = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ];
        let labels2 = vec![
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ];

        let (id1, _) = memtable.get_or_create_series("metric", &labels1);
        let (id2, _) = memtable.get_or_create_series("metric", &labels2);

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_scan_time_range() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let points = vec![
            DataPoint::new("metric", vec![], 100, 1.0),
            DataPoint::new("metric", vec![], 200, 2.0),
            DataPoint::new("metric", vec![], 300, 3.0),
            DataPoint::new("metric", vec![], 400, 4.0),
        ];

        memtable.insert_batch(&points).unwrap();

        let range = TimeRange::new(150, 350);
        let scanned: Vec<_> = memtable.scan(range).collect();

        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].timestamp, 200);
        assert_eq!(scanned[1].timestamp, 300);
    }

    #[test]
    fn test_should_flush_size_threshold() {
        let memtable = TimeSeriesMemTable::new(make_partition());

        // Artificially set memory to just below threshold
        memtable
            .stats
            .memory_bytes
            .store(FLUSH_SIZE_THRESHOLD - 1, Ordering::Relaxed);
        assert!(!memtable.should_flush());

        // Set to at threshold
        memtable
            .stats
            .memory_bytes
            .store(FLUSH_SIZE_THRESHOLD, Ordering::Relaxed);
        assert!(memtable.should_flush());
    }

    #[test]
    fn test_partition_mismatch_error() {
        let partition = TimePartition::new(1000, Duration::from_nanos(100));
        let mut memtable = TimeSeriesMemTable::new(partition);

        // Point before partition
        let point = DataPoint::new("metric", vec![], 999, 1.0);
        let result = memtable.insert(&point);
        assert!(matches!(result, Err(TsmError::PartitionMismatch { .. })));

        // Point after partition
        let point = DataPoint::new("metric", vec![], 1100, 1.0);
        let result = memtable.insert(&point);
        assert!(matches!(result, Err(TsmError::PartitionMismatch { .. })));

        // Point within partition
        let point = DataPoint::new("metric", vec![], 1050, 1.0);
        let result = memtable.insert(&point);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_series() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let point = DataPoint::new(
            "cpu.usage",
            vec![("host".to_string(), "server1".to_string())],
            1000,
            0.75,
        );
        memtable.insert(&point).unwrap();

        let (series_id, _) = memtable
            .get_or_create_series("cpu.usage", &[("host".to_string(), "server1".to_string())]);

        let data = memtable.get_series(series_id);
        assert!(data.is_some());
        assert_eq!(data.unwrap().len(), 1);
        assert_eq!(data.unwrap().get(&1000), Some(&0.75));
    }

    #[test]
    fn test_should_flush_age_threshold() {
        // Create a memtable with a creation time in the past (beyond age threshold)
        let old_created_at = Instant::now() - FLUSH_AGE_THRESHOLD - Duration::from_secs(1);
        let memtable = TimeSeriesMemTable::with_created_at(make_partition(), old_created_at);

        // Should flush due to age, even with no data
        assert!(memtable.should_flush());

        // Create a fresh memtable
        let fresh_memtable = TimeSeriesMemTable::new(make_partition());

        // Fresh memtable should not flush (neither size nor age threshold exceeded)
        assert!(!fresh_memtable.should_flush());
    }

    #[test]
    fn test_age_method() {
        let memtable = TimeSeriesMemTable::new(make_partition());

        // Age should be very small (just created)
        let age = memtable.age();
        assert!(age < Duration::from_secs(1));
    }

    #[test]
    fn test_get_series_meta() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let labels = vec![("host".to_string(), "server1".to_string())];
        let point = DataPoint::new("cpu.usage", labels.clone(), 1000, 0.75);
        memtable.insert(&point).unwrap();

        let (series_id, _) = memtable.get_or_create_series("cpu.usage", &labels);

        let meta = memtable.get_series_meta(series_id);
        assert!(meta.is_some());
        let meta = meta.unwrap();
        assert_eq!(meta.metric_name, "cpu.usage");
        assert_eq!(meta.labels.len(), 1);
        assert_eq!(meta.labels[0], ("host".to_string(), "server1".to_string()));
    }

    #[test]
    fn test_series_ids_iterator() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        // Insert points for 3 different series
        let points = vec![
            DataPoint::new("metric1", vec![], 100, 1.0),
            DataPoint::new("metric2", vec![], 200, 2.0),
            DataPoint::new("metric3", vec![], 300, 3.0),
        ];
        memtable.insert_batch(&points).unwrap();

        let series_ids: Vec<_> = memtable.series_ids().collect();
        assert_eq!(series_ids.len(), 3);
    }

    #[test]
    fn test_series_count_and_point_count() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        assert_eq!(memtable.series_count(), 0);
        assert_eq!(memtable.point_count(), 0);

        // Insert multiple points for same series
        let points = vec![
            DataPoint::new("metric", vec![], 100, 1.0),
            DataPoint::new("metric", vec![], 200, 2.0),
        ];
        memtable.insert_batch(&points).unwrap();

        assert_eq!(memtable.series_count(), 1);
        assert_eq!(memtable.point_count(), 2);

        // Insert point for different series
        let point = DataPoint::new("other_metric", vec![], 300, 3.0);
        memtable.insert(&point).unwrap();

        assert_eq!(memtable.series_count(), 2);
        assert_eq!(memtable.point_count(), 3);
    }

    #[test]
    fn test_scan_empty_range() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let points = vec![
            DataPoint::new("metric", vec![], 100, 1.0),
            DataPoint::new("metric", vec![], 200, 2.0),
        ];
        memtable.insert_batch(&points).unwrap();

        // Scan a range that contains no points
        let range = TimeRange::new(500, 600);
        let scanned: Vec<_> = memtable.scan(range).collect();
        assert!(scanned.is_empty());
    }

    #[test]
    fn test_overwrite_existing_point() {
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let point1 = DataPoint::new("metric", vec![], 100, 1.0);
        memtable.insert(&point1).unwrap();

        // Insert another point with same timestamp (should overwrite)
        let point2 = DataPoint::new("metric", vec![], 100, 2.0);
        memtable.insert(&point2).unwrap();

        // Point count increases (we don't deduplicate in stats for performance)
        // but the BTreeMap will have only one entry for that timestamp
        let (series_id, _) = memtable.get_or_create_series("metric", &[]);
        let data = memtable.get_series(series_id).unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data.get(&100), Some(&2.0)); // Latest value wins
    }

    #[test]
    fn test_flush_to_tsm_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.skulk");

        let mut memtable = TimeSeriesMemTable::new(make_partition());

        // Insert data for multiple series
        let points = vec![
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server1".to_string())],
                1000,
                0.75,
            ),
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server1".to_string())],
                2000,
                0.80,
            ),
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server2".to_string())],
                1500,
                0.65,
            ),
            DataPoint::new("memory.free", vec![], 1000, 4096.0),
            DataPoint::new("memory.free", vec![], 2000, 3500.0),
        ];

        memtable.insert_batch(&points).unwrap();

        assert_eq!(memtable.series_count(), 3);
        assert_eq!(memtable.point_count(), 5);

        // Flush to TSM file
        let handle = memtable.flush(&file_path).unwrap();

        // Verify the handle
        assert_eq!(handle.header.series_count, 3);
        assert_eq!(handle.footer.total_point_count, 5);

        // Verify we can read the file back
        let reader = TsmReader::open(&file_path).unwrap();
        assert_eq!(reader.header().series_count, 3);

        // Verify file CRC
        assert!(reader.verify_file_checksum().unwrap());
    }

    #[test]
    fn test_flush_empty_memtable() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("empty.skulk");

        let memtable = TimeSeriesMemTable::new(make_partition());

        // Flush empty memtable
        let handle = memtable.flush(&file_path).unwrap();

        // Should create a valid file with no series
        assert_eq!(handle.header.series_count, 0);
        assert_eq!(handle.footer.total_point_count, 0);
    }

    #[test]
    fn test_flush_and_read_back_data() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("roundtrip.skulk");

        let mut memtable = TimeSeriesMemTable::new(make_partition());

        // Insert known data
        let labels = vec![("host".to_string(), "server1".to_string())];
        for i in 0..100 {
            let point = DataPoint::new("cpu.usage", labels.clone(), i * 1000, (i as f64) * 0.01);
            memtable.insert(&point).unwrap();
        }

        // Get the series ID for later lookup
        let (series_id, _) = memtable.get_or_create_series("cpu.usage", &labels);

        // Flush
        memtable.flush(&file_path).unwrap();

        // Read back
        let reader = TsmReader::open(&file_path).unwrap();
        let read_points = reader.read_series(series_id).unwrap().unwrap();

        // Verify all points match
        assert_eq!(read_points.len(), 100);
        for (i, (ts, val)) in read_points.iter().enumerate() {
            assert_eq!(*ts, (i as i64) * 1000);
            assert!((val - (i as f64) * 0.01).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_flush_preserves_series_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("meta.skulk");

        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let labels = vec![
            ("host".to_string(), "server1".to_string()),
            ("env".to_string(), "production".to_string()),
        ];
        let point = DataPoint::new("cpu.usage", labels.clone(), 1000, 0.75);
        memtable.insert(&point).unwrap();

        // Get series ID
        let (series_id, _) = memtable.get_or_create_series("cpu.usage", &labels);

        // Flush
        let handle = memtable.flush(&file_path).unwrap();

        // Verify metadata was written (series count)
        assert_eq!(handle.header.series_count, 1);

        // Read back and verify data exists
        let reader = TsmReader::open(&file_path).unwrap();
        let read_points = reader.read_series(series_id).unwrap();
        assert!(read_points.is_some());
        assert_eq!(read_points.unwrap().len(), 1);
    }

    // WAL Integration Tests

    #[test]
    fn test_insert_with_wal() {
        use crate::wal::{SyncMode, WalConfig};

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::None, // Fast for testing
        };
        let mut wal = Wal::new(&wal_dir, config).unwrap();
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let point = DataPoint::new(
            "cpu.usage",
            vec![("host".to_string(), "server1".to_string())],
            1000,
            0.75,
        );

        let seq = memtable.insert_with_wal(&point, &mut wal).unwrap();

        assert_eq!(seq, 1);
        assert_eq!(memtable.stats.point_count(), 1);
        assert_eq!(memtable.stats.series_count(), 1);
    }

    #[test]
    fn test_insert_batch_with_wal() {
        use crate::wal::{SyncMode, WalConfig};

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::None,
        };
        let mut wal = Wal::new(&wal_dir, config).unwrap();
        let mut memtable = TimeSeriesMemTable::new(make_partition());

        let points = vec![
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server1".to_string())],
                1000,
                0.75,
            ),
            DataPoint::new(
                "cpu.usage",
                vec![("host".to_string(), "server1".to_string())],
                2000,
                0.80,
            ),
            DataPoint::new(
                "memory.free",
                vec![],
                1500,
                4096.0,
            ),
        ];

        let sequences = memtable.insert_batch_with_wal(&points, &mut wal).unwrap();

        assert_eq!(sequences.len(), 3);
        assert_eq!(sequences[0], 1);
        assert_eq!(sequences[1], 2);
        assert_eq!(sequences[2], 3);
        assert_eq!(memtable.stats.point_count(), 3);
        assert_eq!(memtable.stats.series_count(), 2);
    }

    #[test]
    fn test_wal_recovery_and_memtable_replay() {
        use crate::wal::{SyncMode, WalConfig};

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        // Write some data with WAL
        let labels = vec![("host".to_string(), "server1".to_string())];
        let (series_id, _) = {
            let config = WalConfig {
                batch_size: 100,
                batch_timeout: Duration::from_millis(10),
                segment_size: 1024 * 1024,
                sync_mode: SyncMode::Fsync, // Use Fsync for durability test
            };
            let mut wal = Wal::new(&wal_dir, config).unwrap();
            let mut memtable = TimeSeriesMemTable::new(make_partition());

            for i in 0..10 {
                let point = DataPoint::new("cpu.usage", labels.clone(), i * 1000, i as f64 * 0.1);
                memtable.insert_with_wal(&point, &mut wal).unwrap();
            }

            // Return series_id for later verification
            memtable.get_or_create_series("cpu.usage", &labels)
        };

        // Simulate crash and recovery: recover WAL entries
        let recovered_entries = Wal::recover(&wal_dir).unwrap();
        assert_eq!(recovered_entries.len(), 10);

        // Replay into a new MemTable
        let mut new_memtable = TimeSeriesMemTable::new(make_partition());
        for entry in &recovered_entries {
            // In real recovery, we'd need to reconstruct metric/labels from series_meta
            // Here we use insert() directly for simplicity since we know the series_id
            if let Some(series) = new_memtable.series_data.get_mut(&entry.series_id) {
                series.insert(entry.timestamp, entry.value);
            } else {
                // Create series with recovered data (in real code, we'd have metric/labels persisted)
                new_memtable.series_data.insert(entry.series_id, BTreeMap::new());
                if let Some(series) = new_memtable.series_data.get_mut(&entry.series_id) {
                    series.insert(entry.timestamp, entry.value);
                }
            }
            new_memtable.stats.increment_points(1);
        }

        // Verify recovered data
        let data = new_memtable.series_data.get(&series_id).unwrap();
        assert_eq!(data.len(), 10);

        for i in 0..10 {
            let ts = i * 1000;
            let expected_value = i as f64 * 0.1;
            assert!((data.get(&ts).unwrap() - expected_value).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_insert_with_wal_partition_mismatch() {
        use crate::wal::{SyncMode, WalConfig};

        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::None,
        };
        let mut wal = Wal::new(&wal_dir, config).unwrap();

        let partition = TimePartition::new(1000, Duration::from_nanos(100));
        let mut memtable = TimeSeriesMemTable::new(partition);

        // Point outside partition
        let point = DataPoint::new("metric", vec![], 999, 1.0);
        let result = memtable.insert_with_wal(&point, &mut wal);
        assert!(matches!(result, Err(TsmError::PartitionMismatch { .. })));
    }
}
