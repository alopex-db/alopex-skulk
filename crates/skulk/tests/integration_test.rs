//! Integration tests for the complete write path and crash recovery scenarios.
//!
//! These tests verify the full data lifecycle:
//! - WAL → MemTable → TSM (full write path)
//! - Crash recovery from WAL
//! - MemTable → TSM flush with atomic durability

use alopex_skulk::tsm::{
    DataPoint, PartitionManager, PartitionManagerConfig, TimePartition, TimeRange,
    TimeSeriesMemTable, TsmReader,
};
use alopex_skulk::wal::{SyncMode, Wal, WalConfig};
use std::time::Duration;
use tempfile::TempDir;

// ============================================================================
// Full Write Path Integration Tests (WAL → MemTable → TSM)
// ============================================================================

/// Tests the complete write path: WAL → MemTable → TSM file.
///
/// This test verifies:
/// 1. Data is first written to WAL (durability)
/// 2. Data is then inserted into MemTable
/// 3. MemTable is flushed to TSM file
/// 4. TSM file can be read back with correct data
#[test]
fn test_full_write_path_wal_memtable_tsm() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let tsm_dir = temp_dir.path().join("tsm");
    std::fs::create_dir_all(&tsm_dir).unwrap();

    // Create WAL with fsync for durability
    let wal_config = WalConfig {
        batch_size: 100,
        batch_timeout: Duration::from_millis(10),
        segment_size: 1024 * 1024,
        sync_mode: SyncMode::Fsync,
    };
    let mut wal = Wal::new(&wal_dir, wal_config).unwrap();

    // Create MemTable for a 1-hour partition
    let partition = TimePartition::new(0, Duration::from_secs(3600));
    let mut memtable = TimeSeriesMemTable::new(partition);

    // Write data points with WAL durability
    let labels = vec![
        ("host".to_string(), "server1".to_string()),
        ("env".to_string(), "prod".to_string()),
    ];

    let points: Vec<DataPoint> = (0..1000)
        .map(|i| {
            DataPoint::new(
                "cpu.usage",
                labels.clone(),
                i * 1_000_000, // Timestamps in nanoseconds
                50.0 + (i as f64 * 0.1).sin() * 10.0,
            )
        })
        .collect();

    // Insert with WAL (following durability contract: WAL first, then MemTable)
    let sequences = memtable.insert_batch_with_wal(&points, &mut wal).unwrap();
    assert_eq!(sequences.len(), 1000);

    // Verify MemTable state
    assert_eq!(memtable.point_count(), 1000);
    assert_eq!(memtable.series_count(), 1);

    // Get series ID for later verification
    let series_id = TimeSeriesMemTable::compute_series_id("cpu.usage", &labels);

    // Flush MemTable to TSM file
    let tsm_path = tsm_dir.join("test.skulk");
    let handle = memtable.flush(&tsm_path).unwrap();

    // Verify TSM file metadata
    assert_eq!(handle.header.series_count, 1);
    assert_eq!(handle.footer.total_point_count, 1000);

    // Read back from TSM file
    let reader = TsmReader::open(&tsm_path).unwrap();
    let read_points = reader.read_series(series_id).unwrap().unwrap();

    assert_eq!(read_points.len(), 1000);

    // Verify data integrity
    for (i, (ts, val)) in read_points.iter().enumerate() {
        assert_eq!(*ts, (i as i64) * 1_000_000);
        let expected_val = 50.0 + (i as f64 * 0.1).sin() * 10.0;
        assert!((val - expected_val).abs() < f64::EPSILON);
    }

    // Verify TSM file checksum
    assert!(reader.verify_file_checksum().unwrap());
    assert!(reader.verify_block_checksum(series_id).unwrap());
}

/// Tests writing multiple series through the full path.
#[test]
fn test_full_write_path_multiple_series() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let tsm_dir = temp_dir.path().join("tsm");
    std::fs::create_dir_all(&tsm_dir).unwrap();

    let wal_config = WalConfig {
        batch_size: 100,
        batch_timeout: Duration::from_millis(10),
        segment_size: 1024 * 1024,
        sync_mode: SyncMode::Fsync,
    };
    let mut wal = Wal::new(&wal_dir, wal_config).unwrap();

    let partition = TimePartition::new(0, Duration::from_secs(3600));
    let mut memtable = TimeSeriesMemTable::new(partition);

    // Create multiple series
    let series_configs = vec![
        (
            "cpu.usage",
            vec![("host".to_string(), "server1".to_string())],
        ),
        (
            "cpu.usage",
            vec![("host".to_string(), "server2".to_string())],
        ),
        (
            "memory.free",
            vec![("host".to_string(), "server1".to_string())],
        ),
        ("disk.io", vec![]),
    ];

    for (metric, labels) in &series_configs {
        for i in 0..100 {
            let point = DataPoint::new(*metric, labels.clone(), i * 1_000_000, (i as f64) * 1.5);
            memtable.insert_with_wal(&point, &mut wal).unwrap();
        }
    }

    assert_eq!(memtable.point_count(), 400);
    assert_eq!(memtable.series_count(), 4);

    // Flush to TSM
    let tsm_path = tsm_dir.join("multi_series.skulk");
    let handle = memtable.flush(&tsm_path).unwrap();

    assert_eq!(handle.header.series_count, 4);
    assert_eq!(handle.footer.total_point_count, 400);

    // Read back and verify each series
    let reader = TsmReader::open(&tsm_path).unwrap();

    for (metric, labels) in &series_configs {
        let series_id = TimeSeriesMemTable::compute_series_id(metric, labels);
        let read_points = reader.read_series(series_id).unwrap().unwrap();
        assert_eq!(read_points.len(), 100);
    }
}

// ============================================================================
// Crash Recovery Integration Tests
// ============================================================================

/// Tests crash recovery: data written to WAL should be recoverable.
///
/// Simulates a crash scenario where:
/// 1. Data is written to WAL and MemTable
/// 2. "Crash" occurs (drop WAL and MemTable without flush)
/// 3. Recovery reads WAL and reconstructs MemTable
/// 4. Recovered data matches original
#[test]
fn test_crash_recovery_from_wal() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    let labels = vec![("host".to_string(), "server1".to_string())];
    let original_points: Vec<(i64, f64)>;

    // Phase 1: Write data and "crash"
    {
        let wal_config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync, // Ensure durability
        };
        let mut wal = Wal::new(&wal_dir, wal_config).unwrap();

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        // Store original data for later comparison
        original_points = (0..100)
            .map(|i| (i * 1_000_000_i64, 42.0 + i as f64 * 0.5))
            .collect();

        for (ts, val) in &original_points {
            let point = DataPoint::new("cpu.usage", labels.clone(), *ts, *val);
            memtable.insert_with_wal(&point, &mut wal).unwrap();
        }

        assert_eq!(memtable.point_count(), 100);

        // "Crash" - drop everything without flushing to TSM
        // WAL is synced, so data should be recoverable
    }

    // Phase 2: Recovery
    {
        // Recover entries from WAL
        let recovered_entries = Wal::recover(&wal_dir).unwrap();
        assert_eq!(
            recovered_entries.len(),
            100,
            "Should recover all 100 entries from WAL"
        );

        // Reconstruct MemTable from recovered entries
        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut recovered_memtable = TimeSeriesMemTable::new(partition);

        // In a real implementation, we'd need series metadata from WAL or a separate store.
        // For this test, we can replay the entries directly.
        let series_id = TimeSeriesMemTable::compute_series_id("cpu.usage", &labels);

        for entry in &recovered_entries {
            assert_eq!(entry.series_id, series_id);

            // In real recovery, we'd reconstruct the DataPoint from stored metadata
            // Here we verify the entry data matches expectations
            let idx = (entry.timestamp / 1_000_000) as usize;
            let expected_val = 42.0 + idx as f64 * 0.5;
            assert!(
                (entry.value - expected_val).abs() < f64::EPSILON,
                "Recovered value mismatch at index {}: expected {}, got {}",
                idx,
                expected_val,
                entry.value
            );
        }

        // Replay into a new MemTable
        for entry in &recovered_entries {
            // Simulate inserting recovered data (without WAL since we're recovering)
            let point = DataPoint::new("cpu.usage", labels.clone(), entry.timestamp, entry.value);
            recovered_memtable.insert(&point).unwrap();
        }

        assert_eq!(recovered_memtable.point_count(), 100);

        // Verify data matches original
        let data = recovered_memtable.get_series(series_id).unwrap();
        for (ts, val) in &original_points {
            assert_eq!(data.get(ts), Some(val));
        }
    }
}

/// Tests that WAL entries survive across multiple reopens.
#[test]
fn test_wal_survives_multiple_reopens() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    // First session: write entries
    {
        let config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(&wal_dir, config).unwrap();

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        for i in 0..50 {
            let point = DataPoint::new("metric1", vec![], i * 1000, i as f64);
            memtable.insert_with_wal(&point, &mut wal).unwrap();
        }
    }

    // Second session: reopen and write more
    {
        let config = WalConfig {
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(&wal_dir, config).unwrap();

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        // WAL should continue from sequence 51
        for i in 50..100 {
            let point = DataPoint::new("metric1", vec![], i * 1000, i as f64);
            let seq = memtable.insert_with_wal(&point, &mut wal).unwrap();
            assert_eq!(
                seq,
                (i + 1) as u64,
                "Sequence should continue from previous session"
            );
        }
    }

    // Recovery: should see all 100 entries
    let recovered = Wal::recover(&wal_dir).unwrap();
    assert_eq!(recovered.len(), 100);

    for (i, entry) in recovered.iter().enumerate() {
        assert_eq!(entry.sequence, (i + 1) as u64);
        assert_eq!(entry.timestamp, (i as i64) * 1000);
        assert!((entry.value - i as f64).abs() < f64::EPSILON);
    }
}

/// Tests recovery after partial write (simulated by corrupting tail).
#[test]
fn test_recovery_with_partial_entries() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    // Write entries
    {
        let config = WalConfig {
            batch_size: 10,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(&wal_dir, config).unwrap();

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        for i in 0..20 {
            let point = DataPoint::new("metric", vec![], i * 1000, i as f64);
            memtable.insert_with_wal(&point, &mut wal).unwrap();
        }
    }

    // Corrupt the tail of the WAL segment (simulate partial write during crash)
    let segments: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "wal")
                .unwrap_or(false)
        })
        .collect();

    if let Some(segment) = segments.last() {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(segment.path())
            .unwrap();
        // Append garbage that looks like a partial entry
        file.write_all(&50u32.to_le_bytes()).unwrap(); // Invalid length
        file.write_all(&[0xDE, 0xAD]).unwrap(); // Truncated data
    }

    // Recovery should still get the valid entries
    let recovered = Wal::recover(&wal_dir).unwrap();
    assert_eq!(
        recovered.len(),
        20,
        "Should recover all valid entries before corruption"
    );
}

// ============================================================================
// MemTable → TSM Flush Integration Tests
// ============================================================================

/// Tests that MemTable flush creates a valid, readable TSM file.
#[test]
fn test_memtable_flush_creates_valid_tsm() {
    let temp_dir = TempDir::new().unwrap();
    let tsm_path = temp_dir.path().join("test.skulk");

    let partition = TimePartition::new(0, Duration::from_secs(3600));
    let mut memtable = TimeSeriesMemTable::new(partition);

    // Insert varied data patterns using Box<dyn Fn> for different closures
    type Pattern = (
        &'static str,
        Vec<(String, String)>,
        std::ops::Range<usize>,
        Box<dyn Fn(usize) -> f64>,
    );
    let patterns: Vec<Pattern> = vec![
        ("constant", vec![], 0..100, Box::new(|_: usize| 42.0)),
        (
            "increasing",
            vec![("type".to_string(), "linear".to_string())],
            0..100,
            Box::new(|i: usize| i as f64),
        ),
        (
            "oscillating",
            vec![("type".to_string(), "sine".to_string())],
            0..100,
            Box::new(|i: usize| (i as f64 * 0.1).sin() * 100.0),
        ),
    ];

    for (metric, labels, range, value_fn) in &patterns {
        for i in range.clone() {
            let point =
                DataPoint::new(*metric, labels.clone(), (i as i64) * 1_000_000, value_fn(i));
            memtable.insert(&point).unwrap();
        }
    }

    assert_eq!(memtable.series_count(), 3);
    assert_eq!(memtable.point_count(), 300);

    // Flush
    let handle = memtable.flush(&tsm_path).unwrap();
    assert_eq!(handle.header.series_count, 3);
    assert_eq!(handle.footer.total_point_count, 300);

    // Verify file structure
    let reader = TsmReader::open(&tsm_path).unwrap();
    assert!(reader.verify_file_checksum().unwrap());

    // Verify each series
    for (metric, labels, _range, value_fn) in &patterns {
        let series_id = TimeSeriesMemTable::compute_series_id(metric, labels);
        let points = reader.read_series(series_id).unwrap().unwrap();
        assert_eq!(points.len(), 100);

        for (i, (ts, val)) in points.iter().enumerate() {
            assert_eq!(*ts, (i as i64) * 1_000_000);
            let expected = value_fn(i);
            assert!(
                (val - expected).abs() < 1e-10,
                "Value mismatch at {}: expected {}, got {}",
                i,
                expected,
                val
            );
        }
    }
}

/// Tests flushing empty MemTable produces valid empty TSM file.
#[test]
fn test_flush_empty_memtable() {
    let temp_dir = TempDir::new().unwrap();
    let tsm_path = temp_dir.path().join("empty.skulk");

    let partition = TimePartition::new(0, Duration::from_secs(3600));
    let memtable = TimeSeriesMemTable::new(partition);

    let handle = memtable.flush(&tsm_path).unwrap();
    assert_eq!(handle.header.series_count, 0);
    assert_eq!(handle.footer.total_point_count, 0);

    // Empty file should be readable
    let reader = TsmReader::open(&tsm_path).unwrap();
    assert_eq!(reader.header().series_count, 0);
    assert!(reader.verify_file_checksum().unwrap());
}

/// Tests PartitionManager flush with atomic durability.
#[test]
#[cfg_attr(windows, ignore)] // Windows has different atomic rename semantics
fn test_partition_manager_atomic_flush() {
    let temp_dir = TempDir::new().unwrap();
    let tsm_dir = temp_dir.path().join("tsm");
    std::fs::create_dir_all(&tsm_dir).unwrap();

    let config = PartitionManagerConfig::default()
        .with_partition_duration(Duration::from_secs(3600))
        .with_max_active_partitions(2);

    let mut manager = PartitionManager::new(config);

    let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

    // Insert data into multiple partitions
    for partition_idx in 0..3 {
        for i in 0..100 {
            let point = DataPoint::new(
                "cpu.usage",
                vec![],
                partition_idx * duration_nanos + i * 1_000_000,
                (partition_idx * 100 + i) as f64,
            );
            manager.insert(&point).unwrap();
        }
    }

    assert_eq!(manager.partition_count(), 3);

    // Flush the oldest partition (should trigger due to count exceeding max)
    let to_flush = manager.partitions_to_flush();
    assert!(!to_flush.is_empty(), "Should have partitions to flush");

    let first_partition = to_flush[0];
    let result = manager.flush_partition(first_partition, &tsm_dir);
    assert!(result.is_ok());

    let handle = result.unwrap().unwrap();
    assert_eq!(handle.footer.total_point_count, 100);

    // Verify atomic file: final file exists, no temp file
    let expected_file = tsm_dir.join(format!("{}.skulk", first_partition));
    let temp_file = tsm_dir.join(format!("{}.skulk.tmp", first_partition));

    assert!(expected_file.exists(), "Final TSM file should exist");
    assert!(!temp_file.exists(), "Temp file should not remain");

    // Partition should be removed after successful flush
    assert_eq!(manager.partition_count(), 2);

    // Verify the flushed file is readable
    let reader = TsmReader::open(&expected_file).unwrap();
    assert!(reader.verify_file_checksum().unwrap());
}

/// Tests that flush failure preserves partition data.
#[test]
fn test_flush_failure_preserves_data() {
    let config =
        PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));

    let mut manager = PartitionManager::new(config);

    // Insert data
    let point = DataPoint::new("cpu.usage", vec![], 1000, 42.0);
    manager.insert(&point).unwrap();

    assert_eq!(manager.partition_count(), 1);
    assert_eq!(manager.stats().total_point_count, 1);

    // Try to flush to non-existent directory (should fail)
    let bad_path = std::path::Path::new("/nonexistent/directory/that/should/not/exist");
    let result = manager.flush_partition(0, bad_path);

    assert!(result.is_err(), "Flush to bad path should fail");

    // Critical: Data must be preserved after failed flush
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
    let range = TimeRange::new(0, 10000);
    let results: Vec<_> = manager.scan(range).collect();
    assert_eq!(results.len(), 1);
    assert!((results[0].value - 42.0).abs() < f64::EPSILON);
}

/// Tests writes are blocked during flush (immutable partition).
#[test]
fn test_writes_blocked_during_flush() {
    let config =
        PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));

    let mut manager = PartitionManager::new(config);

    // Insert initial data
    let point1 = DataPoint::new("cpu.usage", vec![], 1000, 42.0);
    manager.insert(&point1).unwrap();

    // Mark partition as flushing
    manager.mark_flushing(0);

    // Try to insert to the same partition - should fail
    let point2 = DataPoint::new("cpu.usage", vec![], 2000, 43.0);
    let result = manager.insert(&point2);
    assert!(result.is_err(), "Insert to flushing partition should fail");

    // Different partition should still work
    let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;
    let point3 = DataPoint::new("cpu.usage", vec![], duration_nanos + 1000, 44.0);
    assert!(
        manager.insert(&point3).is_ok(),
        "Insert to non-flushing partition should succeed"
    );

    // Unmark flushing - insert should work again
    manager.unmark_flushing(0);
    let point4 = DataPoint::new("cpu.usage", vec![], 3000, 45.0);
    assert!(
        manager.insert(&point4).is_ok(),
        "Insert should succeed after unmark_flushing"
    );
}

// ============================================================================
// End-to-End Scenarios
// ============================================================================

/// Tests a complete workflow: write, crash, recover, continue, flush.
#[test]
fn test_end_to_end_with_crash_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let tsm_dir = temp_dir.path().join("tsm");
    std::fs::create_dir_all(&tsm_dir).unwrap();

    let labels = vec![("host".to_string(), "prod-server".to_string())];
    let series_id = TimeSeriesMemTable::compute_series_id("requests.count", &labels);

    // Phase 1: Initial writes and "crash"
    {
        let wal_config = WalConfig {
            batch_size: 50,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(&wal_dir, wal_config).unwrap();

        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        for i in 0..200 {
            let point = DataPoint::new(
                "requests.count",
                labels.clone(),
                i * 1_000_000,
                (i * 10) as f64,
            );
            memtable.insert_with_wal(&point, &mut wal).unwrap();
        }

        // "Crash" before flush
    }

    // Phase 2: Recovery and continue writing
    let max_sequence_after_recovery;
    {
        // Recover from WAL
        let recovered_entries = Wal::recover(&wal_dir).unwrap();
        assert_eq!(recovered_entries.len(), 200);

        // Reopen WAL for new writes
        let wal_config = WalConfig {
            batch_size: 50,
            batch_timeout: Duration::from_millis(10),
            segment_size: 1024 * 1024,
            sync_mode: SyncMode::Fsync,
        };
        let mut wal = Wal::new(&wal_dir, wal_config).unwrap();

        // New memtable with recovered + new data
        let partition = TimePartition::new(0, Duration::from_secs(3600));
        let mut memtable = TimeSeriesMemTable::new(partition);

        // Replay recovered entries
        for entry in &recovered_entries {
            let point = DataPoint::new(
                "requests.count",
                labels.clone(),
                entry.timestamp,
                entry.value,
            );
            memtable.insert(&point).unwrap();
        }

        // Continue writing new data
        for i in 200..300 {
            let point = DataPoint::new(
                "requests.count",
                labels.clone(),
                i * 1_000_000,
                (i * 10) as f64,
            );
            memtable.insert_with_wal(&point, &mut wal).unwrap();
        }

        assert_eq!(memtable.point_count(), 300);

        max_sequence_after_recovery = wal.current_sequence() - 1;

        // Flush to TSM
        let tsm_path = tsm_dir.join("final.skulk");
        let handle = memtable.flush(&tsm_path).unwrap();
        assert_eq!(handle.footer.total_point_count, 300);

        // After successful flush, truncate WAL
        wal.truncate(max_sequence_after_recovery).unwrap();
    }

    // Phase 3: Verify final state
    {
        let tsm_path = tsm_dir.join("final.skulk");
        let reader = TsmReader::open(&tsm_path).unwrap();

        let points = reader.read_series(series_id).unwrap().unwrap();
        assert_eq!(points.len(), 300);

        for (i, (ts, val)) in points.iter().enumerate() {
            assert_eq!(*ts, (i as i64) * 1_000_000);
            assert!((val - (i * 10) as f64).abs() < f64::EPSILON);
        }
    }
}

/// Tests TSM file time range scanning after flush.
#[test]
fn test_tsm_time_range_scan() {
    let temp_dir = TempDir::new().unwrap();
    let tsm_path = temp_dir.path().join("scan_test.skulk");

    let partition = TimePartition::new(0, Duration::from_secs(3600));
    let mut memtable = TimeSeriesMemTable::new(partition);

    // Insert 1000 points
    for i in 0..1000 {
        let point = DataPoint::new("metric", vec![], i * 1_000_000, i as f64);
        memtable.insert(&point).unwrap();
    }

    memtable.flush(&tsm_path).unwrap();

    let reader = TsmReader::open(&tsm_path).unwrap();

    // Test various scan ranges
    let test_cases = vec![
        (0, 100 * 1_000_000, 100),                // First 100 points
        (500 * 1_000_000, 600 * 1_000_000, 100),  // Middle 100 points
        (900 * 1_000_000, 1100 * 1_000_000, 100), // Last 100 points (including beyond range)
        (0, 1000 * 1_000_000, 1000),              // All points
        (1000 * 1_000_000, 2000 * 1_000_000, 0),  // Beyond range (empty)
        (-100, 0, 0),                             // Before range (empty)
    ];

    for (start, end, expected_count) in test_cases {
        let range = TimeRange::new(start, end);
        let results: Vec<_> = reader.scan(range).collect();
        assert_eq!(
            results.len(),
            expected_count,
            "Range [{}, {}) should return {} points, got {}",
            start,
            end,
            expected_count,
            results.len()
        );

        // Verify all returned points are within range
        for point in &results {
            assert!(
                point.timestamp >= start && point.timestamp < end,
                "Point timestamp {} should be in range [{}, {})",
                point.timestamp,
                start,
                end
            );
        }
    }
}

/// Tests cross-partition scan in PartitionManager.
#[test]
fn test_partition_manager_cross_partition_scan() {
    let config =
        PartitionManagerConfig::default().with_partition_duration(Duration::from_secs(3600));

    let mut manager = PartitionManager::new(config);

    let duration_nanos = Duration::from_secs(3600).as_nanos() as i64;

    // Insert data across 3 partitions
    for partition_idx in 0..3 {
        for i in 0..100 {
            let point = DataPoint::new(
                "cpu.usage",
                vec![],
                partition_idx * duration_nanos + i * 1_000_000,
                (partition_idx * 100 + i) as f64,
            );
            manager.insert(&point).unwrap();
        }
    }

    // Scan across all partitions
    let range = TimeRange::new(0, 3 * duration_nanos);
    let results: Vec<_> = manager.scan(range).collect();
    assert_eq!(results.len(), 300);

    // Scan only first partition
    let range = TimeRange::new(0, duration_nanos);
    let results: Vec<_> = manager.scan(range).collect();
    assert_eq!(results.len(), 100);

    // Scan spanning two partitions
    let range = TimeRange::new(duration_nanos / 2, duration_nanos + duration_nanos / 2);
    let results: Vec<_> = manager.scan(range).collect();
    // Should include points from both partitions within the range
    assert!(!results.is_empty());
    for point in &results {
        assert!(
            point.timestamp >= duration_nanos / 2
                && point.timestamp < duration_nanos + duration_nanos / 2
        );
    }
}
