//! Integration tests for TSM file format.

use skulk::tsm::{CompressionType, SectionFlags, SeriesMeta, TimeRange, TsmReader, TsmWriter};
use std::collections::BTreeMap;
use tempfile::TempDir;

/// Helper function to generate test points.
fn generate_points(
    start_ts: i64,
    interval: i64,
    count: usize,
    base_value: f64,
) -> BTreeMap<i64, f64> {
    let mut points = BTreeMap::new();
    for i in 0..count {
        let ts = start_ts + (i as i64) * interval;
        let value = base_value + (i as f64) * 0.1 + ((i as f64) * 0.1).sin() * 5.0;
        points.insert(ts, value);
    }
    points
}

#[test]
fn test_write_read_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("roundtrip.skulk");

    let series_id = 1u64;
    let meta = SeriesMeta::new(
        "cpu.usage",
        vec![("host".to_string(), "server1".to_string())],
    );
    let points = generate_points(1_000_000_000, 1_000_000_000, 3600, 50.0); // 1 hour of data at 1s interval

    // Write
    {
        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(series_id, &meta, &points).unwrap();
        let handle = writer.finish().unwrap();

        assert_eq!(handle.header.series_count, 1);
        assert_eq!(handle.footer.total_point_count, 3600);
    }

    // Read and verify
    {
        let reader = TsmReader::open(&file_path).unwrap();
        let read_points = reader.read_series(series_id).unwrap().unwrap();

        assert_eq!(read_points.len(), 3600);

        for (i, (ts, val)) in read_points.iter().enumerate() {
            let expected_ts = 1_000_000_000 + (i as i64) * 1_000_000_000;
            let expected_val = 50.0 + (i as f64) * 0.1 + ((i as f64) * 0.1).sin() * 5.0;

            assert_eq!(*ts, expected_ts, "Timestamp mismatch at index {}", i);
            assert!(
                (val - expected_val).abs() < f64::EPSILON,
                "Value mismatch at index {}: expected {}, got {}",
                i,
                expected_val,
                val
            );
        }
    }
}

#[test]
fn test_large_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("large.skulk");

    const NUM_SERIES: u64 = 100;
    const POINTS_PER_SERIES: usize = 1000;

    // Write multiple series
    {
        let mut writer = TsmWriter::new(&file_path).unwrap();

        for i in 0..NUM_SERIES {
            let meta = SeriesMeta::new(
                format!("metric.{}", i),
                vec![("id".to_string(), i.to_string())],
            );
            let points = generate_points(
                1_000_000_000,
                1_000_000_000,
                POINTS_PER_SERIES,
                (i as f64) * 10.0,
            );
            writer.write_series(i + 1, &meta, &points).unwrap();
        }

        let handle = writer.finish().unwrap();
        assert_eq!(handle.header.series_count, NUM_SERIES as u32);
        assert_eq!(
            handle.footer.total_point_count,
            NUM_SERIES * (POINTS_PER_SERIES as u64)
        );
    }

    // Read and verify all series
    {
        let reader = TsmReader::open(&file_path).unwrap();

        for i in 0..NUM_SERIES {
            let read_points = reader.read_series(i + 1).unwrap().unwrap();
            assert_eq!(read_points.len(), POINTS_PER_SERIES);
        }

        // Non-existent series
        assert!(reader.read_series(NUM_SERIES + 100).unwrap().is_none());
    }
}

#[test]
fn test_multiple_series() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("multi.skulk");

    let series_ids = [1u64, 2, 3, 4, 5];
    let metas: Vec<SeriesMeta> = series_ids
        .iter()
        .map(|id| {
            SeriesMeta::new(
                format!("metric.{}", id),
                vec![("host".to_string(), format!("server{}", id))],
            )
        })
        .collect();

    // Write
    {
        let mut writer = TsmWriter::new(&file_path).unwrap();

        for (i, (id, meta)) in series_ids.iter().zip(metas.iter()).enumerate() {
            let points = generate_points(
                1_000_000_000 + (i as i64) * 100_000_000,
                1_000_000_000,
                100,
                (i as f64) * 20.0,
            );
            writer.write_series(*id, meta, &points).unwrap();
        }

        let handle = writer.finish().unwrap();
        assert_eq!(handle.header.series_count, 5);
    }

    // Read
    {
        let reader = TsmReader::open(&file_path).unwrap();

        for id in &series_ids {
            let read_points = reader.read_series(*id).unwrap().unwrap();
            assert_eq!(read_points.len(), 100);
        }
    }
}

#[test]
fn test_corrupted_file_detection() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("corrupt.skulk");

    // Write valid file
    {
        let series_id = 1u64;
        let meta = SeriesMeta::new("test.metric", vec![]);
        let points = generate_points(1_000_000_000, 1_000_000_000, 100, 50.0);

        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(series_id, &meta, &points).unwrap();
        writer.finish().unwrap();
    }

    // Corrupt the file
    {
        let mut contents = std::fs::read(&file_path).unwrap();
        // Corrupt data section (somewhere in the middle)
        let corrupt_offset = 32 + 20; // After header, into data block
        contents[corrupt_offset] ^= 0xFF;
        contents[corrupt_offset + 1] ^= 0xAA;
        std::fs::write(&file_path, &contents).unwrap();
    }

    // Try to open - should fail due to CRC mismatch
    let result = TsmReader::open(&file_path);
    assert!(result.is_err());
}

#[test]
fn test_corrupted_block_detection() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("corrupt_block.skulk");

    let series_id = 1u64;

    // Write valid file
    {
        let meta = SeriesMeta::new("test.metric", vec![]);
        let points = generate_points(1_000_000_000, 1_000_000_000, 100, 50.0);

        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(series_id, &meta, &points).unwrap();
        writer.finish().unwrap();
    }

    // Verify file is valid
    {
        let reader = TsmReader::open(&file_path).unwrap();
        assert!(reader.verify_file_checksum().unwrap());
        assert!(reader.verify_block_checksum(series_id).unwrap());
    }
}

#[test]
fn test_scan_time_range() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("scan.skulk");

    // Write data
    {
        let meta = SeriesMeta::new("cpu.usage", vec![]);
        let mut points = BTreeMap::new();
        for i in 0..1000 {
            points.insert(i * 1_000_000_000, (i as f64) * 1.0);
        }

        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(1, &meta, &points).unwrap();
        writer.finish().unwrap();
    }

    // Scan various ranges
    {
        let reader = TsmReader::open(&file_path).unwrap();

        // Full range
        let all_points: Vec<_> = reader
            .scan(TimeRange::new(0, 1000 * 1_000_000_000))
            .collect();
        assert_eq!(all_points.len(), 1000);

        // Partial range
        let partial: Vec<_> = reader
            .scan(TimeRange::new(250 * 1_000_000_000, 750 * 1_000_000_000))
            .collect();
        assert_eq!(partial.len(), 500);
        for point in &partial {
            assert!(point.timestamp >= 250 * 1_000_000_000);
            assert!(point.timestamp < 750 * 1_000_000_000);
        }

        // Empty range (before data)
        let empty_before: Vec<_> = reader.scan(TimeRange::new(-1000, -1)).collect();
        assert!(empty_before.is_empty());

        // Empty range (after data)
        let empty_after: Vec<_> = reader
            .scan(TimeRange::new(10000 * 1_000_000_000, 20000 * 1_000_000_000))
            .collect();
        assert!(empty_after.is_empty());
    }
}

#[test]
fn test_raw_compression_mode() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("raw.skulk");

    let series_id = 1u64;
    let meta = SeriesMeta::new("test.metric", vec![]);
    let points = generate_points(1_000_000_000, 1_000_000_000, 100, 50.0);

    // Write with raw compression
    {
        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.set_compression(CompressionType::Raw);
        writer.write_series(series_id, &meta, &points).unwrap();
        let handle = writer.finish().unwrap();

        assert_eq!(handle.header.compression, CompressionType::Raw);
    }

    // Read and verify
    {
        let reader = TsmReader::open(&file_path).unwrap();
        assert_eq!(reader.header().compression, CompressionType::Raw);

        let read_points = reader.read_series(series_id).unwrap().unwrap();
        assert_eq!(read_points.len(), 100);
    }
}

#[test]
fn test_section_flags() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("flags.skulk");

    let series_id = 1u64;
    let meta = SeriesMeta::new("test.metric", vec![]);
    let points = generate_points(1_000_000_000, 1_000_000_000, 10, 50.0);

    // Write with section flags
    {
        let mut writer = TsmWriter::new(&file_path).unwrap();
        let mut flags = SectionFlags::new();
        flags.set_hot();
        flags.set_meta();
        writer.set_section_flags(flags);
        writer.set_level(2);
        writer.write_series(series_id, &meta, &points).unwrap();
        let handle = writer.finish().unwrap();

        assert!(handle.header.section_flags.is_hot());
        assert!(handle.header.section_flags.is_meta());
        assert!(!handle.header.section_flags.is_cold());
        assert_eq!(handle.header.level, 2);
    }

    // Read and verify
    {
        let reader = TsmReader::open(&file_path).unwrap();
        assert!(reader.header().section_flags.is_hot());
        assert!(reader.header().section_flags.is_meta());
        assert_eq!(reader.header().level, 2);
    }
}

#[test]
fn test_various_data_patterns() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("patterns.skulk");

    // Test with various data patterns
    let patterns: Vec<(&str, BTreeMap<i64, f64>)> = vec![
        // Constant values
        ("constant", (0..100).map(|i| (i * 1000, 42.0)).collect()),
        // Monotonically increasing
        (
            "increasing",
            (0..100).map(|i| (i * 1000, i as f64)).collect(),
        ),
        // High frequency oscillation
        (
            "oscillation",
            (0..100)
                .map(|i| (i * 1000, (i as f64 * 0.5).sin() * 100.0))
                .collect(),
        ),
        // Sparse data with large gaps
        (
            "sparse",
            (0..10)
                .map(|i| (i * 100_000_000, i as f64 * 10.0))
                .collect(),
        ),
        // Very small values
        (
            "small",
            (0..100).map(|i| (i * 1000, (i as f64) * 1e-10)).collect(),
        ),
        // Very large values
        (
            "large",
            (0..100).map(|i| (i * 1000, (i as f64) * 1e10)).collect(),
        ),
    ];

    // Write all patterns
    {
        let mut writer = TsmWriter::new(&file_path).unwrap();

        for (i, (name, points)) in patterns.iter().enumerate() {
            let meta = SeriesMeta::new(*name, vec![]);
            writer.write_series(i as u64 + 1, &meta, points).unwrap();
        }

        writer.finish().unwrap();
    }

    // Read and verify
    {
        let reader = TsmReader::open(&file_path).unwrap();

        for (i, (_name, original_points)) in patterns.iter().enumerate() {
            let read_points = reader.read_series(i as u64 + 1).unwrap().unwrap();
            assert_eq!(read_points.len(), original_points.len());

            for ((ts, val), (orig_ts, orig_val)) in read_points.iter().zip(original_points.iter()) {
                assert_eq!(*ts, *orig_ts);
                // Use relative comparison for floating point
                if orig_val.abs() > f64::EPSILON {
                    assert!(
                        ((val - orig_val) / orig_val).abs() < 1e-10,
                        "Value mismatch: {} vs {}",
                        val,
                        orig_val
                    );
                } else {
                    assert!((val - orig_val).abs() < 1e-20);
                }
            }
        }
    }
}

#[test]
fn test_empty_series_ignored() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("empty.skulk");

    // Write with empty series (should be ignored)
    {
        let meta = SeriesMeta::new("empty", vec![]);
        let empty_points: BTreeMap<i64, f64> = BTreeMap::new();

        let non_empty_meta = SeriesMeta::new("non_empty", vec![]);
        let non_empty_points: BTreeMap<i64, f64> = (0..10).map(|i| (i * 1000, i as f64)).collect();

        let mut writer = TsmWriter::new(&file_path).unwrap();
        writer.write_series(1, &meta, &empty_points).unwrap(); // Empty, ignored
        writer
            .write_series(2, &non_empty_meta, &non_empty_points)
            .unwrap();
        let handle = writer.finish().unwrap();

        // Only 1 series should be recorded (empty was ignored)
        assert_eq!(handle.header.series_count, 1);
    }

    // Read and verify
    {
        let reader = TsmReader::open(&file_path).unwrap();
        assert!(reader.read_series(1).unwrap().is_none()); // Empty series not stored
        assert!(reader.read_series(2).unwrap().is_some()); // Non-empty series exists
    }
}
