//! Property-based tests for Gorilla compression.
//!
//! Uses proptest to verify lossless round-trip compression for arbitrary data.
//!
//! Note: The Gorilla algorithm uses 32-bit delta encoding, so timestamps with
//! deltas exceeding i32::MAX/MIN will not roundtrip correctly. These tests
//! constrain inputs to realistic time series data that stays within bounds.

use proptest::prelude::*;
use skulk::tsm::CompressedBlock;

/// Strategy for generating realistic timestamps with bounded deltas.
/// Timestamps are sorted and have deltas that fit within 32-bit encoding.
fn timestamp_strategy() -> impl Strategy<Value = Vec<i64>> {
    // Start from a base timestamp, then add bounded deltas
    (
        0i64..1_000_000_000_000i64,                         // base timestamp
        prop::collection::vec(1i64..1_000_000_000, 1..100), // deltas (up to 1 second)
    )
        .prop_map(|(base, deltas)| {
            let mut timestamps = vec![base];
            let mut current = base;
            for delta in deltas {
                current = current.saturating_add(delta);
                timestamps.push(current);
            }
            timestamps
        })
}

/// Strategy for generating realistic float values (excluding special values).
fn value_strategy() -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(-1_000_000.0f64..1_000_000.0, 1..100)
}

/// Strategy for generating typical time series data.
/// Regular intervals with slowly varying values.
fn typical_timeseries_strategy() -> impl Strategy<Value = Vec<(i64, f64)>> {
    (1i64..1000, 1..100usize).prop_flat_map(|(interval, count)| {
        let start_ts = 1_000_000_000_000i64;
        prop::collection::vec(-1000.0f64..1000.0, count).prop_map(move |values| {
            values
                .into_iter()
                .enumerate()
                .map(|(i, v)| (start_ts + (i as i64) * interval, v))
                .collect()
        })
    })
}

proptest! {
    /// Test that compression and decompression is lossless for bounded timestamps.
    #[test]
    fn test_timestamp_roundtrip_proptest(timestamps in timestamp_strategy()) {
        if timestamps.is_empty() {
            return Ok(());
        }

        // Create points with bounded-delta timestamps and constant value
        let points: Vec<(i64, f64)> = timestamps.iter().map(|&ts| (ts, 1.0)).collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        prop_assert_eq!(points.len(), decompressed.len());

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            prop_assert_eq!(original.0, decoded.0, "Timestamp mismatch");
        }
    }

    /// Test that compression and decompression is lossless for arbitrary values.
    #[test]
    fn test_value_roundtrip_proptest(values in value_strategy()) {
        if values.is_empty() {
            return Ok(());
        }

        // Create points with regular timestamps and arbitrary values
        let points: Vec<(i64, f64)> = values
            .iter()
            .enumerate()
            .map(|(i, &v)| (1000 + i as i64 * 1000, v))
            .collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        prop_assert_eq!(points.len(), decompressed.len());

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            prop_assert!(
                (original.1 - decoded.1).abs() < f64::EPSILON,
                "Value mismatch: {} vs {}",
                original.1,
                decoded.1
            );
        }
    }

    /// Test that compression and decompression is lossless for typical time series.
    #[test]
    fn test_typical_timeseries_roundtrip(points in typical_timeseries_strategy()) {
        if points.is_empty() {
            return Ok(());
        }

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        prop_assert_eq!(points.len(), decompressed.len());

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            prop_assert_eq!(original.0, decoded.0, "Timestamp mismatch");
            prop_assert!(
                (original.1 - decoded.1).abs() < f64::EPSILON,
                "Value mismatch: {} vs {}",
                original.1,
                decoded.1
            );
        }
    }

    /// Test compression ratio for regular interval data.
    /// Note: Small datasets have higher overhead and lower ratios.
    #[test]
    fn test_compression_ratio_regular_intervals(count in 50usize..500) {
        let interval = 1_000_000_000i64; // 1 second in nanos
        let points: Vec<(i64, f64)> = (0..count)
            .map(|i| {
                let ts = 1_000_000_000_000i64 + i as i64 * interval;
                let value = 50.0 + (i as f64 * 0.1).sin() * 10.0;
                (ts, value)
            })
            .collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        // Verify lossless
        prop_assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            prop_assert_eq!(original.0, decoded.0);
            prop_assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }

        // Verify compression - expect some compression with varying values
        let raw_size = count * std::mem::size_of::<(i64, f64)>();
        let compressed_size = (block.timestamps.len() + block.values.len()).div_ceil(8);
        let ratio = raw_size as f64 / compressed_size as f64;

        // Regular intervals with varying values should still achieve some compression
        prop_assert!(
            ratio > 1.5,
            "Expected compression ratio >1.5:1, got {:.2}:1",
            ratio
        );
    }

    /// Test that identical consecutive values compress efficiently.
    #[test]
    fn test_identical_values_compression(value in -1000.0f64..1000.0, count in 10usize..100) {
        let points: Vec<(i64, f64)> = (0..count)
            .map(|i| (1000 + i as i64 * 1000, value))
            .collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        // Verify lossless
        prop_assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            prop_assert_eq!(original.0, decoded.0);
            prop_assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }

        // Identical values should compress very efficiently
        // After first value (64 bits), each subsequent should use only 1 bit
        let expected_value_bits = 64 + (count - 1);
        prop_assert!(
            block.values.len() <= expected_value_bits + 10,
            "Expected ~{} bits for values, got {}",
            expected_value_bits,
            block.values.len()
        );
    }

    /// Test edge case: two-point series with bounded delta.
    #[test]
    fn test_two_point_series(
        ts1 in 0i64..1_000_000_000_000i64,
        delta in 1i64..1_000_000_000i64, // Bounded delta to fit in 32-bit encoding
        v1 in -1_000_000.0f64..1_000_000.0,
        v2 in -1_000_000.0f64..1_000_000.0,
    ) {
        let ts2 = ts1.saturating_add(delta);
        let points = vec![(ts1, v1), (ts2, v2)];

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        prop_assert_eq!(2, decompressed.len());
        prop_assert_eq!(ts1, decompressed[0].0);
        prop_assert_eq!(ts2, decompressed[1].0);
        prop_assert!((v1 - decompressed[0].1).abs() < f64::EPSILON);
        prop_assert!((v2 - decompressed[1].1).abs() < f64::EPSILON);
    }

    /// Test varying delta patterns within bounds.
    #[test]
    fn test_varying_deltas(
        base_ts in 0i64..1_000_000_000_000i64,
        deltas in prop::collection::vec(1i64..10000, 2..50),
    ) {
        let mut timestamps = vec![base_ts];
        let mut current = base_ts;
        for delta in deltas {
            current = current.saturating_add(delta);
            timestamps.push(current);
        }

        let points: Vec<(i64, f64)> = timestamps.iter().map(|&ts| (ts, 1.0)).collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        prop_assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            prop_assert_eq!(original.0, decoded.0);
        }
    }
}

#[cfg(test)]
mod additional_tests {
    use super::*;

    /// Test with a large number of points.
    #[test]
    fn test_large_dataset() {
        let count = 10_000;
        let interval = 1_000_000_000i64;

        let points: Vec<(i64, f64)> = (0..count)
            .map(|i| {
                let ts = 1_000_000_000_000i64 + i as i64 * interval;
                let value = 100.0 * (i as f64 / 1000.0).sin();
                (ts, value)
            })
            .collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }

        // Verify compression ratio
        // With varying sine values, expect modest compression (timestamps compress well, values less so)
        let raw_size = count * std::mem::size_of::<(i64, f64)>();
        let compressed_size = (block.timestamps.len() + block.values.len()).div_ceil(8);
        let ratio = raw_size as f64 / compressed_size as f64;

        println!("Compression ratio for {} points: {:.2}:1", count, ratio);
        assert!(
            ratio > 1.5,
            "Expected compression ratio >1.5:1, got {:.2}:1",
            ratio
        );
    }

    /// Test with a large number of points and constant values (best-case compression).
    #[test]
    fn test_large_dataset_constant_values() {
        let count = 10_000;
        let interval = 1_000_000_000i64;

        let points: Vec<(i64, f64)> = (0..count)
            .map(|i| {
                let ts = 1_000_000_000_000i64 + i as i64 * interval;
                (ts, 42.0) // Constant value
            })
            .collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }

        // With constant values and regular intervals, expect excellent compression
        let raw_size = count * std::mem::size_of::<(i64, f64)>();
        let compressed_size = (block.timestamps.len() + block.values.len()).div_ceil(8);
        let ratio = raw_size as f64 / compressed_size as f64;

        println!(
            "Compression ratio for {} points (constant): {:.2}:1",
            count, ratio
        );
        assert!(
            ratio > 10.0,
            "Expected compression ratio >10:1 for constant values, got {:.2}:1",
            ratio
        );
    }

    /// Test with all zeros.
    #[test]
    fn test_all_zeros() {
        let points: Vec<(i64, f64)> = (0..100).map(|i| (i * 1000, 0.0)).collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert_eq!(original.1, decoded.1);
        }
    }

    /// Test monotonically increasing values.
    #[test]
    fn test_monotonic_increase() {
        let points: Vec<(i64, f64)> = (0..100).map(|i| (i * 1000, i as f64 * 0.1)).collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }
    }

    /// Test with microsecond intervals.
    #[test]
    fn test_microsecond_intervals() {
        let points: Vec<(i64, f64)> = (0..100)
            .map(|i| (1_000_000_000_000i64 + i * 1000, 42.0)) // 1 microsecond intervals
            .collect();

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        assert_eq!(points.len(), decompressed.len());
        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }
    }
}
