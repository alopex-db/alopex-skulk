//! Gorilla compression for time series data.
//!
//! This module implements Facebook's Gorilla compression algorithm optimized for
//! time series data, achieving 10:1+ compression ratios for typical workloads.
//!
//! # Algorithm Overview
//!
//! ## Timestamp Encoding (Delta-of-Delta)
//!
//! Timestamps are encoded using delta-of-delta encoding:
//! - First value: 64 bits raw
//! - Subsequent values use variable-length encoding based on delta-of-delta:
//!   - `0`: `'0'` (1 bit)
//!   - `[-63, 64]`: `'10'` + 7 bits
//!   - `[-255, 256]`: `'110'` + 9 bits
//!   - `[-2047, 2048]`: `'1110'` + 12 bits
//!   - else: `'1111'` + 32 bits
//!
//! ## Value Encoding (XOR-based)
//!
//! Float values are encoded using XOR with the previous value:
//! - First value: 64 bits raw (IEEE 754)
//! - Subsequent values:
//!   - XOR = 0: `'0'` (1 bit)
//!   - Same window: `'10'` + meaningful bits
//!   - New window: `'11'` + 5 bits leading + 6 bits length + meaningful bits

use bitvec::prelude::*;

/// Compressed block containing Gorilla-encoded timestamps and values.
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    /// Compressed timestamps using delta-of-delta encoding.
    pub timestamps: BitVec<u8, Msb0>,
    /// Compressed values using XOR encoding.
    pub values: BitVec<u8, Msb0>,
    /// Number of data points in the block.
    pub count: u32,
}

impl CompressedBlock {
    /// Compresses a sequence of (timestamp, value) pairs into a block.
    ///
    /// # Arguments
    ///
    /// * `points` - Slice of (timestamp, value) pairs to compress
    ///
    /// # Returns
    ///
    /// A compressed block containing all the points.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let points = vec![(1000, 1.0), (1001, 1.1), (1002, 1.2)];
    /// let block = CompressedBlock::compress(&points);
    /// assert_eq!(block.count, 3);
    /// ```
    pub fn compress(points: &[(i64, f64)]) -> Self {
        let mut ts_output = BitVec::<u8, Msb0>::new();
        let mut val_output = BitVec::<u8, Msb0>::new();

        let mut ts_encoder = TimestampEncoder::new();
        let mut val_encoder = ValueEncoder::new();

        for &(ts, val) in points {
            ts_encoder.encode(ts, &mut ts_output);
            val_encoder.encode(val, &mut val_output);
        }

        Self {
            timestamps: ts_output,
            values: val_output,
            count: points.len() as u32,
        }
    }

    /// Decompresses the block back to a sequence of (timestamp, value) pairs.
    ///
    /// # Returns
    ///
    /// A vector of (timestamp, value) pairs.
    pub fn decompress(&self) -> Vec<(i64, f64)> {
        let mut ts_decoder = TimestampDecoder::new(&self.timestamps);
        let mut val_decoder = ValueDecoder::new(&self.values);

        let mut result = Vec::with_capacity(self.count as usize);

        for _ in 0..self.count {
            let ts = ts_decoder.decode_next().expect("timestamp decode failed");
            let val = val_decoder.decode_next().expect("value decode failed");
            result.push((ts, val));
        }

        result
    }
}

/// Encoder for timestamps using delta-of-delta encoding.
pub struct TimestampEncoder {
    first_ts: Option<i64>,
    prev_ts: i64,
    prev_delta: i64,
}

impl TimestampEncoder {
    /// Creates a new timestamp encoder.
    pub fn new() -> Self {
        Self {
            first_ts: None,
            prev_ts: 0,
            prev_delta: 0,
        }
    }

    /// Encodes a timestamp into the output bit vector.
    pub fn encode(&mut self, timestamp: i64, output: &mut BitVec<u8, Msb0>) {
        if self.first_ts.is_none() {
            // First timestamp: write 64 bits raw
            self.first_ts = Some(timestamp);
            self.prev_ts = timestamp;
            self.prev_delta = 0;
            for i in (0..64).rev() {
                output.push((timestamp >> i) & 1 == 1);
            }
            return;
        }

        let delta = timestamp - self.prev_ts;
        let delta_of_delta = delta - self.prev_delta;

        if delta_of_delta == 0 {
            // Case 1: delta-of-delta is 0 -> 1 bit '0'
            output.push(false);
        } else if (-63..=64).contains(&delta_of_delta) {
            // Case 2: [-63, 64] -> '10' + 7 bits
            output.push(true);
            output.push(false);
            let encoded = (delta_of_delta + 63) as u8; // shift to unsigned
            for i in (0..7).rev() {
                output.push((encoded >> i) & 1 == 1);
            }
        } else if (-255..=256).contains(&delta_of_delta) {
            // Case 3: [-255, 256] -> '110' + 9 bits
            output.push(true);
            output.push(true);
            output.push(false);
            let encoded = (delta_of_delta + 255) as u16;
            for i in (0..9).rev() {
                output.push((encoded >> i) & 1 == 1);
            }
        } else if (-2047..=2048).contains(&delta_of_delta) {
            // Case 4: [-2047, 2048] -> '1110' + 12 bits
            output.push(true);
            output.push(true);
            output.push(true);
            output.push(false);
            let encoded = (delta_of_delta + 2047) as u16;
            for i in (0..12).rev() {
                output.push((encoded >> i) & 1 == 1);
            }
        } else {
            // Case 5: else -> '1111' + 32 bits
            output.push(true);
            output.push(true);
            output.push(true);
            output.push(true);
            let encoded = delta_of_delta as i32;
            for i in (0..32).rev() {
                output.push((encoded >> i) & 1 == 1);
            }
        }

        self.prev_delta = delta;
        self.prev_ts = timestamp;
    }
}

impl Default for TimestampEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoder for delta-of-delta encoded timestamps.
pub struct TimestampDecoder<'a> {
    data: &'a BitVec<u8, Msb0>,
    pos: usize,
    first_ts: Option<i64>,
    prev_ts: i64,
    prev_delta: i64,
}

impl<'a> TimestampDecoder<'a> {
    /// Creates a new timestamp decoder.
    pub fn new(data: &'a BitVec<u8, Msb0>) -> Self {
        Self {
            data,
            pos: 0,
            first_ts: None,
            prev_ts: 0,
            prev_delta: 0,
        }
    }

    /// Decodes the next timestamp from the bit stream.
    pub fn decode_next(&mut self) -> Option<i64> {
        if self.pos >= self.data.len() {
            return None;
        }

        if self.first_ts.is_none() {
            // First timestamp: read 64 bits
            if self.pos + 64 > self.data.len() {
                return None;
            }
            let mut ts: i64 = 0;
            for _ in 0..64 {
                ts = (ts << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            self.first_ts = Some(ts);
            self.prev_ts = ts;
            self.prev_delta = 0;
            return Some(ts);
        }

        // Read prefix bits to determine encoding
        let delta_of_delta = if !self.data[self.pos] {
            // Case 1: '0' -> delta-of-delta is 0
            self.pos += 1;
            0
        } else if !self.data[self.pos + 1] {
            // Case 2: '10' + 7 bits
            self.pos += 2;
            let mut encoded: i64 = 0;
            for _ in 0..7 {
                encoded = (encoded << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            encoded - 63
        } else if !self.data[self.pos + 2] {
            // Case 3: '110' + 9 bits
            self.pos += 3;
            let mut encoded: i64 = 0;
            for _ in 0..9 {
                encoded = (encoded << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            encoded - 255
        } else if !self.data[self.pos + 3] {
            // Case 4: '1110' + 12 bits
            self.pos += 4;
            let mut encoded: i64 = 0;
            for _ in 0..12 {
                encoded = (encoded << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            encoded - 2047
        } else {
            // Case 5: '1111' + 32 bits
            self.pos += 4;
            let mut encoded: i32 = 0;
            for _ in 0..32 {
                encoded = (encoded << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            encoded as i64
        };

        let delta = self.prev_delta + delta_of_delta;
        let ts = self.prev_ts + delta;
        self.prev_delta = delta;
        self.prev_ts = ts;

        Some(ts)
    }
}

/// Encoder for float values using XOR compression.
pub struct ValueEncoder {
    first_value: Option<u64>,
    prev_value: u64,
    prev_leading: u32,
    prev_trailing: u32,
}

impl ValueEncoder {
    /// Creates a new value encoder.
    pub fn new() -> Self {
        Self {
            first_value: None,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
        }
    }

    /// Encodes a float value into the output bit vector.
    pub fn encode(&mut self, value: f64, output: &mut BitVec<u8, Msb0>) {
        let bits = value.to_bits();

        if self.first_value.is_none() {
            // First value: write 64 bits raw
            self.first_value = Some(bits);
            self.prev_value = bits;
            for i in (0..64).rev() {
                output.push((bits >> i) & 1 == 1);
            }
            return;
        }

        let xor = bits ^ self.prev_value;

        if xor == 0 {
            // Case 1: identical value -> '0'
            output.push(false);
        } else {
            let leading = xor.leading_zeros();
            let trailing = xor.trailing_zeros();

            // Check if we can reuse the previous window
            if leading >= self.prev_leading && trailing >= self.prev_trailing {
                // Case 2: same window -> '10' + meaningful bits
                output.push(true);
                output.push(false);

                let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
                let shifted = xor >> self.prev_trailing;
                for i in (0..meaningful_bits).rev() {
                    output.push((shifted >> i) & 1 == 1);
                }
            } else {
                // Case 3: new window -> '11' + 5 bits leading + 6 bits length + meaningful
                output.push(true);
                output.push(true);

                // 5 bits for leading zeros (0-31)
                let leading_capped = leading.min(31);
                for i in (0..5).rev() {
                    output.push((leading_capped >> i) & 1 == 1);
                }

                // 6 bits for meaningful length (1-64, stored as 0-63)
                let meaningful_bits = 64 - leading - trailing;
                let length_encoded = meaningful_bits - 1;
                for i in (0..6).rev() {
                    output.push((length_encoded >> i) & 1 == 1);
                }

                // Write meaningful bits
                let shifted = xor >> trailing;
                for i in (0..meaningful_bits).rev() {
                    output.push((shifted >> i) & 1 == 1);
                }

                self.prev_leading = leading;
                self.prev_trailing = trailing;
            }
        }

        self.prev_value = bits;
    }
}

impl Default for ValueEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoder for XOR-encoded float values.
pub struct ValueDecoder<'a> {
    data: &'a BitVec<u8, Msb0>,
    pos: usize,
    first_value: Option<u64>,
    prev_value: u64,
    prev_leading: u32,
    prev_trailing: u32,
}

impl<'a> ValueDecoder<'a> {
    /// Creates a new value decoder.
    pub fn new(data: &'a BitVec<u8, Msb0>) -> Self {
        Self {
            data,
            pos: 0,
            first_value: None,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
        }
    }

    /// Decodes the next float value from the bit stream.
    pub fn decode_next(&mut self) -> Option<f64> {
        if self.pos >= self.data.len() {
            return None;
        }

        if self.first_value.is_none() {
            // First value: read 64 bits
            if self.pos + 64 > self.data.len() {
                return None;
            }
            let mut bits: u64 = 0;
            for _ in 0..64 {
                bits = (bits << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            self.first_value = Some(bits);
            self.prev_value = bits;
            return Some(f64::from_bits(bits));
        }

        let xor = if !self.data[self.pos] {
            // Case 1: '0' -> identical value
            self.pos += 1;
            0u64
        } else if !self.data[self.pos + 1] {
            // Case 2: '10' -> same window
            self.pos += 2;
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            let mut xor_value: u64 = 0;
            for _ in 0..meaningful_bits {
                xor_value = (xor_value << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            xor_value << self.prev_trailing
        } else {
            // Case 3: '11' -> new window
            self.pos += 2;

            // Read 5 bits for leading zeros
            let mut leading: u32 = 0;
            for _ in 0..5 {
                leading = (leading << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }

            // Read 6 bits for meaningful length
            let mut length_encoded: u32 = 0;
            for _ in 0..6 {
                length_encoded = (length_encoded << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }
            let meaningful_bits = length_encoded + 1;
            let trailing = 64 - leading - meaningful_bits;

            // Read meaningful bits
            let mut xor_value: u64 = 0;
            for _ in 0..meaningful_bits {
                xor_value = (xor_value << 1) | if self.data[self.pos] { 1 } else { 0 };
                self.pos += 1;
            }

            self.prev_leading = leading;
            self.prev_trailing = trailing;

            xor_value << trailing
        };

        let bits = self.prev_value ^ xor;
        self.prev_value = bits;

        Some(f64::from_bits(bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_block_roundtrip() {
        let points = vec![
            (1000_i64, 1.0_f64),
            (1010, 1.1),
            (1020, 1.2),
            (1030, 1.1),
            (1040, 1.0),
        ];

        let block = CompressedBlock::compress(&points);
        assert_eq!(block.count, 5);

        let decompressed = block.decompress();
        assert_eq!(decompressed.len(), points.len());

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let timestamps = vec![1000_i64, 1010, 1020, 1030, 1100, 2000, 2001];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_value_roundtrip() {
        let values = vec![1.0_f64, 1.0, 1.1, 1.2, 1.1, 2.0, 0.0, -1.0];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = ValueEncoder::new();
        for &val in &values {
            encoder.encode(val, &mut output);
        }

        let mut decoder = ValueDecoder::new(&output);
        for &expected in &values {
            let decoded = decoder.decode_next().expect("should decode");
            assert!((expected - decoded).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_empty_block() {
        let points: Vec<(i64, f64)> = vec![];
        let block = CompressedBlock::compress(&points);
        assert_eq!(block.count, 0);

        let decompressed = block.decompress();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_single_point() {
        let points = vec![(1234567890_i64, std::f64::consts::PI)];
        let block = CompressedBlock::compress(&points);
        assert_eq!(block.count, 1);

        let decompressed = block.decompress();
        assert_eq!(decompressed.len(), 1);
        assert_eq!(decompressed[0].0, points[0].0);
        assert!((decompressed[0].1 - points[0].1).abs() < f64::EPSILON);
    }

    // =====================================================
    // Phase 2: Additional unit tests for Gorilla compression
    // =====================================================

    #[test]
    fn test_timestamp_encoder_zero_delta() {
        // Test case where delta-of-delta is 0 (regular intervals)
        // Should encode as single '0' bit for each subsequent timestamp
        let timestamps = vec![1000_i64, 1010, 1020, 1030, 1040];
        // Interval is constant 10, so delta-of-delta = 0 after first two

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }

        // Verify compression efficiency: with zero delta-of-delta,
        // after first two timestamps, each should use only 1 bit
        // First: 64 bits, Second: 64 bits (delta), Rest: 1 bit each
        // Total expected: 64 + 64 + 3 = 131 bits (approximately)
        assert!(output.len() < 150, "Zero delta should compress well");
    }

    #[test]
    fn test_timestamp_encoder_small_delta() {
        // Test case where delta-of-delta is in [-63, 64] range
        // Should encode as '10' + 7 bits
        let timestamps = vec![1000_i64, 1010, 1025, 1035, 1055];
        // Deltas: 10, 15, 10, 20
        // Delta-of-delta: 5, -5, 10 (all in [-63, 64])

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_timestamp_encoder_medium_delta() {
        // Test case where delta-of-delta is in [-255, 256] range
        // Should encode as '110' + 9 bits
        let timestamps = vec![1000_i64, 1100, 1350, 1400];
        // Deltas: 100, 250, 50
        // Delta-of-delta: 150, -200 (in [-255, 256] range)

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_timestamp_encoder_large_delta() {
        // Test case where delta-of-delta is in [-2047, 2048] range
        // Should encode as '1110' + 12 bits
        let timestamps = vec![1000_i64, 2000, 5000, 5500];
        // Deltas: 1000, 3000, 500
        // Delta-of-delta: 2000, -2500

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_timestamp_encoder_very_large_delta() {
        // Test case where delta-of-delta exceeds [-2047, 2048]
        // Should encode as '1111' + 32 bits
        let timestamps = vec![0_i64, 1_000_000, 100_000_000, 100_001_000];
        // Very irregular intervals

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_value_encoder_identical() {
        // Test case where all values are identical
        // XOR = 0, should encode as single '0' bit each
        let values = vec![42.5_f64; 10];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = ValueEncoder::new();
        for &val in &values {
            encoder.encode(val, &mut output);
        }

        let mut decoder = ValueDecoder::new(&output);
        for &expected in &values {
            let decoded = decoder.decode_next().expect("should decode");
            assert!((expected - decoded).abs() < f64::EPSILON);
        }

        // First value: 64 bits, subsequent: 1 bit each
        // Total: 64 + 9 = 73 bits
        assert!(
            output.len() < 80,
            "Identical values should compress to ~73 bits"
        );
    }

    #[test]
    fn test_value_encoder_varying() {
        // Test case with varying values
        let values = vec![1.0_f64, 1.5, 2.0, 2.5, 3.0, 100.0, -50.0, 0.0];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = ValueEncoder::new();
        for &val in &values {
            encoder.encode(val, &mut output);
        }

        let mut decoder = ValueDecoder::new(&output);
        for &expected in &values {
            let decoded = decoder.decode_next().expect("should decode");
            assert!((expected - decoded).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_value_encoder_same_window_reuse() {
        // Test that encoder reuses leading/trailing zeros window
        // when XOR has similar bit patterns
        let values = vec![1.0_f64, 1.0000001, 1.0000002, 1.0000003];
        // Small differences should have similar XOR patterns

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = ValueEncoder::new();
        for &val in &values {
            encoder.encode(val, &mut output);
        }

        let mut decoder = ValueDecoder::new(&output);
        for &expected in &values {
            let decoded = decoder.decode_next().expect("should decode");
            assert!((expected - decoded).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_edge_case_min_max_timestamps() {
        // Test with a range of timestamps that have reasonable deltas
        // The Gorilla algorithm uses 32-bit deltas, so we test within those bounds
        // This tests various edge cases within the algorithm's design constraints
        let base = 1_000_000_000_000i64; // 1 trillion nanos (~16 min from epoch)
        let timestamps = vec![
            base,
            base + 1_000_000_000,                   // +1 second
            base + 2_000_000_000,                   // +1 second (regular)
            base + 2_000_000_001,                   // +1 nano (tiny delta)
            base + 2_100_000_000,                   // +~100ms
            base + 2_100_000_000 + i32::MAX as i64, // max positive i32 delta
        ];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_edge_case_special_floats() {
        // Test special float values (excluding NaN which doesn't equal itself)
        let values = vec![
            0.0_f64,
            -0.0,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::EPSILON,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = ValueEncoder::new();
        for &val in &values {
            encoder.encode(val, &mut output);
        }

        let mut decoder = ValueDecoder::new(&output);
        for &expected in &values {
            let decoded = decoder.decode_next().expect("should decode");
            // Handle infinity comparison
            if expected.is_infinite() {
                assert_eq!(expected.is_sign_positive(), decoded.is_sign_positive());
                assert!(decoded.is_infinite());
            } else {
                assert!(
                    (expected - decoded).abs() < f64::EPSILON
                        || (expected == 0.0 && decoded == 0.0)
                );
            }
        }
    }

    #[test]
    fn test_compressed_block_large_dataset() {
        // Test with larger dataset typical of time series
        let count = 1000;
        let start_ts = 1_000_000_000_i64;
        let interval = 1_000_000_000_i64; // 1 second in nanos

        let points: Vec<(i64, f64)> = (0..count)
            .map(|i| {
                let ts = start_ts + i as i64 * interval;
                let value = 50.0 + (i as f64 * 0.1).sin() * 10.0;
                (ts, value)
            })
            .collect();

        let block = CompressedBlock::compress(&points);
        assert_eq!(block.count, count);

        let decompressed = block.decompress();
        assert_eq!(decompressed.len(), count as usize);

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }

        // Verify compression ratio
        let raw_size = count as usize * std::mem::size_of::<(i64, f64)>();
        let compressed_size = (block.timestamps.len() + block.values.len()).div_ceil(8);
        let ratio = raw_size as f64 / compressed_size as f64;

        // With varying values (sine wave), expect modest compression
        // Regular intervals compress well, but varying values use more bits
        assert!(
            ratio > 1.5,
            "Expected compression ratio >1.5:1, got {:.2}:1",
            ratio
        );
    }

    #[test]
    fn test_compressed_block_irregular_intervals() {
        // Test with irregular timestamp intervals
        let points = vec![
            (1000_i64, 1.0),
            (1001, 1.1),
            (1100, 2.0),
            (5000, 3.0),
            (5001, 3.1),
            (10000, 4.0),
        ];

        let block = CompressedBlock::compress(&points);
        let decompressed = block.decompress();

        for (original, decoded) in points.iter().zip(decompressed.iter()) {
            assert_eq!(original.0, decoded.0);
            assert!((original.1 - decoded.1).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_negative_timestamps() {
        // Test with negative timestamps (historical data)
        let timestamps = vec![-1_000_000_000_i64, -999_999_000, -999_998_000, 0, 1000];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = TimestampEncoder::new();
        for &ts in &timestamps {
            encoder.encode(ts, &mut output);
        }

        let mut decoder = TimestampDecoder::new(&output);
        for &expected in &timestamps {
            let decoded = decoder.decode_next().expect("should decode");
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn test_negative_values() {
        // Test with negative float values
        let values = vec![-100.0_f64, -50.0, -1.0, -0.5, 0.0, 0.5, 1.0, 50.0, 100.0];

        let mut output = BitVec::<u8, Msb0>::new();
        let mut encoder = ValueEncoder::new();
        for &val in &values {
            encoder.encode(val, &mut output);
        }

        let mut decoder = ValueDecoder::new(&output);
        for &expected in &values {
            let decoded = decoder.decode_next().expect("should decode");
            assert!((expected - decoded).abs() < f64::EPSILON);
        }
    }
}
