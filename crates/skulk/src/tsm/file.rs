//! TSM File Format implementation.
//!
//! This module provides structures and functions for reading and writing TSM files
//! with the `.skulk` extension. The format includes:
//!
//! - Header with magic bytes, version, and metadata
//! - Series index with Bloom filter for efficient lookup
//! - Data blocks with Gorilla compression
//! - Footer with file checksums for integrity verification
//!
//! ## File Structure
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  File Header (32 bytes)                                      │
//! │  - Magic: "ATSM" (4 bytes)                                   │
//! │  - Version: u16 (2 bytes) = 2                                │
//! │  - Min Timestamp: i64 (8 bytes)                              │
//! │  - Max Timestamp: i64 (8 bytes)                              │
//! │  - Series Count: u32 (4 bytes)                               │
//! │  - Compression: u8 (1 byte)                                  │
//! │  - Section Flags: u8 (1 byte)                                │
//! │  - Level: u16 (2 bytes)                                      │
//! │  - Reserved: 2 bytes                                         │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Data Blocks (repeated per series)                           │
//! │  - Series ID, timestamps, values with block CRC              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Series Index Block                                          │
//! │  - Index entries with Bloom filter                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Footer (48 bytes)                                           │
//! │  - Offsets, sizes, CRC, reverse magic                        │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, TsmError};
use crate::tsm::{SeriesId, Timestamp};
use std::collections::BTreeMap;
use std::io::{Read, Write};

/// Magic bytes for TSM file header: "ATSM"
pub const TSM_MAGIC: [u8; 4] = *b"ATSM";

/// Reverse magic bytes for TSM file footer: "MSTA"
pub const TSM_MAGIC_REVERSE: [u8; 4] = *b"MSTA";

/// Current TSM file format version.
pub const TSM_VERSION: u16 = 2;

/// Header size in bytes.
pub const HEADER_SIZE: usize = 32;

/// Footer size in bytes.
pub const FOOTER_SIZE: usize = 48;

/// Compression type for TSM data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression (raw data).
    Raw = 0,
    /// Gorilla compression (default for time series).
    #[default]
    Gorilla = 1,
}

impl CompressionType {
    /// Creates a CompressionType from a u8 value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Raw),
            1 => Some(Self::Gorilla),
            _ => None,
        }
    }
}

/// Timestamp encoding type for per-block encoding information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TimestampEncoding {
    /// Raw encoding (8 bytes × N, uncompressed).
    Raw = 0,
    /// Delta-of-Delta encoding (Gorilla timestamps).
    #[default]
    DeltaOfDelta = 1,
}

impl TimestampEncoding {
    /// Creates a TimestampEncoding from a u8 value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Raw),
            1 => Some(Self::DeltaOfDelta),
            _ => None,
        }
    }
}

/// Value encoding type for per-block encoding information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ValueEncoding {
    /// Raw encoding (8 bytes × N, uncompressed).
    Raw = 0,
    /// Gorilla XOR compression.
    #[default]
    GorillaXor = 1,
}

impl ValueEncoding {
    /// Creates a ValueEncoding from a u8 value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Raw),
            1 => Some(Self::GorillaXor),
            _ => None,
        }
    }
}

/// Section flags indicating the type of data in the TSM file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SectionFlags(u8);

impl SectionFlags {
    /// Hot data section (frequently accessed, recent data).
    pub const HOT: u8 = 0b001;
    /// Cold data section (infrequently accessed, older data).
    pub const COLD: u8 = 0b010;
    /// Metadata section.
    pub const META: u8 = 0b100;

    /// Creates a new SectionFlags with no flags set.
    pub fn new() -> Self {
        Self(0)
    }

    /// Creates a SectionFlags from a raw u8 value.
    pub fn from_u8(value: u8) -> Self {
        Self(value)
    }

    /// Returns the raw u8 value.
    pub fn as_u8(self) -> u8 {
        self.0
    }

    /// Returns true if the hot flag is set.
    pub fn is_hot(self) -> bool {
        self.0 & Self::HOT != 0
    }

    /// Returns true if the cold flag is set.
    pub fn is_cold(self) -> bool {
        self.0 & Self::COLD != 0
    }

    /// Returns true if the meta flag is set.
    pub fn is_meta(self) -> bool {
        self.0 & Self::META != 0
    }

    /// Sets the hot flag.
    pub fn set_hot(&mut self) {
        self.0 |= Self::HOT;
    }

    /// Sets the cold flag.
    pub fn set_cold(&mut self) {
        self.0 |= Self::COLD;
    }

    /// Sets the meta flag.
    pub fn set_meta(&mut self) {
        self.0 |= Self::META;
    }
}

/// TSM file header (32 bytes).
///
/// The header contains metadata about the TSM file including:
/// - Magic bytes for file identification
/// - Version for format compatibility
/// - Timestamp range for quick filtering
/// - Series count for resource allocation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsmHeader {
    /// Magic bytes: "ATSM"
    pub magic: [u8; 4],
    /// File format version (currently 2).
    pub version: u16,
    /// Minimum timestamp in the file.
    pub min_timestamp: i64,
    /// Maximum timestamp in the file.
    pub max_timestamp: i64,
    /// Number of series in the file.
    pub series_count: u32,
    /// Compression type used for data blocks.
    pub compression: CompressionType,
    /// Section flags (hot/cold/meta).
    pub section_flags: SectionFlags,
    /// Compaction level (0 = L0, 1 = L1, etc.).
    pub level: u16,
}

impl Default for TsmHeader {
    fn default() -> Self {
        Self {
            magic: TSM_MAGIC,
            version: TSM_VERSION,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            series_count: 0,
            compression: CompressionType::default(),
            section_flags: SectionFlags::default(),
            level: 0,
        }
    }
}

impl TsmHeader {
    /// Creates a new TSM header with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a TSM header with the specified parameters.
    pub fn with_params(
        min_timestamp: i64,
        max_timestamp: i64,
        series_count: u32,
        compression: CompressionType,
        section_flags: SectionFlags,
        level: u16,
    ) -> Self {
        Self {
            magic: TSM_MAGIC,
            version: TSM_VERSION,
            min_timestamp,
            max_timestamp,
            series_count,
            compression,
            section_flags,
            level,
        }
    }

    /// Writes the header to a writer using little-endian byte order.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Magic (4 bytes)
        writer.write_all(&self.magic)?;
        // Version (2 bytes)
        writer.write_all(&self.version.to_le_bytes())?;
        // Min timestamp (8 bytes)
        writer.write_all(&self.min_timestamp.to_le_bytes())?;
        // Max timestamp (8 bytes)
        writer.write_all(&self.max_timestamp.to_le_bytes())?;
        // Series count (4 bytes)
        writer.write_all(&self.series_count.to_le_bytes())?;
        // Compression (1 byte)
        writer.write_all(&[self.compression as u8])?;
        // Section flags (1 byte)
        writer.write_all(&[self.section_flags.as_u8()])?;
        // Level (2 bytes)
        writer.write_all(&self.level.to_le_bytes())?;
        // Reserved (2 bytes)
        writer.write_all(&[0u8; 2])?;

        Ok(())
    }

    /// Reads a header from a reader using little-endian byte order.
    ///
    /// # Errors
    ///
    /// Returns `TsmError::InvalidMagic` if the magic bytes don't match.
    /// Returns `TsmError::UnsupportedVersion` if the version is not supported.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buf)?;

        // Magic (4 bytes)
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if magic != TSM_MAGIC {
            return Err(TsmError::InvalidMagic(magic));
        }

        // Version (2 bytes)
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version > TSM_VERSION {
            return Err(TsmError::UnsupportedVersion(version));
        }

        // Min timestamp (8 bytes)
        let min_timestamp = i64::from_le_bytes(buf[6..14].try_into().unwrap());

        // Max timestamp (8 bytes)
        let max_timestamp = i64::from_le_bytes(buf[14..22].try_into().unwrap());

        // Series count (4 bytes)
        let series_count = u32::from_le_bytes(buf[22..26].try_into().unwrap());

        // Compression (1 byte)
        let compression = CompressionType::from_u8(buf[26]).unwrap_or_default();

        // Section flags (1 byte)
        let section_flags = SectionFlags::from_u8(buf[27]);

        // Level (2 bytes)
        let level = u16::from_le_bytes(buf[28..30].try_into().unwrap());

        // Reserved (2 bytes) - ignored

        Ok(Self {
            magic,
            version,
            min_timestamp,
            max_timestamp,
            series_count,
            compression,
            section_flags,
            level,
        })
    }
}

/// TSM file footer (48 bytes).
///
/// The footer contains navigation and integrity information:
/// - Offsets and sizes for quick index location
/// - CRC32 checksum for file integrity
/// - Reverse magic bytes for file validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsmFooter {
    /// Offset to the series index from file start.
    pub series_index_offset: u64,
    /// Size of the series index in bytes.
    pub series_index_size: u32,
    /// Offset to the data section from file start.
    pub data_section_offset: u64,
    /// Size of the data section in bytes.
    pub data_section_size: u64,
    /// Total number of data points in the file.
    pub total_point_count: u64,
    /// CRC32 checksum of the entire file (excluding footer).
    pub file_crc32: u32,
    /// Reverse magic bytes: "MSTA"
    pub magic_reverse: [u8; 4],
}

impl Default for TsmFooter {
    fn default() -> Self {
        Self {
            series_index_offset: 0,
            series_index_size: 0,
            data_section_offset: HEADER_SIZE as u64,
            data_section_size: 0,
            total_point_count: 0,
            file_crc32: 0,
            magic_reverse: TSM_MAGIC_REVERSE,
        }
    }
}

impl TsmFooter {
    /// Creates a new TSM footer with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a TSM footer with the specified parameters.
    pub fn with_params(
        series_index_offset: u64,
        series_index_size: u32,
        data_section_offset: u64,
        data_section_size: u64,
        total_point_count: u64,
        file_crc32: u32,
    ) -> Self {
        Self {
            series_index_offset,
            series_index_size,
            data_section_offset,
            data_section_size,
            total_point_count,
            file_crc32,
            magic_reverse: TSM_MAGIC_REVERSE,
        }
    }

    /// Writes the footer to a writer using little-endian byte order.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Series index offset (8 bytes)
        writer.write_all(&self.series_index_offset.to_le_bytes())?;
        // Series index size (4 bytes)
        writer.write_all(&self.series_index_size.to_le_bytes())?;
        // Data section offset (8 bytes)
        writer.write_all(&self.data_section_offset.to_le_bytes())?;
        // Data section size (8 bytes)
        writer.write_all(&self.data_section_size.to_le_bytes())?;
        // Total point count (8 bytes)
        writer.write_all(&self.total_point_count.to_le_bytes())?;
        // File CRC32 (4 bytes)
        writer.write_all(&self.file_crc32.to_le_bytes())?;
        // Magic reverse (4 bytes)
        writer.write_all(&self.magic_reverse)?;
        // Reserved (4 bytes)
        writer.write_all(&[0u8; 4])?;

        Ok(())
    }

    /// Reads a footer from a reader using little-endian byte order.
    ///
    /// # Errors
    ///
    /// Returns `TsmError::InvalidMagic` if the reverse magic bytes don't match.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut buf = [0u8; FOOTER_SIZE];
        reader.read_exact(&mut buf)?;

        // Series index offset (8 bytes)
        let series_index_offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());

        // Series index size (4 bytes)
        let series_index_size = u32::from_le_bytes(buf[8..12].try_into().unwrap());

        // Data section offset (8 bytes)
        let data_section_offset = u64::from_le_bytes(buf[12..20].try_into().unwrap());

        // Data section size (8 bytes)
        let data_section_size = u64::from_le_bytes(buf[20..28].try_into().unwrap());

        // Total point count (8 bytes)
        let total_point_count = u64::from_le_bytes(buf[28..36].try_into().unwrap());

        // File CRC32 (4 bytes)
        let file_crc32 = u32::from_le_bytes(buf[36..40].try_into().unwrap());

        // Magic reverse (4 bytes)
        let magic_reverse: [u8; 4] = buf[40..44].try_into().unwrap();
        if magic_reverse != TSM_MAGIC_REVERSE {
            return Err(TsmError::InvalidMagic(magic_reverse));
        }

        // Reserved (4 bytes) - ignored

        Ok(Self {
            series_index_offset,
            series_index_size,
            data_section_offset,
            data_section_size,
            total_point_count,
            file_crc32,
            magic_reverse,
        })
    }
}

/// Series index entry containing metadata and location of a series' data block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesIndexEntry {
    /// Unique identifier for the series.
    pub series_id: SeriesId,
    /// Name of the metric (e.g., "cpu.usage").
    pub metric_name: String,
    /// Key-value labels for the series.
    pub labels: Vec<(String, String)>,
    /// Offset to the data block from file start.
    pub block_offset: u64,
    /// Size of the data block in bytes.
    pub block_size: u32,
    /// Number of data points in the block.
    pub point_count: u32,
    /// Minimum timestamp in the block.
    pub min_ts: Timestamp,
    /// Maximum timestamp in the block.
    pub max_ts: Timestamp,
}

impl SeriesIndexEntry {
    /// Creates a new series index entry.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        series_id: SeriesId,
        metric_name: String,
        labels: Vec<(String, String)>,
        block_offset: u64,
        block_size: u32,
        point_count: u32,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> Self {
        Self {
            series_id,
            metric_name,
            labels,
            block_offset,
            block_size,
            point_count,
            min_ts,
            max_ts,
        }
    }

    /// Writes the entry to a writer.
    ///
    /// Format:
    /// - series_id: u64
    /// - metric_name_len: u16 + metric_name: UTF-8
    /// - label_count: u16 + labels: [(key_len: u16, key, value_len: u16, value), ...]
    /// - block_offset: u64
    /// - block_size: u32
    /// - point_count: u32
    /// - min_ts: i64
    /// - max_ts: i64
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Series ID (8 bytes)
        writer.write_all(&self.series_id.to_le_bytes())?;

        // Metric name (length-prefixed)
        let metric_bytes = self.metric_name.as_bytes();
        writer.write_all(&(metric_bytes.len() as u16).to_le_bytes())?;
        writer.write_all(metric_bytes)?;

        // Labels (count + entries)
        writer.write_all(&(self.labels.len() as u16).to_le_bytes())?;
        for (key, value) in &self.labels {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            writer.write_all(&(key_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(key_bytes)?;
            writer.write_all(&(value_bytes.len() as u16).to_le_bytes())?;
            writer.write_all(value_bytes)?;
        }

        // Block offset (8 bytes)
        writer.write_all(&self.block_offset.to_le_bytes())?;

        // Block size (4 bytes)
        writer.write_all(&self.block_size.to_le_bytes())?;

        // Point count (4 bytes)
        writer.write_all(&self.point_count.to_le_bytes())?;

        // Min timestamp (8 bytes)
        writer.write_all(&self.min_ts.to_le_bytes())?;

        // Max timestamp (8 bytes)
        writer.write_all(&self.max_ts.to_le_bytes())?;

        Ok(())
    }

    /// Reads an entry from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Series ID (8 bytes)
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let series_id = u64::from_le_bytes(buf8);

        // Metric name (length-prefixed)
        let mut buf2 = [0u8; 2];
        reader.read_exact(&mut buf2)?;
        let metric_len = u16::from_le_bytes(buf2) as usize;
        let mut metric_bytes = vec![0u8; metric_len];
        reader.read_exact(&mut metric_bytes)?;
        let metric_name = String::from_utf8(metric_bytes)
            .map_err(|e| TsmError::DecompressionError(format!("Invalid UTF-8 in metric name: {}", e)))?;

        // Labels (count + entries)
        reader.read_exact(&mut buf2)?;
        let label_count = u16::from_le_bytes(buf2) as usize;
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            // Key
            reader.read_exact(&mut buf2)?;
            let key_len = u16::from_le_bytes(buf2) as usize;
            let mut key_bytes = vec![0u8; key_len];
            reader.read_exact(&mut key_bytes)?;
            let key = String::from_utf8(key_bytes)
                .map_err(|e| TsmError::DecompressionError(format!("Invalid UTF-8 in label key: {}", e)))?;

            // Value
            reader.read_exact(&mut buf2)?;
            let value_len = u16::from_le_bytes(buf2) as usize;
            let mut value_bytes = vec![0u8; value_len];
            reader.read_exact(&mut value_bytes)?;
            let value = String::from_utf8(value_bytes)
                .map_err(|e| TsmError::DecompressionError(format!("Invalid UTF-8 in label value: {}", e)))?;

            labels.push((key, value));
        }

        // Block offset (8 bytes)
        reader.read_exact(&mut buf8)?;
        let block_offset = u64::from_le_bytes(buf8);

        // Block size (4 bytes)
        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let block_size = u32::from_le_bytes(buf4);

        // Point count (4 bytes)
        reader.read_exact(&mut buf4)?;
        let point_count = u32::from_le_bytes(buf4);

        // Min timestamp (8 bytes)
        reader.read_exact(&mut buf8)?;
        let min_ts = i64::from_le_bytes(buf8);

        // Max timestamp (8 bytes)
        reader.read_exact(&mut buf8)?;
        let max_ts = i64::from_le_bytes(buf8);

        Ok(Self {
            series_id,
            metric_name,
            labels,
            block_offset,
            block_size,
            point_count,
            min_ts,
            max_ts,
        })
    }
}

/// Default number of hash functions for Bloom filter (per design spec).
const BLOOM_FILTER_DEFAULT_HASH_COUNT: u8 = 3;

/// Bloom filter for fast series existence checks.
///
/// Uses xxhash64 with k=3 hash functions using different seeds,
/// as specified in the design document:
/// `h_i(x) = xxhash64(x, seed=i) % size_bits`
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array for the filter.
    bits: Vec<u64>,
    /// Number of hash functions to use (default: 3 per design spec).
    hash_count: u8,
}

impl BloomFilter {
    /// Creates a new Bloom filter optimized for the given number of items.
    ///
    /// Uses approximately 10 bits per item for ~1% false positive rate
    /// with k=3 hash functions (per design specification).
    pub fn new(expected_items: usize) -> Self {
        // 10 bits per item gives ~1% false positive rate with k=3
        let num_bits = (expected_items * 10).max(64);
        let num_words = num_bits.div_ceil(64);

        Self {
            bits: vec![0u64; num_words],
            hash_count: BLOOM_FILTER_DEFAULT_HASH_COUNT,
        }
    }

    /// Creates a Bloom filter from raw bits.
    pub fn from_bits(bits: Vec<u64>, hash_count: u8) -> Self {
        Self { bits, hash_count }
    }

    /// Adds a series ID to the filter.
    ///
    /// Uses xxhash64 with seeds 0, 1, 2, ... (hash_count - 1)
    /// as specified in design: `h_i(x) = xxhash64(x, seed=i) % size_bits`
    pub fn insert(&mut self, series_id: SeriesId) {
        let num_bits = self.bits.len() * 64;
        let key = series_id.to_le_bytes();

        for seed in 0..self.hash_count {
            let hash = xxhash_rust::xxh64::xxh64(&key, seed as u64);
            let bit_idx = hash % (num_bits as u64);
            let word_idx = (bit_idx / 64) as usize;
            let bit_pos = bit_idx % 64;
            self.bits[word_idx] |= 1u64 << bit_pos;
        }
    }

    /// Checks if a series ID might be in the filter.
    ///
    /// Returns `true` if the ID might be present (may have false positives).
    /// Returns `false` if the ID is definitely not present.
    pub fn maybe_contains(&self, series_id: SeriesId) -> bool {
        let num_bits = self.bits.len() * 64;
        let key = series_id.to_le_bytes();

        for seed in 0..self.hash_count {
            let hash = xxhash_rust::xxh64::xxh64(&key, seed as u64);
            let bit_idx = hash % (num_bits as u64);
            let word_idx = (bit_idx / 64) as usize;
            let bit_pos = bit_idx % 64;
            if self.bits[word_idx] & (1u64 << bit_pos) == 0 {
                return false;
            }
        }

        true
    }

    /// Returns the raw bits of the filter.
    pub fn bits(&self) -> &[u64] {
        &self.bits
    }

    /// Returns the number of hash functions.
    pub fn hash_count(&self) -> u8 {
        self.hash_count
    }

    /// Writes the Bloom filter to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Number of words (4 bytes)
        writer.write_all(&(self.bits.len() as u32).to_le_bytes())?;
        // Hash count (1 byte) + 3 bytes reserved for alignment
        writer.write_all(&[self.hash_count, 0, 0, 0])?;
        // Bit array
        for word in &self.bits {
            writer.write_all(&word.to_le_bytes())?;
        }
        Ok(())
    }

    /// Reads a Bloom filter from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Number of words (4 bytes)
        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let num_words = u32::from_le_bytes(buf4) as usize;

        // Hash count (1 byte) + 3 bytes reserved
        reader.read_exact(&mut buf4)?;
        let hash_count = buf4[0];

        // Bit array
        let mut bits = Vec::with_capacity(num_words);
        let mut buf8 = [0u8; 8];
        for _ in 0..num_words {
            reader.read_exact(&mut buf8)?;
            bits.push(u64::from_le_bytes(buf8));
        }

        Ok(Self { bits, hash_count })
    }
}

/// Series index for efficient series lookup.
///
/// Contains index entries sorted by series ID and a Bloom filter
/// for quick existence checks.
#[derive(Debug, Clone)]
pub struct SeriesIndex {
    /// Index entries sorted by series ID.
    entries: BTreeMap<SeriesId, SeriesIndexEntry>,
    /// Bloom filter for quick existence checks.
    bloom: BloomFilter,
}

impl SeriesIndex {
    /// Creates a new empty series index.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            bloom: BloomFilter::new(1000), // Default capacity
        }
    }

    /// Creates a series index with the given capacity hint.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            bloom: BloomFilter::new(capacity),
        }
    }

    /// Inserts an entry into the index.
    pub fn insert(&mut self, entry: SeriesIndexEntry) {
        self.bloom.insert(entry.series_id);
        self.entries.insert(entry.series_id, entry);
    }

    /// Gets an entry by series ID.
    ///
    /// Uses the Bloom filter for quick rejection of non-existent series.
    pub fn get(&self, series_id: SeriesId) -> Option<&SeriesIndexEntry> {
        if !self.bloom.maybe_contains(series_id) {
            return None;
        }
        self.entries.get(&series_id)
    }

    /// Checks if a series might exist in the index.
    pub fn maybe_contains(&self, series_id: SeriesId) -> bool {
        self.bloom.maybe_contains(series_id)
    }

    /// Returns an iterator over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&SeriesId, &SeriesIndexEntry)> {
        self.entries.iter()
    }

    /// Returns the number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Writes the index to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Entry count (4 bytes)
        writer.write_all(&(self.entries.len() as u32).to_le_bytes())?;

        // Entries
        for entry in self.entries.values() {
            entry.write_to(writer)?;
        }

        // Bloom filter
        self.bloom.write_to(writer)?;

        Ok(())
    }

    /// Reads an index from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Entry count (4 bytes)
        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let entry_count = u32::from_le_bytes(buf4) as usize;

        // Entries
        let mut entries = BTreeMap::new();
        for _ in 0..entry_count {
            let entry = SeriesIndexEntry::read_from(reader)?;
            entries.insert(entry.series_id, entry);
        }

        // Bloom filter
        let bloom = BloomFilter::read_from(reader)?;

        Ok(Self { entries, bloom })
    }
}

impl Default for SeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// TSM data block containing compressed time series data.
///
/// Each block contains data for a single series with:
/// - Block header with metadata
/// - Per-block encoding types for timestamps and values
/// - Gorilla-compressed timestamps and values
/// - CRC32 checksum for integrity verification
///
/// ## Binary Layout
///
/// ```text
/// Offset  Size    Field
/// ------  ----    -----
/// 0x00    8       series_id (u64 LE)
/// 0x08    4       point_count (u32 LE)
/// 0x0C    8       min_timestamp (i64 LE)
/// 0x14    8       max_timestamp (i64 LE)
/// 0x1C    1       ts_encoding (u8)
/// 0x1D    1       val_encoding (u8)
/// 0x1E    4       ts_data_size (u32 LE)
/// 0x22    N       ts_data[ts_data_size]
/// 0x22+N  4       val_data_size (u32 LE)
/// 0x26+N  M       val_data[val_data_size]
/// 0x26+N+M 4      block_crc32 (u32 LE)
/// ```
#[derive(Debug, Clone)]
pub struct TsmDataBlock {
    /// Series ID this block belongs to.
    pub series_id: SeriesId,
    /// Number of data points in the block.
    pub point_count: u32,
    /// Minimum timestamp in the block.
    pub min_ts: Timestamp,
    /// Maximum timestamp in the block.
    pub max_ts: Timestamp,
    /// Timestamp encoding type.
    pub ts_encoding: TimestampEncoding,
    /// Value encoding type.
    pub val_encoding: ValueEncoding,
    /// Compressed timestamp data.
    pub ts_data: Vec<u8>,
    /// Compressed value data.
    pub val_data: Vec<u8>,
    /// CRC32 checksum of the block (series_id through val_data).
    pub block_crc32: u32,
}

impl TsmDataBlock {
    /// Creates a new TSM data block with the specified encoding types.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        series_id: SeriesId,
        point_count: u32,
        min_ts: Timestamp,
        max_ts: Timestamp,
        ts_encoding: TimestampEncoding,
        val_encoding: ValueEncoding,
        ts_data: Vec<u8>,
        val_data: Vec<u8>,
    ) -> Self {
        let mut block = Self {
            series_id,
            point_count,
            min_ts,
            max_ts,
            ts_encoding,
            val_encoding,
            ts_data,
            val_data,
            block_crc32: 0,
        };
        block.block_crc32 = block.calculate_crc();
        block
    }

    /// Calculates the CRC32 checksum of the block data.
    ///
    /// CRC covers: series_id, point_count, min_ts, max_ts,
    /// ts_encoding, val_encoding, ts_data_size, ts_data, val_data_size, val_data
    fn calculate_crc(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.series_id.to_le_bytes());
        hasher.update(&self.point_count.to_le_bytes());
        hasher.update(&self.min_ts.to_le_bytes());
        hasher.update(&self.max_ts.to_le_bytes());
        hasher.update(&[self.ts_encoding as u8]);
        hasher.update(&[self.val_encoding as u8]);
        hasher.update(&(self.ts_data.len() as u32).to_le_bytes());
        hasher.update(&self.ts_data);
        hasher.update(&(self.val_data.len() as u32).to_le_bytes());
        hasher.update(&self.val_data);
        hasher.finalize()
    }

    /// Verifies the block's CRC32 checksum.
    pub fn verify_crc(&self) -> bool {
        self.block_crc32 == self.calculate_crc()
    }

    /// Writes the block to a writer.
    ///
    /// Binary format as documented in struct definition.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Series ID (8 bytes)
        writer.write_all(&self.series_id.to_le_bytes())?;
        // Point count (4 bytes)
        writer.write_all(&self.point_count.to_le_bytes())?;
        // Min timestamp (8 bytes)
        writer.write_all(&self.min_ts.to_le_bytes())?;
        // Max timestamp (8 bytes)
        writer.write_all(&self.max_ts.to_le_bytes())?;
        // Timestamp encoding (1 byte)
        writer.write_all(&[self.ts_encoding as u8])?;
        // Value encoding (1 byte)
        writer.write_all(&[self.val_encoding as u8])?;
        // Timestamp data size (4 bytes)
        writer.write_all(&(self.ts_data.len() as u32).to_le_bytes())?;
        // Timestamp data
        writer.write_all(&self.ts_data)?;
        // Value data size (4 bytes)
        writer.write_all(&(self.val_data.len() as u32).to_le_bytes())?;
        // Value data
        writer.write_all(&self.val_data)?;
        // Block CRC32 (4 bytes)
        writer.write_all(&self.block_crc32.to_le_bytes())?;

        Ok(())
    }

    /// Reads a block from a reader and verifies its CRC.
    ///
    /// # Errors
    ///
    /// Returns `TsmError::ChecksumMismatch` if CRC verification fails.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Series ID (8 bytes)
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        let series_id = u64::from_le_bytes(buf8);

        // Point count (4 bytes)
        let mut buf4 = [0u8; 4];
        reader.read_exact(&mut buf4)?;
        let point_count = u32::from_le_bytes(buf4);

        // Min timestamp (8 bytes)
        reader.read_exact(&mut buf8)?;
        let min_ts = i64::from_le_bytes(buf8);

        // Max timestamp (8 bytes)
        reader.read_exact(&mut buf8)?;
        let max_ts = i64::from_le_bytes(buf8);

        // Timestamp encoding (1 byte)
        let mut buf1 = [0u8; 1];
        reader.read_exact(&mut buf1)?;
        let ts_encoding = TimestampEncoding::from_u8(buf1[0])
            .ok_or(TsmError::UnsupportedVersion(buf1[0] as u16))?;

        // Value encoding (1 byte)
        reader.read_exact(&mut buf1)?;
        let val_encoding = ValueEncoding::from_u8(buf1[0])
            .ok_or(TsmError::UnsupportedVersion(buf1[0] as u16))?;

        // Timestamp data size (4 bytes)
        reader.read_exact(&mut buf4)?;
        let ts_data_size = u32::from_le_bytes(buf4) as usize;

        // Timestamp data
        let mut ts_data = vec![0u8; ts_data_size];
        reader.read_exact(&mut ts_data)?;

        // Value data size (4 bytes)
        reader.read_exact(&mut buf4)?;
        let val_data_size = u32::from_le_bytes(buf4) as usize;

        // Value data
        let mut val_data = vec![0u8; val_data_size];
        reader.read_exact(&mut val_data)?;

        // Block CRC32 (4 bytes)
        reader.read_exact(&mut buf4)?;
        let block_crc32 = u32::from_le_bytes(buf4);

        let block = Self {
            series_id,
            point_count,
            min_ts,
            max_ts,
            ts_encoding,
            val_encoding,
            ts_data,
            val_data,
            block_crc32,
        };

        // Verify CRC
        let calculated_crc = block.calculate_crc();
        if block_crc32 != calculated_crc {
            return Err(TsmError::ChecksumMismatch {
                expected: block_crc32,
                actual: calculated_crc,
            });
        }

        Ok(block)
    }

    /// Returns the size of the block in bytes when serialized.
    pub fn serialized_size(&self) -> usize {
        8 + // series_id
        4 + // point_count
        8 + // min_ts
        8 + // max_ts
        1 + // ts_encoding
        1 + // val_encoding
        4 + // ts_data_size
        self.ts_data.len() +
        4 + // val_data_size
        self.val_data.len() +
        4 // block_crc32
    }
}

use crate::tsm::gorilla::CompressedBlock;
use crate::tsm::{DataPoint, SeriesMeta, TimeRange};
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Handle to an opened TSM file.
#[derive(Debug)]
pub struct TsmFileHandle {
    /// Path to the TSM file.
    pub path: PathBuf,
    /// The header of the file.
    pub header: TsmHeader,
    /// The footer of the file.
    pub footer: TsmFooter,
}

/// TSM file writer.
///
/// Writes time series data to a TSM file with the following sequence:
/// 1. Header (32 bytes)
/// 2. Data blocks (Gorilla compressed, with per-block CRC)
/// 3. Series index (with Bloom filter)
/// 4. Footer (48 bytes, with file CRC)
pub struct TsmWriter {
    /// Buffered writer for the file.
    writer: BufWriter<File>,
    /// Path to the file being written.
    path: PathBuf,
    /// Current write position.
    position: u64,
    /// Series index entries accumulated during writing.
    series_entries: Vec<SeriesIndexEntry>,
    /// Minimum timestamp seen.
    min_timestamp: i64,
    /// Maximum timestamp seen.
    max_timestamp: i64,
    /// Total point count.
    total_point_count: u64,
    /// Compression type.
    compression: CompressionType,
    /// Section flags.
    section_flags: SectionFlags,
    /// Compaction level.
    level: u16,
}

impl TsmWriter {
    /// Creates a new TSM writer for the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TSM file to create
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created.
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write placeholder header
        let header = TsmHeader::default();
        let mut header_buf = Vec::with_capacity(HEADER_SIZE);
        header.write_to(&mut header_buf)?;
        writer.write_all(&header_buf)?;

        Ok(Self {
            writer,
            path: path.to_path_buf(),
            position: HEADER_SIZE as u64,
            series_entries: Vec::new(),
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            total_point_count: 0,
            compression: CompressionType::Gorilla,
            section_flags: SectionFlags::new(),
            level: 0,
        })
    }

    /// Sets the compression type for this file.
    pub fn set_compression(&mut self, compression: CompressionType) {
        self.compression = compression;
    }

    /// Sets the section flags for this file.
    pub fn set_section_flags(&mut self, flags: SectionFlags) {
        self.section_flags = flags;
    }

    /// Sets the compaction level for this file.
    pub fn set_level(&mut self, level: u16) {
        self.level = level;
    }

    /// Writes a series to the TSM file.
    ///
    /// # Arguments
    ///
    /// * `series_id` - Unique identifier for the series
    /// * `meta` - Series metadata (metric name and labels)
    /// * `points` - Data points as a BTreeMap of timestamp -> value
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    pub fn write_series(
        &mut self,
        series_id: SeriesId,
        meta: &SeriesMeta,
        points: &std::collections::BTreeMap<Timestamp, f64>,
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        // Collect points for compression
        let point_vec: Vec<(i64, f64)> = points.iter().map(|(&ts, &v)| (ts, v)).collect();
        let point_count = point_vec.len() as u32;

        // Get timestamp range
        let min_ts = *points.keys().next().unwrap();
        let max_ts = *points.keys().next_back().unwrap();

        // Update file-level stats
        self.min_timestamp = self.min_timestamp.min(min_ts);
        self.max_timestamp = self.max_timestamp.max(max_ts);
        self.total_point_count += point_count as u64;

        // Compress the data based on compression type
        let (ts_encoding, val_encoding, ts_data, val_data) = if self.compression
            == CompressionType::Gorilla
        {
            let compressed = CompressedBlock::compress(&point_vec);
            let ts_bytes = compressed.timestamps.as_raw_slice().to_vec();
            let val_bytes = compressed.values.as_raw_slice().to_vec();
            (
                TimestampEncoding::DeltaOfDelta,
                ValueEncoding::GorillaXor,
                ts_bytes,
                val_bytes,
            )
        } else {
            // Raw encoding: write timestamps and values separately
            let mut ts_data = Vec::with_capacity(point_vec.len() * 8);
            let mut val_data = Vec::with_capacity(point_vec.len() * 8);
            for (ts, val) in &point_vec {
                ts_data.extend_from_slice(&ts.to_le_bytes());
                val_data.extend_from_slice(&val.to_le_bytes());
            }
            (TimestampEncoding::Raw, ValueEncoding::Raw, ts_data, val_data)
        };

        // Create data block
        let block = TsmDataBlock::new(
            series_id,
            point_count,
            min_ts,
            max_ts,
            ts_encoding,
            val_encoding,
            ts_data,
            val_data,
        );
        let block_offset = self.position;
        let block_size = block.serialized_size() as u32;

        // Write block
        let mut block_buf = Vec::with_capacity(block_size as usize);
        block.write_to(&mut block_buf)?;
        self.writer.write_all(&block_buf)?;
        self.position += block_size as u64;

        // Record index entry
        self.series_entries.push(SeriesIndexEntry::new(
            series_id,
            meta.metric_name.clone(),
            meta.labels.clone(),
            block_offset,
            block_size,
            point_count,
            min_ts,
            max_ts,
        ));

        Ok(())
    }

    /// Finishes writing the TSM file and returns a handle.
    ///
    /// This method:
    /// 1. Writes the series index
    /// 2. Rewrites the header with correct metadata
    /// 3. Calculates and writes the file CRC in the footer
    /// 4. Flushes and syncs the file
    ///
    /// # Errors
    ///
    /// Returns an error if any I/O operation fails.
    pub fn finish(mut self) -> Result<TsmFileHandle> {
        // Data section ends here
        let data_section_offset = HEADER_SIZE as u64;
        let data_section_size = self.position - data_section_offset;

        // Write series index
        let series_index_offset = self.position;
        let mut index = SeriesIndex::with_capacity(self.series_entries.len());
        for entry in &self.series_entries {
            index.insert(entry.clone());
        }
        let mut index_buf = Vec::new();
        index.write_to(&mut index_buf)?;
        self.writer.write_all(&index_buf)?;
        let series_index_size = index_buf.len() as u32;
        self.position += series_index_size as u64;

        // Create final header with correct values
        let header = TsmHeader::with_params(
            self.min_timestamp,
            self.max_timestamp,
            self.series_entries.len() as u32,
            self.compression,
            self.section_flags,
            self.level,
        );

        // Seek to beginning and overwrite header with final values
        self.writer.seek(SeekFrom::Start(0))?;
        header.write_to(&mut self.writer)?;

        // Now calculate CRC of the entire file content (header + data + index)
        // We need to re-read what we wrote
        self.writer.flush()?;

        // Seek to end to write footer placeholder
        self.writer.seek(SeekFrom::End(0))?;

        // Calculate file CRC by re-reading the file
        let file_crc32 = {
            let mut file = File::open(&self.path)?;
            let mut hasher = crc32fast::Hasher::new();
            let mut buffer = [0u8; 8192];
            loop {
                let n = std::io::Read::read(&mut file, &mut buffer)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            hasher.finalize()
        };

        // Write footer
        let footer = TsmFooter::with_params(
            series_index_offset,
            series_index_size,
            data_section_offset,
            data_section_size,
            self.total_point_count,
            file_crc32,
        );
        footer.write_to(&mut self.writer)?;

        // Flush and sync
        self.writer.flush()?;
        let file = self.writer.into_inner().map_err(|e| {
            std::io::Error::other(e.to_string())
        })?;
        file.sync_all()?;

        Ok(TsmFileHandle {
            path: self.path,
            header,
            footer,
        })
    }
}

/// TSM file reader.
///
/// Iterator for scanning data points within a time range.
///
/// This iterator lazily decompresses blocks as they are consumed,
/// providing memory-efficient scanning of large datasets.
pub struct TsmScanIterator<'a> {
    /// Reference to the reader.
    reader: &'a TsmReader,
    /// The time range to scan.
    range: TimeRange,
    /// Index entries that overlap with the range, sorted for iteration.
    entries: Vec<(SeriesId, &'a SeriesIndexEntry)>,
    /// Current position in the entries vector.
    entry_index: usize,
    /// Points from the current decompressed block.
    current_points: Vec<DataPoint>,
    /// Current position within current_points.
    point_index: usize,
}

impl<'a> TsmScanIterator<'a> {
    /// Creates a new scan iterator.
    fn new(reader: &'a TsmReader, range: TimeRange) -> Self {
        // Collect entries that overlap with the range
        let entries: Vec<(SeriesId, &SeriesIndexEntry)> = reader
            .index
            .iter()
            .filter(|(_, entry)| {
                // Keep entries that overlap with the range
                entry.max_ts >= range.start && entry.min_ts < range.end
            })
            .map(|(id, entry)| (*id, entry))
            .collect();

        Self {
            reader,
            range,
            entries,
            entry_index: 0,
            current_points: Vec::new(),
            point_index: 0,
        }
    }

    /// Loads the next block of data points.
    fn load_next_block(&mut self) -> Option<()> {
        while self.entry_index < self.entries.len() {
            let (_series_id, entry) = &self.entries[self.entry_index];
            self.entry_index += 1;

            // Try to read and decompress the block
            let block = match self.reader.read_block(entry) {
                Ok(b) => b,
                Err(_) => continue, // Skip on error, try next block
            };

            let points = match self.reader.decompress_block(&block) {
                Ok(p) => p,
                Err(_) => continue, // Skip on error, try next block
            };

            // Filter points by range and create DataPoints
            self.current_points = points
                .into_iter()
                .filter(|(ts, _)| self.range.contains(*ts))
                .map(|(ts, val)| {
                    DataPoint::new(
                        entry.metric_name.clone(),
                        entry.labels.clone(),
                        ts,
                        val,
                    )
                })
                .collect();

            self.point_index = 0;

            if !self.current_points.is_empty() {
                return Some(());
            }
        }
        None
    }
}

impl Iterator for TsmScanIterator<'_> {
    type Item = DataPoint;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have points in the current block, return the next one
        if self.point_index < self.current_points.len() {
            let point = self.current_points[self.point_index].clone();
            self.point_index += 1;
            return Some(point);
        }

        // Try to load the next block
        if self.load_next_block().is_some() {
            // Recursively call next to get the first point
            return self.next();
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Lower bound: remaining points in current block
        let current_remaining = self.current_points.len().saturating_sub(self.point_index);
        // Upper bound: unknown, as we don't know how many points are in remaining blocks
        (current_remaining, None)
    }
}

/// Opens and reads TSM files, providing:
/// - Series lookup by ID
/// - Time range scanning
/// - Block-level CRC verification
pub struct TsmReader {
    /// Path to the TSM file.
    path: PathBuf,
    /// The file header.
    header: TsmHeader,
    /// The file footer.
    footer: TsmFooter,
    /// The series index.
    index: SeriesIndex,
}

impl TsmReader {
    /// Opens a TSM file for reading.
    ///
    /// This method:
    /// 1. Reads and validates the header
    /// 2. Reads and validates the footer
    /// 3. Optionally verifies the file CRC
    /// 4. Loads the series index
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TSM file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be opened
    /// - The magic bytes are invalid
    /// - The version is unsupported
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Read header
        let header = TsmHeader::read_from(&mut reader)?;

        // Read footer (at end of file)
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let footer = TsmFooter::read_from(&mut reader)?;

        // Read series index
        reader.seek(SeekFrom::Start(footer.series_index_offset))?;
        let index = SeriesIndex::read_from(&mut reader)?;

        // Verify file CRC
        let calculated_crc = Self::calculate_file_crc(path, file_size - FOOTER_SIZE as u64)?;
        if calculated_crc != footer.file_crc32 {
            return Err(TsmError::ChecksumMismatch {
                expected: footer.file_crc32,
                actual: calculated_crc,
            });
        }

        Ok(Self {
            path: path.to_path_buf(),
            header,
            footer,
            index,
        })
    }

    /// Calculates the CRC32 of the file content (excluding footer).
    fn calculate_file_crc(path: &Path, size: u64) -> Result<u32> {
        let mut file = File::open(path)?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = [0u8; 8192];
        let mut remaining = size;

        while remaining > 0 {
            let to_read = remaining.min(buffer.len() as u64) as usize;
            let n = std::io::Read::read(&mut file, &mut buffer[..to_read])?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
            remaining -= n as u64;
        }

        Ok(hasher.finalize())
    }

    /// Returns the file header.
    pub fn header(&self) -> &TsmHeader {
        &self.header
    }

    /// Returns the file footer.
    pub fn footer(&self) -> &TsmFooter {
        &self.footer
    }

    /// Returns the series index.
    pub fn index(&self) -> &SeriesIndex {
        &self.index
    }

    /// Reads data for a specific series.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series ID to read
    ///
    /// # Returns
    ///
    /// Returns `None` if the series doesn't exist, otherwise returns
    /// the decompressed data points.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or decompression fails.
    pub fn read_series(&self, series_id: SeriesId) -> Result<Option<Vec<(Timestamp, f64)>>> {
        // Check index
        let entry = match self.index.get(series_id) {
            Some(e) => e,
            None => return Ok(None),
        };

        // Read and verify block
        let block = self.read_block(entry)?;

        // Decompress
        let points = self.decompress_block(&block)?;

        Ok(Some(points))
    }

    /// Reads a data block at the specified offset.
    fn read_block(&self, entry: &SeriesIndexEntry) -> Result<TsmDataBlock> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(entry.block_offset))?;
        TsmDataBlock::read_from(&mut reader)
    }

    /// Decompresses a data block using per-block encoding types.
    fn decompress_block(&self, block: &TsmDataBlock) -> Result<Vec<(Timestamp, f64)>> {
        use bitvec::prelude::*;

        // Check if both encodings are Gorilla (most common case)
        if block.ts_encoding == TimestampEncoding::DeltaOfDelta
            && block.val_encoding == ValueEncoding::GorillaXor
        {
            let ts_bitvec = BitVec::<u8, Msb0>::from_vec(block.ts_data.clone());
            let val_bitvec = BitVec::<u8, Msb0>::from_vec(block.val_data.clone());
            let compressed = CompressedBlock {
                timestamps: ts_bitvec,
                values: val_bitvec,
                count: block.point_count,
            };
            return Ok(compressed.decompress());
        }

        // Handle mixed or raw encodings
        // Decompress timestamps
        let timestamps: Vec<i64> = if block.ts_encoding == TimestampEncoding::DeltaOfDelta {
            let ts_bitvec = BitVec::<u8, Msb0>::from_vec(block.ts_data.clone());
            // Create a dummy values bitvec with zeros (we'll discard them)
            let val_bitvec = BitVec::<u8, Msb0>::from_vec(block.val_data.clone());
            let compressed = CompressedBlock {
                timestamps: ts_bitvec,
                values: val_bitvec,
                count: block.point_count,
            };
            compressed.decompress().into_iter().map(|(ts, _)| ts).collect()
        } else {
            // Raw format: read timestamps directly
            let mut cursor = std::io::Cursor::new(&block.ts_data);
            let mut timestamps = Vec::with_capacity(block.point_count as usize);
            for _ in 0..block.point_count {
                let mut buf8 = [0u8; 8];
                cursor.read_exact(&mut buf8)?;
                timestamps.push(i64::from_le_bytes(buf8));
            }
            timestamps
        };

        // Decompress values
        let values: Vec<f64> = if block.val_encoding == ValueEncoding::GorillaXor {
            let ts_bitvec = BitVec::<u8, Msb0>::from_vec(block.ts_data.clone());
            let val_bitvec = BitVec::<u8, Msb0>::from_vec(block.val_data.clone());
            let compressed = CompressedBlock {
                timestamps: ts_bitvec,
                values: val_bitvec,
                count: block.point_count,
            };
            compressed.decompress().into_iter().map(|(_, val)| val).collect()
        } else {
            // Raw format: read values directly
            let mut cursor = std::io::Cursor::new(&block.val_data);
            let mut values = Vec::with_capacity(block.point_count as usize);
            for _ in 0..block.point_count {
                let mut buf8 = [0u8; 8];
                cursor.read_exact(&mut buf8)?;
                values.push(f64::from_le_bytes(buf8));
            }
            values
        };

        // Combine timestamps and values
        Ok(timestamps.into_iter().zip(values).collect())
    }

    /// Scans for data points within a time range.
    ///
    /// Returns a lazy iterator that decompresses blocks on-demand as points
    /// are consumed. This is memory-efficient for large scans where only a
    /// portion of results may be needed.
    ///
    /// # Arguments
    ///
    /// * `range` - The time range to scan (start inclusive, end exclusive)
    ///
    /// # Returns
    ///
    /// A lazy iterator over data points within the range. Blocks are
    /// decompressed one at a time as the iterator advances.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let reader = TsmReader::open(path)?;
    /// for point in reader.scan(TimeRange::new(start, end)) {
    ///     println!("{}: {}", point.timestamp, point.value);
    /// }
    /// ```
    pub fn scan(&self, range: TimeRange) -> impl Iterator<Item = DataPoint> + '_ {
        TsmScanIterator::new(self, range)
    }

    /// Verifies the file checksum.
    ///
    /// # Returns
    ///
    /// Returns `true` if the checksum is valid.
    pub fn verify_file_checksum(&self) -> Result<bool> {
        let file = File::open(&self.path)?;
        let file_size = file.metadata()?.len();
        let calculated = Self::calculate_file_crc(&self.path, file_size - FOOTER_SIZE as u64)?;
        Ok(calculated == self.footer.file_crc32)
    }

    /// Verifies the checksum of a specific block.
    ///
    /// # Arguments
    ///
    /// * `series_id` - The series ID of the block to verify
    ///
    /// # Returns
    ///
    /// Returns `true` if the checksum is valid.
    pub fn verify_block_checksum(&self, series_id: SeriesId) -> Result<bool> {
        let entry = match self.index.get(series_id) {
            Some(e) => e,
            None => return Err(TsmError::SeriesNotFound(series_id)),
        };

        let block = self.read_block(entry)?;
        Ok(block.verify_crc())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_size() {
        let header = TsmHeader::default();
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), HEADER_SIZE);
    }

    #[test]
    fn test_header_roundtrip() {
        let header = TsmHeader::with_params(
            1000,
            2000,
            100,
            CompressionType::Gorilla,
            SectionFlags::from_u8(SectionFlags::HOT | SectionFlags::META),
            1,
        );

        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let read_header = TsmHeader::read_from(&mut cursor).unwrap();

        assert_eq!(header, read_header);
    }

    #[test]
    fn test_header_invalid_magic() {
        let mut buf = vec![0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(b"XXXX");

        let mut cursor = Cursor::new(buf);
        let result = TsmHeader::read_from(&mut cursor);

        assert!(matches!(result, Err(TsmError::InvalidMagic(_))));
    }

    #[test]
    fn test_footer_size() {
        let footer = TsmFooter::default();
        let mut buf = Vec::new();
        footer.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), FOOTER_SIZE);
    }

    #[test]
    fn test_footer_roundtrip() {
        let footer = TsmFooter::with_params(1000, 500, 32, 968, 50000, 0x12345678);

        let mut buf = Vec::new();
        footer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let read_footer = TsmFooter::read_from(&mut cursor).unwrap();

        assert_eq!(footer, read_footer);
    }

    #[test]
    fn test_footer_invalid_magic() {
        let mut footer = TsmFooter::default();
        footer.magic_reverse = *b"XXXX";

        let mut buf = Vec::new();
        footer.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let result = TsmFooter::read_from(&mut cursor);

        assert!(matches!(result, Err(TsmError::InvalidMagic(_))));
    }

    #[test]
    fn test_section_flags() {
        let mut flags = SectionFlags::new();
        assert!(!flags.is_hot());
        assert!(!flags.is_cold());
        assert!(!flags.is_meta());

        flags.set_hot();
        assert!(flags.is_hot());
        assert!(!flags.is_cold());

        flags.set_meta();
        assert!(flags.is_hot());
        assert!(flags.is_meta());
    }

    #[test]
    fn test_compression_type_from_u8() {
        assert_eq!(CompressionType::from_u8(0), Some(CompressionType::Raw));
        assert_eq!(CompressionType::from_u8(1), Some(CompressionType::Gorilla));
        assert_eq!(CompressionType::from_u8(2), None);
    }

    #[test]
    fn test_series_index_entry_roundtrip() {
        let entry = SeriesIndexEntry::new(
            12345,
            "cpu.usage".to_string(),
            vec![
                ("host".to_string(), "server1".to_string()),
                ("region".to_string(), "us-east".to_string()),
            ],
            1000,
            500,
            1000,
            1000000,
            2000000,
        );

        let mut buf = Vec::new();
        entry.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let read_entry = SeriesIndexEntry::read_from(&mut cursor).unwrap();

        assert_eq!(entry.series_id, read_entry.series_id);
        assert_eq!(entry.metric_name, read_entry.metric_name);
        assert_eq!(entry.labels, read_entry.labels);
        assert_eq!(entry.block_offset, read_entry.block_offset);
        assert_eq!(entry.block_size, read_entry.block_size);
        assert_eq!(entry.point_count, read_entry.point_count);
        assert_eq!(entry.min_ts, read_entry.min_ts);
        assert_eq!(entry.max_ts, read_entry.max_ts);
    }

    #[test]
    fn test_bloom_filter() {
        let mut bloom = BloomFilter::new(100);

        // Insert some values
        bloom.insert(1);
        bloom.insert(100);
        bloom.insert(1000);

        // Check inserted values
        assert!(bloom.maybe_contains(1));
        assert!(bloom.maybe_contains(100));
        assert!(bloom.maybe_contains(1000));

        // Check non-inserted values (may have false positives)
        // This tests that at least some non-inserted values are correctly rejected
        let mut false_negatives = 0;
        for i in 2..100 {
            if i != 100 && bloom.maybe_contains(i) {
                false_negatives += 1;
            }
        }
        // With 100 items capacity and only 3 inserted, false positive rate should be low
        assert!(false_negatives < 50); // Should reject most non-inserted values
    }

    #[test]
    fn test_bloom_filter_roundtrip() {
        let mut bloom = BloomFilter::new(100);
        bloom.insert(1);
        bloom.insert(100);
        bloom.insert(1000);

        let mut buf = Vec::new();
        bloom.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let read_bloom = BloomFilter::read_from(&mut cursor).unwrap();

        assert!(read_bloom.maybe_contains(1));
        assert!(read_bloom.maybe_contains(100));
        assert!(read_bloom.maybe_contains(1000));
    }

    #[test]
    fn test_series_index() {
        let mut index = SeriesIndex::with_capacity(100);

        let entry1 = SeriesIndexEntry::new(
            1,
            "metric1".to_string(),
            vec![],
            100,
            50,
            100,
            1000,
            2000,
        );
        let entry2 = SeriesIndexEntry::new(
            2,
            "metric2".to_string(),
            vec![],
            200,
            60,
            200,
            3000,
            4000,
        );

        index.insert(entry1);
        index.insert(entry2);

        assert_eq!(index.len(), 2);
        assert!(index.get(1).is_some());
        assert!(index.get(2).is_some());
        assert!(index.get(3).is_none());

        assert!(index.maybe_contains(1));
        assert!(index.maybe_contains(2));
    }

    #[test]
    fn test_series_index_roundtrip() {
        let mut index = SeriesIndex::with_capacity(100);

        index.insert(SeriesIndexEntry::new(
            1,
            "metric1".to_string(),
            vec![("key".to_string(), "value".to_string())],
            100,
            50,
            100,
            1000,
            2000,
        ));
        index.insert(SeriesIndexEntry::new(
            2,
            "metric2".to_string(),
            vec![],
            200,
            60,
            200,
            3000,
            4000,
        ));

        let mut buf = Vec::new();
        index.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let read_index = SeriesIndex::read_from(&mut cursor).unwrap();

        assert_eq!(index.len(), read_index.len());
        assert!(read_index.get(1).is_some());
        assert!(read_index.get(2).is_some());
    }

    #[test]
    fn test_tsm_data_block_crc() {
        let block = TsmDataBlock::new(
            12345,
            100,
            1000000,
            2000000,
            TimestampEncoding::DeltaOfDelta,
            ValueEncoding::GorillaXor,
            vec![1, 2, 3, 4],    // ts_data
            vec![5, 6, 7, 8],    // val_data
        );

        assert!(block.verify_crc());
    }

    #[test]
    fn test_tsm_data_block_roundtrip() {
        let block = TsmDataBlock::new(
            12345,
            100,
            1000000,
            2000000,
            TimestampEncoding::DeltaOfDelta,
            ValueEncoding::GorillaXor,
            vec![1, 2, 3, 4, 5],     // ts_data
            vec![6, 7, 8, 9, 10],    // val_data
        );

        let mut buf = Vec::new();
        block.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let read_block = TsmDataBlock::read_from(&mut cursor).unwrap();

        assert_eq!(block.series_id, read_block.series_id);
        assert_eq!(block.point_count, read_block.point_count);
        assert_eq!(block.min_ts, read_block.min_ts);
        assert_eq!(block.max_ts, read_block.max_ts);
        assert_eq!(block.ts_encoding, read_block.ts_encoding);
        assert_eq!(block.val_encoding, read_block.val_encoding);
        assert_eq!(block.ts_data, read_block.ts_data);
        assert_eq!(block.val_data, read_block.val_data);
        assert_eq!(block.block_crc32, read_block.block_crc32);
    }

    #[test]
    fn test_tsm_data_block_corrupted_crc() {
        let block = TsmDataBlock::new(
            12345,
            100,
            1000000,
            2000000,
            TimestampEncoding::Raw,
            ValueEncoding::Raw,
            vec![1, 2, 3],  // ts_data
            vec![4, 5],     // val_data
        );

        let mut buf = Vec::new();
        block.write_to(&mut buf).unwrap();

        // Corrupt the data (after fixed header: 8+4+8+8+1+1+4 = 34 bytes)
        let data_offset = 34;
        buf[data_offset] ^= 0xFF;

        let mut cursor = Cursor::new(buf);
        let result = TsmDataBlock::read_from(&mut cursor);

        assert!(matches!(result, Err(TsmError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_tsm_writer_reader_roundtrip() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.skulk");

        // Create test data
        let series_id = 12345u64;
        let meta = SeriesMeta::new("cpu.usage", vec![("host".to_string(), "server1".to_string())]);
        let mut points = std::collections::BTreeMap::new();
        for i in 0..100 {
            points.insert(1000000 + i * 1000, 50.0 + (i as f64) * 0.1);
        }

        // Write
        {
            let mut writer = TsmWriter::new(&file_path).unwrap();
            writer.write_series(series_id, &meta, &points).unwrap();
            let handle = writer.finish().unwrap();

            assert_eq!(handle.header.series_count, 1);
            assert_eq!(handle.footer.total_point_count, 100);
        }

        // Read
        {
            let reader = TsmReader::open(&file_path).unwrap();

            assert_eq!(reader.header().series_count, 1);
            assert_eq!(reader.footer().total_point_count, 100);

            let read_points = reader.read_series(series_id).unwrap().unwrap();
            assert_eq!(read_points.len(), 100);

            for (i, (ts, val)) in read_points.iter().enumerate() {
                assert_eq!(*ts, 1000000 + (i as i64) * 1000);
                assert!((val - (50.0 + (i as f64) * 0.1)).abs() < f64::EPSILON);
            }
        }
    }

    #[test]
    fn test_tsm_writer_multiple_series() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_multi.skulk");

        // Create test data for multiple series
        let series1_id = 1u64;
        let series2_id = 2u64;
        let meta1 = SeriesMeta::new("cpu.usage", vec![("host".to_string(), "server1".to_string())]);
        let meta2 = SeriesMeta::new("memory.usage", vec![("host".to_string(), "server1".to_string())]);

        let mut points1 = std::collections::BTreeMap::new();
        let mut points2 = std::collections::BTreeMap::new();
        for i in 0..50 {
            points1.insert(1000000 + i * 1000, 50.0 + (i as f64) * 0.1);
            points2.insert(2000000 + i * 1000, 70.0 + (i as f64) * 0.2);
        }

        // Write
        {
            let mut writer = TsmWriter::new(&file_path).unwrap();
            writer.write_series(series1_id, &meta1, &points1).unwrap();
            writer.write_series(series2_id, &meta2, &points2).unwrap();
            let handle = writer.finish().unwrap();

            assert_eq!(handle.header.series_count, 2);
            assert_eq!(handle.footer.total_point_count, 100);
        }

        // Read
        {
            let reader = TsmReader::open(&file_path).unwrap();

            assert_eq!(reader.header().series_count, 2);

            let read_points1 = reader.read_series(series1_id).unwrap().unwrap();
            let read_points2 = reader.read_series(series2_id).unwrap().unwrap();

            assert_eq!(read_points1.len(), 50);
            assert_eq!(read_points2.len(), 50);

            // Non-existent series
            assert!(reader.read_series(999).unwrap().is_none());
        }
    }

    #[test]
    fn test_tsm_reader_scan() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_scan.skulk");

        // Create test data
        let series_id = 1u64;
        let meta = SeriesMeta::new("cpu.usage", vec![]);
        let mut points = std::collections::BTreeMap::new();
        for i in 0..100 {
            points.insert(i * 1000, (i as f64) * 1.0);
        }

        // Write
        {
            let mut writer = TsmWriter::new(&file_path).unwrap();
            writer.write_series(series_id, &meta, &points).unwrap();
            writer.finish().unwrap();
        }

        // Scan
        {
            let reader = TsmReader::open(&file_path).unwrap();

            // Scan for middle range
            let range = TimeRange::new(25000, 75000);
            let results: Vec<_> = reader.scan(range).collect();

            // Should get points from 25 to 74 (50 points)
            assert_eq!(results.len(), 50);

            for point in &results {
                assert!(point.timestamp >= 25000);
                assert!(point.timestamp < 75000);
            }
        }
    }

    #[test]
    fn test_tsm_verify_checksums() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_crc.skulk");

        // Create test data
        let series_id = 1u64;
        let meta = SeriesMeta::new("test.metric", vec![]);
        let mut points = std::collections::BTreeMap::new();
        for i in 0..10 {
            points.insert(i * 1000, (i as f64) * 1.0);
        }

        // Write
        {
            let mut writer = TsmWriter::new(&file_path).unwrap();
            writer.write_series(series_id, &meta, &points).unwrap();
            writer.finish().unwrap();
        }

        // Verify checksums
        {
            let reader = TsmReader::open(&file_path).unwrap();
            assert!(reader.verify_file_checksum().unwrap());
            assert!(reader.verify_block_checksum(series_id).unwrap());
        }
    }

    #[test]
    fn test_tsm_corrupted_file_detected() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_corrupt.skulk");

        // Create test data
        let series_id = 1u64;
        let meta = SeriesMeta::new("test.metric", vec![]);
        let mut points = std::collections::BTreeMap::new();
        for i in 0..10 {
            points.insert(i * 1000, (i as f64) * 1.0);
        }

        // Write
        {
            let mut writer = TsmWriter::new(&file_path).unwrap();
            writer.write_series(series_id, &meta, &points).unwrap();
            writer.finish().unwrap();
        }

        // Corrupt the file (modify data section)
        {
            let mut contents = std::fs::read(&file_path).unwrap();
            // Corrupt somewhere in the data section (after header)
            contents[HEADER_SIZE + 10] ^= 0xFF;
            std::fs::write(&file_path, &contents).unwrap();
        }

        // Should fail to open due to CRC mismatch
        let result = TsmReader::open(&file_path);
        assert!(matches!(result, Err(TsmError::ChecksumMismatch { .. })));
    }
}
