//! Error and Result types for Skulk TSM operations.

use crate::tsm::SeriesId;
use std::io;
use thiserror::Error;

/// A convenience `Result` type for Skulk operations.
pub type Result<T> = std::result::Result<T, TsmError>;

/// The error type for TSM operations.
#[derive(Debug, Error)]
pub enum TsmError {
    /// Invalid magic bytes in TSM file header.
    #[error("Invalid magic bytes: expected ATSM, got {0:?}")]
    InvalidMagic([u8; 4]),

    /// Unsupported TSM file format version.
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u16),

    /// File checksum does not match expected value.
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Expected CRC32 checksum.
        expected: u32,
        /// Actual computed CRC32 checksum.
        actual: u32,
    },

    /// Requested series was not found in the TSM file.
    #[error("Series not found: {0}")]
    SeriesNotFound(SeriesId),

    /// Error during compression.
    #[error("Compression error: {0}")]
    CompressionError(String),

    /// Error during decompression.
    #[error("Decompression error: {0}")]
    DecompressionError(String),

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),

    /// MemTable has reached capacity and cannot accept more data.
    #[error("MemTable is full, cannot insert")]
    MemTableFull,

    /// Data point timestamp does not belong to the MemTable's partition.
    #[error("Partition mismatch: point timestamp {point_ts} not in partition [{start}, {end})")]
    PartitionMismatch {
        /// Timestamp of the data point.
        point_ts: i64,
        /// Start timestamp of the partition (inclusive).
        start: i64,
        /// End timestamp of the partition (exclusive).
        end: i64,
    },
}
