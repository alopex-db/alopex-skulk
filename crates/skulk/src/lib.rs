//! Skulk - Alopex Time Series Storage Engine
//!
//! This crate provides the core storage primitives for the Alopex Skulk time series database,
//! implementing a high-performance columnar storage engine optimized for time series workloads.
//!
//! # Architecture Overview
//!
//! Skulk uses a Log-Structured Merge-tree (LSM) inspired architecture with time-based partitioning:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                          Write Path                                  │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  Client → WAL (durability) → MemTable (in-memory) → TSM (on-disk)   │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Components
//!
//! - **[`tsm::TimeSeriesMemTable`]**: Time-partition aware in-memory buffer with
//!   efficient series-based storage using BTreeMap for sorted timestamp access.
//!
//! - **[`tsm::PartitionManager`]**: Routes data points to appropriate time partitions
//!   and manages flush lifecycle with oldest-first policy.
//!
//! - **[`tsm::CompressedBlock`]**: Gorilla compression achieving 10-15x compression
//!   ratios for typical time series data through delta-of-delta timestamps and
//!   XOR-based value encoding.
//!
//! - **[`tsm::TsmWriter`] / [`tsm::TsmReader`]**: File I/O for the TSM (Time Series Merge)
//!   format with CRC32 integrity verification and Bloom filter for efficient lookups.
//!
//! - **[`wal::Wal`]**: Write-Ahead Log for crash recovery with configurable sync modes
//!   (None, Fsync, Fdatasync).
//!
//! # File Format (TSM)
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │  File Header (32 bytes)                                              │
//! │  - Magic: "ATSM", Version: 2, Timestamps, Series Count, Flags       │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  Data Blocks (per series)                                            │
//! │  - Series ID, Gorilla-compressed timestamps & values, Block CRC     │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  Series Index with Bloom Filter                                      │
//! │  - Sorted index entries with block offsets and metadata             │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  Footer (48 bytes)                                                   │
//! │  - Offsets, sizes, file CRC32, reverse magic "MSTA"                 │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Durability Guarantees
//!
//! The write path follows a strict durability contract:
//!
//! 1. **WAL First**: Data is written to WAL and fsync'd before MemTable mutation
//! 2. **Atomic Flush**: MemTable flush uses tmp→fsync→rename→fsync pattern
//! 3. **Crash Recovery**: All committed data can be recovered from WAL
//!
//! # Example: Basic Usage
//!
//! ```rust,ignore
//! use skulk::tsm::{TimeSeriesMemTable, TimePartition, DataPoint};
//! use skulk::wal::{Wal, WalConfig, SyncMode};
//! use std::time::Duration;
//!
//! // Create WAL for durability
//! let wal_config = WalConfig {
//!     batch_size: 100,
//!     batch_timeout: Duration::from_millis(10),
//!     segment_size: 64 * 1024 * 1024,
//!     sync_mode: SyncMode::Fsync,
//! };
//! let mut wal = Wal::new("./wal", wal_config)?;
//!
//! // Create MemTable for a 1-hour partition
//! let partition = TimePartition::new(0, Duration::from_secs(3600));
//! let mut memtable = TimeSeriesMemTable::new(partition);
//!
//! // Insert with WAL durability
//! let point = DataPoint::new(
//!     "cpu.usage",
//!     vec![("host".to_string(), "server1".to_string())],
//!     1234567890_000_000_000, // nanoseconds
//!     0.75,
//! );
//! memtable.insert_with_wal(&point, &mut wal)?;
//!
//! // Flush to TSM file
//! let handle = memtable.flush("./data/partition.skulk")?;
//! println!("Wrote {} points", handle.footer.total_point_count);
//! ```
//!
//! # Example: Multi-Partition Management
//!
//! ```rust,ignore
//! use skulk::tsm::{PartitionManager, PartitionManagerConfig, DataPoint, TimeRange};
//! use std::time::Duration;
//!
//! // Configure partition manager
//! let config = PartitionManagerConfig::default()
//!     .with_partition_duration(Duration::from_secs(3600))
//!     .with_size_threshold(64 * 1024 * 1024)
//!     .with_max_active_partitions(4);
//!
//! let mut manager = PartitionManager::new(config);
//!
//! // Insert data - automatically routed to correct partition
//! for i in 0..10000 {
//!     let point = DataPoint::new("metric", vec![], i * 1_000_000, i as f64);
//!     manager.insert(&point)?;
//! }
//!
//! // Scan across partitions
//! let range = TimeRange::new(0, 5000 * 1_000_000);
//! for point in manager.scan(range) {
//!     println!("{}: {}", point.timestamp, point.value);
//! }
//!
//! // Flush oldest partition when threshold reached
//! if let Some(handle) = manager.flush_oldest("./data")? {
//!     println!("Flushed partition to {:?}", handle.path);
//! }
//! ```
//!
//! # Example: Crash Recovery
//!
//! ```rust,ignore
//! use skulk::wal::Wal;
//! use skulk::tsm::{TimeSeriesMemTable, TimePartition, DataPoint};
//! use std::time::Duration;
//!
//! // Recover entries from WAL after crash
//! let recovered_entries = Wal::recover("./wal")?;
//! println!("Recovered {} entries", recovered_entries.len());
//!
//! // Reconstruct MemTable from recovered entries
//! let partition = TimePartition::new(0, Duration::from_secs(3600));
//! let mut memtable = TimeSeriesMemTable::new(partition);
//!
//! for entry in recovered_entries {
//!     // Rebuild DataPoint from entry (requires metadata lookup)
//!     let point = DataPoint::new(
//!         "recovered_metric", // from metadata store
//!         vec![],
//!         entry.timestamp,
//!         entry.value,
//!     );
//!     memtable.insert(&point)?;
//! }
//! ```
//!
//! # Performance Characteristics
//!
//! - **Compression**: 10-15x ratio with Gorilla encoding
//! - **Write Latency**: <1ms for buffered writes, ~10ms with fsync
//! - **Scan Throughput**: >1M points/sec for sequential reads
//! - **Memory Usage**: ~16 bytes per point in MemTable
//!
//! # Feature Flags
//!
//! Currently no optional features. All functionality is included by default.

#![deny(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

pub mod error;
pub mod tsm;
pub mod wal;

pub use error::{Result, TsmError};
pub use tsm::{
    CompressedBlock, DataPoint, SeriesId, SeriesMeta, TimePartition, TimeRange, Timestamp,
};
pub use wal::{SyncMode, Wal, WalConfig, WalEntry};
