//! Skulk - Alopex Time Series Storage Engine
//!
//! This crate provides the core storage primitives for the Alopex Skulk time series database.
//!
//! # Components
//!
//! - [`TimeSeriesMemTable`]: Time-partition aware in-memory buffer
//! - [`CompressedBlock`]: Gorilla compression for time series data
//! - [`TsmWriter`] / [`TsmReader`]: TSM file format I/O
//!
//! # Example
//!
//! ```rust,ignore
//! use skulk::tsm::{TimeSeriesMemTable, TimePartition, DataPoint};
//!
//! // Create a MemTable for a specific time partition
//! let partition = TimePartition::new(start_ts, Duration::from_secs(3600));
//! let mut memtable = TimeSeriesMemTable::new(partition, Default::default());
//!
//! // Insert data points
//! memtable.insert(&DataPoint {
//!     metric: "cpu.usage".to_string(),
//!     labels: vec![("host".to_string(), "server1".to_string())],
//!     timestamp: now_ns,
//!     value: 0.75,
//! })?;
//!
//! // Flush to TSM file when threshold reached
//! if memtable.should_flush() {
//!     memtable.flush(path).await?;
//! }
//! ```

#![deny(missing_docs)]

pub mod error;
pub mod tsm;
pub mod wal;

pub use error::{Result, TsmError};
pub use tsm::{
    CompressedBlock, DataPoint, SeriesId, SeriesMeta, TimePartition, TimeRange, Timestamp,
};
pub use wal::{Wal, WalConfig, WalEntry, SyncMode};
