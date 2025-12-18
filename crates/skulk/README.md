# Skulk

Alopex Skulk - High-performance Time Series Storage Engine for Rust.

[![Crates.io](https://img.shields.io/crates/v/skulk.svg)](https://crates.io/crates/skulk)
[![Documentation](https://docs.rs/skulk/badge.svg)](https://docs.rs/skulk)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](LICENSE)

## Features

- **Time Series Optimized**: Designed specifically for time-stamped data
- **Columnar Storage**: TSM (Time-Structured Merge Tree) file format
- **Memory Efficient**: Adaptive compression with run-length encoding
- **High Throughput**: Optimized for high-volume write workloads
- **Integration Ready**: Built on top of `alopex-core`

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
skulk = "0.1"
```

## Architecture

Skulk implements a TSM-based storage engine with:

- **MemTable**: In-memory buffer for recent writes
- **Partitions**: Time-based data partitioning
- **TSM Files**: Columnar storage format with block-level compression
- **Compaction**: Background merge operations for optimal read performance

## Usage

```rust
use skulk::tsm::PartitionManager;

// Create a partition manager
let manager = PartitionManager::new(config)?;

// Write time series data
manager.write(measurement, timestamp, value)?;

// Query data
let results = manager.query(measurement, time_range)?;
```

## Requirements

- Rust 1.82.0 or later (MSRV)
- `alopex-core` 0.3.0 or later

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Related Projects

- [alopex-core](https://crates.io/crates/alopex-core) - Core storage engine
- [alopex-chirps](https://crates.io/crates/alopex-chirps) - Distributed cluster coordination
