# Changelog

## [0.3.0] - 2026-07-28

### Breaking

- Replaced the v0.2 single-value TSM/Gorilla storage format with wide,
  multi-field Arrow/Parquet storage.
- Removed the legacy `DataPoint` and single-value compatibility path.
- v0.2 TSM and WAL files are rejected. No in-place reader or migration
  tool is provided; export with v0.2 and re-ingest into a new v0.3 data root.

### Added

- Five-type wide rows whose series identity is measurement plus tags.
- Pure-Rust BROTLI q5 Parquet persistence, sparse columns, typed readback, and
  atomic manifest publication.
- Durable batch WAL acknowledgement, crash recovery, single-writer locking,
  compaction/deduplication, hourly partitions, and persisted retention policy.
- HTTP-independent Line Protocol, Prometheus Remote Write v1 float-sample, and
  canonical/single-point JSON decoders behind one bounded ingest service.
- Request, row, expanded-size, buffer, and WAL limits with explicit
  backpressure and correlated partial-success results.

### Known Limitations

- Remote Write v2, metadata, exemplars, and native histograms are unsupported
  and explicitly rejected.
- HTTP serving and query execution remain scheduled for v0.6 and v0.4.
- Fixed end-to-end ingest throughput and p99 latency targets are not yet met;
  measurements and unchanged targets are recorded in
  `crates/skulk/benches/INGEST_BASELINE.md`.

## [0.2.0] - 2025-12-23

### Added
- Retention/TTL manager with DropPartition WAL entries and safe LSN tracking.
- Time-based partition layout and lifecycle management.
- TSM compaction strategy with duplicate resolution.
- WAL truncation integration and DropPartition recovery handling.
- Lifecycle integration tests and API documentation updates.

### Changed
- TSM file format v3 with block-level max_lsn metadata.
- WAL v2 fixed-size entries for data and DropPartition operations.
