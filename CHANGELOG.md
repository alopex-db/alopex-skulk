# Changelog

## [0.3.1] - 2026-07-29

### Fixed

- Durable ingest throughput: ~3-4x on Remote Write -> WAL ACK
  (34-40K -> 95-159K samples/s) and ~2-2.7x on Line Protocol -> WAL ACK
  (34-38K -> 64-107K points/s) on the fixed 10K-point baseline workload.
  Line Protocol decode alone roughly doubled (119-147K -> 225-325K pts/s).
  Root causes removed: per-row deep clones on the WAL path, per-row write
  syscalls, WAL entry residency in memory, per-batch re-cloning of pending
  rows during validation, ingest-time Arrow column building whose output
  was discarded, and repeated series-key construction in the LP decoder.
- WAL checkpoints now stream the retained suffix from the synced log file
  (same temp+fsync+rename+dirsync atomicity) instead of rewriting from
  memory-resident entries.

### Changed (behavior/API, no on-disk format change)

- `Wal::recovered_entries()` returns the open-time replay snapshot only;
  live appends are no longer retained in memory.
- Entry-based append APIs (`Wal::append_durable`/`append_buffered` taking
  `&WalEntry`) were replaced by borrow-based `append_durable_row`/
  `append_buffered_row`/`append_batch`.
- `MeasurementBuffer::append` takes `&SequencedRow`; ingest-time buffering
  now tracks lightweight per-measurement state (`MeasurementState`) and
  Arrow columns are built only at flush.
- v0.3.0-written WAL/Parquet/manifest files are read unchanged; frame
  encoding is byte-identical.

### Known Limitations

- The published 500K pts/s Line Protocol and p99 <10 ms targets remain
  unmet; reaching them requires a row-representation generation change
  (column interning, WAL name dictionary = format revision) tracked for
  v0.4+ / the event-store design. Measurements and gates are recorded in
  `crates/skulk/benches/INGEST_BASELINE.md`.


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
