# Changelog

## [0.4.0] - 2026-07-30

### Added

- HTTP-independent `QueryEngine` entry points for PromQL instant/range,
  SQL-TS, pre-built logical plans, and bounded series/label metadata
  enumeration. Results are typed Arrow `RecordBatch` streams.
- PromQL selectors, range vectors, offsets, scalar/vector arithmetic, required
  range functions, classic-bucket `histogram_quantile`, and
  `sum`/`avg`/`min`/`max`/`count` grouping.
- SQL-TS projection, aliases, field predicates, ordering, limits, standard
  aggregates, and `TIME_BUCKET`, `RATE`, `DELTA`, `DERIVATIVE`, `FIRST`, and
  `LAST`, including floor bucketing for negative timestamps.
- A public `StorageReader` contract with pending + durable
  latest-write-wins reads, Parquet time/tag pruning, field projection,
  composite schema validation, decode accounting, and bounded metadata scans.
- Conservative resolution selection based on full-range coverage, exact
  alignment, and declared semantic capabilities. The v0.4 storage catalog
  contains raw data; rollup production remains a v0.5 concern.
- A complete out-of-order admission policy: one-hour default wall-clock
  window, reject/warn/drop actions, counted warning/drop outcomes, and an
  explicit backfill override.
- Fixed 10-million-point query benchmark and footprint probes. The measured
  24-hour `TIME_BUCKET + AVG` p99 is 41.782 ms over 100 runs, passing the
  `<100 ms` release gate; time pruning decodes zero rows and the absent-tag
  workload prunes 100% of considered row groups.

### Changed

- Crate version is now 0.4.0. The v0.3 WAL, manifest, and Parquet formats are
  unchanged and remain readable.
- Default features are `promql` and `sql-ts`. Both syntax parsers are provided
  by the Alopex Nim parser contract `0.2.0`; Rust performs AST mapping,
  validation, planning, and execution.
- `--no-default-features` remains a pure-Rust embedded profile with no Nim
  artifact, `cc`, or `*-sys` dependency. Its measured release probe is
  3,669,104 bytes, below the 6,000,000-byte ceiling.

### Known Limitations

- PromQL does not yet support vector-to-vector matching modifiers
  (`on`/`ignoring`/`group_left`), subqueries, `@`, negative offset,
  comparison/`bool`, `topk`/`bottomk`/`quantile`, or label manipulation
  functions. Unsupported semantics return an explicit error.
- SQL-TS does not support JOIN, subqueries, window functions, DDL,
  INSERT/UPDATE/DELETE, or HAVING. HTTP query endpoints remain scheduled for
  v0.6.
- Schema is composed lazily from active Parquet files and pending measurement
  state. Conflicting column types or tag/field roles reject the query;
  v0.4 does not add a persistent schema registry.
- Only `x86_64-unknown-linux-gnu` is currently vendored in this source tree.
  Other Linux/macOS/Windows targets require a matching parser artifact via
  `SKULK_NIM_PARSER_LIB_DIR`, and final applications must follow the documented
  rpath/DLL loading convention.
- Durable 10K-point ingest p99 is 66.705 ms against the unchanged `<10 ms`
  stretch goal. This is not the v0.4 query release gate and remains open; see
  `crates/skulk/benches/QUERY_BASELINE.md`.

## [0.3.1] - 2026-07-29

### Fixed

- Durable ingest throughput on the fixed 10K-point baseline workload
  (fully-quiet window, median of 3 runs): Line Protocol -> WAL ACK ~4.9x
  (34-38K -> 180.1K pts/s, all runs above the 150K gate) and Remote
  Write -> WAL ACK ~3.9x (34-40K -> 144.4K samples/s).
  Achieved via single-walk row admission (influxdb3 validator
  architecture), type-state qualified batches, Arc-shared series
  identity, and an escape-free Line Protocol fast path with reference
  fallback (differential-proptest equivalence).
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
- `WideRow` and `SequencedRow` no longer derive serde Serialize/
  Deserialize (they are not persisted via serde; recorded here as a
  public-API change omitted from the original 0.3.1 notes).

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
