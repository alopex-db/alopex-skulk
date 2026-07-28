# Alopex Skulk

Alopex Skulk is an embedded, append-only time-series storage and ingest core
written in Rust. Version 0.3 stores wide, multi-field rows in Arrow memory
batches and Parquet files while keeping acknowledged writes recoverable through
a local WAL.

## Current v0.3 Scope

- Wide rows: one measurement and tag set can hold multiple float, integer,
  unsigned, boolean, and string fields.
- Columnar storage: Arrow in memory and Parquet with pure-Rust BROTLI q5 on disk.
- Durability: batch WAL sync before ACK, manifest-fenced recovery, atomic file
  publication, torn-tail isolation, and single-writer data-root locking.
- Lifecycle: hourly partitions, persisted measurement retention policies,
  idempotent TTL expiry, and DataFusion-free compaction with last-ingest-wins
  deduplication.
- Ingest: HTTP-independent decoders for InfluxDB Line Protocol, Prometheus
  Remote Write v1 float samples, and structured JSON.

HTTP endpoints, PromQL/SQL-TS execution, downsampling, alerts, and distributed
operation are future milestones; they are not part of the v0.3 crate.

## Breaking Change from v0.2

Version 0.3 replaces the v0.2 single-value TSM/Gorilla format and APIs. It does
not read or migrate v0.2 TSM or WAL files; legacy magic is rejected before
the source is modified. There is no migration tool. Export data with v0.2 and
re-ingest it into a new v0.3 data root.

The series identity is now `measurement + tags`; field names are columns and do
not create separate series. The decision history is recorded in the
[public technical specification §1.4](https://github.com/alopex-db/docs/blob/aab328480b21bc66121b85b4e0e1218e9b3d0d68/specs/alopex-skulk-technical-spec.md).

## Ingest Protocols

| Protocol | v0.3 behavior |
| --- | --- |
| Line Protocol | Multi-field wide rows, all five field types, escaping, optional caller timestamp, and line-local rejection |
| Remote Write | Snappy-compressed `prometheus.WriteRequest` v1 float samples; v2, metadata, exemplars, and histograms are explicitly rejected |
| JSON | Canonical `{"metrics":[...]}` batch plus `{"metric":...}` single-point sugar, with item-local schema rejection |

All decoders return the same `IngestBatch`, which the shared `Ingestor` validates,
admits under bounded buffer/WAL pressure, and writes durably.

## Embedded Example

```rust
use alopex_skulk::ingest::line_protocol::LineProtocolDecoder;
use alopex_skulk::ingest::{IngestLimits, Ingestor};
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limits = IngestLimits::default();
    let store = RecoveryStore::open("./skulk-data-v3", RecoveryConfig::default())?;
    let mut ingestor = Ingestor::new(store, limits);

    let batch = LineProtocolDecoder::new(limits).decode(
        b"cpu,host=edge usage=23.5 1609459200000000000",
        1609459200000000000,
    )?;
    let outcome = ingestor.ingest(batch, 1609459200000000000)?;
    assert_eq!(outcome.accepted_count(), 1);
    ingestor.sink_mut().flush_all()?;
    Ok(())
}
```

## Build and Verify

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Rust 1.82 is the minimum supported version. Production dependencies are pure
Rust with no `cc` or FFI `*-sys` crate. Current storage and ingest measurements
are versioned in
[`STORAGE_BASELINE.md`](crates/skulk/benches/STORAGE_BASELINE.md) and
[`INGEST_BASELINE.md`](crates/skulk/benches/INGEST_BASELINE.md); unmet
throughput and p99 targets remain open rather than being relaxed.

## Roadmap

| Version | Scope |
| --- | --- |
| v0.3 | Wide columnar storage, durability, retention/compaction, and three ingest decoders |
| v0.4 | Query planner/executor and PromQL/SQL-TS core |
| v0.5 | Downsampling and continuous queries |
| v0.6 | HTTP server and Prometheus-compatible endpoints |
| v0.7+ | Alerts, sharding, and replication |

## License

Apache-2.0 OR MIT
