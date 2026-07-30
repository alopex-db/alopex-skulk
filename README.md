# Alopex Skulk

Alopex Skulk is an embedded, append-only time-series storage, ingest, and query
engine written in Rust. Version 0.4.0 adds an HTTP-independent query layer over
the v0.3 wide Arrow/Parquet format. PromQL and SQL-TS syntax is parsed by a
versioned Nim shared library; planning, execution, storage, and the
programmatic-plan API remain in Rust.

## v0.4.0 at a Glance

| Area | Delivered in v0.4.0 |
| --- | --- |
| Query API | PromQL instant/range, SQL-TS, pre-built logical plans, Arrow `QueryResult`, and bounded metadata enumeration |
| Execution | Scan/filter/group/function/aggregate pipeline with cancellation, deadlines, and resource limits |
| Storage reads | `StorageReader`, pending + durable latest-write-wins merge, time/tag pruning, field projection, and schema-conflict errors |
| Resolution | Conservative raw/rollup catalog and exactness-based selection; v0.4 ships raw data only |
| Ingest policy | Configurable one-hour O3 window, reject/warn/drop policies, counted outcomes, and explicit backfill override |
| Compatibility | v0.3.0/v0.3.1 WAL, manifest, and Parquet files are read unchanged |

The fixed 10-million-point release workload records a 24-hour
`TIME_BUCKET('1 hour', time) + AVG(value)` p99 of **41.782 ms** over 100 runs,
passing the `<100 ms` gate. The durable 10K-point ingest p99 remains
**66.705 ms** against the unchanged `<10 ms` stretch goal. Conditions,
footprints, and pruning evidence are in
[`QUERY_BASELINE.md`](crates/skulk/benches/QUERY_BASELINE.md).

## Feature Profiles

| Cargo profile | Contents | Native requirement |
| --- | --- | --- |
| Default | `promql` + `sql-ts`, Rust planner/executor/storage | Versioned Nim parser shared library |
| `--no-default-features` | Storage, `StorageReader`, executor, and pre-built logical-plan API | None; pure Rust with no `cc` or `*-sys` dependency |
| `--no-default-features --features promql` | PromQL text frontend plus core | Nim parser |
| `--no-default-features --features sql-ts` | SQL-TS text frontend plus core | Nim parser |

The measured core release probe is 3,669,104 bytes, below the 6,000,000-byte
ceiling. The default query probe plus the Linux Nim library is 6,667,952 bytes.

## Embedded Query Examples

`QueryEngine` operates directly on a `StorageReader`; no HTTP server is needed.
Timestamps are Unix epoch nanoseconds.

```rust
use alopex_skulk::query::QueryEngine;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = 1_700_000_000_000_000_000_i64;
    let store = RecoveryStore::open("./skulk-data-v4", RecoveryConfig::default())?;
    let engine = QueryEngine::new(&store);

    let instant = engine.query_promql(
        r#"cpu_usage{host=~"edge-.+",__field__="value"}"#,
        now,
    )?;
    let hourly = engine.query_sql(
        "SELECT TIME_BUCKET('1 hour', time) AS bucket, \
         AVG(value) AS average FROM cpu_usage \
         WHERE time > NOW() - INTERVAL '24 hours' GROUP BY bucket",
        now,
    )?;

    println!("instant batches: {}", instant.batches().len());
    println!("hourly batches: {}", hourly.batches().len());
    Ok(())
}
```

PromQL reads field `value` by default. Use the reserved `__field__` matcher for
another wide field. SQL-TS reserves `time` for the physical timestamp column.
The public metadata methods are `series`, `label_names`, and `label_values`.

## Ingest Protocols

All decoders return the same `IngestBatch`, which the shared `Ingestor`
validates, admits under bounded buffer/WAL pressure, and writes durably.

| Protocol | Behavior |
| --- | --- |
| Line Protocol | Multi-field wide rows, all five field types, escaping, optional caller timestamp, and line-local rejection |
| Remote Write | Snappy-compressed `prometheus.WriteRequest` v1 float samples; v2, metadata, exemplars, and histograms are rejected |
| JSON | Canonical `{"metrics":[...]}` batch plus `{"metric":...}` single-point form, with item-local schema rejection |

## Build and Verify

Run from the repository root. Use one Cargo job on memory-constrained hosts.

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Rust 1.82 is the minimum supported version.

## Nim Parser Targets and Runtime Loading

The text frontends consume the Alopex Nim parser contract `0.2.0`. This source
tree currently includes a checksummed artifact for:

- `x86_64-unknown-linux-gnu`

The release matrix also recognizes `x86_64-apple-darwin`,
`aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`. A frontend-enabled build
for any target without a copied artifact must provide a matching library and
`CONTRACT_VERSION` file through `SKULK_NIM_PARSER_LIB_DIR`. Core-only builds do
not inspect or link a Nim artifact.

For an external target, build the source of truth from the Alopex repository:

```bash
make nim-parser
bash scripts/test-nim-parser.sh
```

Nim 2.2 or the repository's digest-pinned Docker backend is required. Copy the
resulting `.so`, `.dylib`, or `.dll` and a `CONTRACT_VERSION` containing
`0.2.0` into one directory, then build Skulk with:

```bash
SKULK_NIM_PARSER_LIB_DIR=/absolute/path/to/parser-artifact \
  cargo build --workspace --all-features
```

Skulk's own binaries embed an rpath to the selected library directory on Linux
and macOS. A downstream final executable must propagate the directory exposed
by Cargo as `DEP_SKULK_NIM_PARSER_LIBDIR` from its own `build.rs`:

```rust
fn main() {
    let Ok(dir) = std::env::var("DEP_SKULK_NIM_PARSER_LIBDIR") else {
        return;
    };
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "linux" || target_os == "macos" {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
}
```

Windows applications must place `alopex_sql_parser.dll` beside the executable
or on `PATH`. See
[`nim-parser/README.md`](crates/skulk/nim-parser/README.md) for artifact update
and checksum rules.

## Compatibility and Limits

Version 0.4 is additive over the v0.3 on-disk format. Version 0.3 was the
breaking transition from the v0.2 single-value TSM/Gorilla format; v0.2 data
still requires export and re-ingest into a v0.3+ data root.

The text frontends deliberately implement a documented subset. PromQL excludes
vector-to-vector matching modifiers, subqueries, `@`, negative offset,
comparison/`bool`, `topk`/`bottomk`/`quantile`, and label manipulation
functions. SQL-TS excludes JOIN, subqueries, window functions, DDL, writes, and
HAVING. A measurement whose active files or pending state disagree on a column
type or tag/field role is rejected explicitly; v0.4 has no persistent schema
registry. HTTP endpoints arrive in v0.6.

See [CHANGELOG.md](CHANGELOG.md) for the complete v0.4.0 scope and known
limitations.

## Roadmap

| Version | Scope |
| --- | --- |
| v0.4 | Embedded query planner/executor, PromQL/SQL-TS, StorageReader, and O3 policy |
| v0.5 | Downsampling and continuous queries |
| v0.6 | HTTP server and Prometheus-compatible endpoints |
| v0.7+ | Alerts, sharding, and replication |

## License

Apache-2.0 OR MIT
