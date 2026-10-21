<p align="center">
  <a href="https://alopex-db.github.io/concepts/skulk/">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-skulk-dark-800.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-skulk-light-800.png">
      <img alt="Alopex Skulk" src="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-skulk-light-800.png" width="400">
    </picture>
  </a>
</p>

<h3 align="center">A time-series database you embed, not deploy.</h3>

<p align="center">
  <b>Append-only storage, three ingest protocols, and PromQL + SQL-TS — in one Rust library.</b><br>
  No HTTP server, no sidecar, no C toolchain. The query engine runs inside your process.
</p>

<p align="center">
  <a href="https://alopex-db.github.io/concepts/skulk/">Concept</a> &bull;
  <a href="https://alopex-db.github.io/getting-started/skulk/">Quick Start</a> &bull;
  <a href="https://docs.rs/alopex-skulk">API Docs</a> &bull;
  <a href="./CHANGELOG.md">Changelog</a> &bull;
  <a href="https://github.com/alopex-db/alopex">Alopex DB</a> &bull;
  <a href="https://github.com/sponsors/asopitech">Sponsor</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/alopex-skulk"><img alt="crates.io" src="https://img.shields.io/crates/v/alopex-skulk.svg?label=crates.io&color=5FB4C9"></a>
  <a href="https://docs.rs/alopex-skulk"><img alt="docs.rs" src="https://img.shields.io/docsrs/alopex-skulk?label=docs.rs"></a>
  <a href="https://crates.io/crates/alopex-skulk"><img alt="downloads" src="https://img.shields.io/crates/d/alopex-skulk?color=1E3A5F"></a>
  <a href="#license"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue"></a>
  <a href="https://github.com/sponsors/asopitech"><img alt="Sponsor" src="https://img.shields.io/badge/sponsor-%E2%9D%A4-db61a2"></a>
  <a href="https://github.com/alopex-db/alopex-skulk/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/alopex-db/alopex-skulk?style=flat&color=7EC8E3"></a>
</p>

---

## Install

```bash
# Full: storage + ingest + PromQL and SQL-TS text frontends
cargo add alopex-skulk

# Core only: pure Rust, no Nim parser artifact, no C toolchain
cargo add alopex-skulk --no-default-features
```

Rust 1.82 or newer. The core profile has no `cc` and no `*-sys` dependency —
it builds anywhere Rust builds.

## Query it in your own process

`QueryEngine` reads a `StorageReader` directly. Timestamps are Unix epoch
nanoseconds, and results are typed Arrow `RecordBatch` streams.

```rust
use alopex_skulk::query::QueryEngine;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = 1_700_000_000_000_000_000_i64;
    let store = RecoveryStore::open("./skulk-data-v4", RecoveryConfig::default())?;
    let engine = QueryEngine::new(&store);

    // PromQL, instant
    let instant = engine.query_promql(
        r#"cpu_usage{host=~"edge-.+",__field__="value"}"#,
        now,
    )?;

    // SQL-TS, 24-hour rollup
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

PromQL reads the field `value` by default; use the reserved `__field__`
matcher for another wide field. SQL-TS reserves `time` for the physical
timestamp column. Metadata enumeration is `series`, `label_names`, and
`label_values`.

## Why Skulk

Monitoring an application usually means standing up a time-series server, a
scrape path, and a query endpoint before you can ask a single question about
your own data. Skulk is the same job as a library.

| | Usual time-series stack | Alopex Skulk |
|:--|:--|:--|
| **Deployment** | A server to run, scale, and secure | A crate in your binary |
| **Query path** | HTTP round trip to another process | `QueryEngine` on your own storage |
| **Ingest** | Agent-specific endpoints | Line Protocol, Remote Write, and JSON into one `IngestBatch` |
| **Build** | C libraries, `*-sys` crates | Pure Rust core; frontends optional |
| **Durability** | Usually acknowledged before the write lands | Acknowledged only after the WAL |

It is the time-series member of the [Alopex family](#the-alopex-family): the
engine ([Alopex DB](https://github.com/alopex-db/alopex)) holds long-lived data
you change, Skulk holds numeric series that arrive on a schedule. Both sit on
the same WAL / MemTable / compaction foundation.

## What v0.4 delivers

| Area | Delivered |
| --- | --- |
| Query API | PromQL instant/range, SQL-TS, pre-built logical plans, Arrow `QueryResult`, bounded metadata enumeration |
| Execution | Scan / filter / group / function / aggregate pipeline with cancellation, deadlines, and resource limits |
| Storage reads | `StorageReader`, pending + durable latest-write-wins merge, time and tag pruning, field projection, schema-conflict errors |
| Resolution | Conservative raw/rollup catalog with exactness-based selection; v0.4 ships raw data only |
| Ingest policy | Configurable one-hour out-of-order window, reject/warn/drop actions, counted outcomes, explicit backfill override |
| Compatibility | v0.3.0 and v0.3.1 WAL, manifest, and Parquet files are read unchanged |

### Ingest protocols

Every decoder produces the same `IngestBatch`, which the shared `Ingestor`
validates, admits under bounded buffer and WAL pressure, and writes durably.

| Protocol | Behavior |
| --- | --- |
| **Line Protocol** | Multi-field wide rows, all five field types, escaping, optional caller timestamp, line-local rejection |
| **Prometheus Remote Write** | Snappy-compressed `prometheus.WriteRequest` v1 float samples; v2, metadata, exemplars, and histograms are rejected |
| **JSON** | Canonical `{"metrics":[...]}` batch plus the `{"metric":...}` single-point form, with item-local schema rejection |

Point an existing agent at Skulk without changing what it emits.

### Feature profiles

| Cargo profile | Contents | Native requirement |
| --- | --- | --- |
| Default | `promql` + `sql-ts`, Rust planner / executor / storage | Versioned Nim parser shared library |
| `--no-default-features` | Storage, `StorageReader`, executor, pre-built logical-plan API | None — pure Rust |
| `--no-default-features --features promql` | PromQL text frontend plus core | Nim parser |
| `--no-default-features --features sql-ts` | SQL-TS text frontend plus core | Nim parser |

## Measured, not claimed

From the fixed release workload, with conditions, footprints, and pruning
evidence in [`QUERY_BASELINE.md`](crates/skulk/benches/QUERY_BASELINE.md):

| Measurement | Result | Gate |
|:--|:--|:--|
| 24-hour `TIME_BUCKET('1 hour', time) + AVG(value)` over 10M points, p99 of 100 runs | **41.782 ms** | < 100 ms — passing |
| Durable 10K-point ingest, p99 | **66.705 ms** | < 10 ms stretch goal — **not met** |
| Core release probe size | 3,669,104 bytes | < 6,000,000 bytes — passing |
| Default query probe + Linux Nim library | 6,667,952 bytes | — |

## Status and limits

Pre-1.0. These are deliberate boundaries, not surprises:

- **No HTTP endpoints.** Querying is in-process. HTTP serving and
  Prometheus-compatible endpoints arrive in v0.6.
- **The text frontends are a documented subset.** PromQL excludes
  vector-to-vector matching modifiers, subqueries, `@`, negative offset,
  comparison and `bool`, `topk`/`bottomk`/`quantile`, and label manipulation.
  SQL-TS excludes JOIN, subqueries, window functions, DDL, writes, and HAVING.
- **No persistent schema registry.** A measurement whose active files and
  pending state disagree on a column type or tag/field role is rejected
  explicitly.
- **Format history.** v0.4 is additive over v0.3. v0.3 was the breaking
  transition from the v0.2 single-value TSM/Gorilla format; v0.2 data still
  requires export and re-ingest into a v0.3+ data root.

[CHANGELOG.md](CHANGELOG.md) carries the complete scope and known limitations.

<details>
<summary><b>Building from source, and the Nim parser artifact</b></summary>

Run from the repository root. Use one Cargo job on memory-constrained hosts.

```bash
cargo build --workspace --all-features
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

The text frontends consume the Alopex Nim parser contract `0.2.0`. This source
tree includes a checksummed artifact for `x86_64-unknown-linux-gnu`. The
release matrix also recognizes `x86_64-apple-darwin`, `aarch64-apple-darwin`,
and `x86_64-pc-windows-msvc`. A frontend-enabled build for a target without a
copied artifact must supply a matching library and `CONTRACT_VERSION` file
through `SKULK_NIM_PARSER_LIB_DIR`. Core-only builds never inspect or link a
Nim artifact.

Build the source of truth from the Alopex repository (Nim 2.2, or the
repository's digest-pinned Docker backend):

```bash
make nim-parser
bash scripts/test-nim-parser.sh
```

Copy the resulting `.so`, `.dylib`, or `.dll` plus a `CONTRACT_VERSION`
containing `0.2.0` into one directory, then:

```bash
SKULK_NIM_PARSER_LIB_DIR=/absolute/path/to/parser-artifact \
  cargo build --workspace --all-features
```

Skulk's binaries embed an rpath to the selected library directory on Linux and
macOS. A downstream final executable must propagate the directory Cargo
exposes as `DEP_SKULK_NIM_PARSER_LIBDIR` from its own `build.rs`:

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
or on `PATH`. Artifact update and checksum rules are in
[`nim-parser/README.md`](crates/skulk/nim-parser/README.md).

</details>

## Roadmap

| Version | Scope |
| --- | --- |
| **v0.4** | Embedded query planner and executor, PromQL / SQL-TS, `StorageReader`, out-of-order policy |
| v0.5 | Downsampling and continuous queries |
| v0.6 | HTTP server and Prometheus-compatible endpoints |
| v0.7+ | Alerts, sharding, and replication |

The family roadmap is published at
<https://alopex-db.github.io/roadmap/>.

## The Alopex family

| Project | What it is |
|:--|:--|
| [**alopex**](https://github.com/alopex-db/alopex) | The unified engine — SQL + vector (HNSW) + columnar, embedded to cluster |
| **alopex-skulk** | This repo — time-series storage, ingest, and query |
| [**alopex-chirps**](https://github.com/alopex-db/alopex-chirps) | Gossip and messaging mesh over UDP/QUIC |
| [**alopex-ui**](https://github.com/alopex-db/alopex-ui) | Alopex Data UI — one DataSource contract for data applications |

Two designs are published for comment before the code freezes:
[Trail](https://alopex-db.github.io/concepts/trail/) (logs and events) and
[Alopex OTel](https://alopex-db.github.io/concepts/otel/) (storage and
dashboards in one product, with Skulk holding the metrics path).

## Contributing

Issues and pull requests are welcome. The organization
[contributing guide](https://github.com/alopex-db/.github/blob/main/CONTRIBUTING.md)
covers where each project is tracked and what a pull request needs;
[SECURITY.md](https://github.com/alopex-db/.github/blob/main/SECURITY.md) has
the private route for vulnerability reports.

Performance changes need evidence from the published benchmark contracts, not
notebook timings.

## Sponsor

Skulk is built in the open by [@asopitech](https://github.com/asopitech).
GitHub Sponsors funds continued development across the Alopex family, and
higher tiers follow the work before it is public — $3/month OSS Support,
$12/month Early Access, $99/month Enterprise-focused Early Access.

<p align="center">
  <a href="https://github.com/sponsors/asopitech?metadata_source=alopex_skulk_readme&metadata_campaign=sponsor_early_access&metadata_content=readme_sponsor_section"><img alt="Sponsor @asopitech on GitHub Sponsors" src="https://img.shields.io/badge/GitHub%20Sponsors-%E2%9D%A4%20Sponsor%20%40asopitech-db61a2?logo=githubsponsors&logoColor=white&style=for-the-badge"></a>
  <a href="https://buymeacoffee.com/asopitechia"><img alt="Buy Me a Coffee" src="https://img.shields.io/badge/Buy%20Me%20a%20Coffee-%E2%98%95-FFDD00?style=for-the-badge&logo=buymeacoffee&logoColor=black"></a>
</p>

## License

Apache-2.0 OR MIT, at your option.
