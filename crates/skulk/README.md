# Alopex Skulk

`alopex-skulk` 0.4.0 is an embedded time-series storage, ingest, and query
engine. It stores wide, multi-field rows in Arrow memory batches and Parquet
files, acknowledges durable batches through a local WAL, and exposes an
HTTP-independent query API.

## What 0.4.0 Provides

- PromQL instant and range queries.
- SQL-TS projection, filtering, ordering, limits, standard aggregates, and
  `TIME_BUCKET`, `RATE`, `DELTA`, `DERIVATIVE`, `FIRST`, and `LAST`.
- A public `StorageReader` boundary with pending/durable
  latest-write-wins merge, time/tag pruning, field projection, and schema
  validation.
- Arrow-backed `QueryResult` values plus bounded series/label metadata calls.
- Conservative raw/rollup resolution selection; v0.4 ships raw data only.
- Configurable one-hour out-of-order window with observable reject/warn/drop
  outcomes and an explicit backfill override.

The default features are `promql` and `sql-ts`. They use the Alopex Nim parser
through a versioned C ABI and MessagePack AST. The Rust crate owns semantic
validation, planning, execution, and storage.

For a pure-Rust core without a parser artifact:

```toml
[dependencies]
alopex-skulk = { version = "0.4", default-features = false }
```

For both text frontends:

```toml
[dependencies]
alopex-skulk = "0.4"
```

## Query Example

```rust
use alopex_skulk::query::QueryEngine;
use alopex_skulk::store::recovery::{RecoveryConfig, RecoveryStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = 1_700_000_000_000_000_000_i64;
    let store = RecoveryStore::open("./skulk-data-v4", RecoveryConfig::default())?;
    let engine = QueryEngine::new(&store);

    let result = engine.query_sql(
        "SELECT TIME_BUCKET('1 hour', time) AS bucket, \
         AVG(value) AS average FROM cpu \
         WHERE time > NOW() - INTERVAL '24 hours' GROUP BY bucket",
        now,
    )?;
    println!("{} Arrow batches", result.batches().len());
    Ok(())
}
```

PromQL reads wide field `value` by default; select another field with the
reserved `__field__` matcher.

## Native Parser Artifact

The published source currently vendors the parser for
`x86_64-unknown-linux-gnu`. Frontend-enabled builds for other targets must set
`SKULK_NIM_PARSER_LIB_DIR` to a directory containing the target shared library
and a matching `CONTRACT_VERSION`. Downstream Linux/macOS executables must
propagate `DEP_SKULK_NIM_PARSER_LIBDIR` into an rpath from their final
`build.rs`; Windows must place the DLL beside the executable or on `PATH`.
Core-only builds have no Nim, `cc`, or `*-sys` requirement.

Full target, build, rpath, compatibility, and unsupported-syntax details are in
the [repository README](https://github.com/alopex-db/alopex-skulk#readme).

## Requirements

- Rust 1.82 or later.
- Nim 2.2 only when producing a parser artifact for a target that is not
  already vendored.

## License

Apache-2.0 OR MIT
