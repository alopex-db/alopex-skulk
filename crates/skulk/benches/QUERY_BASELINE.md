# Query Release Baseline

## Status

Skulk v0.4.0 passes the fixed SQL-TS query release gate on the measured host.
The durable ingest latency stretch goal remains unmet and is recorded without
relaxing it.

| Check | Threshold | Measured | Verdict |
| --- | ---: | ---: | --- |
| 24h `TIME_BUCKET 1h + AVG` p99 | `< 100 ms` | **41.782 ms**, 100 runs | PASS |
| Out-of-range time pruning | 0 decoded rows | 33/33 files pruned, 0 rows decoded | PASS |
| Absent-tag row-group reduction | `>= 50%` | 33/33 row groups, **100.00%** | PASS |
| Core release footprint | `<= 6,000,000 B` | **3,669,104 B** | PASS |
| Core native build dependencies | no `cc` / `*-sys` | none in normal/build tree | PASS |
| Default query footprint + Nim library | record | 6,206,224 B + 461,728 B = **6,667,952 B** | RECORDED |
| Durable 10K ingest p99 stretch goal | `< 10 ms` | **66.705 ms**, 100 runs | FAIL (non-query gate) |

## Fixed Workload

- Real ext4 filesystem, one process, one writer, and one Cargo job.
- Target measurement: 100 series distinguished by `host`.
- Target interval and range: one point every 10 seconds for 24 hours.
- Target points: 864,000.
- Background measurement: 9,136,000 points across 1,000 series.
- Database total: exactly 10,000,000 durable Parquet points.
- Query:

  ```sql
  SELECT TIME_BUCKET('1 hour', time) AS bucket,
         AVG(value) AS average
  FROM query_gate
  WHERE time > NOW() - INTERVAL '24 hours'
  GROUP BY bucket
  ```

- The SQL text is parsed through the vendored Nim C ABI on every invocation.
- One untimed correctness query populates the decoded float-series cache before
  the 100-run nearest-rank p99 sample. Cache reuse requires an exact match of
  manifest generation, highest issued ingest sequence, scan request, and field;
  ingest, retention, or manifest publication invalidates the key.

## Measurement Environment

Measured on 2026-07-30 from `release/v0.4.0`, working tree based on `2cf0b96`.

- CPU: AMD Ryzen 5 3500U, 4 cores / 8 threads.
- OS: Linux 6.6.87.2 under WSL2, x86_64.
- Filesystem: ext4 (`/dev/sdd`) for both the repository and `/tmp`.
- Rust: 1.96.0, LLVM 22.1.2.
- Build: `profile.bench`, LTO, one codegen unit, `CARGO_BUILD_JOBS=1`.
- Whole build-and-benchmark command: 7m05.77s wall time, maximum RSS
  1,145,664 KiB, no swap. This maximum includes release LTO linking.

Criterion's post-gate sample reported 31.608–34.953 ms and
24.719–27.335 million target points/s. The release verdict uses the explicit
100-run nearest-rank p99 above, not Criterion's mean interval.

## Ingest Result

The ingest harness uses 10,000 Line Protocol points across 100 series and a
fresh real data root for each of 100 runs. Store open is outside the timed
region. The timed region includes Line Protocol decode, common admission and
validation, WAL encoding/write, and one `sync_data` durability boundary before
acknowledgement.

The measured p99 is 66.705 ms. This misses the published `< 10 ms` stretch goal,
consistent with the fsync-bound gap documented in
[`INGEST_BASELINE.md`](INGEST_BASELINE.md). Task 16 requires this gap to be
recorded; only the `< 100 ms` query threshold is the v0.4 release blocker.

## Footprint

The core probe is the existing `storage_footprint` example built with
`--no-default-features`. Its 3,669,104-byte result is 4,640 bytes (0.13%) above
the v0.3 measured baseline and remains well below the 6.0 MB ceiling.
The corresponding normal/build dependency tree contains no `cc` or `*-sys`
crate.

The default probe is `query_footprint`, which executes both public text-query
frontends so linker garbage collection cannot hide the Rust query stack or Nim
FFI dependency. `ldd` resolves the vendored
`libalopex_sql_parser.so`; the recorded combined footprint is 6,667,952 bytes.

## Reproduce

Run from the repository root with no other Cargo process:

```bash
rtk env CARGO_TARGET_DIR=/tmp/alopex-skulk-v040-task-16 \
  CARGO_BUILD_JOBS=1 cargo bench -j1 --bench query_bench -- --noplot

rtk env CARGO_TARGET_DIR=/tmp/alopex-skulk-v040-task-16 \
  CARGO_BUILD_JOBS=1 cargo build -j1 --release \
  --no-default-features --example storage_footprint
rtk stat -c '%s %n' \
  /tmp/alopex-skulk-v040-task-16/release/examples/storage_footprint
rtk env CARGO_TARGET_DIR=/tmp/alopex-skulk-v040-task-16 \
  cargo tree -p alopex-skulk -e normal,build --no-default-features

rtk env CARGO_TARGET_DIR=/tmp/alopex-skulk-v040-task-16 \
  CARGO_BUILD_JOBS=1 cargo build -j1 --release --example query_footprint
rtk stat -c '%s %n' \
  /tmp/alopex-skulk-v040-task-16/release/examples/query_footprint \
  crates/skulk/nim-parser/vendor/x86_64-unknown-linux-gnu/libalopex_sql_parser.so
```
