# Storage Verification Baseline

Measured on 2026-07-28 with Rust 1.96.0, Linux x86_64 under WSL2, and an ext
filesystem. Benchmarks use 10,000 points, a single writer, real files, and an
`fsync` before durable acknowledgement. The Embedded release budget is fixed
at 4,100,000 bytes. Protocol-level decode and WAL-ACK measurements are tracked
separately in [`INGEST_BASELINE.md`](INGEST_BASELINE.md).

| Check | Result | Evidence |
| --- | --- | --- |
| Minimal columnar release footprint | PASS | 3,664,464 bytes; `opt-level="z"`, fat LTO, one codegen unit, symbols stripped, panic abort |
| Native build dependencies | PASS | No `cc` or `*-sys` crate in the normal/build dependency tree |
| Volatile gauge compression | PASS | Parquet 107,881 B vs v0.2 Gorilla 167,512 B (1.553× smaller) |
| Repeated-value compression | PASS | Parquet 23,433 B vs v0.2 Gorilla 167,512 B (7.149× smaller) |
| Parquet durable throughput | FAIL | Volatile 260.05–333.04 K points/s; repeated 326.21–406.12 K points/s |
| WAL + durable batch + flush | FAIL | 22.288–33.702 K points/s |

The throughput goal remains 500 K points/s. The measured Parquet time was
24.623–38.454 ms per 10 K points; the complete storage path took
296.72–448.66 ms. These measurements do not establish p99, and even their
fastest values exceed the `<10 ms` latency goal. Treat throughput and p99 as
open tuning work; do not relax the targets. A preliminary, broader recovery
path binary built with speed-optimized release settings measured 8,761,304
bytes and is not the minimal footprint budget probe.

Reproduce the checks from the repository root:

```bash
rtk cargo test --all-features fixed_volatile_and_repeated_parquet_files_beat_v02_gorilla -- --exact --nocapture
rtk cargo bench --bench storage_bench
rtk cargo build --release --example storage_footprint
rtk stat -c '%s %n' target/release/examples/storage_footprint
rtk cargo tree -p alopex-skulk --all-features --edges normal,build
```
