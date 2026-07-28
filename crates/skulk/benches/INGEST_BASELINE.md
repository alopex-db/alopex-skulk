# Ingest Verification Baseline

Measured on 2026-07-28 with Rust 1.96.0, Linux x86_64 under WSL2, and an ext
filesystem. The fixed workload is 10,000 points across 100 series, one writer,
one batch, and Criterion with 10 samples, a 1-second warm-up, and at least
3 seconds of measurement. Durable results include decode, common validation,
WAL encoding, and one `fsync` before ACK; they exclude the later Parquet flush.

## Result Summary

| Path | Fixed target | Result | Measured throughput | Time per 10K |
| --- | ---: | --- | ---: | ---: |
| Line Protocol decode | 500 K points/s | FAIL | 118.85–147.41 K/s | 67.839–84.139 ms |
| Remote Write v1 decode | 100 K samples/s | PASS | 201.48–217.88 K/s | 45.898–49.634 ms |
| Line Protocol → WAL ACK | 500 K points/s | FAIL | 34.141–37.760 K/s | 264.83–292.90 ms |
| Remote Write v1 → WAL ACK | 100 K samples/s | FAIL | 34.112–39.819 K/s | 251.14–293.16 ms |

The batch latency goal remains p99 `<10 ms`. Criterion does not establish p99
here, and even the fastest measured 10K interval exceeds 10 ms, so the latency
goal is not met. Thresholds were not relaxed.

## Fixed Payloads

- Line Protocol: 10,000 single-field lines, 100 `host` tag values, explicit ns timestamps.
- Remote Write: Snappy-compressed `prometheus.WriteRequest`, 100 TimeSeries with 100 float samples each.
- Durable ACK: a fresh real data root per sample; store open/setup is outside the timed closure.
- Correctness: `tests/ingest_integration.rs` separately proves all three protocols through WAL, flush, reopen, and Parquet readback.

## Open Tuning Work

1. Profile Line Protocol parsing and per-row owned string/BTreeMap construction; decode alone is below target.
2. Profile shared validation, WAL frame encoding, and batch append; both protocols converge near 35–40 K/s once durability is included.
3. Add a dedicated latency harness with enough repetitions to establish p99 under the same filesystem and fsync contract.
4. Keep the published 500 K/100 K and p99 `<10 ms` targets unchanged until reproducible measurements pass.

Reproduce from the repository root:

```bash
rtk cargo test --test ingest_integration
rtk cargo bench --bench ingest_bench -- --noplot
```
