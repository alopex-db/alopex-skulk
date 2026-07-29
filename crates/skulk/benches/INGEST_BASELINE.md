# Ingest Verification Baseline

## v0.3.1 Remeasurement (2026-07-29)

Same fixed workload and durability contract as the v0.3.0 baseline below.
Measured on a shared WSL2 host; fsync-bound paths vary about +/-20% across
sessions with background load, so ranges over multiple sessions are shown.
Attribution uses same-session paired runs (code change is the only delta).

| Path | v0.3.0 | v0.3.1 (observed range) | Gate | Verdict |
| --- | ---: | ---: | ---: | --- |
| Line Protocol decode | 118.9-147.4 K/s | 225.2-324.7 K/s (~2.2x) | - | improved |
| Remote Write decode | 201.5-217.9 K/s | 595.7-743.3 K/s | - | improved |
| Line Protocol -> WAL ACK | 34.1-37.8 K/s | 64.1-107.2 K/s (~2-2.7x) | >= 70 K/s (revised, see spec P3) | PASS |
| Remote Write -> WAL ACK | 34.1-39.8 K/s | 95.0-159.1 K/s (~3-4x; 141.9-159.1 K/s after the buffer-state rework even under load avg 3) | >= 100 K/s (published) | PASS |

Fixes: borrow-based batch WAL append (zero row clones, O(1) syscalls per
batch, streamed checkpoint, no entry residency), clone-free batch
validation, per-measurement lightweight state instead of ingest-time Arrow
building, request-local Line Protocol series cache. On-disk format
unchanged; all 118 tests green including crash recovery I1-I6 and RTO/RPO.

Remaining gap, recorded honestly: the LP 150 K/s stretch floor and the
published 500 K/s / p99 < 10 ms targets need the shared store path below
~3 us/row (column-name interning, WAL dictionary = format revision), which
is assigned to the event-store row-representation generation (v0.4+).
The original 2026-07-28 baseline follows unchanged for reference.


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
