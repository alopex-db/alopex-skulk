# Alopex Skulk

> Time-series database built on Alopex Core. Gorilla compression, automatic TTL/downsampling, PromQL & SQL-TS queries. Scales from embedded to distributed.

**Skulk** (noun): A group of foxes. Like a skulk silently tracking prey, Alopex Skulk quietly collects and manages your time-series data in the background.

## Core Values

- **Ephemeral**: Data is consumable — automatic TTL and downsampling for freshness management
- **Streaming**: High-speed ingestion, continuous queries, real-time alerts
- **Observable**: Unified foundation for metrics, logs, and traces

## Features

| Feature | Description |
|---------|-------------|
| **Gorilla Compression** | 10:1+ compression ratio with Delta-of-Delta timestamps and XOR-encoded values |
| **Automatic Lifecycle** | Time-based TTL, cascading downsampling (1s → 1h → 1d) |
| **PromQL Compatible** | Query with familiar Prometheus syntax |
| **SQL-TS Extension** | `TIME_BUCKET`, `RATE`, `DELTA`, `FIRST`, `LAST` functions |
| **Multi-Mode Deployment** | Embedded → Single-Node → Distributed cluster |
| **Alopex Core Foundation** | Built on battle-tested WAL, MemTable, and Compaction |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Layer                           │
│  (Prometheus, Telegraf, Grafana, Custom Apps)               │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────┴───────────────────────────────────┐
│                   Ingest Gateway                            │
│  Line Protocol │ Prometheus Remote Write │ JSON API         │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────┴───────────────────────────────────┐
│                   Query Engine                              │
│  PromQL Parser │ SQL-TS Parser │ Planner │ Executor         │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────┴───────────────────────────────────┐
│                  Lifecycle Manager                          │
│  TTL Manager │ Downsampler │ Retention Policy               │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────┴───────────────────────────────────┐
│                   Storage Layer                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ TSM Engine (Time-Structured Merge)                  │   │
│  │  MemTable → Immutable → TSM Files (.skulk)          │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Alopex Core (WAL, Compaction)                       │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Comparison with Alopex DB

| Aspect | Alopex DB | Alopex Skulk |
|--------|-----------|--------------|
| **Use Case** | RAG/AI, Knowledge Base, OLTP | Monitoring, IoT, Log Analysis |
| **Data Lifespan** | Long-term (asset) | Short-term rotation (consumable) |
| **Primary Ops** | CRUD + Vector Search | Append + Aggregate + TTL Delete |
| **Query Pattern** | Point lookup, JOIN, Vector | Range aggregation, Downsampling |
| **Delete Pattern** | Explicit DELETE | Automatic TTL expiry |

## Quick Start

### Embedded Mode

```rust
use alopex_skulk::EmbeddedTSDB;

let db = EmbeddedTSDB::open("./tsdb_data")?;

// Write metrics
db.write_line_protocol(
    "cpu,host=server1,region=ap usage_user=23.5,usage_system=12.3"
)?;

// Query with PromQL
let result = db.query_promql("rate(cpu{host='server1'}[5m])")?;

// Query with SQL-TS
let result = db.query_sql(
    "SELECT TIME_BUCKET('1h', time), AVG(usage_user)
     FROM cpu
     WHERE time > NOW() - INTERVAL '24h'
     GROUP BY 1"
)?;
```

### Server Mode

```bash
# Start server
alopex-skulk --config /etc/skulk/config.toml

# Write data (Line Protocol)
curl -X POST http://localhost:8086/write \
  -d 'cpu,host=server1 usage=45.2'

# Query (PromQL)
curl -G http://localhost:8086/api/v1/query \
  --data-urlencode 'query=rate(cpu[5m])'
```

## Ingest Protocols

| Protocol | Endpoint | Format |
|----------|----------|--------|
| Line Protocol | `POST /write` | `metric,tag=value field=value timestamp` |
| Prometheus Remote Write | `POST /api/v1/write` | Snappy-compressed Protobuf |
| JSON | `POST /api/v1/ingest` | `{"metric": "cpu", "tags": {...}, "fields": {...}}` |

## Data Lifecycle

```
Raw Data (1s resolution)
    │ TTL: 72h
    ▼
Hourly Aggregate (1h resolution)
    │ TTL: 30d
    ▼
Daily Aggregate (1d resolution)
    │ TTL: 1y
    ▼
Archive/Delete
```

## Roadmap

| Version | Milestone | Dependencies |
|---------|-----------|--------------|
| v0.1 | TSM Core (MemTable, Gorilla, File Format) | alopex-core v0.2 |
| v0.2 | Lifecycle (TTL, Partitioning, Compaction) | Skulk v0.1 |
| v0.3 | Ingest (Line Protocol, Remote Write) | Skulk v0.2 |
| v0.4 | Query (PromQL, SQL-TS) | Skulk v0.3 |
| v0.5 | Downsampling & Continuous Query | Skulk v0.4 |
| v0.6 | HTTP Server & Prometheus Compat | Skulk v0.5 |
| v0.7 | Alert Engine | Skulk v0.6 |
| v0.8 | Distributed (Sharding) | Skulk v0.7 + Chirps v0.3 |
| v0.9 | Replication (Raft) | Skulk v0.8 + Chirps v0.6 |
| v1.0 | Stable Release | Full test coverage |

## Documentation

- [Requirements Specification](../design/requirements-tsdb.md)
- [Design Specification](../design/design-spec-tsdb.md)
- [Technical Specification](../design/technical-spec-tsdb.md)
- [Milestone Dependencies](../design/milestones.md)

## Related Projects

- [Alopex DB](https://github.com/alopex-rs/alopex) - Vector-enabled embedded database
- [Alopex Chirps](https://github.com/alopex-rs/chirps) - Cluster mesh networking (QUIC + SWIM + Raft)

## License

Apache-2.0
