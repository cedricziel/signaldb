---
name: architecture
description: SignalDB architecture reference - FDAP stack, write/query data flow, service components, deployment models, and dual catalog system. Use when understanding how components fit together, data flow, or system design.
user-invocable: false
---

# SignalDB Architecture Reference

## FDAP Stack

SignalDB is built on Flight, DataFusion, Arrow, Parquet:

- **Flight**: Apache Arrow Flight for zero-copy inter-service gRPC communication
- **DataFusion**: SQL query processing engine (used by Querier)
- **Arrow**: In-memory columnar format used throughout the entire pipeline
- **Parquet**: Persistent columnar storage via Iceberg table format

**Critical rule**: Always use Arrow/Parquet types re-exported by DataFusion to ensure version compatibility:
```rust
// CORRECT
use datafusion::arrow::array::StringArray;
use datafusion::parquet::arrow::ArrowWriter;

// WRONG - version mismatch risk
use arrow::array::StringArray;
```

## Write Path

```
OTLP Client (gRPC :4317 / HTTP :4318)
    -> Acceptor (validates auth, converts OTLP->Arrow, writes to WAL)
    -> Writer via Flight do_put (transforms v1->v2 schema, writes to WAL)
    -> WalProcessor (background, 5s interval)
    -> Iceberg Tables (Parquet files in object store)
```

Key details:
1. Acceptor writes to WAL before acknowledging client
2. Acceptor converts OTLP protobuf -> Arrow RecordBatches using Flight schemas (v1)
3. Writer transforms v1 Flight schema -> v2 Iceberg schema (field renames, type conversions, computed partition fields)
4. Writer's WalProcessor reads WAL entries every 5s, writes Parquet via DataFusion
5. Processed WAL entries are marked and cleaned up

## Query Path

```
HTTP Client (Tempo API)
    -> Router (:3000 HTTP, :50053 Flight)
    -> Querier via Flight do_get (:50054)
    -> DataFusion SQL against Iceberg tables
    -> Parquet files in object store
    -> Results stream back as Arrow RecordBatches
```

Key details:
1. Router validates auth, discovers Queriers via `QueryExecution` capability
2. Flight tickets encode query type + tenant context: `find_trace:{tenant}:{dataset}:{trace_id}`
3. Querier uses `TenantCatalog` to bridge DataFusion 3-level model to Iceberg 2-level namespace
4. Results stream back as Arrow RecordBatches via Flight

## Service Components

| Service | Ports | Capability | Key Files |
|---------|-------|------------|-----------|
| **Acceptor** | gRPC:4317, HTTP:4318 | `TraceIngestion` | `src/acceptor/` |
| **Writer** | Flight:50061 (standalone), 50051 (mono) | `TraceIngestion`, `Storage` | `src/writer/` |
| **Router** | HTTP:3000, Flight:50053 | `Routing` | `src/router/` |
| **Querier** | Flight:50054 | `QueryExecution` | `src/querier/` |
| **Compactor** | None (background task) | `Compaction` | `src/compactor/` |

## Deployment Models

- **Monolithic** (`cargo run --bin signaldb`): All services in one process, shared SQLite catalog. Compactor included if enabled in config.
- **Microservices**: Independent binaries, shared catalog (PostgreSQL or SQLite)
- **Hybrid**: Mix of co-located and distributed services

**Note**: Monolithic mode integrates the Compactor service (Phase 2 - full execution) when `[compactor].enabled = true`. The compactor runs a background planning loop that identifies compaction candidates based on file count and size thresholds, and executes compaction by rewriting Parquet files and committing changes atomically to Iceberg tables.

## Dual Catalog System

1. **Service Catalog** (`Catalog`): PostgreSQL/SQLite for service discovery, tenant management, API keys, datasets
2. **Iceberg Catalog** (`CatalogManager`): SQLite-only SQL catalog named `"signaldb"` for Iceberg table metadata

The Iceberg catalog only supports SQLite (not PostgreSQL). This is distinct from the service catalog.
