---
name: architecture
description: SignalDB architecture reference - FDAP stack, write/query data flow, service components, deployment models, and dual catalog system. Use when understanding how components fit together, data flow, or system design.
user-invocable: false
sources:
  - docs/architecture/overview.md
  - src/signaldb-bin/src/**
  - Cargo.toml
---

# SignalDB Architecture Reference

## FDAP Stack

SignalDB is built on Flight, DataFusion, Arrow, Parquet (explained with
external references in `docs/architecture/fdap.md`):

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
    -> Acceptor (validates auth, enforces rate limits + storage quotas, converts OTLP->Arrow, writes to WAL)
    -> Writer via Flight do_put (transforms v1->v2 schema, writes to WAL)
    -> WalProcessor (background, 5s interval)
    -> Iceberg Tables (Parquet files in object store)
```

Key details:

1. Acceptor writes to WAL before acknowledging client
2. Acceptor converts OTLP protobuf -> Arrow RecordBatches using Flight schemas (v1)
3. Writer transforms v1 Flight schema -> v2 Iceberg schema (field renames, type conversions, computed partition fields, materialized `label_<key>` columns for configured attributes across all four signals, allowlists resolved per tenant (tenant schema override replaces the global set); new tables (all signals) store attributes as typed `Map<String,String>` columns for exact any-attribute matching)
4. Writer's `do_put` acks after its WAL flush (it does NOT commit synchronously); WalProcessor reads WAL entries every 5s (exponential backoff on failure) and writes Parquet via DataFusion, coalescing commits per `(tenant, dataset, table)` (`[writer].commit_interval` / `max_uncommitted_rows`) to cap the Iceberg/catalog write rate. Data is queryable once committed; `do_action("flush")` forces an immediate commit (read-your-writes)
5. Processed WAL entries are marked and cleaned up

Alongside the WAL loop the writer runs a **table reconciler** (`src/writer/src/reconcile.rs`): a startup pass plus one every `[writer].table_reconcile_interval` (default 5m, `0s` = startup only) over the tenant registry, ensuring every registered tenant/dataset holds a table for each signal type enabled for that tenant — including a tenant's `default_dataset` when no dataset row exists, which is what admin-API tenant creation leaves behind. It needs a tenant source on the writer's `CatalogManager` (attached in both `writer/src/main.rs` and `signaldb-bin/src/main.rs`, which start the loop independently). Provisioning goes through the same `CatalogManager::ensure_table` the write path uses, so failures are warn-level and degrade to create-on-first-write. Correspondingly, the querier reads a missing signal table as empty rather than erroring (`query/table_lookup.rs`).

## Query Path

```
HTTP Client (Tempo / Pyroscope / Loki APIs)
    -> Router (:3000 HTTP, :50053 Flight)
    -> Querier via Flight do_get (:50054)
    -> DataFusion SQL against Iceberg tables
    -> Parquet files in object store
    -> Results stream back as Arrow RecordBatches
```

Key details:

1. Router validates auth, discovers Queriers via `QueryExecution` capability
2. Flight tickets encode query type + tenant context: `find_trace:{tenant}:{dataset}:{trace_id}[:{start}:{end}]` (optional unix-second time hints). Single-trace lookups prune Parquet row groups via the `trace_id` bloom filter (min/max stats never prune a high-cardinality point lookup); see the `storage-layout` skill's Parquet bloom filters note
3. Querier uses `TenantCatalog` to bridge DataFusion 3-level model to Iceberg 2-level namespace
4. Results stream back as Arrow RecordBatches via Flight (trace not found -> Flight `not_found` status). HTTP responses also return the server span's trace context and stage timings (`Server-Timing`/`traceresponse`; see `docs/users/response-trace-context.md`). Query wall-clock is bounded by the querier's `query_timeout`; the caller's Flight per-request deadline is derived from it plus a grace margin so the callee's `deadline_exceeded` (HTTP 504) wins over an opaque client-side `cancelled`
5. Standalone querier also serves Tempo's `tempopb.Querier` gRPC protocol on the Flight port (see `tempo-api` skill)
6. Router also nests query APIs for every signal, each lowering a parsed query to a DataFusion `Expr`/aggregate over the signal's Iceberg tables (never a SQL string, matching the trace path):
   - `/loki` (LogQL, epic #366): `LogsService` executes log queries (streams) and metric queries (`rate`/`count_over_time`/`sum by`, `date_bin` bucketed matrix); `LogsService` also backs `/loki` labels/values/series and `detected_fields` (sampled attribute-field discovery: name, inferred type, approx cardinality — epic #737 L3).
   - `/prometheus` (PromQL, epic #328): `MetricsService` executes selectors, `sum/avg/min/max/count [by]`, and `rate`/`increase` over `metrics_gauge`+`metrics_sum` (`date_bin` bucketed matrix), plus `histogram_quantile(phi, metric)` interpolated from `metrics_histogram` OTLP buckets; `MetricsService` also backs `/prometheus` labels/values/series. The router serves a `/prometheus/api/v1/label_stats` extension (per-label cardinality) directly from the catalog's `attribute_stats`, without a Querier round-trip. `histogram_quantile` over an inner `rate()`, binary ops, and `topk` remain pending (#335).
   - `/pyroscope` (profiles) via `ProfileService`.
   - `POST /api/v1/query` (native **Query IR**, epic query-ir-core): the first-party structured query surface (versioned JSON IR over `logs`/`traces`, not a dialect string). `endpoints/query.rs` stamps the server clock and forwards a `query_ir:{tenant}:{dataset}:{json}` ticket to the querier's `IrService`, which validates + lowers the IR to a DataFusion `DataFrame` (promotion-invariant field resolution, three-valued absent semantics) and returns the declared `rows`/`series`/`table` envelope. The UI (Explore "Query" tab) and CLI (`query --ir`) consume it via generated clients. See `tempo-api` skill and `docs/users/querying-ir.md`.
7. Router serves the explore UI (static SPA from `src/ui`) at the **root** (SPA fallback behind the API routes) from the directory named by `SIGNALDB_UI_DIR` (`src/router/src/ui.rs`); unset -> placeholder page, set-but-invalid -> startup failure. Container images ship the assets preinstalled. Browser auth: `POST/DELETE /ui/session` (`src/router/src/endpoints/session.rs`) sets/clears an HttpOnly `signaldb_session` cookie the auth middleware accepts in place of auth headers; login tenant is optional (response lists memberships; sole membership auto-selected, several → UI tenant picker) and each request re-validates `X-Tenant-ID` against memberships; `GET /api/v1/whoami` returns the authenticated tenant + datasets.
8. Router is also the **OAuth 2.1 authorization server** for MCP connectors (change: mcp-oauth-dcr), off unless `[mcp.oauth]` is enabled: discovery, DCR, `/oauth/authorize` + a React consent screen at `/oauth/consent`, and `/oauth/token`. Claude.ai/ChatGPT register with no headers; the opaque, audience-bound access token carries its own tenant + read scopes. See the `multi-tenancy` skill and `docs/users/mcp.md`.

## Service Components

| Service       | Ports                                   | Capability                  | Key Files         |
| ------------- | --------------------------------------- | --------------------------- | ----------------- |
| **Acceptor**  | gRPC:4317, HTTP:4318                    | `TraceIngestion`            | `src/acceptor/`   |
| **Writer**    | Flight:50061 (standalone), 50051 (mono) | `TraceIngestion`, `Storage` | `src/writer/`     |
| **Router**    | HTTP:3000, Flight:50053                 | `Routing`                   | `src/router/`     |
| **Querier**   | Flight:50054                            | `QueryExecution`            | `src/querier/`    |
| **Compactor** | None (background task)                  | `Compaction`                | `src/compactor/`  |
| **MCP**       | HTTP:8228 (loopback default, `/mcp`)    | none (client of Router)     | `src/mcp-server/` |

## Deployment Models

- **Monolithic** (`cargo run --bin signaldb`): All services in one process, shared SQLite catalog. Compactor included (enabled by default; opt out with `[compactor].enabled = false`).
- **Microservices**: Independent binaries, shared catalog (PostgreSQL or SQLite)
- **Hybrid**: Mix of co-located and distributed services

**Note**: Monolithic mode runs the same compactor lifecycle loop as the standalone service (`compactor::service::CompactorService`) when `[compactor].enabled = true` (the default; retention enforcement — 30d for each of traces, logs, metrics and profiles — and orphan cleanup are also both on by default), and serves the compactor Flight endpoint (50055) plus observability HTTP on `[compactor].metrics_addr`. The lifecycle loop covers:

- Compaction planning and execution (Parquet rewrite for storage efficiency), scoped to one closed `timestamp_hour` partition per job and committed as an Iceberg delta (remove inputs / add outputs) so cost tracks the partition rather than the table and concurrent ingest does not invalidate the commit
- Retention enforcement, snapshot expiration, and orphan file cleanup. Orphan liveness comes from the union of the retained snapshots' manifests (never snapshot age), and a re-validation pass rebuilds that set before every real deletion batch, unconditionally
- Distributed-lease expiry (every 30s) for multi-instance safety

Each rewrite also runs a read-only attribute-stats pass logging per-key presence/cardinality + advisory materialization candidates (epic #737 L4a). Compaction, retention enforcement, orphan cleanup and lease expiry each run on their **own** task at their own cadence (`compactor::lifecycle`), so a long compaction cycle no longer delays the others (#1011); retention and compaction may therefore commit against one table concurrently — Iceberg optimistic concurrency retries on conflict — while retention never overlaps *itself* (orphan cleanup pre-expires snapshots through the same enforcer, and both paths are serialized behind one gate). Every cycle iteration is also guarded with `catch_unwind` (caught, counted in `CycleHealth`, retried with backoff) so a bug in one cycle can't end that task permanently either — `compactor_cycle_panics_total`/`compactor_cycle_down` surface it, while `/health` stays a pure liveness probe (always `200`) so a recovering cycle never trips a container restart. `catch_unwind` needs an unwinding panic strategy; this workspace's `[profile.release]` sets `panic = "abort"`, so the guard isn't load-bearing in a release build today. The admin `compact_now` action likewise runs `execute_candidate` on its own Flight request task, so an operator-triggered compaction can overlap a retention, snapshot-expiration or orphan-cleanup cycle on the same table. Per-table serialization across *all* entry points is still owed (task 6.4). Cross-process safety rests on catalog CAS plus the compaction commit's input-scoped validation. Every rewrite also runs the advisory attribute-stats pass (persisted to the catalog's `attribute_stats` table; dry-run promotion decisions under `[compactor.attr_promotion]` — epic #737 Layer 4).

## Dual Catalog System

1. **Service Catalog** (`Catalog`): PostgreSQL/SQLite for service discovery, tenant management, API keys, datasets. Monolith startup syncs config tenants into it; on a truly-empty first boot (no tenants in config or catalog) it auto-provisions a `default` tenant and prints the generated API key once (`common::bootstrap::bootstrap_default_tenant`)
2. **Iceberg Catalog** (`CatalogManager`): SQLite-only SQL catalog named `"signaldb"` for Iceberg table metadata

The Iceberg catalog only supports SQLite (not PostgreSQL). This is distinct from the service catalog.
