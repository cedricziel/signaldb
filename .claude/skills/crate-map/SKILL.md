---
name: crate-map
description: SignalDB crate map - workspace members, module locations within common/writer/querier/router crates, and key root files. Use when navigating the codebase, finding where code lives, or understanding module boundaries.
user-invocable: false
sources:
  - Cargo.toml
---

# SignalDB Crate Map

## Workspace Members

| Crate                 | Path                   | Type       | Description                                                                                                                                                                                                                                      |
| --------------------- | ---------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **common**            | `src/common/`          | Library    | Shared everything: config, auth, WAL, Flight, catalog, schema, storage                                                                                                                                                                           |
| **acceptor**          | `src/acceptor/`        | Library    | OTLP gRPC/HTTP ingestion endpoint                                                                                                                                                                                                                |
| **writer**            | `src/writer/`          | Library    | Iceberg-based data persistence (the "Ingester")                                                                                                                                                                                                  |
| **router**            | `src/router/`          | Library    | HTTP API gateway + Flight routing layer                                                                                                                                                                                                          |
| **querier**           | `src/querier/`         | Library    | DataFusion query execution engine                                                                                                                                                                                                                |
| **compactor**         | `src/compactor/`       | Library    | Complete data lifecycle: compaction planning/execution (Phase 1-2), retention enforcement, snapshot expiration, orphan cleanup (Phase 3); binary is `signaldb-compactor`                                                                         |
| **pyroscope-api**     | `src/pyroscope-api/`   | Library    | Pyroscope-compatible API types (flamebearer, profile types)                                                                                                                                                                                      |
| **tempo-api**         | `src/tempo-api/`       | Library    | Grafana Tempo API types and protobuf definitions                                                                                                                                                                                                 |
| **loki-api**          | `src/loki-api/`        | Library    | Loki HTTP API response types (LogQL query surface)                                                                                                                                                                                               |
| **prometheus-api**    | `src/prometheus-api/`  | Library    | Prometheus HTTP API response types (PromQL query surface)                                                                                                                                                                                        |
| **logql**             | `src/logql/`           | Library    | LogQL lexer, AST, and parser. Syntax only — no product dependency (`thiserror` alone); lowering to DataFusion lives in `querier/query/logql.rs`. Published as `logql-parser`, imported as `logql` (crates.io `logql` is taken)                   |
| **traceql**           | `src/traceql/`         | Library    | TraceQL parser for the supported equality subset. Syntax only — no product dependency; lowering lives in `querier/query/search_filter.rs`. Published as `traceql-parser`, imported as `traceql`                                                  |
| **query-ir**          | `src/query-ir/`        | Library    | SignalDB's own signal-agnostic query IR: document model, validation, field resolution. Leaf crate (serde only) so a document can be built and validated without the query engine; `publish = false`. Re-exported as `common::query_ir`            |
| **ql-ir**             | `src/ql-ir/`           | Library    | Lowers parsed LogQL/TraceQL onto the query IR (design D6). Depends on `logql`, `traceql`, `query-ir` — which is its job — but on nothing FDAP, so a caller can turn query text into an executable document without the engine. TraceQL only so far; `publish = false` |
| **schema-model**      | `src/schema-model/`    | Library    | OTel Weaver semantic-convention model: parser (single-doc JSON/YAML or multi-file `model/` tree), resolver (flat attribute/entity/metric defs + reverse indexes), subset validator; dependency-light so `common` and its build script can use it |
| **signaldb-bin**      | `src/signaldb-bin/`    | Binary     | The `signaldb` executable: monolith by default, or one service via a subcommand (`signaldb router`, …); every service crate exposes `cli::Args` + `cli::run`                                                                                     |
| **signaldb-api**      | `src/signaldb-api/`    | Library    | Hand-written admin API DTOs (utoipa `ToSchema`); the admin half of the code-first OpenAPI schemas (management DTOs live in `router`)                                                                                                             |
| **signaldb-cli**      | `src/signaldb-cli/`    | Binary     | CLI for tenant, API key, dataset management; shell completions incl. dynamic tenant IDs                                                                                                                                                          |
| **signaldb-sdk**      | `src/signaldb-sdk/`    | Library    | Generated Rust HTTP client (progenitor) from the OpenAPI spec; hand-written `retry` (retry-on-throttle policy as the progenitor `ClientHooks::exec` override) and `builder` (`ClientBuilder`, the sole construction path for CLI/MCP)            |
| **mcp-server**        | `src/mcp-server/`      | Library    | Model Context Protocol server (`signaldb mcp`); credential-forwarding client over `signaldb-sdk`, serves MCP at `/mcp`; `audit.rs` audits, traces, meters, and bounds every tool call                                                            |
| **grafana-plugin**    | `src/grafana-plugin/`  | Plugin     | Grafana datasource (TypeScript frontend + Rust backend); the backend is a standalone cargo workspace, not a root workspace member                                                                                                                |
| **signal-producer**   | `src/signal-producer/` | Binary     | Test data generator (OTLP traces)                                                                                                                                                                                                                |
| **tests-integration** | `tests-integration/`   | Test crate | End-to-end integration tests                                                                                                                                                                                                                     |
| **xtask**             | `xtask/`               | Binary     | Code generation and build tasks: OpenAPI-derived Rust SDK + TypeScript UI client, and `tempo-api`'s protobuf bindings (`src/tempopb.rs`, moved out of a build script). `generate` writes, `check` verifies                                                                                                                                                                |

## The `common` Crate (most important)

This is the shared foundation. Key modules:

| Module                     | Path                                  | Purpose                                                                                 |
| -------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------- |
| `config`                   | `src/common/src/config/mod.rs`        | Configuration structs, TOML parsing, env vars                                           |
| `auth`                     | `src/common/src/auth/`                | Authenticator, middleware, validation, TenantContext, password/session-token hashing    |
| `catalog`                  | `src/common/src/catalog.rs`           | Service catalog (PostgreSQL/SQLite)                                                     |
| `cli`                      | `src/common/src/cli.rs`               | Common CLI functionality shared across binaries                                         |
| `dataset`                  | `src/common/src/dataset.rs`           | `DataSetType` enum (signal type naming)                                                 |
| `model`                    | `src/common/src/model/`               | Trace/span data models                                                                  |
| `ratelimit`                | `src/common/src/ratelimit.rs`         | Per-tenant token-bucket ingest rate limiting                                            |
| `self_monitoring`          | `src/common/src/self_monitoring/`     | Dogfooding: app metrics, profiling, suppression                                         |
| `tenant_api`               | `src/common/src/tenant_api.rs`        | Tenant API shared types and validation                                                  |
| `testing`                  | `src/common/src/testing/`             | Test utilities (config builder)                                                         |
| `catalog_manager`          | `src/common/src/catalog_manager.rs`   | CatalogManager singleton for Iceberg catalog                                            |
| `wal`                      | `src/common/src/wal/mod.rs`           | Write-Ahead Log implementation                                                          |
| `wal::framing`             | `src/common/src/wal/framing.rs`       | WAL record framing v1: segment header, length + CRC-32 record headers, legacy detection |
| `wal::manager`             | `src/common/src/wal/manager.rs`       | Per-tenant/dataset/signal WAL fanout shared by the acceptor and the writer              |
| `flight`                   | `src/common/src/flight/`              | Flight schemas, conversions, transport                                                  |
| `flight/schema.rs`         | `src/common/src/flight/schema.rs`     | Arrow schema definitions for OTLP data                                                  |
| `flight/transport.rs`      | `src/common/src/flight/transport.rs`  | InMemoryFlightTransport, connection pooling                                             |
| `iceberg`                  | `src/common/src/iceberg/`             | Consolidated Iceberg integration                                                        |
| `iceberg/mod.rs`           |                                       | Catalog creation, object store builders                                                 |
| `iceberg/schemas.rs`       |                                       | Schema creation functions for traces/logs/metrics, partition specs                      |
| `iceberg/names.rs`         |                                       | Naming utilities: `build_table_identifier`, `build_namespace`, `build_table_location`   |
| `iceberg/table_manager.rs` |                                       | IcebergTableManager with catalog caching                                                |
| `schema`                   | `src/common/src/schema/`              | Schema definitions and parsing                                                          |
| `schema/schema_parser.rs`  |                                       | TOML schema parser with inheritance                                                     |
| `storage`                  | `src/common/src/storage.rs`           | Object store creation from DSN                                                          |
| `service_bootstrap`        | `src/common/src/service_bootstrap.rs` | Service registration + heartbeat                                                        |

## The `writer` Crate

| Module                | Path                                 | Purpose                                       |
| --------------------- | ------------------------------------ | --------------------------------------------- |
| `processor.rs`        | `src/writer/src/processor.rs`        | WalProcessor -- background WAL->Iceberg       |
| `schema_transform.rs` | `src/writer/src/schema_transform.rs` | Flight v1 -> Iceberg v2 transform             |
| `storage/iceberg.rs`  | `src/writer/src/storage/iceberg.rs`  | IcebergTableWriter -- table creation + writes |
| `flight_iceberg.rs`   | `src/writer/src/flight_iceberg.rs`   | IcebergWriterFlightService                    |

## The `querier` Crate

| Module                                 | Path                                     | Purpose                                   |
| -------------------------------------- | ---------------------------------------- | ----------------------------------------- |
| `flight.rs`                            | `src/querier/src/flight.rs`              | QuerierFlightService, TenantCatalog       |
| `query`                                | `src/querier/src/query/`                 | Query execution modules                   |
| `query/table_ref.rs`                   | `src/querier/src/query/table_ref.rs`     | Safe table reference with slug validation |
| `query/trace.rs`                       | `src/querier/src/query/trace.rs`         | Trace query handlers                      |
| `query/logs.rs` / `query/logql.rs`     | `src/querier/src/query/`                 | LogQL log query execution + Expr lowering |
| `query/metrics.rs` / `query/promql.rs` | `src/querier/src/query/`                 | PromQL metrics query execution + lowering |
| `query/error.rs`                       | `src/querier/src/query/error.rs`         | Query error types                         |
| `query/search_filter.rs`               | `src/querier/src/query/search_filter.rs` | Search filter parsing/handling            |
| `services`                             | `src/querier/src/services/`              | Service implementations                   |

## The `router` Crate

| Module                   | Path                                    | Purpose                                                                        |
| ------------------------ | --------------------------------------- | ------------------------------------------------------------------------------ |
| `lib.rs`                 | `src/router/src/lib.rs`                 | Router assembly: route mounting, auth layers                                   |
| `main.rs`                | `src/router/src/main.rs`                | Standalone router binary                                                       |
| `discovery.rs`           | `src/router/src/discovery.rs`           | Cached service discovery for the router                                        |
| `endpoints/tempo.rs`     | `src/router/src/endpoints/tempo.rs`     | Tempo-compatible API handlers                                                  |
| `endpoints/logql.rs`     | `src/router/src/endpoints/logql.rs`     | Loki-compatible API handlers under `/loki` — log and metric queries, labels, values, series, `detected_fields`; dispatches Flight tickets to the querier |
| `endpoints/pyroscope.rs` | `src/router/src/endpoints/pyroscope.rs` | Pyroscope-compatible profile query handlers                                    |
| `endpoints/admin.rs`     | `src/router/src/endpoints/admin.rs`     | Admin API for tenant/key/dataset CRUD                                          |
| `endpoints/tenant.rs`    | `src/router/src/endpoints/tenant.rs`    | Tenant self-service API                                                        |
| `endpoints/session.rs`   | `src/router/src/endpoints/session.rs`   | UI session login/logout (`/ui/session`) + `/api/v1/whoami`                     |
| `endpoints/flight.rs`    | `src/router/src/endpoints/flight.rs`    | Router Flight service                                                          |

## The `compactor` Crate

| Module                  | Path                            | Purpose                                                      |
| ----------------------- | ------------------------------- | ------------------------------------------------------------ |
| `main.rs`               | `src/compactor/src/main.rs`     | `signaldb-compactor` binary entry point                      |
| `planner.rs`            | `src/compactor/src/planner.rs`  | Compaction planning -- one candidate per closed partition    |
| `executor.rs`           | `src/compactor/src/executor.rs` | Compaction execution -- rewrites one partition, delta commit |
| `scheduler/`            | `src/compactor/src/scheduler/`  | Round-robin per-tenant scheduling (Phase 4)                  |
| `lease/`                | `src/compactor/src/lease/`      | Distributed leases for multi-instance safety (Phase 4)       |
| `flight.rs`             | `src/compactor/src/flight.rs`   | CompactorFlightService (Flight :50055)                       |
| `http.rs`               | `src/compactor/src/http.rs`     | Observability HTTP endpoint (/metrics, /status, /health)     |
| `rewriter.rs`           | `src/compactor/src/rewriter.rs` | Parquet file rewriting logic                                 |
| `commit.rs`             | `src/compactor/src/commit.rs`   | Atomic commit to Iceberg tables                              |
| `metrics.rs`            | `src/compactor/src/metrics.rs`  | Prometheus metrics for compactor operations                  |
| `retention/`            | `src/compactor/src/retention/`  | Phase 3: Retention enforcement                               |
| `retention/config.rs`   |                                 | Retention policy configuration with 3-tier overrides         |
| `retention/enforcer.rs` |                                 | Retention enforcement engine, partition dropping             |
| `orphan/`               | `src/compactor/src/orphan/`     | Phase 3: Orphan file cleanup                                 |
| `orphan/config.rs`      |                                 | Orphan cleanup configuration                                 |
| `orphan/detector.rs`    |                                 | 4-phase orphan detection algorithm                           |
| `iceberg/`              | `src/compactor/src/iceberg/`    | Iceberg extensions for compactor                             |
| `iceberg/partition.rs`  |                                 | Partition operations: list, parse, drop                      |
| `iceberg/snapshot.rs`   |                                 | Snapshot operations: list, expire                            |
| `iceberg/manifest.rs`   |                                 | Manifest parsing, file reference extraction                  |

## Key Root Files

| File                   | Purpose                                                                                                                                         |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `Cargo.toml`           | Workspace definition + shared dependencies                                                                                                      |
| `schemas.toml`         | Signal type schema definitions (compiled into binary)                                                                                           |
| `signaldb.dist.toml`   | Example configuration file                                                                                                                      |
| `compose.yml`          | Development environment setup                                                                                                                   |
| `Dockerfile`           | Multi-stage build for all services                                                                                                              |
| `vendor/otel-semconv/` | Vendored OpenTelemetry semconv `model/` at the self-monitoring pin (`cargo xtask vendor-semconv`); source of the bundled `otel` schema registry |
| `otel/registry/`       | SignalDB's own semconv registry (`signaldb.*` attributes; Weaver-checked in CI); source of the bundled `signaldb` schema registry               |
