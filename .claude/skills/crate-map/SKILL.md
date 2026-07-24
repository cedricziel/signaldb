---
name: crate-map
description: SignalDB crate map - workspace members, module locations within common/writer/querier/router crates, and key root files. Use when navigating the codebase, finding where code lives, or understanding module boundaries.
user-invocable: false
sources:
  - Cargo.toml
---

# SignalDB Crate Map

## Workspace Members

| Crate | Path | Type | Description |
|-------|------|------|-------------|
| **common** | `src/common/` | Library | Shared everything: config, auth, WAL, Flight, catalog, schema, storage |
| **acceptor** | `src/acceptor/` | Binary + Library | OTLP gRPC/HTTP ingestion endpoint |
| **writer** | `src/writer/` | Binary + Library | Iceberg-based data persistence (the "Ingester") |
| **router** | `src/router/` | Binary + Library | HTTP API gateway + Flight routing layer |
| **querier** | `src/querier/` | Binary + Library | DataFusion query execution engine |
| **compactor** | `src/compactor/` | Binary + Library | Complete data lifecycle: compaction planning/execution (Phase 1-2), retention enforcement, snapshot expiration, orphan cleanup (Phase 3); binary is `signaldb-compactor` |
| **pyroscope-api** | `src/pyroscope-api/` | Library | Pyroscope-compatible API types (flamebearer, profile types) |
| **tempo-api** | `src/tempo-api/` | Library | Grafana Tempo API types and protobuf definitions |
| **loki-api** | `src/loki-api/` | Library | Loki HTTP API response types (LogQL query surface) |
| **prometheus-api** | `src/prometheus-api/` | Library | Prometheus HTTP API response types (PromQL query surface) |
| **signaldb-bin** | `src/signaldb-bin/` | Binary | Monolithic mode runner (all services in one process) |
| **signaldb-api** | `src/signaldb-api/` | Library | OpenAPI-generated admin API types |
| **signaldb-cli** | `src/signaldb-cli/` | Binary | CLI for tenant, API key, dataset management |
| **signaldb-sdk** | `src/signaldb-sdk/` | Library | Generated SDK client |
| **grafana-plugin** | `src/grafana-plugin/` | Plugin | Grafana datasource (TypeScript frontend + Rust backend); the backend is a standalone cargo workspace, not a root workspace member |
| **signal-producer** | `src/signal-producer/` | Binary | Test data generator (OTLP traces) |
| **tests-integration** | `tests-integration/` | Test crate | End-to-end integration tests |
| **xtask** | `xtask/` | Binary | Build automation tasks |

## The `common` Crate (most important)

This is the shared foundation. Key modules:

| Module | Path | Purpose |
|--------|------|---------|
| `config` | `src/common/src/config/mod.rs` | Configuration structs, TOML parsing, env vars |
| `auth` | `src/common/src/auth/` | Authenticator, middleware, validation, TenantContext |
| `catalog` | `src/common/src/catalog.rs` | Service catalog (PostgreSQL/SQLite) |
| `cli` | `src/common/src/cli.rs` | Common CLI functionality shared across binaries |
| `dataset` | `src/common/src/dataset.rs` | `DataSetType` enum (signal type naming) |
| `model` | `src/common/src/model/` | Trace/span data models |
| `ratelimit` | `src/common/src/ratelimit.rs` | Per-tenant token-bucket ingest rate limiting |
| `self_monitoring` | `src/common/src/self_monitoring/` | Dogfooding: app metrics, profiling, suppression |
| `tenant_api` | `src/common/src/tenant_api.rs` | Tenant API shared types and validation |
| `testing` | `src/common/src/testing/` | Test utilities (config builder) |
| `catalog_manager` | `src/common/src/catalog_manager.rs` | CatalogManager singleton for Iceberg catalog |
| `wal` | `src/common/src/wal/mod.rs` | Write-Ahead Log implementation |
| `flight` | `src/common/src/flight/` | Flight schemas, conversions, transport |
| `flight/schema.rs` | `src/common/src/flight/schema.rs` | Arrow schema definitions for OTLP data |
| `flight/transport.rs` | `src/common/src/flight/transport.rs` | InMemoryFlightTransport, connection pooling |
| `iceberg` | `src/common/src/iceberg/` | Consolidated Iceberg integration |
| `iceberg/mod.rs` | | Catalog creation, object store builders |
| `iceberg/schemas.rs` | | Schema creation functions for traces/logs/metrics, partition specs |
| `iceberg/names.rs` | | Naming utilities: `build_table_identifier`, `build_namespace`, `build_table_location` |
| `iceberg/table_manager.rs` | | IcebergTableManager with catalog caching |
| `schema` | `src/common/src/schema/` | Schema definitions and parsing |
| `schema/schema_parser.rs` | | TOML schema parser with inheritance |
| `storage` | `src/common/src/storage.rs` | Object store creation from DSN |
| `service_bootstrap` | `src/common/src/service_bootstrap.rs` | Service registration + heartbeat |

## The `writer` Crate

| Module | Path | Purpose |
|--------|------|---------|
| `processor.rs` | `src/writer/src/processor.rs` | WalProcessor -- background WAL->Iceberg |
| `schema_transform.rs` | `src/writer/src/schema_transform.rs` | Flight v1 -> Iceberg v2 transform |
| `storage/iceberg.rs` | `src/writer/src/storage/iceberg.rs` | IcebergTableWriter -- table creation + writes |
| `flight_iceberg.rs` | `src/writer/src/flight_iceberg.rs` | IcebergWriterFlightService |

## The `querier` Crate

| Module | Path | Purpose |
|--------|------|---------|
| `flight.rs` | `src/querier/src/flight.rs` | QuerierFlightService, TenantCatalog |
| `query` | `src/querier/src/query/` | Query execution modules |
| `query/table_ref.rs` | `src/querier/src/query/table_ref.rs` | Safe table reference with slug validation |
| `query/trace.rs` | `src/querier/src/query/trace.rs` | Trace query handlers |
| `query/logs.rs` / `query/logql.rs` | `src/querier/src/query/` | LogQL log query execution + Expr lowering |
| `query/metrics.rs` / `query/promql.rs` | `src/querier/src/query/` | PromQL metrics query execution + lowering |
| `query/error.rs` | `src/querier/src/query/error.rs` | Query error types |
| `query/search_filter.rs` | `src/querier/src/query/search_filter.rs` | Search filter parsing/handling |
| `services` | `src/querier/src/services/` | Service implementations |

## The `router` Crate

| Module | Path | Purpose |
|--------|------|---------|
| `lib.rs` | `src/router/src/lib.rs` | Router assembly: route mounting, auth layers |
| `main.rs` | `src/router/src/main.rs` | Standalone router binary |
| `discovery.rs` | `src/router/src/discovery.rs` | Cached service discovery for the router |
| `endpoints/tempo.rs` | `src/router/src/endpoints/tempo.rs` | Tempo-compatible API handlers |
| `endpoints/logql.rs` | `src/router/src/endpoints/logql.rs` | Loki-compatible API handlers under `/loki` (stubs until LogQL execution lands) |
| `endpoints/pyroscope.rs` | `src/router/src/endpoints/pyroscope.rs` | Pyroscope-compatible profile query handlers |
| `endpoints/admin.rs` | `src/router/src/endpoints/admin.rs` | Admin API for tenant/key/dataset CRUD |
| `endpoints/tenant.rs` | `src/router/src/endpoints/tenant.rs` | Tenant self-service API |
| `endpoints/flight.rs` | `src/router/src/endpoints/flight.rs` | Router Flight service |

## The `compactor` Crate

| Module | Path | Purpose |
|--------|------|---------|
| `main.rs` | `src/compactor/src/main.rs` | `signaldb-compactor` binary entry point |
| `planner.rs` | `src/compactor/src/planner.rs` | Compaction planning -- identifies candidates |
| `executor.rs` | `src/compactor/src/executor.rs` | Compaction execution -- rewrites Parquet files |
| `scheduler/` | `src/compactor/src/scheduler/` | Round-robin per-tenant scheduling (Phase 4) |
| `lease/` | `src/compactor/src/lease/` | Distributed leases for multi-instance safety (Phase 4) |
| `flight.rs` | `src/compactor/src/flight.rs` | CompactorFlightService (Flight :50055) |
| `http.rs` | `src/compactor/src/http.rs` | Observability HTTP endpoint (/metrics, /status, /health) |
| `rewriter.rs` | `src/compactor/src/rewriter.rs` | Parquet file rewriting logic |
| `commit.rs` | `src/compactor/src/commit.rs` | Atomic commit to Iceberg tables |
| `metrics.rs` | `src/compactor/src/metrics.rs` | Prometheus metrics for compactor operations |
| `retention/` | `src/compactor/src/retention/` | Phase 3: Retention enforcement |
| `retention/config.rs` | | Retention policy configuration with 3-tier overrides |
| `retention/enforcer.rs` | | Retention enforcement engine, partition dropping |
| `orphan/` | `src/compactor/src/orphan/` | Phase 3: Orphan file cleanup |
| `orphan/config.rs` | | Orphan cleanup configuration |
| `orphan/detector.rs` | | 4-phase orphan detection algorithm |
| `iceberg/` | `src/compactor/src/iceberg/` | Iceberg extensions for compactor |
| `iceberg/partition.rs` | | Partition operations: list, parse, drop |
| `iceberg/snapshot.rs` | | Snapshot operations: list, expire |
| `iceberg/manifest.rs` | | Manifest parsing, file reference extraction |

## Key Root Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace definition + shared dependencies |
| `schemas.toml` | Signal type schema definitions (compiled into binary) |
| `signaldb.dist.toml` | Example configuration file |
| `compose.yml` | Development environment setup |
| `Dockerfile` | Multi-stage build for all services |
