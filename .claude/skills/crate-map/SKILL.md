---
name: crate-map
description: SignalDB crate map - workspace members, module locations within common/writer/querier/router crates, and key root files. Use when navigating the codebase, finding where code lives, or understanding module boundaries.
user-invocable: false
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
| **compactor** | `src/compactor/` | Library | Compaction planner and executor for Parquet file optimization with rewrites and atomic commits (Phase 2) |
| **tempo-api** | `src/tempo-api/` | Library | Grafana Tempo API types and protobuf definitions |
| **signaldb-bin** | `src/signaldb-bin/` | Binary | Monolithic mode runner (all services in one process) |
| **signaldb-api** | `src/signaldb-api/` | Library | OpenAPI-generated admin API types |
| **signaldb-cli** | `src/signaldb-cli/` | Binary | CLI for tenant, API key, dataset management |
| **signaldb-sdk** | `src/signaldb-sdk/` | Library | Generated SDK client |
| **grafana-plugin** | `src/grafana-plugin/` | Plugin | Grafana datasource (TypeScript frontend + Rust backend) |
| **signal-producer** | `src/signal-producer/` | Binary | Test data generator (OTLP traces) |
| **tests-integration** | `tests-integration/` | Test crate | End-to-end integration tests |
| **xtask** | `xtask/` | Binary | Build automation tasks |

## The `common` Crate (most important)

This is the shared foundation. Key modules:

| Module | Path | Purpose |
|--------|------|---------|
| `config` | `src/common/src/config/mod.rs` | Configuration structs, TOML parsing, env vars |
| `auth` | `src/common/src/auth.rs` | Authenticator, TenantContext, API key validation |
| `catalog` | `src/common/src/catalog.rs` | Service catalog (PostgreSQL/SQLite) |
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
| `query/error.rs` | `src/querier/src/query/error.rs` | Query error types |
| `query/promql` | `src/querier/src/query/promql/` | PromQL query support |
| `services` | `src/querier/src/services/` | Service implementations |

## The `router` Crate

| Module | Path | Purpose |
|--------|------|---------|
| `tempo.rs` | `src/router/src/tempo.rs` | Tempo-compatible API handlers |
| `admin.rs` | `src/router/src/admin.rs` | Admin API for tenant/key/dataset CRUD |
| `service_registry.rs` | `src/router/src/service_registry.rs` | Cached service discovery |

## Key Root Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Workspace definition + shared dependencies |
| `schemas.toml` | Signal type schema definitions (compiled into binary) |
| `signaldb.dist.toml` | Example configuration file |
| `docker-compose.yml` | Development environment setup |
| `Dockerfile` | Multi-stage build for all services |
