# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SignalDB is a distributed observability signal database built on the FDAP stack (Flight, DataFusion, Arrow, Parquet). It's designed for cost-effective storage and querying of metrics, logs, and traces with native OTLP support and Tempo API compatibility.

## Development Commands

### Build and Test

```bash
cargo build                # Build all workspace members
cargo build --release      # Release build with optimizations
cargo test                 # Run all tests across workspace
cargo test -p <package>    # Run tests for specific package (common, writer, acceptor, etc.)
cargo test <test_name>     # Run specific test by name
cargo test -- --nocapture  # Run tests with output visible
RUST_LOG=debug cargo test test_name -- --nocapture  # With logging
```

### Running Services

```bash
cargo run --bin signaldb                # Monolithic mode (all services in one process)
cargo run --bin signaldb -- acceptor    # OTLP ingestion (ports 4317/4318)
cargo run --bin signaldb -- router      # HTTP router (port 3000) + Flight (port 50053)
cargo run --bin signaldb -- writer      # Data persistence (Flight port 50061)
cargo run --bin signaldb -- querier     # Query execution (Flight port 50054)
cargo run --bin signaldb -- compactor   # Compaction, retention, orphan cleanup
cargo run --bin signaldb -- mcp         # MCP server sidecar
# One binary: `signaldb` is the monolith, `signaldb <service>` runs one service.
```

### Local Development

```bash
./scripts/run-dev.sh              # Monolithic mode with local file storage
./scripts/run-dev.sh services     # Microservices mode (logs to .data/logs/)
./scripts/run-dev.sh --sqlite     # SQLite mode (default, no dependencies)
./scripts/run-dev.sh --with-deps --postgres  # With PostgreSQL via docker compose
```

Storage locations: WAL files in `.data/wal/`, Parquet data in `.data/storage/`, SQLite in `.data/*.db`

### Pre-Commit Workflow

The project uses cargo-husky for pre-commit hooks that automatically run:

```bash
cargo fmt                  # Format code (runs automatically on commit)
cargo clippy --workspace --all-targets --all-features  # Lint (runs automatically on commit)
cargo machete --with-metadata  # Check for unused dependencies (run manually before commit)
cargo deny check           # License and security auditing
```

### Docker

```bash
docker compose up          # Start PostgreSQL, Grafana, MinIO, and SignalDB services
docker compose up --build  # Build images first, then start
docker compose build       # Build all service images
```

MinIO: Console at <http://localhost:9001>, API at <http://localhost:9000> (credentials: minioadmin/minioadmin)

### Grafana Plugin

```bash
npm install                     # From workspace root - install all dependencies
npm run grafana:dev             # Watch and rebuild frontend
npm run grafana:build           # Production build
cd src/grafana-plugin && npm run build:backend  # Build Rust backend
```

## Architecture Overview

### Workspace Members

- **acceptor** (`src/acceptor/`): OTLP HTTP/gRPC ingestion endpoint
- **router** (`src/router/`): Stateless routing layer with Tempo-compatible API
- **writer** (`src/writer/`): Stateful ingestion service (the "Ingester")
- **querier** (`src/querier/`): Query execution engine for stored data
- **common** (`src/common/`): Shared configuration, discovery, and data models
- **tempo-api** (`src/tempo-api/`): Grafana Tempo compatibility layer
- **signaldb-bin** (`src/signaldb-bin/`): Monolithic mode runner
- **grafana-plugin** (`src/grafana-plugin/`): Native Grafana datasource plugin
- **signal-producer** (`src/signal-producer/`): Test data generator
- **tests-integration** (`tests-integration/`): Integration test suite

### Data Flow

**Write Path**: Client → Acceptor (OTLP) → WAL → Writer (Flight) → Iceberg Tables (Parquet)
**Query Path**: Client → Router (HTTP) → Querier (Flight) → DataFusion → Iceberg Tables

### FDAP Stack

- **Flight**: Apache Arrow Flight for zero-copy inter-service communication
- **DataFusion**: SQL query processing engine
- **Arrow**: In-memory columnar data format
- **Parquet**: Persistent columnar storage with Iceberg table format

**Key Principle**: Use Arrow & Parquet types re-exported by DataFusion to ensure version compatibility.

### Service Discovery

- Catalog-based discovery using PostgreSQL or SQLite with heartbeat-based health checking
- ServiceBootstrap pattern for automatic registration with capability-based routing
- Service capabilities: TraceIngestion (Acceptor), Storage (Writer), QueryExecution (Querier), Routing (Router)

### Configuration

Precedence: defaults → TOML file (`signaldb.toml`) → environment variables (`SIGNALDB_*`)

```bash
cp signaldb.dist.toml signaldb.toml  # Copy and edit for your environment
```

Key sections: `[database]`, `[storage]`, `[discovery]`, `[wal]`, `[schema]`, `[auth]`

## Multi-Tenancy & Authentication

SignalDB provides tenant isolation with API key-based authentication:

```toml
[auth]
[[auth.tenants]]
id = "acme"
name = "Acme Corporation"
default_dataset = "production"

[[auth.tenants.api_keys]]
key = "sk-acme-prod-key-123"
name = "Production Key"
```

**Request Headers**:

- `Authorization: Bearer <api-key>`
- `X-Tenant-ID: <tenant>`
- `X-Dataset-ID: <dataset>` (optional)

**Isolation**: WAL organized by tenant/dataset (`.wal/{tenant}/{dataset}/{signal}/`), Iceberg tables namespaced per tenant.

**Table lifecycle**: the writer runs a signal-table reconciler (startup pass plus every `[writer].table_reconcile_interval`, default 5m) that ensures every registered tenant/dataset holds a table for each signal type enabled for that tenant, so a dataset is queryable before its first write. The ingest path still load-or-creates on demand, so a failing reconciler degrades to create-on-first-write. `POST /api/v1/tenants/{id}/tables/create` is the manual trigger. Queries against a signal with no table return an empty result, never an error. See `docs/operations/table-provisioning.md`.

## Storage Configuration

```toml
[storage]
dsn = "file:///path/to/data"  # Local filesystem
# dsn = "memory://"           # In-memory (testing)
# dsn = "s3://bucket/path"    # S3-compatible (MinIO, AWS)
```

For S3/MinIO, set environment variables:

```bash
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```

## Key Development Patterns

### Query IR is our own query surface

Everything first-party that reads data — the Explore UI, CLI, MCP tools,
tests, benchmarks, debugging — goes through the Query IR
(`POST /api/v1/query`, see `docs/users/querying-ir.md`), never the
Tempo/Loki/Prometheus/Pyroscope compatibility APIs. Those exist for external
clients (Grafana) and are lossy by design. If the IR can't express something
we need, extend the IR (a logical field, a stage) rather than reaching for a
compat endpoint.

### Flight Communication

Flight schemas defined in `src/common/flight/schema.rs` with conversions in `conversion/` subdirectory. Pattern: zero-copy RecordBatch transfer, connection pooling, streaming for large datasets.

### WAL Integration

Write-Ahead Log provides durability: Acceptor writes to WAL before acknowledgment → Writer processes entries → persists to Parquet → marks entries processed.

### Schema/Catalog

Located in `src/common/src/schema/`. Uses Apache Iceberg with SQL catalog backend (SQLite or PostgreSQL).

```toml
[schema]
catalog_type = "sql"
catalog_uri = "sqlite::memory:"  # or sqlite:///path/to/catalog.db
```

## Compactor Service

Manages compaction, retention, and orphan file cleanup.
Run: `cargo run --bin signaldb -- compactor`
Default: 30-day retention, dry_run=false, orphan cleanup enabled.
See `docs/operations/compactor/` for configuration and troubleshooting.

## Development Guidelines

- Test Driven Development: write tests before implementing features; all tests pass before committing
- Use testcontainers for integration tests involving external services
- Rust coding standards: @docs/contributing/rust.md

## Commit Guidelines

Use semantic commits for all changes.
