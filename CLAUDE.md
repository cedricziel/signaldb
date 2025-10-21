# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SignalDB is a distributed observability signal database built on the FDAP stack (Flight, DataFusion, Arrow, Parquet). It's designed for cost-effective storage and querying of metrics, logs, and traces with native OTLP support and Tempo API compatibility.

## Development Commands

### Configuration Setup
```bash
cp signaldb.dist.toml signaldb.toml  # Copy distribution config
# Edit signaldb.toml for your environment
```

### Build and Test
```bash
cargo build                # Build all workspace members
cargo build --release      # Release build with optimizations
cargo test                 # Run all tests across workspace
cargo test -p <package>    # Run tests for specific package
cargo test <test_name>     # Run specific test by name
cargo test -- --nocapture  # Run tests with output visible
cargo run                  # Run in monolithic mode (all services)
cargo clippy --workspace --all-targets --all-features  # Check for code quality issues
cargo deny check           # License and security auditing
cargo machete --with-metadata  # Check for unused dependencies (enhanced analysis)
cargo fmt                  # Format code (run before committing)
```

### Building Docker Images
```bash
# Build all services
docker compose build

# Build specific service
docker compose build signaldb-writer
docker compose build signaldb-acceptor
docker compose build signaldb-querier
docker compose build signaldb-router

# Build with no cache (clean build)
docker compose build --no-cache

# Build and start services
docker compose up --build

# Using helper script
./scripts/build-images.sh           # Build all services
./scripts/build-images.sh writer    # Build specific service
```

**Docker Build Details**:
- Uses multi-stage Dockerfile with Alpine Linux for minimal image sizes
- Each service image is ~15-25MB (stripped binaries on Alpine base)
- Build cache optimization: dependencies cached separately from source code
- First build: ~5-10 minutes (downloads dependencies, compiles everything)
- Incremental builds: <1 minute (only recompiles changed code)
- Images use musl libc (Alpine-compatible) instead of glibc
- Non-root user execution for security
- Build artifacts stored in `target/release/`

**Image Specifications**:
- `signaldb/acceptor:latest` - OTLP ingestion service
- `signaldb/router:latest` - HTTP API and Flight endpoint
- `signaldb/writer:latest` - Data persistence and storage
- `signaldb/querier:latest` - Query execution engine

### Running Individual Services
```bash
# Main SignalDB services
cargo run --bin signaldb   # Monolithic mode (all services in one)
cargo run --bin acceptor   # OTLP ingestion service (ports 4317/4318)
cargo run --bin router     # HTTP router (port 3000) + Flight (port 50053)
cargo run --bin writer     # Data ingestion and storage service
cargo run --bin querier    # Query execution engine (port 9000)
```

### Local Development Mode
Use the `run-dev.sh` script for a quick local development setup with local file storage:

```bash
# Run in monolithic mode (single process, easiest for development)
./scripts/run-dev.sh

# Run as microservices (separate processes, logs to .data/logs/)
./scripts/run-dev.sh services

# With PostgreSQL (starts via docker compose)
./scripts/run-dev.sh --with-deps --postgres

# SQLite mode (no dependencies, default)
./scripts/run-dev.sh --sqlite
```

**What it does**:
- Creates local storage directories (`.data/wal`, `.data/storage`, `.data/logs`)
- Configures services to use local filesystem instead of MinIO
- Uses SQLite by default (no dependencies) or PostgreSQL (with `--with-deps`)
- Sets up proper service discovery and inter-service communication
- Handles graceful shutdown with Ctrl+C

**Storage locations**:
- WAL files: `.data/wal/acceptor/`, `.data/wal/writer/`
- Parquet data: `.data/storage/`
- SQLite databases: `.data/signaldb.db`, `.data/catalog.db`
- Service logs: `.data/logs/*.log` (services mode only)

**Modes**:
- **Monolithic** (default): All services in one process, logs to stdout, easiest for debugging
- **Services**: Separate processes per service, logs to individual files, closer to production

### Infrastructure
```bash
docker compose up          # Start PostgreSQL, Grafana, MinIO, and supporting services
docker compose up --build  # Build images first, then start services
```

The docker-compose setup includes:
- **PostgreSQL**: Service discovery and catalog backend
- **Grafana**: Visualization and dashboards
- **MinIO**: S3-compatible object storage (API: port 9000, Console: port 9001)
- **SignalDB services**: Acceptor, Writer, Querier, Router (built from local Dockerfile)

**Docker Compose Behavior**:
- First run: Automatically builds all SignalDB service images from source
- Subsequent runs: Uses cached images unless you specify `--build`
- Images are tagged as `signaldb/{service}:latest`
- Built from multi-stage `Dockerfile` with Alpine-based minimal runtime

**Storage Architecture**:
- Writer and Querier services are configured to use MinIO (S3) for Parquet data storage
- WAL (Write-Ahead Log) still uses local volumes for durability guarantees
- MinIO bucket `signaldb` is automatically created on startup via `minio-init` service

MinIO credentials (default):
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Console URL: http://localhost:9001
- API URL: http://localhost:9000

## Architecture Overview

### Core Components (Workspace Members)

**SignalDB Services**:
- **Acceptor** (`src/acceptor/`): OTLP HTTP/gRPC ingestion endpoint
- **Router** (`src/router/`): Stateless routing layer with Tempo-compatible API
- **Writer** (`src/writer/`): Stateful ingestion service (the "Ingester")
- **Querier** (`src/querier/`): Query execution engine for stored data
- **Common** (`src/common/`): Shared configuration, discovery, and data models
- **Tempo API** (`src/tempo-api/`): Grafana Tempo compatibility layer
- **SignalDB Binary** (`src/signaldb-bin/`): Monolithic mode runner

### Data Flow

**Write Path**: Client → Acceptor (OTLP) → WAL → Writer (Flight) → Parquet Storage
**Query Path**: Client → Router (HTTP) → Querier (Flight) → DataFusion → Parquet Files

### Service Discovery & Communication

**Discovery mechanism**:
- **Catalog-based**: PostgreSQL/SQLite-backed metadata store with heartbeat-based health checking
- **ServiceBootstrap pattern**: Automatic service registration with capability-based discovery
- **Flight transport**: High-performance inter-service communication with connection pooling

**Service capabilities**:
- **TraceIngestion**: Services that accept OTLP trace data (Acceptor)
- **Storage**: Services that persist data to storage (Writer)
- **QueryExecution**: Services that execute queries (Querier)
- **Routing**: Services that provide HTTP APIs (Router)

### Configuration

Configuration precedence: defaults → TOML file (`signaldb.toml`) → environment variables (`SIGNALDB_*`)

Copy `signaldb.dist.toml` to `signaldb.toml` and adjust for your environment.

Key sections: `[database]`, `[storage]`, `[discovery]`, `[wal]`, `[schema]`

## Key Development Patterns

### FDAP Stack Components

SignalDB is built on the FDAP stack:
- **F**light - Apache Arrow Flight for inter-service communication
- **D**ataFusion - Apache DataFusion for SQL query processing
- **A**rrow - Apache Arrow for in-memory columnar data
- **P**arquet - Apache Parquet for persistent columnar storage

**Key Principle**: Prefer using Arrow & Parquet types re-exported by DataFusion to ensure version compatibility.

### Arrow Flight Integration

The system uses Apache Arrow Flight extensively for inter-service communication. Flight schemas are defined in `src/common/flight/schema.rs` with conversions in the `conversion/` subdirectory.

**Flight Communication Pattern**:
- Zero-copy data transfer using Arrow RecordBatches
- Service discovery via capability-based routing
- Connection pooling for performance
- Streaming support for large datasets

### Service Registration

Services register themselves with discovery backends on startup. Look at existing patterns in acceptor/router for implementing new services.

### Data Processing

**WAL Integration**:
- Write-Ahead Log provides durability guarantees for incoming data
- Acceptor writes to WAL before acknowledgment
- Writer processes WAL entries and persists to Parquet
- Automatic WAL entry marking and cleanup

**Flight Communication**:
- Apache Arrow Flight for zero-copy data transfer between services
- Service discovery via capability-based routing
- Connection pooling and automatic failover
- Streaming support for large datasets

### Storage Integration

SignalDB uses a pluggable storage system with DSN-based configuration:

**Storage Configuration**:
```toml
[storage]
dsn = "file:///path/to/data"  # Local filesystem storage
# dsn = "memory://"           # In-memory storage (for testing)
# dsn = "s3://bucket-name/path"  # S3-compatible storage (MinIO, AWS S3)
```

Supported storage backends:
- **Local filesystem**: `file:///path/to/data`
- **In-memory**: `memory://` (for testing and development)
- **S3-compatible**: `s3://bucket-name/path` (MinIO, AWS S3, etc.)
- **Future**: Azure Blob, GCP Cloud Storage via DSN format

**MinIO/S3 Configuration**:

To use MinIO for object storage, configure environment variables:
```bash
# MinIO endpoint (local docker-compose setup)
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1  # MinIO requires a region

# Then use S3 storage DSN
SIGNALDB_STORAGE_DSN=s3://signaldb/data
```

For docker-compose services, add to environment (already configured in `compose.yml`):
```yaml
environment:
  AWS_ENDPOINT_URL: http://minio:9000
  AWS_ACCESS_KEY_ID: minioadmin
  AWS_SECRET_ACCESS_KEY: minioadmin
  AWS_REGION: us-east-1
  AWS_ALLOW_HTTP: "true"
  SIGNALDB_STORAGE_DSN: s3://signaldb/data
```

The bucket `signaldb` is automatically created by the `minio-init` service in docker-compose.

To manually create buckets via MinIO console (http://localhost:9001) or using `mc` CLI:
```bash
# Install MinIO client
brew install minio/stable/mc  # macOS
# Or download from https://min.io/download

# Configure MinIO alias
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create bucket
mc mb local/signaldb
```

### Schema and Catalog Integration

SignalDB uses Apache Iceberg for table format and catalog management:

**Schema Configuration**:
```toml
[schema]
catalog_type = "sql"                    # sql (recommended) or memory
catalog_uri = "sqlite::memory:"         # In-memory SQLite catalog
# catalog_uri = "sqlite:///path/to/catalog.db"  # Persistent SQLite catalog
```

**Schema Module**: Located in `src/common/src/schema/`
- Direct integration with Iceberg's native Catalog trait
- SQL catalog backend with SQLite (in-memory or persistent)
- Memory catalog backend for testing and development
- Foundation for future PostgreSQL catalog backends
- Object store integration for table data storage

## Testing

### Test Organization
- Unit tests: Alongside source code in each module
- Integration tests: `tests/` directory in workspace root and component-specific `tests/` dirs
- E2E tests: Component-specific, often using testcontainers

### Running Tests
```bash
# All tests
cargo test

# Specific package
cargo test -p common
cargo test -p writer
cargo test -p acceptor

# Specific test
cargo test test_name
cargo test test_name -- --nocapture  # With output

# Tests with logging
RUST_LOG=debug cargo test test_name -- --nocapture
```

### Test Infrastructure Patterns

**Service Testing**:
- Integration tests often use testcontainers for PostgreSQL
- Flight-based tests require service discovery setup
- WAL tests need temporary directories

## Deployment Modes

SignalDB supports two deployment modes:

### Monolithic Mode
Single binary with all services running in one process:
```bash
cargo run --bin signaldb
```
- Ideal for development and small deployments
- All services communicate via localhost
- Shared SQLite catalog for service discovery
- Zero-configuration startup

### Microservices Mode
Independent services for scalable production:
```bash
# Start each service independently
cargo run --bin signaldb-acceptor &
cargo run --bin signaldb-router &
cargo run --bin signaldb-writer &
cargo run --bin signaldb-querier &
```
- Horizontal scaling of individual components
- PostgreSQL recommended for service discovery
- Requires proper configuration for inter-service communication

## Grafana Datasource Plugin

SignalDB includes a native Grafana datasource plugin located in `src/grafana-plugin/`. The plugin provides unified querying for traces, metrics, and logs through a single datasource.

### Plugin Architecture

**Frontend** (TypeScript/React):
- Query editor with signal type selection (traces, metrics, logs)
- Configuration editor for Router connection settings
- Built with Grafana plugin SDK (@grafana/data, @grafana/ui)

**Backend** (Rust):
- Built with grafana-plugin-sdk (0.4.0)
- Connects to SignalDB Router (HTTP or Flight)
- Implements datasource query and health check handlers
- Binary name: `gpx_signaldb_datasource`

### Development Commands

```bash
# From workspace root
npm install                     # Install all workspace dependencies

# Frontend development
npm run grafana:dev             # Watch and rebuild frontend
npm run grafana:build           # Production build

# Backend development (from src/grafana-plugin)
cd src/grafana-plugin
npm run build:backend           # Build Rust backend
npm run dev:backend             # Watch and rebuild backend

# Full plugin build
npm run build                   # Build both frontend and backend
```

### Plugin Development

Start the plugin development environment:

```bash
# 1. Start main SignalDB services (from root)
docker compose up

# 2. Build and start Grafana with plugin (from src/grafana-plugin)
cd src/grafana-plugin
npm install
npm run build                   # Build plugin
docker compose up               # Start Grafana dev server
```

Access Grafana at http://localhost:3000 (admin/admin). The SignalDB datasource will be available in Administration > Plugins.

### Plugin Configuration

Configure the datasource with:
- **Router URL**: SignalDB Router address (default: http://localhost:3001)
- **Protocol**: HTTP or Arrow Flight
- **Timeout**: Query timeout in seconds (default: 30)

### Query Types

The plugin supports three signal types:

1. **Traces**: Query using TraceQL syntax
   - Example: `{ service.name = "my-service" } | duration > 100ms`

2. **Metrics**: Query using PromQL syntax
   - Example: `up{job="my-job"}`

3. **Logs**: Query using LogQL syntax
   - Example: `{app="my-app"} |= "error"`

### Plugin Structure

```
src/grafana-plugin/
├── backend/              # Rust backend (grafana-plugin-sdk)
│   ├── src/
│   │   └── main.rs      # Datasource implementation
│   └── Cargo.toml
├── src/                  # TypeScript frontend
│   ├── components/
│   │   ├── ConfigEditor.tsx
│   │   └── QueryEditor.tsx
│   ├── datasource.ts    # Datasource class
│   ├── module.ts        # Plugin entry point
│   ├── plugin.json      # Plugin metadata
│   └── types.ts         # TypeScript types
├── dist/                # Compiled binaries and frontend
├── build-backend.sh     # Backend build script
├── docker-compose.yaml  # Development environment
└── package.json         # npm configuration
```

## Development Memories
- For arrow & parquet try using the ones re-exported by datafusion
- We need to run cargo fmt after bigger chunks of work to apply the canonical formatting
- Run cargo machete --with-metadata before committing and remove unused dependencies
- We need to format the code before committing
- Always run cargo commands from the workspace root
- Run cargo clippy and fix all warnings before committing
- Project uses Rust edition 2024, requires Rust 1.86.0+ minimum
- The Grafana plugin uses npm workspaces - install dependencies from workspace root
- Plugin backend must be built before starting Grafana dev server

## Code Quality Standards

### Clippy Compliance
- Write clippy-compliant code from the start to maintain high code quality
- Use direct variable interpolation in format strings: `format!("{variable}")` not `format!("{}", variable)`
- Prefer `!is_empty()` over `len() > 0` for clarity
- Use `vec![...]` instead of creating empty Vec and pushing elements
- Avoid `assert!(false, ...)` - use `panic!(...)` or `unreachable!()` instead
- Use `panic!("message")` for intentional panics rather than `assert!(false, "message")`

### String Formatting Best Practices
```rust
// ✅ Good - direct variable interpolation
format!("Service {service_id} at {address}")
log::info!("Discovered {count} services with capability {capability:?}")

// ❌ Avoid - positional arguments
format!("Service {} at {}", service_id, address)
log::info!("Discovered {} services with capability {:?}", count, capability)
```

### Collection Patterns
```rust
// ✅ Good - use vec! macro
let items = vec![item1, item2, item3];

// ❌ Avoid - push after creation
let mut items = Vec::new();
items.push(item1);
items.push(item2);
```

### Error Handling
```rust
// ✅ Good - explicit panic
panic!("Failed to initialize: {error}");

// ❌ Avoid - assert false
assert!(false, "Failed to initialize: {}", error);
```

## Dependency Management

### Unused Dependency Detection
- Always use `cargo machete --with-metadata` for comprehensive unused dependency detection
- The basic `cargo machete` command may miss dependencies that are only unused in specific configurations
- The `--with-metadata` flag provides enhanced analysis that catches more unused dependencies
- Remove unused dependencies promptly to keep the dependency graph clean and reduce build times

### Dependency Hygiene Best Practices
```bash
# ✅ Good - comprehensive dependency analysis
cargo machete --with-metadata

# ❌ Less effective - basic analysis may miss unused deps
cargo machete
```

Benefits of clean dependency management:
- Faster build times
- Reduced security surface area
- Smaller binary sizes
- Easier dependency auditing
- Less potential for dependency conflicts
