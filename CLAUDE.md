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
cargo test                 # Run all tests across workspace
cargo run                  # Run in monolithic mode (all services)
cargo clippy --workspace --all-targets --all-features  # Check for code quality issues
cargo deny check           # License and security auditing
cargo machete --with-metadata  # Check for unused dependencies (enhanced analysis)
```

### Running Individual Services
```bash
cargo run --bin acceptor   # OTLP ingestion service (ports 4317/4318)
cargo run --bin router     # HTTP router (port 3000) + Flight (port 50053)
cargo run --bin writer     # Data ingestion and storage service
cargo run --bin querier    # Query execution engine (port 9000)
```

### Infrastructure
```bash
docker compose up          # Start PostgreSQL, Grafana, and supporting services
```

## Architecture Overview

### Core Components (Workspace Members)

- **Acceptor** (`src/acceptor/`): OTLP HTTP/gRPC ingestion endpoint
- **Router** (`src/router/`): Stateless routing layer with Tempo-compatible API
- **Writer** (`src/writer/`): Stateful ingestion service (the "Ingester")
- **Querier** (`src/querier/`): Query execution engine for stored data
- **Common** (`src/common/`): Shared configuration, discovery, and data models
- **Tempo API** (`src/tempo-api/`): Grafana Tempo compatibility layer

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

Configuration precedence: defaults → TOML file (`signaldb.toml`) → environment variables (`SIGNALDB__*`)

Copy `signaldb.dist.toml` to `signaldb.toml` and adjust for your environment.

Key sections: `[database]`, `[storage]`, `[discovery]`, `[wal]`, `[schema]`

## Key Development Patterns

### Arrow Flight Integration

The system uses Apache Arrow Flight extensively for inter-service communication. Flight schemas are defined in `src/common/flight/schema.rs` with conversions in the `conversion/` subdirectory.

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
```

Supported storage backends:
- **Local filesystem**: `file:///path/to/data`
- **In-memory**: `memory://` (for testing and development)
- **Future**: S3, Azure Blob, GCP Cloud Storage via DSN format

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

Integration tests are in workspace root `tests/` and individual component `tests/` directories. Some tests use testcontainers for PostgreSQL.

## Deployment Modes

Signaldb has a microservices and a monolothic mode

## Development Memories
- For arrow & parquet try using the ones re-exported by datafusion
- We need to run cargo fmt after bigger chunks of work to apply the canonical formatting
- Run cargo machete --with-metadata before committing and remove unused dependencies
- We need to format the code before committing
- Always run cargo commands from the workspace root
- Run cargo clippy and fix all warnings before committing

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
