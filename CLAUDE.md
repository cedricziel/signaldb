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

### Running Individual Services
```bash
# Main SignalDB services
cargo run --bin signaldb   # Monolithic mode (all services in one)
cargo run --bin acceptor   # OTLP ingestion service (ports 4317/4318)
cargo run --bin router     # HTTP router (port 3000) + Flight (port 50053)
cargo run --bin writer     # Data ingestion and storage service
cargo run --bin querier    # Query execution engine (port 9000)

# Heraclitus Kafka server
cargo run --bin heraclitus # Standalone Kafka-compatible server (port 9092)
cargo run --bin heraclitus -- --kafka-port 9092 --http-port 9093
cargo run --bin heraclitus -- --storage-path memory://  # In-memory mode
```

### Infrastructure
```bash
docker compose up          # Start PostgreSQL, Grafana, and supporting services
```

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

**Heraclitus** (`src/heraclitus/`):
- Standalone Kafka-compatible protocol server (separate from SignalDB)
- Full Kafka wire protocol implementation (v0-v3 for most APIs)
- Kafka client library compatibility (rdkafka, kafka-go, etc.)
- Uses Apache Arrow/Parquet for storage
- Built-in consumer group coordination and SASL authentication

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

### Heraclitus Kafka Protocol Development

**Protocol Implementation**: `src/heraclitus/src/protocol_v2/`
- Uses `kafka-protocol` crate for wire protocol encoding/decoding
- Each Kafka API has its own handler module
- Protocol version negotiation via ApiVersions handshake

**Supported APIs** (v0-v3 for most):
- Producer: Produce, InitProducerId
- Consumer: Fetch, ListOffsets, OffsetCommit, OffsetFetch
- Consumer Groups: JoinGroup, SyncGroup, Heartbeat, LeaveGroup
- Admin: CreateTopics, DeleteTopics, Metadata
- Auth: SaslHandshake, SaslAuthenticate

**Storage Backend**:
- Uses Arrow/Parquet format via BatchWriter
- Configurable: filesystem (`file://`) or in-memory (`memory://`)
- State management for topics, partitions, offsets, consumer groups

**Testing with librdkafka**:
- E2E tests use rdkafka Rust client
- Requires dedicated thread runtime (see helpers.rs)
- Cannot share Tokio runtime due to C library blocking I/O

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
cargo test -p heraclitus
cargo test -p writer

# Specific test
cargo test test_name
cargo test test_name -- --nocapture  # With output

# Tests with logging
RUST_LOG=debug cargo test test_name -- --nocapture
```

### Test Infrastructure Patterns

**Heraclitus Test Threading Model**:
- Tests use dedicated OS threads with isolated Tokio runtimes for server instances
- Critical for librdkafka (C library) compatibility - cannot share async runtime
- Pattern: `std::thread::spawn` + `Runtime::new()` for complete isolation
- See `src/heraclitus/tests/integration/helpers.rs` for reference implementation

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

## Development Memories
- For arrow & parquet try using the ones re-exported by datafusion
- We need to run cargo fmt after bigger chunks of work to apply the canonical formatting
- Run cargo machete --with-metadata before committing and remove unused dependencies
- We need to format the code before committing
- Always run cargo commands from the workspace root
- Run cargo clippy and fix all warnings before committing
- Project uses Rust edition 2024, requires Rust 1.86.0+ minimum

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
