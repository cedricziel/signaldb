# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SignalDB is a distributed observability signal database built on the FDAP stack (Flight, DataFusion, Arrow, Parquet). It's designed for cost-effective storage and querying of metrics, logs, and traces with native OTLP support and Tempo API compatibility.

## Development Commands

### Build and Test
```bash
cargo build                # Build all workspace members
cargo test                 # Run all tests across workspace
cargo run                  # Run in monolithic mode (all services)
cargo deny check           # License and security auditing
cargo machete              # Check for unused dependencies
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
- **Messaging** (`src/messaging/`): Message queue abstraction (memory queues)
- **Tempo API** (`src/tempo-api/`): Grafana Tempo compatibility layer

### Data Flow

**Write Path**: Client → Acceptor (OTLP) → Router → Writer(s) → WAL/Memory/Parquet
**Query Path**: Client → Querier → Writers (Flight) + Storage (Parquet) → Merged results

### Service Discovery

Discovery mechanism:
- **Catalog-based**: PostgreSQL/SQLite-backed metadata store with heartbeat-based health checking

### Configuration

Configuration precedence: defaults → TOML file (`signaldb.toml`) → environment variables (`SIGNALDB_*`)

Key sections: `[database]`, `[storage]`, `[queue]`, `[discovery]`

## Key Development Patterns

### Arrow Flight Integration

The system uses Apache Arrow Flight extensively for inter-service communication. Flight schemas are defined in `src/common/flight/schema.rs` with conversions in the `conversion/` subdirectory.

### Service Registration

Services register themselves with discovery backends on startup. Look at existing patterns in acceptor/router for implementing new services.

### Message Processing

Use the messaging abstraction in `src/messaging/` for async batch processing. Supports acknowledgment patterns and different backends.

### Storage Integration

Writers persist data to Parquet files via object_store abstraction. Storage adapters support filesystem, S3, Azure, GCP backends.

## Current Development Status

Active work areas (see `next-steps.md`):
- Extracting Catalog setup into shared helper for microservices
- Moving from polling to watch-based discovery mechanisms  
- Adding graceful service deregistration
- Separating monolithic binary into individual service binaries
- Integrating configuration management across services

## Testing

Integration tests are in workspace root `tests/` and individual component `tests/` directories. Some tests use testcontainers for PostgreSQL.

## Deployment Modes

Signaldb has a microservices and a monolothic mode

## Development Memories
- For arrow & parquet try using the ones re-exported by datafusion
- We need to run cargo fmt after bigger chunks of work to apply the canonical formatting
- Run cargo machete before committing and remove unused dependencies
- We need to format the code before committing
- Always run cargo commands from the workspace root
- run cargo clippy and fix all warnings before committing
