# SignalDB

A high-performance observability data platform built with the FDAP stack (Flight, DataFusion, Arrow, Parquet).

## Project Goals

Building a database for observability signals (metrics/logs/traces) with focus on:

* **Cost-effective storage** - Efficient columnar storage with Parquet
* **Open standards ingestion** - Native OTLP, Prometheus, and other standard protocols
* **Effective querying** - Fast SQL queries powered by DataFusion
* **Tool compatibility** - Seamless integration with Grafana, Perses, and other analysis tools
* **Easy operation** - Configless deployments and flexible architecture

## Architecture

SignalDB supports multiple deployment models:

### Monolithic Deployment
Single binary (`signaldb`) that includes all services - ideal for development and small deployments.

### Microservices Deployment
Independent services for scalable production deployments:
- **signaldb-acceptor**: OTLP data ingestion (gRPC port 4317, HTTP port 4318)
- **signaldb-router**: Query routing and service discovery (Flight port 50053, HTTP API port 3000)
- **signaldb-writer**: Data persistence (Flight port 50051)
- **signaldb-querier**: Query processing

### Service Discovery
All services register in a shared catalog for automatic discovery:
- Heartbeat-based health checking
- Automatic deregistration on shutdown
- Background polling for real-time updates
- Load balancing across available services

## Database Support

- **PostgreSQL**: Production deployments with full SQL capabilities
- **SQLite**: Development, testing, and single-node deployments (configless operation)

## Quick Start

### Configless Operation (SQLite)
```bash
# Monolithic deployment - zero configuration required
cargo run --bin signaldb

# Microservices deployment  
cargo run --bin signaldb-acceptor &
cargo run --bin signaldb-router &
cargo run --bin signaldb-writer &
```

### PostgreSQL Configuration
```toml
# signaldb.toml
[database]
dsn = "postgres://user:password@localhost:5432/signaldb"

[discovery]
dsn = "postgres://user:password@localhost:5432/signaldb"
heartbeat_interval = "30s"
poll_interval = "60s"
ttl = "300s"
```

## Development

### Prerequisites
- Rust 1.70+
- Protocol Buffers compiler

### Building
```bash
# All binaries
cargo build --release

# Specific deployment model
cargo build --release --bin signaldb           # Monolithic
cargo build --release --bin signaldb-acceptor  # Microservices
cargo build --release --bin signaldb-router    # Microservices  
cargo build --release --bin signaldb-writer    # Microservices
```

### Testing
```bash
# Unit and integration tests
cargo test

# Database compatibility tests
cargo test -p common catalog_integration

# Deployment testing
./scripts/test-deployment.sh

# Docker-based testing
docker-compose -f docker-compose.test.yml up
```

## Configuration

signaldb can be configured using a TOML configuration file or environment variables. The configuration is loaded in the following order:

1. Default values
2. TOML configuration file (default: `signaldb.toml`)
3. Environment variables (prefixed with `SIGNALDB_`)

### Database Configuration

The database configuration controls where SignalDB stores its metadata:

```toml
[database]
dsn = "sqlite://.data/signaldb.db"  # SQLite database path (default)
```

Environment variable: `SIGNALDB_DATABASE_DSN`

### Queue Configuration

SignalDB uses an internal queue system for processing incoming data. The queue can be configured with the following options:

```toml
[queue]
dsn = "memory://"             # Queue backend DSN (default: memory://)
max_batch_size = 1000         # Maximum number of items per batch
max_batch_wait = "10s"        # Maximum time to wait before processing a non-full batch
```

Environment variables:

* `SIGNALDB_QUEUE_DSN`: Queue backend DSN
* `SIGNALDB_QUEUE_MAX_BATCH_SIZE`: Maximum batch size
* `SIGNALDB_QUEUE_MAX_BATCH_WAIT`: Maximum batch wait time (supports human-readable durations like "10s", "1m")

Currently supported queue backends:
* `memory://`: In-memory queue for single-node deployments

### Storage Configuration

SignalDB supports multiple storage backends for storing observability data:

```toml
[storage]
default = "local"  # Name of the default storage adapter to use

[storage.adapters.local]  # Configure a storage adapter named "local"
type = "filesystem"  # Storage backend type
url = "file:///data"  # Storage URL
prefix = "traces"  # Prefix for all objects in this storage
```

Environment variables:

* `SIGNALDB_STORAGE_DEFAULT`: Name of the default storage adapter
* `SIGNALDB_STORAGE_ADAPTERS_<NAME>_TYPE`: Storage type for adapter
* `SIGNALDB_STORAGE_ADAPTERS_<NAME>_URL`: Storage URL for adapter
* `SIGNALDB_STORAGE_ADAPTERS_<NAME>_PREFIX`: Storage prefix for adapter

## What is the FDAP stack?

The FDAP stack is a set of technologies that can be used to build a data acquisition and processing system.
It is composed of the following components:

* **F**light - Apache Arrow Flight
* **D**ataFusion - Apache DataFusion
* **A**rrow - Apache Arrow
* **P**arquet - Apache Parquet

<https://www.influxdata.com/glossary/fdap-stack/>

## License

AGPL-3.0
