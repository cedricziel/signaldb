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

SignalDB is built on the FDAP stack with Apache Arrow Flight as the primary inter-service communication protocol, providing high-performance data transfer and native observability signal processing.

### Core Design Principles
- **Flight-First Communication**: Apache Arrow Flight for zero-copy, high-throughput data transfer
- **WAL-Based Durability**: Write-Ahead Log ensures data persistence and crash recovery
- **Catalog-Based Discovery**: Database-backed service registry with automatic health monitoring
- **Columnar Storage**: Efficient Parquet storage with DataFusion query processing

### Deployment Models

#### Monolithic Deployment
Single binary (`signaldb`) that includes all services - ideal for development and small deployments.
- All services communicate via localhost Flight endpoints
- Shared SQLite catalog for service discovery
- Zero-configuration startup with sensible defaults

#### Microservices Deployment
Independent services for scalable production deployments:
- **signaldb-acceptor**: OTLP data ingestion (gRPC port 4317, HTTP port 4318)
- **signaldb-router**: Query routing and Tempo API compatibility (HTTP port 3000, Flight port 50053)
- **signaldb-writer**: Data persistence with WAL durability (Flight port 50051)
- **signaldb-querier**: DataFusion-powered query execution (Flight port 9000)

### Data Flow Architecture

**Write Path**: 
```
OTLP Client → Acceptor → WAL → Writer → Parquet Storage
     ↓           ↓        ↓       ↓           ↓
   gRPC/HTTP   Flight   Disk   Flight   Object Store
```

**Query Path**:
```
Client → Router → Querier → DataFusion → Parquet Files
   ↓       ↓        ↓          ↓           ↓
 HTTP   Flight   Flight   SQL Engine   Storage
```

### Service Discovery & Communication
All services register in a shared catalog for automatic discovery:
- **PostgreSQL/SQLite catalog** with heartbeat-based health checking
- **Apache Arrow Flight** for high-performance inter-service communication
- **Automatic service registration** with capability-based routing
- **Connection pooling** and load balancing across available services
- **Graceful shutdown** with proper service deregistration

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

SignalDB can be configured using a TOML configuration file or environment variables. The configuration is loaded in the following order:

1. Default values
2. TOML configuration file (default: `signaldb.toml`)
3. Environment variables (prefixed with `SIGNALDB_`)

### Service Discovery Configuration

Configure the shared catalog used for service discovery and coordination:

```toml
[discovery]
dsn = "sqlite://signaldb.db"           # Database connection string
heartbeat_interval = "30s"             # Service heartbeat frequency
poll_interval = "60s"                  # Discovery polling frequency  
ttl = "300s"                           # Service timeout threshold
```

Environment variables:
* `SIGNALDB_DISCOVERY_DSN`: Database connection string
* `SIGNALDB_DISCOVERY_HEARTBEAT_INTERVAL`: Heartbeat interval
* `SIGNALDB_DISCOVERY_POLL_INTERVAL`: Polling interval
* `SIGNALDB_DISCOVERY_TTL`: Service TTL

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

### WAL (Write-Ahead Log) Configuration

SignalDB implements Write-Ahead Logging for data durability and crash recovery. The WAL ensures that incoming OTLP data is persisted to disk before acknowledgment, providing strong durability guarantees.

**WAL Features:**
- **Durability**: All data written to WAL before acknowledgment
- **Recovery**: Automatic replay of unprocessed entries on restart
- **Batching**: Efficient batch processing with configurable flush policies
- **Monitoring**: WAL entry tracking and processing status

**Environment Variables:**

* `WRITER_WAL_DIR`: WAL directory for writer service (default: `.wal/writer`)
* `ACCEPTOR_WAL_DIR`: WAL directory for acceptor service (default: `.wal/acceptor`)
* `WAL_MAX_SEGMENT_SIZE`: Maximum size per WAL segment (default: 1MB)
* `WAL_MAX_BUFFER_ENTRIES`: Buffer size before forced flush (default: 1000)
* `WAL_FLUSH_INTERVAL`: Automatic flush interval (default: 10s)

⚠️ **Production Warning**: Default WAL directories use local paths that **do not persist** across container restarts. Configure persistent volumes for production deployments.

**Data Flow with WAL:**
1. Acceptor receives OTLP data
2. Data written to Acceptor WAL (durability checkpoint)
3. Data forwarded to Writer via Flight
4. Writer processes and stores to Parquet
5. WAL entries marked as processed

**Example Docker Compose Configuration:**

```yaml
services:
  signaldb-writer:
    environment:
      WRITER_WAL_DIR: "/data/wal"
    volumes:
      - writer-wal:/data/wal  # Persistent storage
volumes:
  writer-wal:
    driver: local
```

**Example Kubernetes Configuration:**

```yaml
spec:
  containers:
  - name: writer
    env:
    - name: WRITER_WAL_DIR
      value: "/data/wal"
    volumeMounts:
    - name: wal-storage
      mountPath: /data/wal
  volumes:
  - name: wal-storage
    persistentVolumeClaim:
      claimName: writer-wal-pvc
```

For detailed WAL persistence configuration, see [docs/deployment/wal-persistence.md](docs/deployment/wal-persistence.md).

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
