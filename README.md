# SignalDB

A high-performance observability data platform built with the FDAP stack (Flight, DataFusion, Arrow, Parquet).

## Project Goals

Building a database for observability signals (metrics/logs/traces) with focus on:

* **Cost-effective storage** - Efficient columnar storage with Apache Iceberg and Parquet
* **ACID transactions** - Full transaction support with commit/rollback for data integrity
* **Open standards ingestion** - Native OTLP, Prometheus, and other standard protocols
* **Effective querying** - Fast SQL queries powered by DataFusion with Iceberg table format
* **Tool compatibility** - Seamless integration with Grafana, Perses, and other analysis tools
* **Easy operation** - Configless deployments and flexible architecture

## Architecture

SignalDB is built on the FDAP stack with Apache Arrow Flight as the primary inter-service communication protocol, providing high-performance data transfer and native observability signal processing.

### Core Design Principles

* **Flight-First Communication**: Apache Arrow Flight for zero-copy, high-throughput data transfer

* **WAL-Based Durability**: Write-Ahead Log ensures data persistence and crash recovery
* **Catalog-Based Discovery**: Database-backed service registry with automatic health monitoring
* **Iceberg Table Format**: ACID transactions with Apache Iceberg for reliable data management
* **Columnar Storage**: Efficient Parquet storage with DataFusion query processing
* **Performance Optimization**: Intelligent batch processing and connection pooling

### Deployment Models

#### Monolithic Deployment

Single binary (`signaldb`) that includes all services - ideal for development and small deployments.

* All services communicate via localhost Flight endpoints
* Shared SQLite catalog for service discovery
* Zero-configuration startup with sensible defaults

#### Microservices Deployment

Independent services for scalable production deployments:

* **signaldb-acceptor**: OTLP data ingestion (gRPC port 4317, HTTP port 4318)
* **signaldb-router**: Query routing and Tempo API compatibility (HTTP port 3000, Flight port 50053)
* **signaldb-writer**: Data persistence with WAL durability (Flight port 50061)
* **signaldb-querier**: DataFusion-powered query execution (Flight port 50054)

### Data Flow Architecture

**Write Path**:

```
OTLP Client → Acceptor → WAL → Writer → Iceberg Tables (Parquet)
     ↓           ↓        ↓       ↓              ↓
   gRPC/HTTP   Flight   Disk   Flight    Object Store + ACID
```

**Query Path**:

```
Client → Router → Querier → DataFusion → Iceberg Tables
   ↓       ↓        ↓          ↓              ↓
 HTTP   Flight   Flight   SQL Engine   Parquet + Metadata
```

### Service Discovery & Communication

All services register in a shared catalog for automatic discovery:

* **PostgreSQL/SQLite catalog** with heartbeat-based health checking
* **Apache Arrow Flight** for high-performance inter-service communication
* **Automatic service registration** with capability-based routing
* **Connection pooling** and load balancing across available services
* **Graceful shutdown** with proper service deregistration

## Storage Architecture

### Iceberg Table Format

SignalDB uses Apache Iceberg as the table format for reliable data management:

* **ACID Transactions**: Full transaction support with commit/rollback operations
* **Schema Evolution**: Safe schema changes without data migration
* **Time Travel**: Query historical data states and track changes over time
* **Metadata Management**: Efficient table metadata and partition management
* **Performance Optimization**: Intelligent batch processing and connection pooling

### Storage Features

* **Intelligent Batch Processing**: Automatic splitting of large batches (50K rows, 128MB default)

* **Connection Pooling**: Optimized catalog operations with configurable pool settings
* **Retry Logic**: Exponential backoff for transient failures with configurable policies
* **Memory Management**: Memory-aware batch processing to prevent OOM issues
* **Concurrent Processing**: Configurable concurrent batch processing for optimal throughput

## Compactor Service

The Compactor Service manages the complete data lifecycle for observability signals, providing automatic storage optimization and retention enforcement.

### Three-Phase Lifecycle Management

**Phase 1: Dry-Run Planning**
* Analyzes partition statistics to identify compaction opportunities
* Validates potential improvements without modifying data
* Safe exploration mode for testing and analysis

**Phase 2: Active Compaction**
* Consolidates small Parquet files into larger, more efficient files
* Reduces file count and improves query performance
* Maintains Iceberg snapshot isolation during operations

**Phase 3: Retention & Lifecycle Management** ✨ **NEW**
* **Retention Enforcement**: Automatically drops expired partitions based on configurable policies
* **Snapshot Expiration**: Maintains bounded metadata size by expiring old snapshots
* **Orphan Cleanup**: Reclaims storage by detecting and deleting unreferenced files

### Configurable Retention Policies

SignalDB uses a **3-tier override hierarchy** for flexible retention management:

```text
Global Defaults → Tenant Overrides → Dataset Overrides
```

**Example Configuration:**

```toml
[compactor.retention]
enabled = true
dry_run = false  # Always test with true first!
retention_check_interval = "1h"
grace_period = "1h"  # Safety margin

# Global defaults (all tenants) - these are the built-in defaults
traces = "7d"
logs = "30d"
metrics = "90d"
snapshots_to_keep = 10

# Production tenant override (tables keyed by tenant ID)
[compactor.retention.tenant_overrides.production]
traces = "30d"  # Keep production traces longer

# Critical dataset override (highest priority, keyed by dataset ID)
[compactor.retention.tenant_overrides.production.dataset_overrides.critical]
traces = "90d"  # Critical data kept 90 days
```

**Retention Resolution Example:**
* `production/critical` → **90 days** (dataset override wins)
* `production/default` → **30 days** (tenant override wins)
* `dev/staging` → **7 days** (global default wins)

### Orphan File Cleanup

Automatically identifies and deletes unreferenced Parquet files using a **4-phase safety-first algorithm**:

1. **Build Reference Set**: Scan all live snapshots and collect referenced file paths
2. **Scan Object Store**: List all Parquet files in storage
3. **Identify Orphans**: Files not in reference set AND older than grace period
4. **Revalidate & Delete**: Optional re-check before deletion (prevents race conditions)

**Safety Features:**
* **Grace Period**: 24-hour default prevents deletion of in-flight writes
* **Revalidation**: Catches concurrent writes before deletion
* **Dry-Run Mode**: Test orphan detection without actual deletion
* **Batch Processing**: Resumable progress tracking for crash recovery

```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = false  # Always test with true first!
cleanup_interval_hours = 24
grace_period_hours = 24
revalidate_before_delete = true  # Extra safety
```

### Running the Compactor

```bash
# Monolithic mode (recommended)
cargo run --bin signaldb

# Standalone compactor service
cargo run --bin signaldb-compactor

# Development with local storage
./scripts/run-dev.sh
```

### Monitoring & Metrics

Key metrics exposed on port 9091 (Prometheus format):

**Retention Metrics:**
* `compactor_partitions_dropped_total` - Partitions dropped
* `compactor_snapshots_expired_total` - Snapshots expired
* `compactor_retention_duration_ms_total` - Enforcement duration

**Orphan Cleanup Metrics:**
* `compactor_files_deleted_total` - Files successfully deleted
* `compactor_bytes_freed_total` - Storage reclaimed
* `compactor_orphan_candidates_identified_total` - Orphan files detected
* `compactor_deletion_failures_total` - Deletion errors (should be 0)

**Example Prometheus Query:**

```promql
# Storage reclaimed (last 24h)
increase(compactor_bytes_freed_total[24h])
```

### Documentation

* **Comprehensive README**: [`src/compactor/README.md`](src/compactor/README.md)
* **Configuration Reference**: [`docs/operations/compactor/phase3-configuration.md`](docs/operations/compactor/phase3-configuration.md)
* **Operations Guide**: [`docs/operations/compactor/phase3-operations.md`](docs/operations/compactor/phase3-operations.md)
* **Troubleshooting**: [`docs/operations/compactor/phase3-troubleshooting.md`](docs/operations/compactor/phase3-troubleshooting.md)

### Safety Best Practices

1. **Always Start with Dry-Run**: Set `dry_run = true` and monitor logs before enabling
2. **Test on Non-Production First**: Use test tenant with short retention for validation
3. **Use Grace Periods**: Minimum 1 hour for retention, 24 hours for orphan cleanup
4. **Monitor Metrics**: Set up alerts for unexpected partition drops or deletion failures
5. **Keep Sufficient Snapshots**: Maintain 5-10 snapshots for query isolation

## Multi-Tenancy & Authentication

SignalDB provides robust multi-tenant isolation with API key-based authentication:

### Tenant Isolation Architecture

* **Tenant Identification**: Each request requires `tenant_id` and optional `dataset_id`
* **WAL Isolation**: Write-Ahead Logs organized by tenant/dataset (.wal/{tenant}/{dataset}/{signal}/)
* **Catalog Isolation**: Iceberg tables namespaced per tenant and dataset
* **Authentication**: API key validation with configurable tenant access

### Authentication Flow

```
OTLP Client Request
    ↓ Headers: Authorization: Bearer <api-key>
    ↓          X-Tenant-ID: <tenant>
    ↓          X-Dataset-ID: <dataset> (optional)
    ↓
gRPC/HTTP Auth Middleware
    ↓ Extract credentials from headers/metadata
    ↓ Validate API key against tenant configuration
    ↓ Verify dataset access permissions
    ↓ Create TenantContext
    ↓
Service Handler
    ↓ Access tenant_id and dataset_id from context
    ↓ Route data to tenant-specific WAL and storage
```

### Configuration

Configure multi-tenancy in `signaldb.toml`:

```toml
[auth]

[[auth.tenants]]
id = "acme"
slug = "acme"
name = "Acme Corporation"
default_dataset = "production"

[[auth.tenants.api_keys]]
key = "sk-acme-prod-key-123"
name = "Production Key"

[[auth.tenants.datasets]]
id = "production"
slug = "production"
is_default = true
```

### Authenticated OTLP Requests

**gRPC Example**:

```bash
grpcurl \
  -H "authorization: Bearer sk-acme-prod-key-123" \
  -H "x-tenant-id: acme" \
  -H "x-dataset-id: production" \
  -d @ \
  localhost:4317 \
  opentelemetry.proto.collector.trace.v1.TraceService/Export < traces.json
```

**HTTP Example**:

```bash
curl -X POST http://localhost:4318/v1/traces \
  -H "Authorization: Bearer sk-acme-prod-key-123" \
  -H "X-Tenant-ID: acme" \
  -H "X-Dataset-ID: production" \
  -H "Content-Type: application/json" \
  -d @traces.json
```

### Authentication Errors

* **400 Bad Request**: Missing or malformed authentication headers
* **401 Unauthorized**: Invalid API key
* **403 Forbidden**: API key valid but lacks access to specified tenant/dataset

## Database Support

* **PostgreSQL**: Production deployments with full SQL capabilities
* **SQLite**: Development, testing, and single-node deployments (configless operation)

## Quick Start

### Configless Operation (SQLite)

```bash
# Monolithic deployment - zero configuration required
cargo run --bin signaldb
```

Only the monolithic `signaldb` binary runs configless. The standalone microservices binaries (`signaldb-acceptor`, `signaldb-router`, `signaldb-writer`, `signaldb-querier`) validate their configuration at startup and refuse to run with the in-memory defaults: they require a shared `[discovery]` DSN and a SQL-backed `[schema]` catalog (for example the PostgreSQL configuration below, or use `./scripts/run-dev.sh services` which sets this up for you).

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

* Rust stable toolchain (the project tracks stable Rust with edition 2024; no MSRV policy)

* Protocol Buffers compiler

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
docker compose -f docker-compose.test.yml up
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
* `SIGNALDB__DISCOVERY__HEARTBEAT_INTERVAL`: Heartbeat interval
* `SIGNALDB__DISCOVERY__POLL_INTERVAL`: Polling interval
* `SIGNALDB_DISCOVERY_TTL`: Service TTL

Note: field names containing underscores (like `heartbeat_interval`) must use the double-underscore form (`SIGNALDB__SECTION__FIELD_NAME`); the single-underscore prefix only works for fields without underscores in their names.

### Database Configuration

The database configuration controls where SignalDB stores its metadata:

```toml
[database]
dsn = "sqlite://.data/signaldb.db"  # SQLite database path (default)
```

Environment variable: `SIGNALDB_DATABASE_DSN`

### Iceberg Table Configuration

SignalDB uses Apache Iceberg for table format and catalog management:

```toml
[schema]
catalog_type = "sql"                    # sql (recommended) or memory
catalog_uri = "sqlite::memory:"         # In-memory SQLite catalog
# catalog_uri = "sqlite:///path/to/catalog.db"  # Persistent SQLite catalog

# Default schemas to create for new tenants
[schema.default_schemas]
traces_enabled = true
logs_enabled = true
metrics_enabled = true
```

Environment variables:

* `SIGNALDB__SCHEMA__CATALOG_TYPE`: Catalog backend type (sql or memory)
* `SIGNALDB__SCHEMA__CATALOG_URI`: Catalog database connection string

### Performance Optimization Configuration

Configure batch processing and connection pooling for optimal performance:

```toml
# Example configuration for production workloads
# These settings are automatically optimized and typically don't need adjustment

# Connection pooling (managed automatically)
# - Catalog connections use sqlx default pool settings

# Batch optimization (automatic)
# - Max rows per batch: 50,000
# - Max memory per batch: 128MB
# - Auto-splitting: enabled
# - Concurrent batches: 4
# - Catalog caching: enabled (5 min TTL)
```

These optimizations are enabled by default and automatically tuned for most workloads.

### Storage Configuration

SignalDB stores observability data (Parquet files) in an object store selected by a single DSN:

```toml
[storage]
dsn = "file:///.data/storage"          # Local filesystem
# dsn = "memory://"                    # In-memory storage for testing (default)
# dsn = "s3://my-bucket/signaldb/"     # S3-compatible object storage
```

Environment variable: `SIGNALDB_STORAGE_DSN`

### WAL (Write-Ahead Log) Configuration

SignalDB implements Write-Ahead Logging for data durability and crash recovery. The WAL ensures that incoming OTLP data is persisted to disk before acknowledgment, providing strong durability guarantees.

**WAL Features:**

* **Durability**: All data written to WAL before acknowledgment
* **Recovery**: Automatic replay of unprocessed entries on restart
* **Batching**: Efficient batch processing with configurable flush policies
* **Monitoring**: WAL entry tracking and processing status

**Environment Variables:**

* `WRITER_WAL_DIR`: WAL directory for writer service (default: `.wal/writer`)
* `ACCEPTOR_WAL_DIR`: WAL directory for acceptor service (default: `.wal/acceptor`)

**TOML Configuration:**

```toml
[wal]
wal_dir = ".data/wal"
max_segment_size = 67108864           # 64MB
max_buffer_entries = 1000
flush_interval = "30s"
max_buffer_size_bytes = 134217728     # 128MB
```

The built-in defaults are 64MB segments, a 1000-entry buffer, and a 30s flush interval.

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

For detailed WAL persistence configuration, see [docs/operations/wal-persistence.md](docs/operations/wal-persistence.md).

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
