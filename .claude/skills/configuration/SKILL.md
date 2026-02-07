---
name: configuration
description: SignalDB configuration reference - all TOML sections, environment variables, database/discovery/storage/WAL/schema/auth/queue settings, and service ports. Use when working with configuration, environment variables, or TOML settings.
user-invocable: false
---

# SignalDB Configuration Reference

## Precedence

defaults -> TOML file (`signaldb.toml`) -> environment variables (`SIGNALDB_*`)

## All Configuration Sections

### Database (Service Catalog)
```toml
[database]
dsn = "sqlite://.data/signaldb.db"   # or postgres://user:pass@host/db
```
Env: `SIGNALDB_DATABASE_DSN`

### Discovery (Service Registration)
```toml
[discovery]
dsn = "sqlite://.data/signaldb.db"   # Falls back to [database].dsn
heartbeat_interval = "30s"
poll_interval = "60s"
ttl = "300s"
```
Env: `SIGNALDB_DISCOVERY_DSN`, `SIGNALDB_DISCOVERY_HEARTBEAT_INTERVAL`, `SIGNALDB_DISCOVERY_POLL_INTERVAL`, `SIGNALDB_DISCOVERY_TTL`

### Storage (Object Store for Parquet)
```toml
[storage]
dsn = "file:///.data/storage"
# dsn = "memory://"
# dsn = "s3://bucket/prefix"
```
Env: `SIGNALDB_STORAGE_DSN`

For S3/MinIO:
```bash
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```

### WAL
```toml
[wal]
wal_dir = ".data/wal"
max_segment_size = 67108864          # 64 MB
max_buffer_entries = 1000
flush_interval_secs = 30
```
Env: `WRITER_WAL_DIR`, `ACCEPTOR_WAL_DIR`, `WAL_MAX_SEGMENT_SIZE`, `WAL_MAX_BUFFER_ENTRIES`, `WAL_FLUSH_INTERVAL_SECS`

### Iceberg Schema Catalog
```toml
[schema]
catalog_type = "sql"
catalog_uri = "sqlite::memory:"      # or sqlite:///path/to/catalog.db
```
Env: `SIGNALDB_SCHEMA_CATALOG_TYPE`, `SIGNALDB_SCHEMA_CATALOG_URI`

**Note**: Only SQLite supported for Iceberg catalog (not PostgreSQL).

### Authentication
```toml
[auth]
enabled = true
admin_api_key = "sk-admin-key"

[[auth.tenants]]
id = "acme"
slug = "acme"
name = "Acme Corporation"
default_dataset = "production"

[[auth.tenants.datasets]]
id = "production"
slug = "prod"
is_default = true

[[auth.tenants.datasets]]
id = "archive"
slug = "archive"
[auth.tenants.datasets.storage]
dsn = "s3://acme-archive/signals"   # Per-dataset storage override

[[auth.tenants.api_keys]]
key = "sk-acme-prod-key-123"
name = "Production Key"
```

### Queue
```toml
[queue]
dsn = "memory://"
max_batch_size = 1000
max_batch_wait = "10s"
```
Env: `SIGNALDB_QUEUE_DSN`, `SIGNALDB_QUEUE_MAX_BATCH_SIZE`, `SIGNALDB_QUEUE_MAX_BATCH_WAIT`

## Service Ports (Defaults)

| Service | Protocol | Port |
|---------|----------|------|
| Acceptor | gRPC | 4317 |
| Acceptor | HTTP | 4318 |
| Writer | Flight | 50061 (standalone), 50051 (monolithic) |
| Router | HTTP | 3000 |
| Router | Flight | 50053 |
| Querier | Flight | 50054 |

## Key File

Config structs: `src/common/src/config/mod.rs`
Example config: `signaldb.dist.toml`
