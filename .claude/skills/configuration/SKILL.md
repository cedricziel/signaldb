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

### Compactor
```toml
[compactor]
enabled = false                        # Default disabled
tick_interval = "5m"                  # Planning cycle interval
target_file_size_mb = 128             # Target size after compaction
file_count_threshold = 10             # Min files to trigger compaction
min_input_file_size_kb = 1024         # Min file size to consider (1MB)
max_files_per_job = 50                # Max files per compaction job
```
Env: `SIGNALDB__COMPACTOR__ENABLED`, `SIGNALDB__COMPACTOR__TICK_INTERVAL`, `SIGNALDB__COMPACTOR__TARGET_FILE_SIZE_MB`, `SIGNALDB__COMPACTOR__FILE_COUNT_THRESHOLD`, `SIGNALDB__COMPACTOR__MIN_INPUT_FILE_SIZE_KB`, `SIGNALDB__COMPACTOR__MAX_FILES_PER_JOB`

**Note**: Environment variables for compactor use double-underscore (`__`) separator to support field names with underscores.

## Service Ports (Defaults)

| Service | Protocol | Port |
|---------|----------|------|
| Acceptor | gRPC | 4317 |
| Acceptor | HTTP | 4318 |
| Writer | Flight | 50061 (standalone), 50051 (monolithic) |
| Router | HTTP | 3000 |
| Router | Flight | 50053 |
| Querier | Flight | 50054 |
| Compactor | None | (background task, no network endpoint) |

## Key File

Config structs: `src/common/src/config/mod.rs`
Example config: `signaldb.dist.toml`
