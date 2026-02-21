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

#### Retention Enforcement (Phase 3)
```toml
[compactor.retention]
enabled = false                       # Enable retention enforcement (opt-in)
dry_run = true                        # Log actions without executing (safe default)
retention_check_interval = "1h"       # Interval between retention checks
grace_period = "1h"                   # Safety margin before cutoff
timezone = "UTC"                      # Timezone for logging
snapshots_to_keep = 5                 # Keep last N snapshots per table

# Global defaults (per signal type)
traces = "7d"
logs = "30d"
metrics = "90d"

# Tenant overrides (optional)
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces = "30d"
logs = "7d"
metrics = "90d"

# Dataset overrides (highest priority)
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces = "90d"
```
Env: `SIGNALDB__COMPACTOR__RETENTION__ENABLED`, `SIGNALDB__COMPACTOR__RETENTION__DRY_RUN`, `SIGNALDB__COMPACTOR__RETENTION__RETENTION_CHECK_INTERVAL`, `SIGNALDB__COMPACTOR__RETENTION__TRACES`, `SIGNALDB__COMPACTOR__RETENTION__LOGS`, `SIGNALDB__COMPACTOR__RETENTION__METRICS`, `SIGNALDB__COMPACTOR__RETENTION__GRACE_PERIOD`, `SIGNALDB__COMPACTOR__RETENTION__TIMEZONE`, `SIGNALDB__COMPACTOR__RETENTION__SNAPSHOTS_TO_KEEP`

#### Orphan Cleanup (Phase 3)
```toml
[compactor.orphan_cleanup]
enabled = false                       # Enable orphan cleanup (opt-in)
dry_run = true                        # Log orphans without deleting (safe default)
cleanup_interval_hours = 24           # Run cleanup every N hours
grace_period_hours = 24               # Don't delete files younger than this
revalidate_before_delete = true       # Re-check file status before deletion
max_snapshot_age_hours = 720          # Consider snapshots within last N hours
batch_size = 1000                     # Process N files per batch
rate_limit_delay_ms = 0               # Delay between batches in milliseconds
```
Env: `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__CLEANUP_INTERVAL_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__GRACE_PERIOD_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__REVALIDATE_BEFORE_DELETE`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_SNAPSHOT_AGE_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__BATCH_SIZE`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__RATE_LIMIT_DELAY_MS`

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
