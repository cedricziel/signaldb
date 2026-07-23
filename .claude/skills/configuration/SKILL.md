---
name: configuration
description: SignalDB configuration reference - all TOML sections, environment variables, database/discovery/storage/WAL/schema/auth/queue settings, and service ports. Use when working with configuration, environment variables, or TOML settings.
user-invocable: false
sources:
  - src/common/src/config/mod.rs
  - signaldb.dist.toml
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
Env: `SIGNALDB_DISCOVERY_DSN`, `SIGNALDB_DISCOVERY_TTL`. Multi-word fields need the double-underscore form: `SIGNALDB__DISCOVERY__HEARTBEAT_INTERVAL`, `SIGNALDB__DISCOVERY__POLL_INTERVAL` (the single-underscore form splits to `discovery.heartbeat.interval` and silently does nothing).

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
flush_interval = "30s"
max_buffer_size_bytes = 134217728    # 128 MB
```
Env: `WRITER_WAL_DIR`, `ACCEPTOR_WAL_DIR` (read directly by the binaries, not via figment). Sizing knobs use the double-underscore form: `SIGNALDB__WAL__MAX_SEGMENT_SIZE`, `SIGNALDB__WAL__MAX_BUFFER_ENTRIES`, `SIGNALDB__WAL__FLUSH_INTERVAL`.

### Iceberg Schema Catalog
```toml
[schema]
catalog_type = "sql"
catalog_uri = "sqlite::memory:"      # or sqlite:///path/to/catalog.db
```
Env: `SIGNALDB__SCHEMA__CATALOG_TYPE`, `SIGNALDB__SCHEMA__CATALOG_URI` (double-underscore form). Beware: `signaldb.dist.toml` and `scripts/run-dev.sh` mention/set the single-underscore forms `SIGNALDB_SCHEMA_CATALOG_TYPE`/`SIGNALDB_SCHEMA_CATALOG_URI`, which split to `schema.catalog.type` and silently do nothing.

**Note**: Only SQLite supported for Iceberg catalog (not PostgreSQL).

### Authentication

Tenant auth is always enforced on the tenant-facing APIs; there is no
on/off switch (the former `enabled` flag was removed in #601).

```toml
[auth]
admin_api_key = "sk-admin-key"           # Required for /api/v1/admin/*
internal_service_key = "sk-internal"     # Shared secret for service-to-service
                                         # Flight calls; unset = Flight ports
                                         # accept unauthenticated calls

# Default per-tenant ingest rate limits (unset fields = unlimited)
[auth.default_limits]
max_ingest_requests_per_sec = 100
max_ingest_bytes_per_sec = 10485760      # 10 MiB/s
burst_seconds = 2.0

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
max_candidates_per_cycle = 20         # Max candidates per scheduling cycle (0 = unlimited)
max_per_tenant = 5                    # Max candidates per tenant per cycle (0 = unlimited)
lease_ttl_seconds = 300               # Compaction lease validity without renewal
metrics_addr = "0.0.0.0:9091"         # Observability HTTP endpoint ("" = disabled)
```
Env: `SIGNALDB__COMPACTOR__ENABLED`, `SIGNALDB__COMPACTOR__TICK_INTERVAL`, `SIGNALDB__COMPACTOR__TARGET_FILE_SIZE_MB`, `SIGNALDB__COMPACTOR__FILE_COUNT_THRESHOLD`, `SIGNALDB__COMPACTOR__MIN_INPUT_FILE_SIZE_KB`, `SIGNALDB__COMPACTOR__MAX_FILES_PER_JOB`, `SIGNALDB__COMPACTOR__MAX_CANDIDATES_PER_CYCLE`, `SIGNALDB__COMPACTOR__MAX_PER_TENANT`, `SIGNALDB__COMPACTOR__LEASE_TTL_SECONDS`, `SIGNALDB__COMPACTOR__METRICS_ADDR` (or `COMPACTOR_METRICS_ADDR`)

**Note**: Environment variables for compactor use double-underscore (`__`) separator to support field names with underscores.

#### Retention Enforcement (Phase 3)
```toml
[compactor.retention]
enabled = false                       # Enable retention enforcement (opt-in)
dry_run = true                        # Log actions without executing (safe default)
retention_check_interval = "1h"       # Interval between retention checks
grace_period = "1h"                   # Safety margin before cutoff
timezone = "UTC"                      # Timezone for logging
snapshots_to_keep = 10                # Keep last N snapshots per table (default: 10)

# Global defaults (per signal type, humantime durations)
traces = "7d"
logs = "30d"
metrics = "90d"

# Tenant overrides (optional) -- a map keyed by tenant ID
[compactor.retention.tenant_overrides.production]
traces = "30d"
logs = "7d"
metrics = "90d"

# Dataset overrides (highest priority) -- a map keyed by dataset ID
[compactor.retention.tenant_overrides.production.dataset_overrides.critical]
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
max_live_files_threshold = 500000     # Skip cleanup when estimated live files exceed this (0 = no cap)
```
Env: `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__CLEANUP_INTERVAL_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__GRACE_PERIOD_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__REVALIDATE_BEFORE_DELETE`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_SNAPSHOT_AGE_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__BATCH_SIZE`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_LIVE_FILES_THRESHOLD`

### Querier (Resource Limits)
```toml
[querier]
memory_limit_mb = 4096                # Unset = unbounded (startup warning)
memory_pool_fraction = 0.8            # Fraction usable before spill/fail (0.0-1.0)
query_timeout = "60s"                 # Wall-clock timeout per Flight query
max_sql_rows = 1000000                # Row cap for raw SQL over Flight
max_search_limit = 1000               # Upper bound for client `limit` on /api/search
max_concurrent_queries_per_tenant = 8 # Unset = unlimited
```

### Self-Monitoring (Dogfooding)
```toml
[self_monitoring]
enabled = false
endpoint = "http://localhost:4317"    # OTLP gRPC endpoint of the acceptor
interval = "60s"
tenant_id = "_system"
dataset_id = "_monitoring"
trace_sample_ratio = 0.1              # 0.0-1.0; OTEL_TRACES_SAMPLER env vars win
```

### Profiling (Continuous Profiling)
```toml
[profiling]
enabled = false
pyroscope_url = "http://localhost:4040"
cpu_sample_rate = 100                 # Hz
memory_profiling = false              # Needs `jemalloc-profiling` build feature
```

### Tenants (Per-Tenant Schema Overrides)
```toml
[tenants]
default_tenant = "default"            # Tenant ID used when none is specified
# Per-tenant schema override map: [tenants.tenants.<tenant_id>]
```

## Service Ports (Defaults)

| Service | Protocol | Port |
|---------|----------|------|
| Acceptor | gRPC | 4317 |
| Acceptor | HTTP | 4318 |
| Writer | Flight | 50061 (standalone), 50051 (monolithic) |
| Router | HTTP | 3000 |
| Router | Flight | 50053 |
| Querier | Flight | 50054 |
| Compactor | Flight | 50055 (`COMPACTOR_FLIGHT_ADDR`) |
| Compactor | HTTP (metrics/status/health) | 9091 (`metrics_addr`, default `0.0.0.0:9091`) |

## Key File

Config structs: `src/common/src/config/mod.rs`
Example config: `signaldb.dist.toml`
