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

`wal_dir` is the base directory: the acceptor uses `{wal_dir}/acceptor` and the writer `{wal_dir}/writer` (default `.data/wal/acceptor` / `.data/wal/writer`). The service-specific env overrides `ACCEPTOR_WAL_DIR` / `WRITER_WAL_DIR` (read directly by the binaries, not via figment; also available as `--wal-dir`) point at the full service directory and win over `[wal].wal_dir`. Sizing knobs use the double-underscore form: `SIGNALDB__WAL__MAX_SEGMENT_SIZE`, `SIGNALDB__WAL__MAX_BUFFER_ENTRIES`, `SIGNALDB__WAL__FLUSH_INTERVAL` (as does `SIGNALDB__WAL__WAL_DIR`).

### Iceberg Schema Catalog

```toml
[schema]
catalog_type = "sql"
catalog_uri = "sqlite::memory:"      # or sqlite:///path/to/catalog.db
```

Env: `SIGNALDB__SCHEMA__CATALOG_TYPE`, `SIGNALDB__SCHEMA__CATALOG_URI` (double-underscore form). Beware: `signaldb.dist.toml` and `scripts/run-dev.sh` mention/set the single-underscore forms `SIGNALDB_SCHEMA_CATALOG_TYPE`/`SIGNALDB_SCHEMA_CATALOG_URI`, which split to `schema.catalog.type` and silently do nothing.

**Note**: Only SQLite supported for Iceberg catalog (not PostgreSQL).

#### Materialized labels

```toml
[schema.materialized_labels]
logs = ["namespace", "pod"]   # also: traces / metrics / profiles
```

Per-signal allowlists of attribute keys promoted from the `*_attributes` JSON into dedicated `label_<key>` columns at ingest, so they match exactly (and support regex / ordered comparisons) instead of the substring-in-JSON approximation. Default empty. Applies to tables created after the change; older tables fall back to JSON matching. Per-tenant: a tenant schema override (`[auth.tenants.schema.materialized_labels]`) replaces the global set wholesale — resolved at table creation and in the writer's transforms. See `docs/architecture/storage-layout.md#materialized-labels`.

### Authentication

Tenant auth is always enforced on the tenant-facing APIs; there is no
on/off switch (the former `enabled` flag was removed in #601).

```toml
[auth]
admin_api_key = "sk-admin-key"           # Required for /api/v1/admin/*
internal_service_key = "sk-internal"     # Shared secret for service-to-service
                                         # Flight calls; unset = Flight ports
                                         # accept unauthenticated calls

# Default per-tenant rate limits and quotas; unset fields = unlimited.
# Rate limits return 429 / RESOURCE_EXHAUSTED; count quotas return
# 429 with error code "quota_exceeded". Every HTTP 429 (router query
# surfaces, admin quotas, acceptor OTLP/HTTP and Prometheus remote_write)
# carries Retry-After (whole seconds, rounded up, >= 1), X-RateLimit-Limit,
# and X-RateLimit-Burst computed from the token bucket's actual state; the
# router's query 429 body is the JSON ApiError envelope
# ({"status":"error","errorType":"rate_limited","error":"...","retryAfterMs":N}).
[auth.default_limits]
max_ingest_requests_per_sec = 100
max_ingest_bytes_per_sec = 10485760   # 10 MiB/s
max_query_requests_per_sec = 100      # router HTTP query API
max_api_keys = 10                     # active (non-revoked) keys
max_datasets = 25
max_storage_bytes = 107374182400      # 100 GiB live Iceberg data files (eventually consistent)
burst_seconds = 10.0                  # seconds of budget a tenant may burst;
                                       # generous by default so an Explore page
                                       # load or an agent's multi-tool
                                       # investigation isn't throttled

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

# Per-tenant override; takes precedence over [auth.default_limits]
[auth.tenants.limits]
max_ingest_requests_per_sec = 500
max_query_requests_per_sec = 500
```

### Compactor

```toml
[compactor]
enabled = true                        # Default enabled
tick_interval = "5m"                  # Planning cycle interval
target_file_size_mb = 128             # Target size after compaction
file_count_threshold = 10             # Min files to trigger compaction
max_input_file_size_kb = 65536        # Max file size to consider (64MB); larger files are left alone
partition_lateness = "10m"            # Late-data allowance; only closed hour partitions are compacted
memory_limit_mb = 512                 # Rewrite memory budget; larger partitions spill to disk
scan_batch_size = 1024                # Rows per batch into the sort (0 = DataFusion's 8192); bounds the unspillable first reservation on wide rows
sort_spill_reservation_mb = 10        # Spill-merge headroom, taken out of memory_limit_mb
max_candidates_per_cycle = 20         # Max candidates per scheduling cycle (0 = unlimited)
max_per_tenant = 5                    # Max candidates per tenant per cycle (0 = unlimited)
lease_ttl_seconds = 300               # Compaction lease validity without renewal
metrics_addr = "0.0.0.0:9091"         # Observability HTTP endpoint ("" = disabled)
```

Env: `SIGNALDB__COMPACTOR__ENABLED`, `SIGNALDB__COMPACTOR__TICK_INTERVAL`, `SIGNALDB__COMPACTOR__TARGET_FILE_SIZE_MB`, `SIGNALDB__COMPACTOR__FILE_COUNT_THRESHOLD`, `SIGNALDB__COMPACTOR__MAX_INPUT_FILE_SIZE_KB`, `SIGNALDB__COMPACTOR__PARTITION_LATENESS`, `SIGNALDB__COMPACTOR__MEMORY_LIMIT_MB`, `SIGNALDB__COMPACTOR__SCAN_BATCH_SIZE`, `SIGNALDB__COMPACTOR__SORT_SPILL_RESERVATION_MB`, `SIGNALDB__COMPACTOR__MAX_CANDIDATES_PER_CYCLE`, `SIGNALDB__COMPACTOR__MAX_PER_TENANT`, `SIGNALDB__COMPACTOR__LEASE_TTL_SECONDS`, `SIGNALDB__COMPACTOR__METRICS_ADDR` (or `COMPACTOR_METRICS_ADDR`)

**Note**: Environment variables for compactor use double-underscore (`__`) separator to support field names with underscores.

#### Retention Enforcement (Phase 3)

```toml
[compactor.retention]
enabled = true                        # Enabled by default; deletes data past retention!
dry_run = false                       # Default false; set true to log without deleting
retention_check_interval = "1h"       # Interval between retention checks
grace_period = "1h"                   # Safety margin before cutoff
timezone = "UTC"                      # Timezone for logging
snapshots_to_keep = 10                # Keep last N snapshots per table (default: 10)

# Global defaults (per signal type, humantime durations; default 30d each)
traces = "30d"
logs = "30d"
metrics = "30d"
profiles = "30d"

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

#### Attribute Auto-Promotion (epic #737)

```toml
[compactor.attr_promotion]
enabled = false               # Decision pass off by default
dry_run = true                # Log-only (schema-changing rewrite not yet implemented)
max_labels_per_table = 32     # Width budget incl. pinned [schema.materialized_labels]
min_presence = 0.005          # Min fraction of rows carrying the key
min_query_hits = 1            # Min accumulated query demand
promote_streak = 3            # Consecutive over-threshold cycles (hysteresis)
max_promotions_per_cycle = 4
```

Scores persisted attribute stats (compactor scan stats + querier demand counters in the catalog's `attribute_stats` table) as demand × presence; rejects capped-cardinality and generated-looking keys; pinned `[schema.materialized_labels]` entries are never demoted. Env: `SIGNALDB__COMPACTOR__ATTR_PROMOTION__*`.

#### Orphan Cleanup (Phase 3)

```toml
[compactor.orphan_cleanup]
enabled = true                        # Enable orphan cleanup (default: true)
dry_run = false                       # Set true to log orphans without deleting
cleanup_interval_hours = 24           # Run cleanup every N hours
grace_period_hours = 24               # Don't delete files younger than this
batch_size = 1000                     # Process N files per batch
max_live_files_threshold = 500000     # Skip cleanup when estimated live files exceed this (0 = no cap)
```

Env: `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__CLEANUP_INTERVAL_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__GRACE_PERIOD_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__REVALIDATE_BEFORE_DELETE`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_SNAPSHOT_AGE_HOURS`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__BATCH_SIZE`, `SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_LIVE_FILES_THRESHOLD`

### Querier (Resource Limits)

```toml
[querier]
memory_limit_mb = 4096                # Unset = unbounded (startup warning)
memory_pool_fraction = 0.8            # Fraction usable before spill/fail (0.0-1.0)
parquet_metadata_cache_mb = 128       # Parquet footer cache budget; 0 disables. Separate from memory_limit_mb
query_timeout = "60s"                 # Wall-clock timeout per Flight query
max_sql_rows = 1000000                # Row cap for raw SQL over Flight
max_search_limit = 1000               # Upper bound for client `limit` on /api/search
max_concurrent_queries_per_tenant = 8 # Unset = unlimited
trace_search_via_ir = false           # TEMPORARY (ir-single-lowering): route trace search through the query-IR planner
logql_via_ir = false                  # TEMPORARY (ir-single-lowering): route LogQL through the query-IR planner, falling back on Inexpressible
```

### Writer (Commit Coalescing)

```toml
[writer]
commit_interval = "5s"        # Max wait before a table's rows are committed (liveness). "0s" = commit every tick
max_uncommitted_rows = 100000 # Row ceiling that triggers an earlier commit for bursts (a cap, never a minimum)
metadata_previous_versions_max = 100 # Previous metadata.json versions retained per table (older deleted on commit)
table_reconcile_interval = "5m"      # How often to re-run the signal-table reconciler over the tenant registry; "0s" = startup pass only
wal_marker_retention = "30d"         # How long ANOTHER writer id's WAL idempotency marker is kept on a table; "0s" disables retirement
```

The writer commits ingested data to Iceberg asynchronously via its background
loop, coalescing pending entries per `(tenant, dataset, table)`: a group commits
when `commit_interval` elapses **or** its rows reach `max_uncommitted_rows`,
whichever comes first. This caps the Iceberg snapshot / catalog-metadata write
rate independent of ingest rate. Ingested data becomes queryable once committed
(bounded by `commit_interval`); a client needing read-your-writes forces an
immediate commit with the writer Flight `do_action("flush")`.

The writer also runs the signal-table reconciler: a pass at startup, then one
every `table_reconcile_interval`, ensuring every registered tenant/dataset holds
a table for each signal type enabled for that tenant. `"0s"` keeps the startup
pass and disables the periodic re-run. See
`docs/operations/table-provisioning.md`.

Each WAL→Iceberg commit records an idempotency marker as a table property keyed
by the committing WAL's writer id, and a new writer id appears whenever a WAL
directory is created or wiped — so without retirement the property set grows
forever and every entry is paid for in `metadata.json` on every read and
commit. `wal_marker_retention` is how long another writer id's marker is kept
before it is deleted. A marker is live evidence that its writer committed rows
it may not have marked processed yet, so this must comfortably exceed the
longest a writer could be down while still holding undrained WAL entries;
retiring one too early makes that writer re-insert those rows as duplicates.
Markers written before markers carried a commit time are only retired once the
writer process has itself been up longer than the window.

### MCP (Model Context Protocol server)

The `signaldb mcp` server (a subcommand of the `signaldb` binary). A thin, credential-forwarding client: it
validates the caller's bearer and forwards it to the router — it holds no key of
its own. Off by default. See `docs/users/mcp.md`.

```toml
[mcp]
enabled = false                      # Off by default; set true to run the server
bind_address = "127.0.0.1:8228"      # Streamable HTTP at /mcp; loopback default.
                                     # Non-loopback bind forwards live bearer
                                     # credentials and must sit behind TLS.
router_url = "http://localhost:3000" # Router HTTP API to forward to
router_timeout = 30                  # Seconds per forwarded request (default 30)
max_concurrent_tool_calls = 8        # Tool calls in flight per MCP session (default 8);
                                     # excess calls wait 2 s for a permit, then fail with
                                     # "too many concurrent tool calls (limit N)"
```

Env (multi-word fields need the double-underscore form): `SIGNALDB__MCP__ENABLED`, `SIGNALDB__MCP__BIND_ADDRESS`, `SIGNALDB__MCP__ROUTER_URL`, `SIGNALDB__MCP__ROUTER_TIMEOUT`, `SIGNALDB__MCP__MAX_CONCURRENT_TOOL_CALLS`. The sidecar reads `[self_monitoring]` too (via `--config`, `signaldb.toml`, or `SIGNALDB__SELF_MONITORING__*`): when enabled it exports `POST /mcp` server spans, `tools/call {tool}` spans, per-call audit events, and the `signaldb.mcp.*` metrics as service `signaldb-mcp`. The MCP server ships in the monolithic image, so it can run as a sidecar container from the same image via `entrypoint: [signaldb-mcp]`.

#### MCP OAuth 2.1 authorization server

Served by the **router** (not the sidecar), off by default. Enables one-click
connector registration from Claude.ai / ChatGPT (OAuth 2.1 + DCR). Tokens are
opaque, catalog-backed, and audience-bound to `resource_url`. See
`docs/users/mcp.md`.

```toml
[mcp.oauth]
enabled = false                                    # off by default
issuer_url = "https://signaldb.example.org"        # this AS, as clients reach it (required when enabled)
resource_url = "https://signaldb.example.org/mcp"  # MCP resource tokens bind to (required when enabled)
access_token_ttl = "1h"                            # default 1h
refresh_token_ttl = "30d"                          # default 30d
authorization_code_ttl = "60s"                     # default 60s
```

Env: `SIGNALDB__MCP__OAUTH__ENABLED`, `SIGNALDB__MCP__OAUTH__ISSUER_URL`, `SIGNALDB__MCP__OAUTH__RESOURCE_URL`. The sidecar advertises the resource via its own `--oauth-resource-url` / `--oauth-issuer-url` flags (env `SIGNALDB__MCP__OAUTH__RESOURCE_URL` / `_ISSUER_URL`).

### Self-Monitoring (Dogfooding)

```toml
[self_monitoring]
enabled = false
endpoint = "http://localhost:4317"    # OTLP gRPC endpoint of the acceptor
interval = "60s"
tenant_id = "_system"
dataset_id = "_monitoring"
trace_sample_ratio = 0.1              # 0.0-1.0; OTEL_TRACES_SAMPLER env vars win
environment = "production"           # deployment.environment.name resource attribute
profiles_enabled = false             # CPU self-profiling -> OTLP profiles into this tenant
profile_sample_rate_hz = 99          # sampling frequency
profile_interval = "60s"             # one profile window per interval
heap_profiles_enabled = false        # also export a jemalloc heap (inuse_space/bytes) profile
```

`profiles_enabled` works even when `enabled` is false (it needs only the
endpoint/tenant/credentials, not the OTel SDK) and requires
`auth.admin_api_key`. It is mutually exclusive with `[profiling]` below —
both drive the one SIGPROF sampler, so if both are set the external
`[profiling]` agent wins. `heap_profiles_enabled` requires the
`jemalloc-profiling` build feature + `MALLOC_CONF=prof:true` at runtime;
it uses jemalloc (no SIGPROF) so it runs alongside CPU profiling or
`[profiling]`. Both share `profile_interval`. Unsupported on Windows, and
unsupported on the default musl container images — those are built without
`jemalloc-profiling` because jemalloc's unwinder crashes there; use
`ghcr.io/cedricziel/signaldb:main-glibc-profiling` instead (amd64 only; see
`docs/operations/binaries.md`).

### Profiling (Continuous Profiling)

External Pyroscope push, distinct from the self-monitoring OTLP path above.

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

| Service   | Protocol                     | Port                                          |
| --------- | ---------------------------- | --------------------------------------------- |
| Acceptor  | gRPC                         | 4317                                          |
| Acceptor  | HTTP                         | 4318                                          |
| Writer    | Flight                       | 50061 (standalone), 50051 (monolithic)        |
| Router    | HTTP                         | 3000                                          |
| Router    | Flight                       | 50053                                         |
| Querier   | Flight                       | 50054                                         |
| Compactor | Flight                       | 50055 (`COMPACTOR_FLIGHT_ADDR`)               |
| Compactor | HTTP (metrics/status/health) | 9091 (`metrics_addr`, default `0.0.0.0:9091`) |

## Key File

Config structs: `src/common/src/config/mod.rs`
Example config: `signaldb.dist.toml`
