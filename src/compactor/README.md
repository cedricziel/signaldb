# SignalDB Compactor Service

The Compactor Service manages the data lifecycle for observability signals
(traces, logs, and metrics) stored in Iceberg tables:

1. **Compacts small Parquet files** into larger, more efficient files
   (planning is driven by real Iceberg manifest data, not directory listings)
2. **Enforces retention policies** by dropping expired partitions
   (3-tier hierarchy: global defaults → tenant overrides → dataset overrides)
3. **Expires old snapshots** to keep metadata bounded
4. **Cleans up orphan files** to reclaim storage
   (grace period, revalidation before delete, and a `max_live_files_threshold`
   memory guard that skips oversized tables instead of risking OOM)

All operations respect Iceberg's transactional guarantees and snapshot
isolation. Destructive features default to off (`enabled = false`) and to
dry-run (`dry_run = true`) when enabled.

## Configuration

Configured via the `[compactor]` section of `signaldb.toml` or environment
variables (`SIGNALDB__COMPACTOR__...`, double underscores between nesting
levels). Retention durations are humantime strings; orphan-cleanup intervals
are integer hours.

```toml
[compactor]
enabled = true
tick_interval = "5m"  # Interval between compaction planning cycles

[compactor.retention]
enabled = true
dry_run = true                    # Start with dry-run; set false to enforce
retention_check_interval = "1h"
traces = "7d"
logs = "30d"
metrics = "90d"
grace_period = "1h"
snapshots_to_keep = 10

# Tenant override (map keyed by tenant id)
[compactor.retention.tenant_overrides.production]
traces = "30d"

# Dataset override (highest priority, map keyed by dataset id)
[compactor.retention.tenant_overrides.production.dataset_overrides.critical]
traces = "90d"

[compactor.orphan_cleanup]
enabled = true
dry_run = true                    # Start with dry-run; set false to delete
cleanup_interval_hours = 24
grace_period_hours = 24
batch_size = 1000
revalidate_before_delete = true
max_snapshot_age_hours = 720
max_live_files_threshold = 500000
```

Full reference: [docs/operations/compactor/phase3-configuration.md](../../docs/operations/compactor/phase3-configuration.md).

## Running

```bash
# Standalone
cargo run --bin signaldb-compactor

# Monolithic mode (includes compactor)
cargo run --bin signaldb

# With debug logging
RUST_LOG=debug,compactor=trace cargo run --bin signaldb-compactor
```

## Multi-Instance Safety (Phase 4)

Multiple compactor instances can safely share one catalog. Coordination is
lease-based via a `compactor_leases` table in the SQL catalog (SQLite or
PostgreSQL):

- A lease on `(tenant, dataset, table, partition)` guarantees at most one
  instance compacts a partition at a time
- Held leases are renewed in the background every `ttl / 3`; leases from
  crashed instances expire after `lease_ttl_seconds` (default 300s) and are
  swept every 30 seconds
- Round-robin scheduling across tenants with `max_candidates_per_cycle` and
  `max_per_tenant` caps

### Flight Admin Endpoints

Each instance serves an Arrow Flight admin endpoint (default port `50055`,
override with `COMPACTOR_FLIGHT_ADDR`) with three DoAction verbs:

| Action | Effect |
|--------|--------|
| `compact_now` | Run a full plan → lease → execute cycle immediately |
| `compact_status` | Return active leases + cumulative metrics as JSON |
| `compact_dry_run` | Plan candidates without executing, return JSON list |

Covered by `tests-integration/tests/compactor/multi_instance.rs`.

## Observability (Phase 6)

HTTP endpoints on `metrics_addr` (default `0.0.0.0:9091`, override with
`COMPACTOR_METRICS_ADDR`, disable by setting it to an empty string):

- `GET /metrics` — Prometheus text exposition of all compaction, retention,
  and orphan-cleanup counters (authoritative names: `src/http.rs`)
- `GET /status` — JSON document with instance ID, uptime, and all counters
- `GET /health` — liveness probe (returns `ok`)

```bash
curl -s localhost:9091/metrics | grep compactor
curl -s localhost:9091/status | jq .
```

Counters are process-global (no per-tenant labels). Monitoring queries and
alert examples: [docs/operations/compactor/phase3-operations.md](../../docs/operations/compactor/phase3-operations.md).

## Testing

```bash
# Unit tests
cargo test -p compactor

# Integration tests (tests-integration/tests/compactor/)
cargo test -p tests-integration --test basic_compaction
cargo test -p tests-integration --test retention_cutoff
cargo test -p tests-integration --test partition_drop
cargo test -p tests-integration --test snapshot_expiration
cargo test -p tests-integration --test orphan_cleanup
cargo test -p tests-integration --test retention_failure_scenarios
cargo test -p tests-integration --test multi_instance
```

## Documentation

- Configuration reference: [docs/operations/compactor/phase3-configuration.md](../../docs/operations/compactor/phase3-configuration.md)
- Operations guide: [docs/operations/compactor/phase3-operations.md](../../docs/operations/compactor/phase3-operations.md)
- Troubleshooting: [docs/operations/compactor/phase3-troubleshooting.md](../../docs/operations/compactor/phase3-troubleshooting.md)
