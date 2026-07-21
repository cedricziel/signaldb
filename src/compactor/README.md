# SignalDB Compactor Service

The Compactor Service is a critical component of SignalDB that manages the complete data lifecycle for observability signals (traces, logs, and metrics). It provides:

- **Phase 1**: Dry-run compaction planning and validation
- **Phase 2**: Active compaction execution for Parquet file consolidation
- **Phase 3**: Comprehensive retention enforcement and storage lifecycle management
- **Phase 4**: Multi-instance safety via distributed leases, fair scheduling, and Flight admin endpoints
- **Phase 6**: Observability — Prometheus metrics, JSON status, and health endpoints

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Configuration](#configuration)
  - [Retention Policies](#retention-policies)
  - [Orphan File Cleanup](#orphan-file-cleanup)
- [Architecture](#architecture)
- [Usage](#usage)
- [Metrics](#metrics)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Overview

The Compactor Service runs alongside other SignalDB components to:

1. **Compact small Parquet files** into larger, more efficient files
2. **Enforce retention policies** by dropping expired partitions
3. **Expire old snapshots** to maintain bounded metadata size
4. **Clean up orphan files** to reclaim storage

All operations respect Iceberg's transactional guarantees and snapshot isolation to ensure zero impact on concurrent queries.

## Features

### Phase 1: Compaction Planning (Dry-Run)

- Analyzes partition statistics to identify compaction opportunities
- Validates that compaction would benefit storage efficiency
- No actual file modifications (safe exploration mode)

### Phase 2: Compaction Execution

- Consolidates small Parquet files into larger files
- Optimizes read performance by reducing file count
- Maintains Iceberg snapshot isolation during compaction
- Preserves data integrity through transactional operations

### Phase 3: Retention & Lifecycle Management ✨ NEW

#### Retention Enforcement

- **3-Tier Policy Hierarchy**: Global defaults → Tenant overrides → Dataset overrides
- **Per-Signal Type Policies**: Separate retention for traces, logs, and metrics
- **Grace Period Protection**: Prevents premature deletion due to clock skew
- **Dry-Run Mode**: Test policies before enabling enforcement
- **Timezone-Aware**: Configure timezone for scheduling and logging (internal storage uses UTC)

#### Snapshot Expiration

- Keeps configurable number of recent snapshots (default: 5)
- Expires older snapshots to prevent metadata bloat
- Ensures at least one snapshot always remains
- Coordinates with partition drops for efficiency

#### Orphan File Cleanup

- **4-Phase Detection Algorithm**:
  1. Build live file reference set from all snapshots
  2. Scan object store for all Parquet files
  3. Identify orphan candidates (not in reference set + older than grace period)
  4. Optional revalidation before deletion (race condition protection)
- **Safety First**: 24-hour grace period default, dry-run mode, tenant isolation
- **Batch Processing**: Configurable batch sizes with progress tracking
- **Resumability**: Checkpoint-based progress tracking for crash recovery
- **Bounded Memory**: `max_live_files_threshold` skips cleanup for very large
  tables instead of risking OOM, recorded in
  `compactor_orphan_cleanup_skipped_total` (see issue #475)

### Phase 4: Multi-Instance Safety + Scheduling

- **Distributed Leases**: Instances coordinate through a `compactor_leases`
  table in the SQL catalog (SQLite or PostgreSQL). A lease on
  `(tenant, dataset, table, partition)` guarantees at most one instance
  compacts a partition at a time; leases from crashed instances expire after
  `lease_ttl_seconds` and are swept every 30 seconds
- **Round-Robin Scheduling**: Fair candidate selection across tenants with
  `max_candidates_per_cycle` and `max_per_tenant` caps
- **Flight Admin Endpoints** (port 50055, `COMPACTOR_FLIGHT_ADDR`):
  - `compact_now` — trigger compaction on demand
  - `compact_status` — active leases + cumulative metrics as JSON
  - `compact_dry_run` — return the current compaction plan without executing

### Phase 6: Observability

- **Prometheus metrics** at `GET /metrics` (default port 9091)
- **JSON status document** at `GET /status` — instance ID, uptime, and all
  compaction/retention/orphan counters
- **Liveness probe** at `GET /health`

## Configuration

The compactor is configured through the `[compactor]` section in `signaldb.toml` or via environment variables with the `SIGNALDB_COMPACTOR_` prefix.

### Basic Configuration

```toml
[compactor]
enabled = true
compaction_interval_secs = 3600  # Run compaction every hour
dry_run = false  # Set to true for planning mode only
```

### Retention Policies

```toml
[compactor.retention]
enabled = true
retention_check_interval_secs = 3600  # Check retention every hour
dry_run = false  # Set to true to log actions without executing

# Global defaults (per-signal type)
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30

# Safety settings
grace_period_hours = 1  # Don't drop partitions within 1 hour of cutoff
timezone = "UTC"        # Timezone for logging (internal uses UTC)
snapshots_to_keep = 5   # Keep last 5 snapshots per table

# Tenant-level overrides
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 30    # Production traces kept for 30 days
logs_retention_days = 7       # Production logs kept for 7 days
metrics_retention_days = 90   # Production metrics kept for 90 days

# Dataset-level overrides (highest priority)
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces_retention_days = 90    # Critical dataset traces kept for 90 days
```

#### Retention Policy Hierarchy

The compactor uses a 3-tier override system:

1. **Global Defaults**: Apply to all tenants/datasets unless overridden
2. **Tenant Overrides**: Apply to all datasets within a tenant
3. **Dataset Overrides**: Apply to specific tenant+dataset combinations (highest priority)

**Example:**
```toml
# Global: traces = 7 days
traces_retention_days = 7

# Tenant "acme": traces = 14 days (overrides global)
[[compactor.retention.tenant_overrides]]
tenant_id = "acme"
traces_retention_days = 14

# Dataset "acme/production": traces = 30 days (overrides tenant)
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "production"
traces_retention_days = 30
```

For tenant "acme", dataset "production": **30 days** (dataset override wins)
For tenant "acme", dataset "staging": **14 days** (tenant override wins)
For tenant "other": **7 days** (global default wins)

### Orphan File Cleanup

```toml
[compactor.orphan_cleanup]
enabled = true
grace_period_hours = 24              # Don't delete files younger than 24 hours
cleanup_interval_hours = 24          # Run cleanup once per day
batch_size = 1000                    # Process 1000 files per batch
dry_run = false                      # Set to true to log without deleting
revalidate_before_delete = true      # Re-check file status before deletion
max_snapshot_age_hours = 720         # 30 days - consider snapshots for reference
```

**Safety Configuration:**

- **`grace_period_hours`**: Protects against deleting files from in-flight writes
- **`revalidate_before_delete`**: Adds an extra validation step before deletion (catches race conditions)
- **`dry_run`**: Enable to identify orphans without actually deleting them
- **`batch_size`**: Smaller batches = more checkpoints = better resumability

**Performance Tuning:**

- **`cleanup_interval_hours`**: Increase for lower overhead, decrease for faster reclamation
- **`batch_size`**: Increase for faster processing, decrease for more frequent progress tracking
- **`max_snapshot_age_hours`**: Increase to scan more history, decrease to reduce scan time

### Environment Variable Overrides

All configuration can be overridden via environment variables:

```bash
# Enable retention enforcement
SIGNALDB_COMPACTOR_RETENTION_ENABLED=true

# Set trace retention to 14 days
SIGNALDB_COMPACTOR_RETENTION_TRACES_RETENTION_DAYS=14

# Enable dry-run mode
SIGNALDB_COMPACTOR_RETENTION_DRY_RUN=true

# Set orphan cleanup grace period
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_GRACE_PERIOD_HOURS=48
```

## Architecture

### Data Flow

```text
┌─────────────────────────────────────────────────────────────┐
│                     Compactor Service                        │
│                                                              │
│  ┌────────────────┐  ┌────────────────┐  ┌───────────────┐ │
│  │   Compaction   │  │   Retention    │  │    Orphan     │ │
│  │   Scheduler    │  │   Scheduler    │  │   Cleanup     │ │
│  │  (Phase 1+2)   │  │   (Phase 3)    │  │  (Phase 3)    │ │
│  └────────┬───────┘  └────────┬───────┘  └───────┬───────┘ │
│           │                   │                   │          │
│           └───────────┬───────┴───────────┬───────┘          │
│                       ▼                   ▼                  │
│              ┌─────────────────┐  ┌──────────────┐          │
│              │ Iceberg Catalog │  │ Object Store │          │
│              └─────────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### Retention Enforcement Flow

```text
1. Compute Retention Cutoff
   ├─ Resolve policy (global → tenant → dataset)
   ├─ Apply grace period
   └─ Compute cutoff timestamp

2. Identify Expired Partitions
   ├─ List all partitions for table
   ├─ Extract partition timestamp (hour field)
   └─ Filter partitions older than cutoff

3. Drop Expired Partitions
   ├─ Execute ALTER TABLE DROP PARTITION
   ├─ Creates new Iceberg snapshot
   └─ Log dropped partitions with context

4. Expire Old Snapshots
   ├─ List all snapshots
   ├─ Keep N most recent
   └─ Expire older snapshots

5. Cleanup Orphan Files (coordinated)
   ├─ Wait 5 seconds after snapshot expiration
   ├─ Build live file reference set
   ├─ Scan object store
   ├─ Identify orphans (not in set + older than grace period)
   ├─ Optional: Revalidate before deletion
   └─ Delete in batches with progress tracking
```

## Usage

### Running the Compactor

#### Standalone Mode

```bash
# Run compactor as standalone service
cargo run --bin signaldb-compactor

# With specific config file
cargo run --bin signaldb-compactor -- --config /path/to/signaldb.toml

# With debug logging
RUST_LOG=debug,compactor=trace cargo run --bin signaldb-compactor
```

#### Monolithic Mode

The compactor runs automatically when using monolithic mode:

```bash
# Run all services including compactor
cargo run --bin signaldb

# Or using the dev script
./scripts/run-dev.sh
```

### Enabling Retention Enforcement

**Step 1: Enable Dry-Run Mode**

```toml
[compactor.retention]
enabled = true
dry_run = true  # Log what would be deleted
traces_retention_days = 7
```

Start the compactor and monitor logs:

```bash
RUST_LOG=info,compactor=debug cargo run --bin signaldb-compactor
```

Look for log entries like:

```text
[DRY-RUN] Would drop partition: tenant=acme dataset=prod table=traces hour=2026-01-15-10
[DRY-RUN] Would expire snapshot: snapshot_id=123456 age=8days
[DRY-RUN] Would delete orphan file: data/acme/prod/traces/hour=2026-01-15-10/data-001.parquet
```

**Step 2: Enable for Test Tenant**

```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "test"
traces_retention_days = 1  # Short retention for testing
```

Set `dry_run = false` and restart. Verify partitions are dropped correctly.

**Step 3: Rollout to Production**

```toml
[compactor.retention]
enabled = true
dry_run = false
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30

# Add production tenant overrides as needed
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 30
```

### Enabling Orphan Cleanup

**Step 1: Identify Orphans (Dry-Run)**

```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = true  # Don't delete, just identify
grace_period_hours = 24
```

Monitor logs for orphan candidates:

```text
[DRY-RUN] Identified 42 orphan files for tenant=acme dataset=prod table=traces
[DRY-RUN] Would delete: data/acme/prod/traces/hour=2026-01-01-10/orphan-001.parquet (age=5days)
```

**Step 2: Enable Cleanup**

```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = false
revalidate_before_delete = true  # Extra safety
```

Monitor metrics `compactor_files_deleted_total` to verify cleanup is working.

### Multi-Instance Deployment

Multiple compactor instances can safely share one catalog. Coordination is
entirely lease-based — no additional configuration beyond pointing every
instance at the same `[database]` is required:

```bash
# Instance 1
COMPACTOR_FLIGHT_ADDR=0.0.0.0:50055 COMPACTOR_METRICS_ADDR=0.0.0.0:9091 \
  cargo run --bin signaldb-compactor

# Instance 2 (same machine: distinct ports; separate hosts: defaults are fine)
COMPACTOR_FLIGHT_ADDR=0.0.0.0:50056 COMPACTOR_METRICS_ADDR=0.0.0.0:9092 \
  cargo run --bin signaldb-compactor
```

Operational properties:

- A partition is compacted by at most one instance at a time (lease mutual
  exclusion on `(tenant, dataset, table, partition)`)
- If an instance crashes mid-job, its leases expire after `lease_ttl_seconds`
  (default 300s) and any other instance picks up the partition
- Set `lease_ttl_seconds` comfortably above your longest expected compaction
  job; a too-short TTL risks two instances working the same partition back to
  back (never concurrently)
- Inspect active leases on any instance via the Flight `compact_status` action
  or `GET /status` on the metrics port

Covered by `tests-integration/tests/compactor/multi_instance.rs`.

## Metrics

The compactor serves observability endpoints on `metrics_addr`
(default `0.0.0.0:9091`, override with `COMPACTOR_METRICS_ADDR`, disable by
setting it to an empty string):

- `GET /metrics` — Prometheus text exposition
- `GET /status` — JSON document with instance ID, uptime, and all counters
- `GET /health` — liveness probe (returns `ok`)

```bash
curl -s localhost:9091/metrics | grep compactor
curl -s localhost:9091/status | jq .
```

### Compaction Metrics (Phase 2)

- `compactor_jobs_started_total` - Compaction jobs started
- `compactor_jobs_succeeded_total` - Jobs completed successfully
- `compactor_jobs_failed_total` - Jobs failed
- `compactor_conflicts_detected_total` - Iceberg commit conflicts detected
- `compactor_retries_attempted_total` - Retries after commit conflicts
- `compactor_input_files_total` / `compactor_output_files_total` - File consolidation ratio
- `compactor_bytes_before_compaction_total` / `compactor_bytes_after_compaction_total` - Size reduction
- `compactor_compaction_duration_ms_total` - Cumulative job wall-clock time

### Retention Metrics (Phase 3)

- `compactor_retention_cutoffs_computed_total` - Cutoffs computed
- `compactor_partitions_evaluated_total` - Partitions checked
- `compactor_partitions_dropped_total` - Partitions dropped
- `compactor_snapshots_expired_total` - Snapshots expired
- `compactor_bytes_reclaimed_total` - Storage reclaimed by retention
- `compactor_retention_duration_ms_total` - Cumulative enforcement wall-clock time

### Orphan Cleanup Metrics (Phase 3)

- `compactor_orphan_candidates_identified_total` - Orphans found
- `compactor_files_deleted_total` - Files deleted
- `compactor_bytes_freed_total` - Storage reclaimed by orphan deletion
- `compactor_deletion_failures_total` - Deletion errors
- `compactor_orphan_cleanup_skipped_total{reason}` - Cleanup runs skipped
  (e.g. `reason="live_files_threshold_exceeded"` when the live file set
  exceeds `max_live_files_threshold`)

### Example Prometheus Queries

**Storage reclaimed in the last 24h:**
```promql
increase(compactor_bytes_freed_total[24h]) + increase(compactor_bytes_reclaimed_total[24h])
```

**File consolidation effectiveness (input:output ratio, last 7d):**
```promql
increase(compactor_input_files_total[7d]) / increase(compactor_output_files_total[7d])
```

**Alert on deletion failures:**
```promql
increase(compactor_deletion_failures_total[1h]) > 0
```

## Troubleshooting

### No Partitions Being Dropped

**Symptoms:** `compactor_partitions_dropped_total` is zero, but data should be expired.

**Possible Causes:**

1. **Retention not enabled:**
   - Check: `enabled = true` in `[compactor.retention]`
   - Check logs for "Retention enforcement disabled"

2. **Dry-run mode enabled:**
   - Check: `dry_run = false` in config
   - Look for `[DRY-RUN]` in logs

3. **Grace period too large:**
   - Check: `grace_period_hours` setting
   - Compute actual cutoff: `now - retention_days - grace_period_hours`
   - Data must be older than cutoff

4. **No data older than retention:**
   - Check partition timestamps in Iceberg catalog
   - Use query: `SELECT DISTINCT hour FROM traces_table ORDER BY hour`

**Resolution:**

```bash
# Check effective retention cutoff
RUST_LOG=debug,compactor::retention=trace cargo run --bin signaldb-compactor

# Look for log line:
# "Computed retention cutoff: tenant=X dataset=Y signal=traces cutoff=2026-01-25T10:00:00Z"
```

### Orphan Cleanup Not Deleting Files

**Symptoms:** `compactor_orphans_identified_total` > 0 but `compactor_files_deleted_total` is zero.

**Possible Causes:**

1. **Dry-run mode enabled:**
   - Check: `dry_run = false` in `[compactor.orphan_cleanup]`

2. **Revalidation failing:**
   - Check logs for "Revalidation: file now referenced"
   - Files may have been referenced in new snapshots

3. **Grace period not met:**
   - Files must be older than `grace_period_hours`
   - Check file modification times

4. **Permission errors:**
   - Check logs for "Failed to delete file"
   - Verify object store credentials and permissions

**Resolution:**

```bash
# Enable detailed logging
RUST_LOG=debug,compactor::orphan=trace cargo run --bin signaldb-compactor

# Check for revalidation logs
grep "Revalidation" .data/logs/compactor.log

# Temporarily disable revalidation for testing
# (in config: revalidate_before_delete = false)
```

### High Memory Usage During Orphan Cleanup

**Symptoms:** Compactor using excessive memory during cleanup runs.

**Possible Causes:**

1. **Large reference set:**
   - Many snapshots + many files = large HashSet in memory

2. **Large object store listings:**
   - Millions of files being listed at once

**Resolution:**

```toml
[compactor.orphan_cleanup]
# Reduce snapshot window
max_snapshot_age_hours = 168  # 7 days instead of 30

# Reduce batch size
batch_size = 500  # Down from 1000

# Increase cleanup interval (less frequent = less peak memory)
cleanup_interval_hours = 48
```

### Concurrent Query Failures During Retention

**Symptoms:** Queries failing with "Snapshot not found" during retention enforcement.

**Cause:** Queries started using old snapshot that was expired before query completed.

**Resolution:**

```toml
[compactor.retention]
# Keep more snapshots
snapshots_to_keep = 10  # Increase from 5

# Run retention less frequently
retention_check_interval_secs = 7200  # Every 2 hours instead of 1
```

### Partition Drop Failures

**Symptoms:** Logs show "Failed to drop partition" errors.

**Possible Causes:**

1. **Catalog connection issues:**
   - Check PostgreSQL/SQLite connectivity
   - Look for "Failed to execute SQL" in logs

2. **Invalid partition format:**
   - Partitions must use `hour = "YYYY-MM-DD-HH"` format
   - Check Iceberg partition spec

3. **Concurrent modifications:**
   - Another process modifying the table
   - Retry will resolve (operations are idempotent)

**Resolution:**

```bash
# Check catalog connectivity
psql -h localhost -U signaldb -d signaldb -c "SELECT * FROM services LIMIT 1"

# Verify partition format
RUST_LOG=debug,compactor::iceberg=trace cargo run --bin signaldb-compactor
```

## Development

### Running Tests

```bash
# Run all compactor tests
cargo test -p compactor

# Run integration tests only
cargo test -p tests-integration --test compactor_retention

# Run specific test file
cargo test -p tests-integration --test retention_cutoff

# Run with output
cargo test -p compactor -- --nocapture

# With debug logging
RUST_LOG=debug cargo test -p compactor -- --nocapture
```

### Integration Test Structure

The compactor has comprehensive integration tests in `tests-integration/tests/compactor/`:

- **`retention_cutoff.rs`** - Policy resolution and cutoff computation (5 tests)
- **`partition_drop.rs`** - Partition dropping with isolation (5 tests)
- **`snapshot_expiration.rs`** - Snapshot expiration logic (4 tests)
- **`orphan_cleanup.rs`** - Orphan detection and deletion (5 tests)
- **`retention_enforcer.rs`** - End-to-end retention enforcement (6 tests)
- **`compactor_service.rs`** - Service integration (8 tests)

**Total: 33 integration tests**

### Adding New Retention Policies

1. **Update Configuration:**

   Edit `src/compactor/src/retention/config.rs`:

   ```rust
   #[derive(Debug, Clone, Serialize, Deserialize)]
   pub struct RetentionConfig {
       pub enabled: bool,
       pub new_policy: Duration,  // Add new field
       // ...
   }
   ```

2. **Update Policy Resolver:**

   Edit `src/compactor/src/retention/policy.rs`:

   ```rust
   impl RetentionPolicyResolver {
       pub fn compute_cutoff(&self, ctx: &Context) -> RetentionCutoff {
           // Add logic for new policy
       }
   }
   ```

3. **Add Tests:**

   Add test in `tests-integration/tests/compactor/retention_cutoff.rs`:

   ```rust
   #[tokio::test]
   async fn test_new_policy_resolution() -> Result<()> {
       // Test new policy behavior
   }
   ```

4. **Update Documentation:**

   Update this README and `docs/compactor/phase3-implementation-plan.md`.

### Phase 3 Implementation Details

For detailed implementation information, see:

- **Implementation Plan**: `/docs/compactor/phase3-implementation-plan.md`
- **Retention Design**: `/docs/compactor-phase3-retention-design.md`
- **Orphan Cleanup Design**: `/docs/design/compactor-phase3-orphan-cleanup.md`
- **Test Strategy**: `/docs/compactor/phase3-integration-test-strategy.md`

## License

This component is part of SignalDB and is licensed under the project's license terms.
