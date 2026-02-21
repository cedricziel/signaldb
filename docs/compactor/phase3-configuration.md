# Phase 3 Configuration Reference

Complete reference for configuring SignalDB Compactor Phase 3: Retention Enforcement and Lifecycle Management.

## Table of Contents

- [Configuration Overview](#configuration-overview)
- [Retention Configuration](#retention-configuration)
- [Orphan Cleanup Configuration](#orphan-cleanup-configuration)
- [Environment Variables](#environment-variables)
- [Configuration Examples](#configuration-examples)
- [Validation Rules](#validation-rules)

## Configuration Overview

Phase 3 configuration is located in the `[compactor]` section of `signaldb.toml` or via environment variables with the `SIGNALDB_COMPACTOR_` prefix.

**Configuration Precedence:**
1. Environment variables (highest priority)
2. `signaldb.toml` configuration file
3. Default values (lowest priority)

**Configuration Files:**
- **Production:** `/etc/signaldb/signaldb.toml`
- **Development:** `./signaldb.toml` (copy from `signaldb.dist.toml`)
- **Container:** `/config/signaldb.toml` (mounted volume)

## Retention Configuration

### `[compactor.retention]`

Controls automatic retention enforcement and partition lifecycle management.

#### Basic Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Enable retention enforcement (opt-in) |
| `dry_run` | `bool` | `true` | Log actions without executing (safe default) |
| `retention_check_interval_secs` | `u64` | `3600` | Interval between retention checks (seconds) |
| `timezone` | `string` | `"UTC"` | Timezone for logging (internal uses UTC) |

**Example:**
```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 3600  # 1 hour
timezone = "America/New_York"         # For logging only
```

#### Global Retention Periods

Default retention periods for all tenants/datasets (unless overridden).

| Field | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `traces_retention_days` | `u32` | `7` | 1-36500 | Trace data retention (days) |
| `logs_retention_days` | `u32` | `3` | 1-36500 | Log data retention (days) |
| `metrics_retention_days` | `u32` | `30` | 1-36500 | Metric data retention (days) |

**Example:**
```toml
[compactor.retention]
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30
```

**Signal Type Mapping:**
- `traces` → `traces` table
- `logs` → `logs` table
- `metrics` → All metrics tables (`metrics_gauge`, `metrics_counter`, `metrics_histogram`, `metrics_summary`)

#### Safety Settings

| Field | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `grace_period_hours` | `u32` | `1` | 0-168 | Safety margin before cutoff (hours) |
| `snapshots_to_keep` | `u32` | `5` | 1-1000 | Minimum snapshots to retain per table |

**Example:**
```toml
[compactor.retention]
grace_period_hours = 2     # 2-hour safety margin
snapshots_to_keep = 10     # Keep last 10 snapshots
```

**Grace Period Explained:**

The grace period prevents premature deletion due to clock skew or timing issues.

```text
Computed Cutoff = NOW - retention_days - grace_period_hours

Example:
- NOW = 2026-02-09 10:00:00 UTC
- retention_days = 7
- grace_period_hours = 1

Cutoff = 2026-02-09 10:00:00 - 7 days - 1 hour
       = 2026-02-02 09:00:00 UTC

Partitions older than 2026-02-02 09:00:00 are dropped.
```

#### Tenant Overrides

Override global retention periods for specific tenants.

**Structure:**
```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "tenant-name"
traces_retention_days = 14      # Optional override
logs_retention_days = 7         # Optional override
metrics_retention_days = 60     # Optional override
```

**Example:**
```toml
# Production tenant keeps data longer
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 30
logs_retention_days = 7
metrics_retention_days = 90

# Dev tenant keeps data shorter
[[compactor.retention.tenant_overrides]]
tenant_id = "dev"
traces_retention_days = 1
logs_retention_days = 1
metrics_retention_days = 3
```

**Partial Overrides:**

You can override only specific signal types:

```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "special"
traces_retention_days = 90  # Only override traces
# logs and metrics use global defaults
```

#### Dataset Overrides

Override retention periods for specific tenant+dataset combinations (highest priority).

**Structure:**
```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "tenant-name"

[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "dataset-name"
traces_retention_days = 90      # Optional override
logs_retention_days = 14        # Optional override
metrics_retention_days = 180    # Optional override
```

**Example:**
```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "acme"
traces_retention_days = 14  # Tenant default: 14 days

# Critical dataset keeps data much longer
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces_retention_days = 90  # Dataset override: 90 days

# Staging dataset uses short retention
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "staging"
traces_retention_days = 3  # Dataset override: 3 days
```

**Resolution Example:**

With this configuration:
```toml
[compactor.retention]
traces_retention_days = 7  # Global default

[[compactor.retention.tenant_overrides]]
tenant_id = "acme"
traces_retention_days = 14  # Tenant override

[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces_retention_days = 90  # Dataset override
```text

Results:
- `acme/critical` → **90 days** (dataset override)
- `acme/production` → **14 days** (tenant override)
- `other/anything` → **7 days** (global default)

### Complete Retention Example

```toml
[compactor.retention]
# Basic settings
enabled = true
dry_run = false
retention_check_interval_secs = 3600
timezone = "UTC"

# Global defaults
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30

# Safety
grace_period_hours = 1
snapshots_to_keep = 5

# Production tenant
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 30
logs_retention_days = 7
metrics_retention_days = 90

# Production critical dataset
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces_retention_days = 90
logs_retention_days = 14
metrics_retention_days = 180

# Production staging dataset
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "staging"
traces_retention_days = 1
logs_retention_days = 1
metrics_retention_days = 3

# Development tenant
[[compactor.retention.tenant_overrides]]
tenant_id = "dev"
traces_retention_days = 1
logs_retention_days = 1
metrics_retention_days = 3
```

## Orphan Cleanup Configuration

### `[compactor.orphan_cleanup]`

Controls automatic detection and deletion of orphaned Parquet files.

#### Basic Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Enable orphan cleanup (opt-in) |
| `dry_run` | `bool` | `true` | Log orphans without deleting (safe default) |
| `cleanup_interval_hours` | `u64` | `24` | Interval between cleanup runs (hours) |

**Example:**
```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 24  # Run once per day
```

#### Safety Settings

| Field | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `grace_period_hours` | `u32` | `24` | 1-720 | Don't delete files younger than this (hours) |
| `revalidate_before_delete` | `bool` | `true` | - | Re-check file status before deletion |
| `max_snapshot_age_hours` | `u64` | `720` | 24-8760 | Consider snapshots within this age (hours) |

**Example:**
```toml
[compactor.orphan_cleanup]
grace_period_hours = 48          # 2-day grace period
revalidate_before_delete = true  # Extra safety
max_snapshot_age_hours = 168     # 7-day snapshot window
```

**Safety Mechanism Explained:**

1. **Grace Period:** Files younger than `grace_period_hours` are never deleted, even if orphaned.
   - Protects against in-flight writes
   - Prevents race conditions with compaction
   - Default 24 hours is conservative

2. **Revalidation:** Before deleting, re-check if file is still orphaned.
   - Catches concurrent writes that referenced the file
   - Adds ~10% overhead but prevents data loss
   - Recommended: always `true`

3. **Snapshot Age Window:** Only scan snapshots within this age for building reference set.
   - Reduces memory usage for tables with many snapshots
   - Files referenced by older snapshots may be incorrectly identified as orphans
   - Should be larger than your longest query duration

#### Performance Settings

| Field | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `batch_size` | `usize` | `1000` | 10-10000 | Files to process per batch |
| `rate_limit_delay_ms` | `u64` | `0` | 0-10000 | Delay between batches (milliseconds) |

**Example:**
```toml
[compactor.orphan_cleanup]
batch_size = 500              # Smaller batches = more checkpoints
rate_limit_delay_ms = 1000    # 1 second delay between batches
```

**Tuning Guidance:**

- **Small batches** (100-500): More frequent progress checkpoints, better resumability, higher overhead
- **Large batches** (1000-5000): Faster processing, less overhead, coarser checkpoints
- **Rate limiting**: Use if hitting object store rate limits (S3 throttling)

### Complete Orphan Cleanup Example

```toml
[compactor.orphan_cleanup]
# Basic settings
enabled = true
dry_run = false
cleanup_interval_hours = 24

# Safety (conservative defaults)
grace_period_hours = 24
revalidate_before_delete = true
max_snapshot_age_hours = 720  # 30 days

# Performance
batch_size = 1000
rate_limit_delay_ms = 0

# Example: More aggressive cleanup for dev
# [compactor.orphan_cleanup]
# grace_period_hours = 1          # 1 hour grace period
# cleanup_interval_hours = 1      # Run every hour
# max_snapshot_age_hours = 24     # Only last 24h snapshots
```

## Environment Variables

All configuration can be overridden via environment variables.

### Naming Convention

```
SIGNALDB_COMPACTOR_<SECTION>_<FIELD>
```

Nested fields use double underscores:
```
SIGNALDB_COMPACTOR_<SECTION>_<SUBSECTION>__<FIELD>
```

### Retention Environment Variables

**Basic:**
```bash
SIGNALDB_COMPACTOR_RETENTION_ENABLED=true
SIGNALDB_COMPACTOR_RETENTION_DRY_RUN=false
SIGNALDB_COMPACTOR_RETENTION_RETENTION_CHECK_INTERVAL_SECS=3600
SIGNALDB_COMPACTOR_RETENTION_TIMEZONE="UTC"
```

**Retention Periods:**
```bash
SIGNALDB_COMPACTOR_RETENTION_TRACES_RETENTION_DAYS=7
SIGNALDB_COMPACTOR_RETENTION_LOGS_RETENTION_DAYS=3
SIGNALDB_COMPACTOR_RETENTION_METRICS_RETENTION_DAYS=30
```

**Safety:**
```bash
SIGNALDB_COMPACTOR_RETENTION_GRACE_PERIOD_HOURS=1
SIGNALDB_COMPACTOR_RETENTION_SNAPSHOTS_TO_KEEP=5
```

**Tenant Overrides:**

Tenant overrides are not supported via environment variables. Use configuration file for complex overrides.

### Orphan Cleanup Environment Variables

```bash
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_ENABLED=true
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_DRY_RUN=false
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_CLEANUP_INTERVAL_HOURS=24
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_GRACE_PERIOD_HOURS=24
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_REVALIDATE_BEFORE_DELETE=true
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_MAX_SNAPSHOT_AGE_HOURS=720
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_BATCH_SIZE=1000
SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_RATE_LIMIT_DELAY_MS=0
```

### Example: Docker Compose

```yaml
version: '3.8'
services:
  signaldb:
    image: signaldb:latest
    environment:
      # Enable retention with env vars
      SIGNALDB_COMPACTOR_RETENTION_ENABLED: "true"
      SIGNALDB_COMPACTOR_RETENTION_DRY_RUN: "false"
      SIGNALDB_COMPACTOR_RETENTION_TRACES_RETENTION_DAYS: "7"
      SIGNALDB_COMPACTOR_RETENTION_LOGS_RETENTION_DAYS: "3"

      # Enable orphan cleanup
      SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_ENABLED: "true"
      SIGNALDB_COMPACTOR_ORPHAN_CLEANUP_DRY_RUN: "false"
    volumes:
      - ./signaldb.toml:/config/signaldb.toml
```

## Configuration Examples

### Example 1: Development Environment

Short retention, frequent cleanup:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 300  # 5 minutes
traces_retention_days = 1
logs_retention_days = 1
metrics_retention_days = 1
grace_period_hours = 0  # No grace period for testing
snapshots_to_keep = 2

[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 1
grace_period_hours = 1  # 1 hour grace period
batch_size = 100
```

### Example 2: Production Environment

Standard retention, conservative cleanup:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 3600  # 1 hour
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30
grace_period_hours = 1
snapshots_to_keep = 5

[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 24  # Once per day
grace_period_hours = 24
revalidate_before_delete = true
max_snapshot_age_hours = 720  # 30 days
batch_size = 1000
```

### Example 3: Multi-Tenant Production

Different retention per tenant:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 3600

# Default for all tenants
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30

grace_period_hours = 1
snapshots_to_keep = 5

# Enterprise tenant - longer retention
[[compactor.retention.tenant_overrides]]
tenant_id = "enterprise"
traces_retention_days = 90
logs_retention_days = 30
metrics_retention_days = 180

# Trial tenant - shorter retention
[[compactor.retention.tenant_overrides]]
tenant_id = "trial"
traces_retention_days = 3
logs_retention_days = 1
metrics_retention_days = 7

[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 24
grace_period_hours = 24
revalidate_before_delete = true
```

### Example 4: High-Volume Environment

Optimized for performance:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 7200  # Every 2 hours
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30
grace_period_hours = 1
snapshots_to_keep = 3  # Fewer snapshots

[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 48  # Every 2 days
grace_period_hours = 24
revalidate_before_delete = true
max_snapshot_age_hours = 168  # 7 days
batch_size = 5000  # Larger batches
rate_limit_delay_ms = 100  # Rate limiting
```

### Example 5: Compliance-Focused

Long retention, strict safety:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 3600
traces_retention_days = 90  # 90-day compliance requirement
logs_retention_days = 90
metrics_retention_days = 90
grace_period_hours = 24  # 24-hour grace period
snapshots_to_keep = 30  # Keep many snapshots

[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 168  # Once per week
grace_period_hours = 168  # 1-week grace period
revalidate_before_delete = true
max_snapshot_age_hours = 2160  # 90 days
batch_size = 500  # Smaller batches for safety
```

## Validation Rules

Configuration is validated at startup. Invalid configuration will cause startup to fail with clear error messages.

### Retention Validation

- `enabled` must be `true` or `false`
- `dry_run` must be `true` or `false`
- `retention_check_interval_secs` must be > 0
- `traces_retention_days` must be 1-36500 (1 day to 100 years)
- `logs_retention_days` must be 1-36500
- `metrics_retention_days` must be 1-36500
- `grace_period_hours` must be 0-168 (0 to 1 week)
- `snapshots_to_keep` must be 1-1000
- `timezone` must be valid IANA timezone (e.g., "UTC", "America/New_York")
- `tenant_id` in overrides must not be empty
- `dataset_id` in overrides must not be empty

### Orphan Cleanup Validation

- `enabled` must be `true` or `false`
- `dry_run` must be `true` or `false`
- `cleanup_interval_hours` must be > 0
- `grace_period_hours` must be 1-720 (1 hour to 30 days)
- `revalidate_before_delete` must be `true` or `false`
- `max_snapshot_age_hours` must be 24-8760 (1 day to 1 year)
- `batch_size` must be 10-10000
- `rate_limit_delay_ms` must be 0-10000

### Validation Errors

Example error messages:

```
ERROR: Invalid retention configuration: traces_retention_days must be between 1 and 36500, got 0
ERROR: Invalid timezone: 'Foo/Bar' is not a valid IANA timezone
ERROR: Invalid orphan cleanup configuration: grace_period_hours must be between 1 and 720, got 0
ERROR: Tenant override for tenant_id '' has empty tenant_id
```

## Additional Resources

- [Phase 3 Operations Guide](phase3-operations.md)
- [Phase 3 Troubleshooting Guide](phase3-troubleshooting.md)
- [Compactor README](../../src/compactor/README.md)
