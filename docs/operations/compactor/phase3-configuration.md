---
audience: operator
type: reference
status: living
sources:
  - src/compactor/src/**
  - src/common/src/config/mod.rs
---

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

Phase 3 configuration is located in the `[compactor]` section of `signaldb.toml` or via environment variables with the `SIGNALDB__COMPACTOR__` prefix (double underscores separate nesting levels).

**Configuration Precedence:**
1. Environment variables (highest priority)
2. `signaldb.toml` configuration file
3. Default values (lowest priority)

**Configuration Files:**
- **Production:** `/etc/signaldb/signaldb.toml`
- **Development:** `./signaldb.toml` (copy from `signaldb.dist.toml`)
- **Container:** the compactor's `--config` flag defaults to `./signaldb.toml` relative to the working directory. If you mount a config file elsewhere (e.g. `/config/signaldb.toml`), you must pass `--config /config/signaldb.toml` explicitly or the mounted file is silently ignored.

**Duration syntax:** retention durations are humantime strings (`"1h"`, `"7d"`, `"30d"`, `"90d"`), not integers. Orphan-cleanup intervals are plain integer hour counts.

## Retention Configuration

### `[compactor.retention]`

Controls automatic retention enforcement and partition lifecycle management.

#### Basic Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `false` | Enable retention enforcement (opt-in) |
| `dry_run` | `bool` | `true` | Log actions without executing (safe default) |
| `retention_check_interval` | duration string | `"1h"` | Interval between retention checks |
| `timezone` | `string` | `"UTC"` | Timezone for logging (internal uses UTC) |

**Example:**
```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval = "1h"
timezone = "America/New_York"  # For logging only
```

#### Global Retention Periods

Default retention periods for all tenants/datasets (unless overridden).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `traces` | duration string | `"7d"` | Trace data retention |
| `logs` | duration string | `"30d"` | Log data retention |
| `metrics` | duration string | `"90d"` | Metric data retention |

**Example:**
```toml
[compactor.retention]
traces = "7d"
logs = "30d"
metrics = "90d"
```

**Signal Type Mapping:**
- `traces` → `traces` table
- `logs` → `logs` table
- `metrics` → any table whose name starts with `metrics_` (`metrics_gauge`, `metrics_sum`, `metrics_histogram` by default)

#### Safety Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `grace_period` | duration string | `"1h"` | Safety margin before cutoff |
| `snapshots_to_keep` | `usize` (optional) | `10` | Minimum snapshots to retain per table |

**Example:**
```toml
[compactor.retention]
grace_period = "2h"     # 2-hour safety margin
snapshots_to_keep = 10  # Keep last 10 snapshots
```

**Grace Period Explained:**

The grace period prevents premature deletion due to clock skew or timing issues.

```text
Computed Cutoff = NOW - retention - grace_period

Example:
- NOW = 2026-02-09 10:00:00 UTC
- traces = "7d"
- grace_period = "1h"

Cutoff = 2026-02-09 10:00:00 - 7 days - 1 hour
       = 2026-02-02 09:00:00 UTC

Partitions older than 2026-02-02 09:00:00 are dropped.
```

#### Tenant Overrides

Override global retention periods for specific tenants. `tenant_overrides` is a map keyed by tenant ID.

**Structure:**
```toml
[compactor.retention.tenant_overrides.<tenant-id>]
traces = "14d"    # Optional override
logs = "7d"       # Optional override
metrics = "60d"   # Optional override
```

**Example:**
```toml
# Production tenant keeps data longer
[compactor.retention.tenant_overrides.production]
traces = "30d"
logs = "7d"
metrics = "90d"

# Dev tenant keeps data shorter
[compactor.retention.tenant_overrides.dev]
traces = "1d"
logs = "1d"
metrics = "3d"
```

**Partial Overrides:**

You can override only specific signal types:

```toml
[compactor.retention.tenant_overrides.special]
traces = "90d"  # Only override traces
# logs and metrics use global defaults
```

#### Dataset Overrides

Override retention periods for specific tenant+dataset combinations (highest priority). `dataset_overrides` is a map keyed by dataset ID, nested inside a tenant override.

**Structure:**
```toml
[compactor.retention.tenant_overrides.<tenant-id>.dataset_overrides.<dataset-id>]
traces = "90d"    # Optional override
logs = "14d"      # Optional override
metrics = "180d"  # Optional override
```

**Example:**
```toml
[compactor.retention.tenant_overrides.acme]
traces = "14d"  # Tenant default: 14 days

# Critical dataset keeps data much longer
[compactor.retention.tenant_overrides.acme.dataset_overrides.critical]
traces = "90d"  # Dataset override: 90 days

# Staging dataset uses short retention
[compactor.retention.tenant_overrides.acme.dataset_overrides.staging]
traces = "3d"  # Dataset override: 3 days
```

**Resolution Example:**

With this configuration:
```toml
[compactor.retention]
traces = "7d"  # Global default

[compactor.retention.tenant_overrides.acme]
traces = "14d"  # Tenant override

[compactor.retention.tenant_overrides.acme.dataset_overrides.critical]
traces = "90d"  # Dataset override
```

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
retention_check_interval = "1h"
timezone = "UTC"

# Global defaults
traces = "7d"
logs = "30d"
metrics = "90d"

# Safety
grace_period = "1h"
snapshots_to_keep = 10

# Production tenant
[compactor.retention.tenant_overrides.production]
traces = "30d"
logs = "7d"
metrics = "90d"

# Production critical dataset
[compactor.retention.tenant_overrides.production.dataset_overrides.critical]
traces = "90d"
logs = "14d"
metrics = "180d"

# Production staging dataset
[compactor.retention.tenant_overrides.production.dataset_overrides.staging]
traces = "1d"
logs = "1d"
metrics = "3d"

# Development tenant
[compactor.retention.tenant_overrides.dev]
traces = "1d"
logs = "1d"
metrics = "3d"
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

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `grace_period_hours` | `u64` | `24` | Don't delete files younger than this (hours) |
| `revalidate_before_delete` | `bool` | `true` | Re-check file status before deletion |
| `max_snapshot_age_hours` | `u64` | `720` | Consider snapshots within this age (hours) |

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

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | `usize` | `1000` | Files to process per batch |
| `max_live_files_threshold` | `usize` | `500000` | Skip cleanup for tables whose estimated live file count exceeds this cap (`0` disables the cap; bounds memory; skips recorded in `compactor_orphan_cleanup_skipped_total`) |

**Example:**
```toml
[compactor.orphan_cleanup]
batch_size = 500                    # Smaller batches = more checkpoints
max_live_files_threshold = 500000   # Skip huge tables instead of OOMing
```

**Tuning Guidance:**

- **Small batches** (100-500): More frequent progress checkpoints, better resumability, higher overhead
- **Large batches** (1000-5000): Faster processing, less overhead, coarser checkpoints
- **Live-file threshold**: If cleanup is skipped for a table, run snapshot expiration and compaction first to reduce file counts before raising or disabling the cap

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
max_live_files_threshold = 500000

# Example: More aggressive cleanup for dev
# [compactor.orphan_cleanup]
# grace_period_hours = 1          # 1 hour grace period
# cleanup_interval_hours = 1      # Run every hour
# max_snapshot_age_hours = 24     # Only last 24h snapshots
```

## Environment Variables

All scalar configuration can be overridden via environment variables.

### Naming Convention

Nested compactor keys use the double-underscore form: `SIGNALDB__` prefix, with `__` between each nesting level:

```
SIGNALDB__COMPACTOR__<SECTION>__<FIELD>
```

### Retention Environment Variables

**Basic:**
```bash
SIGNALDB__COMPACTOR__RETENTION__ENABLED=true
SIGNALDB__COMPACTOR__RETENTION__DRY_RUN=false
SIGNALDB__COMPACTOR__RETENTION__RETENTION_CHECK_INTERVAL=1h
SIGNALDB__COMPACTOR__RETENTION__TIMEZONE="UTC"
```

**Retention Periods:**
```bash
SIGNALDB__COMPACTOR__RETENTION__TRACES=7d
SIGNALDB__COMPACTOR__RETENTION__LOGS=30d
SIGNALDB__COMPACTOR__RETENTION__METRICS=90d
```

**Safety:**
```bash
SIGNALDB__COMPACTOR__RETENTION__GRACE_PERIOD=1h
SIGNALDB__COMPACTOR__RETENTION__SNAPSHOTS_TO_KEEP=10
```

**Tenant Overrides:**

Tenant overrides are not supported via environment variables. Use the configuration file for overrides.

### Orphan Cleanup Environment Variables

```bash
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED=true
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN=false
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__CLEANUP_INTERVAL_HOURS=24
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__GRACE_PERIOD_HOURS=24
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__REVALIDATE_BEFORE_DELETE=true
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_SNAPSHOT_AGE_HOURS=720
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__BATCH_SIZE=1000
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_LIVE_FILES_THRESHOLD=500000
```

### Example: Docker Compose

```yaml
version: '3.8'
services:
  signaldb:
    image: signaldb:latest
    environment:
      # Enable retention with env vars
      SIGNALDB__COMPACTOR__RETENTION__ENABLED: "true"
      SIGNALDB__COMPACTOR__RETENTION__DRY_RUN: "false"
      SIGNALDB__COMPACTOR__RETENTION__TRACES: "7d"
      SIGNALDB__COMPACTOR__RETENTION__LOGS: "30d"

      # Enable orphan cleanup
      SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED: "true"
      SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN: "false"
    volumes:
      - ./signaldb.toml:/config/signaldb.toml
    # Required: the binary reads ./signaldb.toml by default, so the
    # mounted path must be passed explicitly.
    command: ["--config", "/config/signaldb.toml"]
```

## Configuration Examples

### Example 1: Development Environment

Short retention, frequent cleanup:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval = "5m"
traces = "1d"
logs = "1d"
metrics = "1d"
grace_period = "0s"  # No grace period for testing
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
retention_check_interval = "1h"
traces = "7d"
logs = "30d"
metrics = "90d"
grace_period = "1h"
snapshots_to_keep = 10

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
retention_check_interval = "1h"

# Default for all tenants
traces = "7d"
logs = "30d"
metrics = "90d"

grace_period = "1h"
snapshots_to_keep = 10

# Enterprise tenant - longer retention
[compactor.retention.tenant_overrides.enterprise]
traces = "90d"
logs = "30d"
metrics = "180d"

# Trial tenant - shorter retention
[compactor.retention.tenant_overrides.trial]
traces = "3d"
logs = "1d"
metrics = "7d"

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
retention_check_interval = "2h"
traces = "7d"
logs = "30d"
metrics = "90d"
grace_period = "1h"
snapshots_to_keep = 3  # Fewer snapshots

[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 48  # Every 2 days
grace_period_hours = 24
revalidate_before_delete = true
max_snapshot_age_hours = 168  # 7 days
batch_size = 5000  # Larger batches
```

### Example 5: Compliance-Focused

Long retention, strict safety:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval = "1h"
traces = "90d"  # 90-day compliance requirement
logs = "90d"
metrics = "90d"
grace_period = "24h"  # 24-hour grace period
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

Retention configuration is validated at startup: `RetentionConfig::validate` runs when the retention enforcer is constructed, and invalid retention configuration causes startup to fail with an error message.

### Retention Validation

- `traces`, `logs`, and `metrics` retention durations must be positive (non-zero) — globally and in every tenant/dataset override
- `grace_period` must not be negative

No other retention fields are validated; there are no enforced value ranges, and `retention_check_interval` is not checked.

### Orphan Cleanup Validation

Orphan-cleanup values are currently **not** validated at startup. `OrphanCleanupConfig::validate` exists (it requires `cleanup_interval_hours`, `grace_period_hours`, `batch_size`, and `max_snapshot_age_hours` to all be > 0) but is not wired into the compactor's startup path, so invalid values — for example `grace_period_hours = 0` — are accepted and take effect as written. Review these values carefully before deploying.

### Validation Errors

Example retention error messages:

```
Invalid retention period for traces: 0ns must be positive
Invalid retention configuration for tenant 'acme': Invalid retention period for logs: 0ns must be positive
```

## Additional Resources

- [Phase 3 Operations Guide](phase3-operations.md)
- [Phase 3 Troubleshooting Guide](phase3-troubleshooting.md)
- [Compactor README](../../../src/compactor/README.md)
