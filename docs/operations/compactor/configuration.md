---
audience: operator
type: reference
status: living
sources:
  - src/compactor/src/**
  - src/common/src/config/mod.rs
---

# Compactor Configuration Reference

Complete reference for configuring SignalDB Compactor retention and lifecycle management (retention enforcement, snapshot expiration, and orphan-file cleanup).

## Table of Contents

- [Configuration Overview](#configuration-overview)
- [Compaction Settings](#compaction-settings)
- [Retention Configuration](#retention-configuration)
- [Orphan Cleanup Configuration](#orphan-cleanup-configuration)
- [Attribute Promotion Configuration](#attribute-promotion-configuration)
- [Environment Variables](#environment-variables)
- [Configuration Examples](#configuration-examples)
- [Validation Rules](#validation-rules)

## Configuration Overview

Compactor lifecycle configuration is located in the `[compactor]` section of `signaldb.toml` or via environment variables with the `SIGNALDB__COMPACTOR__` prefix (double underscores separate nesting levels).

**Configuration Precedence:**

1. Environment variables (highest priority)
2. `signaldb.toml` configuration file
3. Default values (lowest priority)

**Configuration Files:**

- **Production:** `/etc/signaldb/signaldb.toml`
- **Development:** `./signaldb.toml` (copy from `signaldb.dist.toml`)
- **Container:** the compactor's `--config` flag (the shared `signaldb` option, usable as `signaldb compactor --config …` or `signaldb --config … compactor`) defaults to `./signaldb.toml` relative to the working directory. If you mount a config file elsewhere (e.g. `/config/signaldb.toml`), you must pass `--config /config/signaldb.toml` explicitly or the mounted file is silently ignored.

**Duration syntax:** retention durations are humantime strings (`"1h"`, `"7d"`, `"30d"`, `"90d"`), not integers. Orphan-cleanup intervals are plain integer hour counts.

## Compaction Settings

### `[compactor]`

Controls compaction planning: which files are merged into larger ones and when a table qualifies.

| Field                      | Type            | Default          | Description                                                                                                                                                                                    |
| -------------------------- | --------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`                  | `bool`          | `true`           | Enable the compactor service                                                                                                                                                                   |
| `tick_interval`            | duration string | `"5m"`           | Interval between compaction planning cycles                                                                                                                                                    |
| `target_file_size_mb`      | integer (MB)    | `128`            | Target output file size after compaction                                                                                                                                                       |
| `file_count_threshold`     | integer         | `10`             | Minimum number of _small_ files (see below) required to trigger compaction                                                                                                                     |
| `max_input_file_size_kb`   | integer (KB)    | `65536` (64 MB)  | Maximum input file size considered for compaction. Files at or above this size are treated as already compacted and are left alone                                                             |
| `partition_lateness`       | duration string | `"10m"`          | How long an hour partition stays open for late-arriving data after its hour ends; only closed partitions are compacted                                                                         |
| `memory_limit_mb`          | integer (MB)    | `512`            | Budget for the rewrite's **DataFusion operators** (the sort above all), which spill to disk past it. Not a total: see the caveat below                                                         |
| `target_partitions`        | integer         | `1`              | DataFusion partition fan-out for the rewrite (`0` = available parallelism). Each partition sorts independently and they share `memory_limit_mb`, so raising this divides the budget            |
| `max_partition_input_mb`   | integer (MB)    | `2048`           | Upper bound on the summed size of a partition's eligible input files. Partitions above it are declined with a warning and counted, rather than attempted and failed every cycle (`0` = no cap) |
| `max_candidates_per_cycle` | integer         | `20`             | Maximum candidates processed per scheduling cycle (`0` = unlimited)                                                                                                                            |
| `max_per_tenant`           | integer         | `5`              | Maximum candidates per tenant per cycle (`0` = unlimited)                                                                                                                                      |
| `lease_ttl_seconds`        | integer         | `300`            | How long a compaction lease stays valid without renewal                                                                                                                                        |
| `metrics_addr`             | `string`        | `"0.0.0.0:9091"` | Observability HTTP endpoint (`""` = disabled)                                                                                                                                                  |

**How the cadences interact:** each lifecycle cycle runs on its own task, so
these intervals are independent of one another — a compaction pass that runs
long does not postpone retention, orphan cleanup, or the 30s stale-lease sweep
(the sweep cadence is fixed and not configurable; `lease_ttl_seconds` controls
only how long a lease survives without renewal). Two consequences worth
knowing. First, independent cadences do not mean independent access to a table:
within one compactor process, compaction commits, retention partition drops and
snapshot expiration take a per-table lock, so on any one table they take turns
rather than commit concurrently — a long rewrite defers that table's next
retention pass, and other tables are unaffected. Across _separate_ compactor
instances there is no such lock, so those commits can still race; that shows up
as Iceberg commit conflicts which both paths retry
(`compactor_conflicts_detected_total`, `compactor_retries_attempted_total`).
Compaction spends that same retry budget on transient infrastructure failures —
object store blips, network hiccups, catalog contention — so
`compactor_retries_attempted_total` can advance without conflicts; deterministic
failures (validation, schema) are not retried at all.
Second, a cycle disabled with `enabled = false` gets no task at all rather than
a task that wakes up and returns.

**How file selection works:** compaction exists to merge many small ingest files into few large ones. Only files **smaller than** `max_input_file_size_kb` count as compaction inputs; when at least `file_count_threshold` such files exist, the table becomes a candidate and its small files are rewritten toward `target_file_size_mb`. Files at or above the maximum are considered "already big" — re-reading and rewriting them buys nothing, so they never trigger compaction on their own. The default of 64 MB is half the default 128 MB target output size, which keeps freshly ingested files (typically tens to hundreds of KB) always eligible.

**How the output target is enforced:** `target_file_size_mb` bounds the real, Parquet-encoded (compressed) size of each output file, not an in-memory estimate. Before writing, compaction sets the table's `write.target-file-size-bytes` property to the configured target if it differs — a metadata-only commit, a no-op once the value already matches — so the Parquet writer's own bytes-written tracking decides where to roll to a new file. This reconciliation runs on every rewrite, so changing `target_file_size_mb` takes effect on the next compaction cycle without recreating tables.

**How compaction is scoped:** a compaction job operates on exactly one `timestamp_hour` partition and commits a **delta** — the input files are removed and the compacted outputs added in a single snapshot, leaving every other partition referenced as it was. Two consequences matter operationally:

- Write amplification is proportional to the partition being compacted, not to the table. Compacting a new hour no longer rewrites months of history.
- Concurrent ingest does not invalidate the commit. Only a change to the job's own input files (retention dropping the partition, or a second compactor) is a conflict.

Jobs are restricted to **closed** partitions: an hour partition becomes eligible once its hour has ended and `partition_lateness` has elapsed. The partition still receiving writes is exactly the one whose files would change under a running rewrite, so leaving it alone is what lets compaction and ingest coexist. Raise `partition_lateness` if your sources deliver data well after the fact; it is a late-data allowance, not a commit-cadence knob.

**Sizing a compactor's memory.** The three knobs interact, so tune them together:

```
peak job memory  ≈  memory_limit_mb  +  target_file_size_mb  +  small fixed overhead
per-sorter share  =  memory_limit_mb / max(target_partitions, 1)
```

- `memory_limit_mb` is the accounted half: DataFusion's operators spill past it.
- `target_file_size_mb` is the unaccounted half: the chunker accumulates one output file outside the pool. Keep it comfortably **below** `memory_limit_mb`, or the part the pool does not control dominates the part it does.
- The per-sorter share must stay above roughly **64 MB**. Below that a spilling sort has no room for a batch plus the reservation its spill merge needs, so it fails instead of spilling — the #1064 failure in miniature. With the default `target_partitions = 1` the share is the whole pool.

The compactor logs a warning at startup for either incoherent combination rather than refusing to start: an operator who has measured their workload may want an unusual ratio, and a background service should say so loudly rather than not run.

The defaults (512 MB pool, 128 MB target, fan-out 1) put peak job memory around 640 MB with the full pool behind one sorter. Raise `memory_limit_mb` to spill less on large partitions; lower it if the compactor shares a process with the other services (monolithic mode) and you would rather trade speed for footprint.

**Why partitions can be declined for size:** the planner gates on file _count_ and per-file size, never on the total, so a partition too large to rewrite within `memory_limit_mb` would be selected every cycle, fail after a full read-and-sort, and be selected again — spending compaction capacity entirely on work that cannot currently succeed. `max_partition_input_mb` declines those up front. A declined partition stays uncompacted until the cap is raised or the rewrite can handle it, so watch `compactor_oversized_partitions_skipped_total`: a non-zero and growing value means real work is being turned away, not that nothing needs doing.

**Failure cooldown (not configurable):** the same "do not spend capacity on work that cannot succeed" reasoning covers failures the planner cannot predict — a schema error, a corrupt input file, a rewrite that always exhausts the pool. When a compaction job fails, the scheduler suppresses that partition for 15 minutes, doubling per consecutive failure up to a 6-hour ceiling; a success clears the suppression and resets the escalation. The windows are fixed constants rather than settings. Commit conflicts do not count as failures — they mean another actor committed first and the job should be retried. Watch `compactor_cooldown_partitions_skipped_total`, and see [Operations](operations.md#compaction-backoff).

**What `memory_limit_mb` actually bounds:** the pool covers the rewrite's **DataFusion operators** — the partition sort above all — which spill to disk rather than growing past it. The rewrite streams its partition rather than collecting it, so the memory outside the pool is bounded too: the chunker holds at most one output file's worth of batches, and the attribute-statistics pass holds per-key state capped by cardinality. Neither grows with the size of the partition. Peak process memory for a job is therefore roughly the pool plus one `target_file_size_mb`, not the pool plus the whole partition.

**What the rewrite sorts by (not configurable):** the table's own declared sort order — time-leading, one key per signal (see [Storage Layout](../../architecture/storage-layout.md#declared-sort-order)). There is deliberately no compactor setting for it: the declaration is what the query engine is told about the data, so a second knob here could only make the two disagree. Output files record the order they were written in, which is how a partition of pre-declaration files becomes fully attested.

**Example:**

```toml
[compactor]
enabled = true
tick_interval = "5m"
target_file_size_mb = 128
file_count_threshold = 10
max_input_file_size_kb = 65536  # 64 MB; files >= this are left alone
partition_lateness = "10m"      # only compact hours that closed 10m ago
memory_limit_mb = 512           # rewrites spill past this instead of growing the heap
```

> **Removed setting (breaking change, issue #925):**
>
> - `[compactor.orphan_cleanup] revalidate_before_delete` no longer exists. Re-validation now runs unconditionally before any real deletion: orphan detection derives its live set from the retained snapshots' manifests and is correct on its own, so re-validation is defense-in-depth rather than the switch that made cleanup safe. A dry run skips it, since it deletes nothing. The key is silently ignored if left in a config file — these structs do not reject unknown keys — so remove it when upgrading.

> **Removed settings (breaking change, issue #934):**
>
> - `min_input_file_size_kb` was replaced by `max_input_file_size_kb` with **inverted semantics**. The old minimum-size filter excluded exactly the small ingest files compaction exists to merge, so a default deployment never compacted anything. There is no backward-compat alias; deployments setting the old key must switch to the new one.
> - `max_files_per_job` was removed. It was never enforced under the whole-table execution model that preceded partition-scoped compaction (issue #933); per-partition input caps are tracked separately.

## Retention Configuration

### `[compactor.retention]`

Controls automatic retention enforcement and partition lifecycle management.

> **Tenant scope:** retention (and compaction and orphan cleanup) applies to
> **all active tenants** — both config-defined tenants and those created via
> the admin API — because the compactor enumerates the source-agnostic tenant
> registry. An admin-API tenant with no `[[auth.tenants]]` block is still
> subject to the default 30-day retention; set overrides or disable retention
> if that is not intended.

#### Basic Settings

| Field                      | Type            | Default | Description                                  |
| -------------------------- | --------------- | ------- | -------------------------------------------- |
| `enabled`                  | `bool`          | `true`  | Enable retention enforcement (on by default) |
| `dry_run`                  | `bool`          | `false` | When `true`, log actions without executing   |
| `retention_check_interval` | duration string | `"1h"`  | Interval between retention checks            |
| `timezone`                 | `string`        | `"UTC"` | Timezone for logging (internal uses UTC)     |

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

| Field      | Type            | Default | Description            |
| ---------- | --------------- | ------- | ---------------------- |
| `traces`   | duration string | `"30d"` | Trace data retention   |
| `logs`     | duration string | `"30d"` | Log data retention     |
| `metrics`  | duration string | `"30d"` | Metric data retention  |
| `profiles` | duration string | `"30d"` | Profile data retention |

> **Warning:** Retention enforcement is enabled by default with `dry_run = false`, so a default deployment deletes data older than 30 days. To keep data indefinitely, set `[compactor.retention].enabled = false`; to keep it longer, raise the per-signal durations or use tenant/dataset overrides.

**Example:**

```toml
[compactor.retention]
traces = "7d"
logs = "30d"
metrics = "90d"
profiles = "14d"
```

**Signal Type Mapping:**

- `traces` → `traces` table
- `logs` → `logs` table
- `metrics` → any table whose name starts with `metrics_` (`metrics_gauge`, `metrics_sum`, `metrics_histogram` by default)
- `profiles` → `profiles` table

This mapping is the single predicate deciding which catalog tables the
lifecycle owns. A table it does not classify gets no retention, no snapshot
expiration, and no orphan cleanup — which is how `profiles` accumulated an
unbounded metadata backlog before [#1014](https://github.com/cedricziel/signaldb/issues/1014).

#### Safety Settings

| Field               | Type               | Default | Description                           |
| ------------------- | ------------------ | ------- | ------------------------------------- |
| `grace_period`      | duration string    | `"1h"`  | Safety margin before cutoff           |
| `snapshots_to_keep` | `usize` (optional) | `10`    | Minimum snapshots to retain per table |

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

Per-file deletion lines are logged at `DEBUG` (see [Operations](operations.md#enabling-orphan-cleanup)); the per-batch and per-run summaries at `INFO`.

### `[compactor.orphan_cleanup]`

Controls automatic detection and deletion of orphaned files: data
Parquet no retained snapshot references, and unreferenced metadata files
(metadata.json versions outside the metadata-log, manifest lists and
manifests of expired snapshots).

#### Basic Settings

| Field                    | Type   | Default | Description                               |
| ------------------------ | ------ | ------- | ----------------------------------------- |
| `enabled`                | `bool` | `true`  | Enable orphan cleanup (on by default)     |
| `dry_run`                | `bool` | `false` | When `true`, log orphans without deleting |
| `cleanup_interval_hours` | `u64`  | `24`    | Interval between cleanup runs (hours)     |

**Example:**

```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = false
cleanup_interval_hours = 24  # Run once per day
```

#### Safety Settings

| Field                | Type  | Default | Description                                  |
| -------------------- | ----- | ------- | -------------------------------------------- |
| `grace_period_hours` | `u64` | `24`    | Don't delete files younger than this (hours) |

**Example:**

```toml
[compactor.orphan_cleanup]
grace_period_hours = 48          # 2-day grace period
```

The live-file set is the union of every snapshot still retained in table
metadata — there is no snapshot-age window. Snapshot expiration (see
`[compactor.retention].snapshots_to_keep`) is what makes files eligible
for cleanup.

**Safety Mechanism Explained:**

1. **Grace Period:** Files younger than `grace_period_hours` are never deleted, even if orphaned.
   - Protects against in-flight writes
   - Prevents race conditions with compaction
   - Default 24 hours is conservative

2. **Revalidation:** Before deleting, re-check if file is still orphaned.
   - Catches concurrent writes that referenced the file
   - Adds ~10% overhead but prevents data loss
   - Not configurable: runs before every real deletion batch. A dry run skips it, since it deletes nothing

3. **Retained-Snapshot Live Set:** The reference set is the union of every snapshot still retained in table metadata; snapshot expiration is what makes files eligible for cleanup.
   - Reduces memory usage for tables with many snapshots
   - Files referenced by older snapshots may be incorrectly identified as orphans
   - Should be larger than your longest query duration

#### Performance Settings

| Field                      | Type    | Default  | Description                                                                                                                                                                |
| -------------------------- | ------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `batch_size`               | `usize` | `1000`   | Files to process per batch                                                                                                                                                 |
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
- **What the cap actually bounds**: detection holds one 64-bit fingerprint per live file plus one entry per orphan candidate — it does not hold the object-store listing or the decoded manifest entries, and a manifest shared by several retained snapshots is read once. At the default cap a table therefore costs single-digit megabytes, so raising it is reasonable on a compactor with memory to spare; see [How detection scales](operations.md#how-detection-scales)

### Complete Orphan Cleanup Example

```toml
[compactor.orphan_cleanup]
# Basic settings
enabled = true
dry_run = false
cleanup_interval_hours = 24

# Safety (conservative defaults)
grace_period_hours = 24

# Performance
batch_size = 1000
max_live_files_threshold = 500000

# Example: More aggressive cleanup for dev
# [compactor.orphan_cleanup]
# grace_period_hours = 1          # 1 hour grace period
# cleanup_interval_hours = 1      # Run every hour
```

## Attribute Promotion Configuration

### `[compactor.attr_promotion]`

Attribute auto-promotion (epic #737) turns frequently queried attribute keys into materialized `label_<key>` columns at compaction time. Every rewrite already runs a read-only attribute-statistics pass; when this section is enabled, a decision pass scores the persisted statistics (query demand x row presence) against guardrails and — with `dry_run = false` — acts on the result during the same rewrite.

```toml
[compactor.attr_promotion]
enabled = true
dry_run = true    # observe decisions first; set false to act on them
max_labels_per_table = 32
min_presence = 0.005
min_query_hits = 1
promote_streak = 3
max_promotions_per_cycle = 4
```

| Setting                    | Type    | Default | Description                                                                                                                       |
| -------------------------- | ------- | ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`                  | boolean | `false` | Run the promotion decision pass on each rewrite                                                                                   |
| `dry_run`                  | boolean | `true`  | Log decisions only; never change schemas or data                                                                                  |
| `max_labels_per_table`     | integer | `32`    | Schema-width budget: maximum materialized `label_<key>` columns per table, pinned `[schema.materialized_labels]` entries included |
| `min_presence`             | float   | `0.005` | Minimum fraction of rows a key must appear in to be promotable                                                                    |
| `min_query_hits`           | integer | `1`     | Minimum accumulated query-demand hits for a key to be promotable                                                                  |
| `promote_streak`           | integer | `3`     | Consecutive over-threshold cycles before promotion (hysteresis)                                                                   |
| `max_promotions_per_cycle` | integer | `4`     | Maximum promotions per rewrite cycle                                                                                              |

**`dry_run` semantics:**

- `dry_run = true` (default): the pass only logs an `Attribute promotion decision` line per table. No schema or data changes.
- `dry_run = false`: the compactor **acts** on promote decisions at the next rewrite of each table. It evolves the table schema (adds the promoted columns through a metadata-only commit), backfills the column values from the attributes map while rewriting the files, and commits the rewrite through the normal replace path. See the [operations guide](operations.md#attribute-promotion) for the observable sequence.

The guardrails live in the decision engine and apply in both modes: machine-generated keys (embedded UUIDs, long hex or digit runs) are never promoted, keys whose distinct-value tracking hit the analyzer cap are rejected, a key must qualify for `promote_streak` consecutive cycles, and the schema-width budget caps the total number of label columns. Pinned `[schema.materialized_labels]` entries are never demoted or otherwise touched. Demotion (dropping unqueried auto-promoted columns) is decided and logged but not yet acted on.

**Recommendation:** run with `dry_run = true` for several compaction cycles and review the `Attribute promotion decision` log lines. Flip to `false` only once the keys they announce are ones you want as columns.

## Environment Variables

All scalar configuration can be overridden via environment variables.

### Naming Convention

Nested compactor keys use the double-underscore form: `SIGNALDB__` prefix, with `__` between each nesting level:

```
SIGNALDB__COMPACTOR__<SECTION>__<FIELD>
```

### Compaction Environment Variables

```bash
SIGNALDB__COMPACTOR__ENABLED=true
SIGNALDB__COMPACTOR__TICK_INTERVAL=5m
SIGNALDB__COMPACTOR__TARGET_FILE_SIZE_MB=128
SIGNALDB__COMPACTOR__FILE_COUNT_THRESHOLD=10
SIGNALDB__COMPACTOR__MAX_INPUT_FILE_SIZE_KB=65536
SIGNALDB__COMPACTOR__PARTITION_LATENESS=10m
SIGNALDB__COMPACTOR__MEMORY_LIMIT_MB=512
```

`SIGNALDB__COMPACTOR__MIN_INPUT_FILE_SIZE_KB` and `SIGNALDB__COMPACTOR__MAX_FILES_PER_JOB` no longer exist (see [Compaction Settings](#compaction-settings)).

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
SIGNALDB__COMPACTOR__RETENTION__PROFILES=14d
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
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_SNAPSHOT_AGE_HOURS=720
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__BATCH_SIZE=1000
SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_LIVE_FILES_THRESHOLD=500000
```

### Example: Docker Compose

```yaml
version: "3.8"
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
batch_size = 500  # Smaller batches for safety
```

## Validation Rules

Retention configuration is validated at startup: `RetentionConfig::validate` runs when the retention enforcer is constructed, and invalid retention configuration causes startup to fail with an error message.

### Retention Validation

- `traces`, `logs`, `metrics`, and `profiles` retention durations must be positive (non-zero) — globally and in every tenant/dataset override
- `grace_period` must not be negative

No other retention fields are validated; there are no enforced value ranges, and `retention_check_interval` is not checked.

### Orphan Cleanup Validation

Orphan-cleanup values are currently **not** validated at startup. `OrphanCleanupConfig::validate` exists (it requires `cleanup_interval_hours`, `grace_period_hours`, and `batch_size` to all be > 0) but is not wired into the compactor's startup path, so invalid values — for example `grace_period_hours = 0` — are accepted and take effect as written. Review these values carefully before deploying.

### Validation Errors

Example retention error messages:

```
Invalid retention period for traces: 0ns must be positive
Invalid retention configuration for tenant 'acme': Invalid retention period for logs: 0ns must be positive
```

## Additional Resources

- [Operations Guide](operations.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Compactor README](https://github.com/cedricziel/signaldb/blob/main/src/compactor/README.md)

> Note: every compaction rewrite also runs a read-only attribute-statistics pass that logs per-key presence, approximate cardinality, and advisory materialization candidates (`Attribute-stats analyzer` log line), and persists the per-key statistics to the service catalog's `attribute_stats` table (joined there with query-demand counters flushed by the querier). This statistics pass requires no configuration and changes no table data. The promotion decision pass built on those statistics is configured via [`[compactor.attr_promotion]`](#attribute-promotion-configuration).
