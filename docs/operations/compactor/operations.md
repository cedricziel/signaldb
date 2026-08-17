---
audience: operator
type: how-to
status: living
sources:
  - src/compactor/src/**
---

# Compactor Operations Guide

This guide covers day-to-day operations for SignalDB Compactor retention and lifecycle management (retention enforcement, snapshot expiration, and orphan-file cleanup).

## Table of Contents

- [Overview](#overview)
- [Enabling Retention Enforcement](#enabling-retention-enforcement)
- [Enabling Orphan Cleanup](#enabling-orphan-cleanup)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Common Operations](#common-operations)
- [Emergency Procedures](#emergency-procedures)
- [Performance Tuning](#performance-tuning)
- [Attribute Promotion](#attribute-promotion)

## Overview

The compactor provides automatic data lifecycle management through:

1. **Compaction**: Merges many small ingest files into fewer large ones
2. **Retention Enforcement**: Drops expired partitions based on configurable policies
3. **Snapshot Expiration**: Maintains bounded metadata by expiring old snapshots
4. **Orphan Cleanup**: Reclaims storage by deleting unreferenced files

Compaction, retention and snapshot expiration are Iceberg metadata commits and respect its transactional guarantees and snapshot isolation. Orphan cleanup is different in kind: it deletes objects from storage outside any commit. It is snapshot-aware (a file is a candidate only when no retained snapshot references it), bounded by a grace period, and re-validated against a freshly rebuilt live set immediately before each deletion batch — but it is not atomic, and a deletion cannot be rolled back by a snapshot.

Within one compactor process, the three committing actors take turns per table: compaction, retention partition drops, and snapshot expiration each acquire that table's lock before doing their work, so they cannot interleave on the same table. Different tables never wait on each other. This covers both ways compaction is triggered — the background cycle and the `compact_now` Flight action, which an admin can fire at any moment — so a manually triggered compaction cannot land in the middle of a retention pass. It is an in-process ordering only: across multiple compactor instances, safety still rests on Iceberg catalog CAS and on compaction validating that its own input files are still live at commit time. The practical consequence to know about is that a long rewrite delays that table's next retention pass until it finishes; the pass is deferred, never skipped.

> **Compaction is partition-scoped.** A job operates on exactly one closed
> `timestamp_hour` partition and commits a _delta_ — its input files are removed
> and the compacted outputs added in a single snapshot, leaving every other
> partition referenced as it was. Two consequences matter operationally: cost is
> proportional to the partition being compacted rather than to the table, and
> concurrent ingest does not invalidate the commit. A partition becomes eligible
> once its hour has ended and `[compactor] partition_lateness` (default `10m`)
> has elapsed — the partition still receiving writes is the one whose files
> would change under a running rewrite, so it is deliberately left alone.
> Rewrites run their **DataFusion operators** under a `[compactor]
memory_limit_mb` budget (default 512 MB), spilling to disk past it. The
> rewrite streams the partition in two passes rather than collecting it, so
> what sits outside that budget is bounded by one output file rather than by
> the partition. The rewrite sorts with a fan-out of `[compactor]
target_partitions` (default `1`): the sorters share the one budget, so
> raising the fan-out divides it and can exhaust the pool on concurrency
> alone. The compactor warns at startup when these settings cannot work
> together — a target file size at or above the pool, or a per-sorter share
> too small for a spilling sort to use — since neither is visible from any
> single value.
>
> **What the rewrite sorts by:** the table's own declared sort order (see
> [Storage Layout](../../architecture/storage-layout.md#declared-sort-order)),
> not a key list held by the compactor. Its output files record that order, so
> a partition that held files written before the declaration existed comes out
> fully attested — compaction is how such files converge, and there is no
> backfill job. A table that has no declaration yet is still sorted by the
> canonical key, but its output is written unattested, since there is no
> declared order for it to claim.
>
> **Default behavior:** The compactor and retention enforcement are **enabled by default** with `dry_run = false` and a 30-day retention period for traces, logs, metrics, and profiles. A default deployment deletes data older than 30 days. To keep data indefinitely, set `[compactor.retention].enabled = false`; to keep it longer, raise the per-signal durations. Orphan cleanup is also **enabled by default** with `dry_run = false` and physically reclaims files no retained snapshot references — data Parquet and unreferenced metadata files (old metadata.json versions, expired snapshots' manifests) alike; set `[compactor.orphan_cleanup].enabled = false` to opt out or `dry_run = true` to observe first.

## Enabling Retention Enforcement

### Step 1: Plan Your Retention Policies

Determine appropriate retention periods for each signal type:

| Signal Type | Typical Retention | Production Example            |
| ----------- | ----------------- | ----------------------------- |
| Traces      | 7-30 days         | 7 days (dev), 30 days (prod)  |
| Logs        | 3-14 days         | 3 days (dev), 7 days (prod)   |
| Metrics     | 30-90 days        | 30 days (dev), 90 days (prod) |

Consider:

- Regulatory requirements (GDPR, HIPAA, etc.)
- Storage costs vs. query needs
- Incident investigation timeframes
- Audit requirements

### Step 2: Configure Retention Policies

Edit `signaldb.toml`:

```toml
[compactor.retention]
enabled = true
dry_run = true  # Start with dry-run mode
retention_check_interval = "1h"  # Check every hour

# Global defaults
traces = "7d"
logs = "30d"
metrics = "90d"

# Safety settings
grace_period = "1h"      # Safety margin
timezone = "UTC"         # For logging
snapshots_to_keep = 10   # Keep last 10 snapshots
```

### Step 3: Test with Dry-Run Mode

Start the compactor with dry-run enabled:

```bash
# Start compactor (logs to stdout; redirect to a file if you want to tail it)
cargo run --bin signaldb -- compactor 2>&1 | tee compactor.log

# Or in monolithic mode via the dev script (logs to .data/logs/monolithic.log)
./scripts/run-dev.sh

# Monitor logs. Note: retention logs use "[DRY RUN]" (space), orphan
# cleanup logs use "[DRY-RUN]" (hyphen) — match both:
tail -f .data/logs/monolithic.log | grep -E "DRY.RUN"
```

Look for log entries like:

```text
INFO compactor::retention::enforcer: [DRY RUN] Would drop expired partitions signaldb.tenant.id=acme signaldb.dataset.id=prod signaldb.table=traces signaldb.job.dry_run=true signaldb.job.partitions_dropped=48 signaldb.job.bytes_reclaimed=1073741824
DEBUG compactor::retention::enforcer: [DRY RUN] Would drop partition tenant_id=acme dataset_id=prod table_name=traces partition_hour=Some("492245") file_count=12 size_bytes=Some(10485760)
```

The per-partition breakdown is debug-level; run with
`RUST_LOG=info,compactor::retention=debug` to see it.

**Validate:**

- Partitions identified for deletion are expected
- Cutoff timestamps are correct
- No unexpected data would be deleted

### Step 4: Enable for Test Environment

Once dry-run looks good, enable for a test tenant:

```toml
[compactor.retention]
enabled = true
dry_run = false  # Enable actual deletion

# Use short retention for testing
traces = "1d"  # 1 day for fast testing

[compactor.retention.tenant_overrides.test]
traces = "1d"
logs = "1d"
metrics = "1d"
```

Restart the compactor and verify. Signal data is stored in Iceberg tables on the object store (not in PostgreSQL), so use the compactor's observability endpoint and log output:

```bash
# Record the retention counters before the cycle
curl -s localhost:9091/status | jq .retention

# Wait for retention cycle (check interval + processing time)
# Typically 1-5 minutes

# Verify partitions were dropped
curl -s localhost:9091/status | jq .retention
curl -s localhost:9091/metrics | grep compactor_partitions_dropped_total

# Check the drop logs
grep "Dropped expired partitions" .data/logs/monolithic.log

# Confirm old data is gone by querying through the router
# (Tempo search API; expired time ranges should return no results)
curl -s "http://localhost:3000/api/search?start=<old-unix-ts>&end=<old-unix-ts>" \
  -H "Authorization: Bearer <api-key>"
```

### Step 5: Rollout to Production

After successful test tenant validation:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval = "1h"

# Production retention periods
traces = "7d"
logs = "30d"
metrics = "90d"

# Production tenant overrides
[compactor.retention.tenant_overrides.production]
traces = "30d"  # Keep production traces longer
logs = "7d"
metrics = "90d"

# Critical dataset overrides
[compactor.retention.tenant_overrides.production.dataset_overrides.critical]
traces = "90d"  # Critical data kept 90 days
```

**Rollout Checklist:**

- [ ] Dry-run validation completed
- [ ] Test tenant validation successful
- [ ] Retention periods reviewed and approved
- [ ] Monitoring and alerts configured
- [ ] Backup/restore procedures verified
- [ ] Stakeholders notified

## Enabling Orphan Cleanup

### Step 1: Identify Orphan Files (Dry-Run)

Enable orphan cleanup in dry-run mode:

```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = true  # Don't delete, just identify
grace_period_hours = 24
cleanup_interval_hours = 24
batch_size = 1000
```

Start the compactor and monitor its stdout (or `.data/logs/monolithic.log` when using `./scripts/run-dev.sh`):

```bash
tail -f .data/logs/monolithic.log | grep -E "(orphan|cleanup)"
```

Look for:

```text
INFO compactor::orphan::detector: Starting orphan detection tenant_id=acme dataset_id=prod table_name=traces
INFO compactor::orphan::detector: Identified orphan candidates tenant_id=acme dataset_id=prod table_name=traces orphan_candidates=42
INFO compactor::orphan::cleaner: Starting batch deletion of orphan files signaldb.job.candidates=42 signaldb.job.dry_run=true signaldb.job.batch_size=100
INFO compactor::orphan::cleaner: Batch deletion complete signaldb.job.files_deleted=42 signaldb.job.bytes_reclaimed=2147483648 signaldb.job.deletion_failures=0 signaldb.job.dry_run=true
```

The per-file lines (`[DRY-RUN] Would delete orphan file …` for each dry-run
candidate, `Deleted orphan file …` for each successful deletion, with `path`,
`size_bytes`, `table`) are logged at `DEBUG` — a backlog run can delete tens
of thousands of files in one pass, which would otherwise flood the log at
startup. Failed deletions still log `Failed to delete orphan file` at
`ERROR`. Enable the per-file lines with
`RUST_LOG=compactor::orphan::cleaner=debug` when auditing individual
deletions; the per-batch and per-run summaries above stay at `INFO`.

**Validate:**

- Orphan count seems reasonable (expect 0-5% of total files)
- Ages are all beyond grace period (24+ hours)
- No recently modified files flagged

### Step 2: Enable Cleanup

After validating orphan identification:

```toml
[compactor.orphan_cleanup]
enabled = true
dry_run = false  # Enable actual deletion
grace_period_hours = 24
cleanup_interval_hours = 24
batch_size = 1000
```

Restart and monitor:

```bash
# Monitor deletion progress
watch -n 5 'curl -s localhost:9091/metrics | grep compactor_files_deleted_total'

# Check logs for errors (stdout, or monolithic.log with run-dev.sh)
tail -f .data/logs/monolithic.log | grep -E "(ERROR|Failed to delete)"
```

### Step 3: Verify Storage Reclamation

After cleanup runs:

```bash
# Check metrics
curl -s localhost:9091/metrics | grep -E "compactor_(orphan_candidates_identified|files_deleted|bytes_freed)"

# Example output (counters are process-global, no per-tenant labels):
# compactor_orphan_candidates_identified_total 42
# compactor_files_deleted_total 42
# compactor_bytes_freed_total 2147483648
```

**Validation:**

- `compactor_files_deleted_total` should equal `compactor_orphan_candidates_identified_total`
- `compactor_bytes_freed_total` shows actual storage reclaimed
- No deletion failures (`compactor_deletion_failures_total` = 0)

## Monitoring and Metrics

### Key Metrics to Monitor

All lifecycle counters are exported at `localhost:9091/metrics` (see `src/compactor/src/http.rs` for the authoritative list). Counters are process-global — there are no per-tenant, per-dataset, or per-table labels. The labelled metrics are `compactor_orphan_cleanup_skipped_total{reason="live_files_threshold_exceeded"}` and the `cycle="compaction"|"lease_expiry"|"retention"|"orphan_cleanup"` label on `compactor_cycle_panics_total` / `compactor_cycle_down` (see [Lifecycle Task Recovery](#lifecycle-task-recovery) below).

#### Compaction Retries

A failed compaction attempt is classified as `conflict` (a lost
optimistic-concurrency race), `transient` (object store blip, network hiccup,
catalog contention), or `terminal` (validation, schema, malformed input).
Conflicts and transient failures are retried with exponential backoff;
terminal failures fail on the first attempt, because retrying repeats the whole
rewrite to reach the same error.

```promql
# Retries cover both conflicts and transient infrastructure failures
increase(compactor_retries_attempted_total[1h])

# How much of that is contention specifically
increase(compactor_conflicts_detected_total[1h])
```

Retries far in excess of conflicts mean infrastructure flakiness rather than
contention. The `error_class` field on each job's failure log says which class
a given failure was.

#### Lease Recovery

**Stale Leases Expired:**

A partition whose compactor instance crashed stays unclaimable until its lease
is expired. The lease-expiry task sweeps every 30s on its own task, so this
counter keeps advancing even while a long compaction cycle is in flight.

```promql
# Leases reclaimed from crashed instances (last 24h)
increase(compactor_stale_leases_expired_total[24h])
```

The counter says a lease reached its expiry without a successful renewal; it
does not say why. Before touching `lease_ttl_seconds`, rule out the causes in
order of likelihood: instances dying mid-compaction (restarts, OOM kills),
renewal calls failing against the catalog, catalog or network latency
swallowing the `ttl / 3` renewal window, and process pauses (long GC-like
stalls, suspended containers). A TTL that is simply too short for your job
durations is the last of these, not the first.

#### Lifecycle Task Recovery

Each of the four lifecycle tasks (compaction, lease expiry, retention, orphan
cleanup) guards its own iterations against panics via `catch_unwind`: a panic
is caught, counted, and the task retries on its normal cadence plus a short
exponential backoff, rather than the task ending permanently. This closes the
same class of failure #1011 fixed for slow cycles — a bug in one cycle no
longer takes that cycle down for good.

> `catch_unwind` requires an unwinding panic strategy. This workspace's
> `[profile.release]` sets `panic = "abort"`, so **in a release build a
> lifecycle-cycle panic still aborts the whole compactor process** — the
> guard is exercised by `cargo test` (default `unwind` strategy) but is not
> yet load-bearing in a production binary. If you rely on this recovery
> behavior, build with `panic = "unwind"` instead.

```promql
# Panics recovered per cycle (last 24h) — should normally be flat at 0
increase(compactor_cycle_panics_total[24h])

# Is any cycle currently in its post-panic backoff?
compactor_cycle_down
```

`compactor_cycle_down{cycle="..."}` is `1` only while that cycle sits in its
post-panic backoff window; it clears as soon as the backoff ends and the
cycle resumes retrying, so it does not stay latched after a one-off failure.

`/health` deliberately does **not** reflect this state — it is a pure
liveness probe (`200 "ok"` whenever the process is serving requests) so that
a cycle recovering on its own backoff schedule never causes a container
orchestrator to restart the process mid-recovery, which would abort the
in-flight retry and turn a bounded backoff into a crash-restart loop. Watch
`compactor_cycle_down` and `/status` for the actual per-cycle state instead:

```bash
curl -s localhost:9091/status | jq '.lifecycle'
```

**Alerting:** any nonzero `increase(compactor_cycle_panics_total[1h])` is
worth paging on — a healthy compactor should show zero. A cycle that keeps
reappearing in `compactor_cycle_down` indicates a persistent bug rather than
a transient failure; check the logs for the cycle's name and panic message
(`tracing::error!` logs it on every recovery) before assuming a restart will
fix it.

#### Retention Enforcement

**Partitions Dropped:**

```promql
# Partitions dropped (last 24h)
increase(compactor_partitions_dropped_total[24h])

# Partitions evaluated vs dropped
increase(compactor_partitions_evaluated_total[24h])
```

**Retention Duration:**

```promql
# Wall-clock milliseconds spent enforcing retention per second
rate(compactor_retention_duration_ms_total[5m])
```

**Bytes Reclaimed by Retention:**

```promql
increase(compactor_bytes_reclaimed_total[24h])
```

**Unclassifiable Files:**

Data files whose `timestamp_hour` partition value could not be determined
from the manifest entry (or the legacy file-path fallback). Such files are
kept and excluded from retention, so a non-zero value means some data is
never expired — investigate the table's manifests.

```promql
# Should be 0; alert if it grows
increase(compactor_unclassifiable_files_total[24h])
```

#### Compaction Backoff

**Partitions Skipped While Cooling Down:**

When a compaction job fails, the scheduler suppresses that partition for 15
minutes, doubling on each consecutive failure up to a 6-hour ceiling. A
success clears the suppression and resets the escalation. Commit conflicts are
not failures for this purpose — they mean another actor committed first and the
job should be retried, not backed off.

The counter increments once per partition per cycle that is withheld, so it
climbs steadily while a partition is stuck rather than reporting a single
event.

```promql
# Steady growth means some partition cannot be compacted at all
increase(compactor_cooldown_partitions_skipped_total[1h])
```

A non-zero value is not itself an error: it is the compactor declining to spend
capacity on work that just failed. A value that never returns to zero means a
partition is permanently stuck — find it in the logs, which name the tenant,
dataset, table, partition, and consecutive failure count at each skip:

```bash
journalctl -u signaldb-compactor | grep "cooling down"
```

#### Orphan Cleanup

**Storage Reclaimed:**

```promql
# Total storage freed (last 24h)
increase(compactor_bytes_freed_total[24h])

# Storage reclaimed rate (bytes/second)
rate(compactor_bytes_freed_total[5m])
```

**Cleanup Success Rate:**

```promql
# Success rate (should be ~100%)
sum(rate(compactor_files_deleted_total[5m]))
/
sum(rate(compactor_orphan_candidates_identified_total[5m]))
```

**Deletion Failures:**

```promql
# Should be 0 or very low
increase(compactor_deletion_failures_total[1h])
```

**Skipped Cleanups:**

```promql
# Cleanup runs skipped because the live-file estimate exceeded
# max_live_files_threshold
increase(compactor_orphan_cleanup_skipped_total[24h])
```

### Recommended Alerts

#### Critical Alerts

**High Deletion Failure Rate:**

```yaml
alert: CompactorHighDeletionFailureRate
expr: |
  rate(compactor_deletion_failures_total[5m]) > 0.01
for: 10m
labels:
  severity: critical
annotations:
  summary: "Compactor orphan deletion failures"
  description: "{{ $value }} deletion failures/sec"
```

**Retention Enforcement Stuck:**

```yaml
# No last-run timestamp metric exists; alert on the cutoff-computation
# counter stalling instead (it increments on every retention cycle).
alert: CompactorRetentionStuck
expr: |
  increase(compactor_retention_cutoffs_computed_total[2h]) == 0
for: 15m
labels:
  severity: critical
annotations:
  summary: "Compactor retention hasn't run in 2 hours"
```

#### Warning Alerts

**High Orphan Rate:**

```yaml
alert: CompactorHighOrphanRate
expr: |
  increase(compactor_orphan_candidates_identified_total[24h]) > 10000
for: 1h
labels:
  severity: warning
annotations:
  summary: "Unusually many orphan file candidates"
  description: "{{ $value }} orphan candidates identified in 24h"
```

**Orphan Cleanup Skipped:**

```yaml
alert: CompactorOrphanCleanupSkipped
expr: |
  increase(compactor_orphan_cleanup_skipped_total[24h]) > 0
for: 1h
labels:
  severity: warning
annotations:
  summary: "Orphan cleanup skipped (live-file threshold exceeded)"
```

### Grafana Dashboard

Example dashboard queries:

**Panel: Storage Reclaimed (Bytes)**

```promql
# Orphan cleanup
increase(compactor_bytes_freed_total[24h])
# Retention enforcement
increase(compactor_bytes_reclaimed_total[24h])
```

**Panel: Partitions Dropped Over Time**

```promql
rate(compactor_partitions_dropped_total[5m]) * 300
```

**Panel: Retention Duration**

```promql
rate(compactor_retention_duration_ms_total[5m])
```

## Common Operations

### Adjusting Retention Periods

To change retention for a tenant:

1. **Update Configuration:**

```toml
[compactor.retention.tenant_overrides.production]
traces = "14d"  # Changed from 30d to 14d
```

2. **Reload Configuration:**

```bash
# Restart compactor (graceful)
pkill -TERM compactor
cargo run --bin signaldb -- compactor

# Or restart monolithic service
systemctl restart signaldb
```

3. **Monitor Next Retention Cycle:**

```bash
# Wait for next retention check (check interval)
# Monitor logs for new cutoff (stdout, or monolithic.log with run-dev.sh).
# The cutoff line is debug-level: run with RUST_LOG=info,compactor::retention=debug
tail -f .data/logs/monolithic.log | grep "Retention cutoff computed"

# Expected log:
# DEBUG compactor::retention::enforcer: Retention cutoff computed tenant_id=production dataset_id=default table_name=traces cutoff_timestamp=2026-01-26 10:00:00 UTC retention_period=14d source=Tenant
```

### Force Immediate Retention Check

To trigger retention enforcement immediately (without waiting for interval):

```bash
# Option 1: Restart compactor (runs on startup)
systemctl restart signaldb-compactor

# Option 2: Send SIGUSR1 signal (if implemented)
pkill -USR1 compactor

# Option 3: Temporarily reduce interval
# In signaldb.toml:
# retention_check_interval = "1m"
# Then restart
```

### Force Immediate Orphan Cleanup

To trigger orphan cleanup immediately:

```bash
# Restart compactor (cleanup runs on startup)
systemctl restart signaldb-compactor

# Monitor progress (journalctl for systemd units; stdout otherwise)
journalctl -u signaldb-compactor -f | grep orphan
```

### Verify Retention Cutoff Computation

To check what the current retention cutoff would be:

```bash
# Enable debug logging (logs go to stdout)
RUST_LOG=debug,compactor::retention=trace cargo run --bin signaldb -- compactor 2>&1 | \
  grep "Retention cutoff computed"

# Example output:
# DEBUG compactor::retention::enforcer: Retention cutoff computed tenant_id=acme dataset_id=prod table_name=traces cutoff_timestamp=2026-01-25 09:00:00 UTC retention_period=7d source=Global
```

### Inspect Orphan Candidates

To see what files would be identified as orphans (without deleting):

```bash
# Enable dry-run mode
# In signaldb.toml:
[compactor.orphan_cleanup]
enabled = true
dry_run = true

# Restart and check logs (stdout, or monolithic.log with run-dev.sh)
tail -f .data/logs/monolithic.log | grep "DRY-RUN.*Would delete"

# Example output:
# INFO compactor::orphan::cleaner: [DRY-RUN] Would delete orphan file path=acme/prod/traces/data/orphan-001.parquet size_bytes=10485760 last_modified=2026-02-04T10:00:00Z table=acme/prod/traces
```

### Check Storage Utilization

Signal data lives on the object store, not in the SQL catalog (the catalog's `iceberg_tables` table only maps table names to metadata locations). Inspect the object store directly:

```bash
# Local filesystem storage
du -sh .data/storage
find .data/storage -name "*.parquet" | wc -l

# Per-tenant/dataset/table breakdown (paths are {tenant}/{dataset}/{table}/...)
du -sh .data/storage/*/*/*

# Check object store directly (S3 example)
aws s3 ls s3://signaldb-data/ --recursive --summarize | grep "Total Size"

# Bytes reclaimed by retention and cleanup so far
curl -s localhost:9091/metrics | grep -E "compactor_bytes_(freed|reclaimed)_total"
```

## Emergency Procedures

### Emergency: Stop All Retention Operations

If retention is deleting unexpected data:

```bash
# Option 1: Disable in config and restart
# In signaldb.toml:
[compactor.retention]
enabled = false

systemctl restart signaldb-compactor

# Option 2: Stop compactor immediately
systemctl stop signaldb-compactor
# or
pkill -KILL compactor
```

### Emergency: Stop Orphan Cleanup

If orphan cleanup is deleting live files (should never happen with proper grace period):

```bash
# Disable orphan cleanup
# In signaldb.toml:
[compactor.orphan_cleanup]
enabled = false

systemctl restart signaldb-compactor
```

**Investigate** (against compactor stdout, journalctl, or `.data/logs/monolithic.log`):

```bash
# Check revalidation logs
grep -i "revalidation" .data/logs/monolithic.log
# e.g. "File no longer orphan after revalidation, skipping deletion"
#      "Revalidation failed, skipping file for safety"

# Verify grace period is filtering recent files
grep "within grace period" .data/logs/monolithic.log
# e.g. "Skipping recent file (within grace period)"
```

### Emergency: Restore Accidentally Deleted Data

Retention enforcement drops partitions as Iceberg commits, so the pre-drop snapshot survives until snapshot expiration removes it and orphan cleanup deletes the underlying files. However, **SignalDB currently has no supported restore path**: snapshot metadata is not queryable via SQL (there is no `iceberg_snapshots` table in the catalog database), and time-travel queries (`FOR SYSTEM_TIME AS OF`) are not supported by the query path.

1. **Stop the Compactor Immediately** so snapshot expiration and orphan cleanup cannot delete the files still referenced by the pre-drop snapshot:

```bash
systemctl stop signaldb-compactor
```

2. **Locate the Pre-Drop Snapshot in the Iceberg Metadata (object store):**

```bash
# Local filesystem storage: metadata JSON lives next to the table data
ls .data/storage/<tenant>/<dataset>/traces/metadata/

# Inspect the snapshot list (snapshot-id, timestamp-ms, summary)
jq '.snapshots[] | {snapshot_id: ."snapshot-id", timestamp_ms: ."timestamp-ms", summary}' \
  .data/storage/<tenant>/<dataset>/traces/metadata/<latest>.metadata.json
```

3. **Restore:** rolling the table back to the pre-drop snapshot requires manual Iceberg surgery with external Iceberg tooling; it is not currently possible through SignalDB itself.

**Prevention:**

- Always test retention in dry-run mode first
- Use test tenants before production rollout
- Keep `snapshots_to_keep` high enough for recovery window
- Monitor `compactor_partitions_dropped_total` for unexpected spikes

## Performance Tuning

### Retention Enforcement Performance

**Symptoms:**

- Retention checks taking too long (> 5 minutes)
- High CPU usage during retention cycles

**Tuning Options:**

```toml
[compactor.retention]
# Increase check interval (less frequent = less overhead)
retention_check_interval = "2h"

# Reduce snapshots to keep (less metadata to process)
snapshots_to_keep = 3
```

### Orphan Cleanup Performance

**Symptoms:**

- Cleanup taking hours to complete
- High memory usage during cleanup
- Object store rate limiting

**Tuning Options:**

```toml
[compactor.orphan_cleanup]
# Reduce batch size (less memory, more checkpoints)
batch_size = 500

# Run less frequently (lower peak load)
cleanup_interval_hours = 48  # Every 2 days

# Keep fewer snapshots retained (less history to scan)
[compactor.retention]
snapshots_to_keep = 5

# Bound memory by skipping tables with too many estimated live files
# (skips are counted in compactor_orphan_cleanup_skipped_total)
max_live_files_threshold = 500000
```

#### How detection scales

Detection cost per table is bounded by three quantities, none of which grows
with the number of snapshots that reference the same file:

| Resource                       | Cost                                                                 | Notes                                                                                                                                                                     |
| ------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Manifest-list reads            | one per retained snapshot                                            | `snapshots_to_keep` is the lever                                                                                                                                          |
| Manifest file reads            | one per _distinct_ manifest                                          | manifests shared by several retained snapshots are deduplicated by path before any of them is fetched, so a manifest referenced by all 14 retained snapshots is read once |
| Manifest entries in memory     | one manifest's worth                                                 | entries are streamed; only the live path is inspected, never collected                                                                                                    |
| Live set in memory             | one 64-bit fingerprint per live file (≈16 bytes per hash-table slot) | a 500k-file table costs single-digit MB, not the ~75 MB the same set of path strings would                                                                                |
| Object-store listing in memory | zero                                                                 | the `data/` (and `metadata/`) listing is streamed and each entry is decided as it arrives                                                                                 |
| Candidates in memory           | one entry per orphan candidate                                       | this, plus the live set, is the working set of a cleanup pass                                                                                                             |

Consequences for tuning:

- Peak memory tracks the **live file count** and the **orphan count**, not the
  total object-store listing length and not the snapshot count. A table with
  tens of thousands of files across a handful of shared manifests is cheap.
- Lowering `snapshots_to_keep` reduces manifest-list reads and can shrink the
  live set (files referenced only by expired snapshots stop being protected),
  but it does not change the per-file memory cost.
- `max_live_files_threshold` remains the backstop for pathological tables. It
  is evaluated from manifest-list metadata _before_ any manifest is fetched,
  so a table over the cap costs one manifest-list read per retained snapshot
  and nothing else.

Two deliberate non-goals:

- **Fingerprints, not paths.** The live set stores a 64-bit hash of each live
  path. A collision can only make an orphan look live — the file is kept, not
  deleted — so the failure direction is "reclaim later", never "delete a live
  file".
- **One table-scoped listing, not per-partition listings.** Orphans can sit
  under partition prefixes that table metadata no longer mentions (that is
  precisely what makes them orphans), so cleanup lists the whole `data/`
  prefix. Listing per partition prefix would issue more requests for the same
  objects and would silently skip orphans in dropped partitions.

### Concurrent Operation Tuning

**Symptoms:**

- Queries failing during retention operations
- Snapshot conflicts

**Tuning Options:**

```toml
[compactor.retention]
# Keep more snapshots (longer isolation window)
snapshots_to_keep = 10

# Run retention less frequently
retention_check_interval = "2h"

# Stagger operations (retention at 2 AM, cleanup at 3 AM)
```

### Object Store Optimization

For S3-compatible stores:

```bash
# Increase connection pool size
export AWS_MAX_ATTEMPTS=10
export AWS_RETRY_MODE=adaptive

# Use faster instance types for cleanup
# (more CPU = faster manifest reading)

# Enable S3 Transfer Acceleration
export AWS_S3_USE_ACCELERATE_ENDPOINT=true
```

---

## Attribute Promotion

With [`[compactor.attr_promotion]`](configuration.md#attribute-promotion-configuration) enabled and `dry_run = false`, the compactor promotes qualifying attribute keys to materialized `label_<key>` columns as part of a normal compaction rewrite. Each acted-on promotion makes two commits per table:

1. **Schema flip** (before the rewrite): a metadata-only `AddSchema` + `SetCurrentSchema` commit adds the promoted columns. No data files change; readers null-fill the new columns until the rewrite lands.
2. **Rewrite/delta commit** (the normal compaction commit): every row _in the partition being compacted_ is rewritten with the label values backfilled from its attributes (resource, then scope, then record attributes). Existing label columns are recomputed too, healing rows the writer left null during the transition window. Because compaction is partition-scoped, backfill reaches a table's older rows as their partitions are compacted, not all at once.

A schema-evolution failure is logged as a warning and the compaction continues under the old schema — promotion never fails a rewrite.

**What operators see in the logs:**

- `Attribute promotion decision` (info) — per table: `dry_run`, `promote`, `demote`, and `building` (keys still accumulating their hysteresis streak).
- `Added materialized label columns via schema evolution` (info) — the schema flip landed; lists the table, new schema id, and columns.
- `Failed to evolve schema for attribute promotion; continuing compaction without it` (warn) — the flip failed; the rewrite proceeded without new columns.
- The usual `Rewrote table data into compacted files` line covers the backfilled rewrite — there is no separate backfill log line, and no promotion-specific Prometheus metric yet.

Demotion candidates are dropped at rewrite (schema commit without the column, after the promote half; the rewrite then omits it — attribute data stays in the map tier). Pinned `[schema.materialized_labels]` entries are never demoted. Note: demand counters are cumulative today, so a once-queried key is not demoted until a demand-decay window lands (follow-up).

## Additional Resources

- [Configuration Reference](configuration.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Compactor README](https://github.com/cedricziel/signaldb/blob/main/src/compactor/README.md)

> Note: every compaction rewrite also runs a read-only attribute-statistics pass that logs per-key presence, approximate cardinality, and advisory materialization candidates (`Attribute-stats analyzer` log line), and persists the per-key statistics to the service catalog's `attribute_stats` table (joined there with query-demand counters flushed by the querier). This statistics pass requires no configuration and changes no table data; the promotion pass built on it is covered in [Attribute Promotion](#attribute-promotion).
