---
audience: operator
type: how-to
status: living
sources:
  - src/compactor/src/**
---

# Compactor Troubleshooting Guide

Comprehensive troubleshooting guide for SignalDB Compactor retention and lifecycle management (retention enforcement, snapshot expiration, and orphan-file cleanup).

## Table of Contents

- [Quick Diagnosis](#quick-diagnosis)
- [Retention Issues](#retention-issues)
- [Orphan Cleanup Issues](#orphan-cleanup-issues)
- [Performance Issues](#performance-issues)
- [Data Integrity Issues](#data-integrity-issues)
- [Lease and Recovery Issues](#lease-and-recovery-issues)
- [Debug Procedures](#debug-procedures)
- [Common Error Messages](#common-error-messages)
- [Attribute Promotion](#attribute-promotion)

## Quick Diagnosis

### Health Check Commands

```bash
# Check compactor is running
ps aux | grep compactor

# Check logs for errors. The standalone compactor logs to stdout —
# use journalctl for systemd units, or .data/logs/monolithic.log when
# running via ./scripts/run-dev.sh
journalctl -u signaldb-compactor -n 100 | grep ERROR
# or
tail -100 .data/logs/monolithic.log | grep ERROR

# JSON status snapshot (counters + instance metadata)
curl -s localhost:9091/status | jq .

# Check metrics endpoint
curl -s localhost:9091/metrics | grep compactor

# Check retention activity (counter increments every retention cycle)
curl -s localhost:9091/metrics | grep compactor_retention_cutoffs_computed_total

# Check recent orphan cleanup
curl -s localhost:9091/metrics | grep compactor_orphan_candidates_identified_total
```

### Quick Status Check

```bash
# All-in-one status check
cat << 'EOF' > /tmp/compactor_status.sh
#!/bin/bash
echo "=== Compactor Status ==="
echo "Process: $(pgrep -f compactor | wc -l) running"
echo ""
echo "=== Status Snapshot ==="
curl -s localhost:9091/status
echo ""
echo "=== Retention Metrics ==="
curl -s localhost:9091/metrics | grep -E "compactor_(partitions_dropped|retention_duration_ms|bytes_reclaimed)" | tail -3
echo ""
echo "=== Orphan Cleanup Metrics ==="
curl -s localhost:9091/metrics | grep -E "compactor_(files_deleted|orphan_candidates_identified)" | tail -3
EOF

chmod +x /tmp/compactor_status.sh
/tmp/compactor_status.sh
```

## Retention Issues

### Issue 1: No Partitions Being Dropped

**Symptoms:**

- `compactor_partitions_dropped_total` metric is 0
- Data older than retention period still exists
- No drop operations in logs

**Diagnostic Steps:**

```bash
# 1. Check if retention is enabled
grep -A 5 "compactor.retention" signaldb.toml

# 2. Check for dry-run mode
grep "dry_run" signaldb.toml | grep retention

# 3. Check computed cutoff in logs (compactor logs to stdout)
RUST_LOG=debug,compactor::retention=trace cargo run --bin signaldb-compactor 2>&1 | \
  grep "Retention cutoff computed"

# 4. Check retention counters via the status endpoint
curl -s localhost:9091/status | jq .retention

# 5. Inspect the actual data files on the object store
# (partition metadata is in Iceberg metadata files, not in PostgreSQL)
find .data/storage/<tenant>/<dataset>/traces -name "*.parquet" -mtime +7 | head
```

**Common Causes and Solutions:**

| Cause                      | Verification                                       | Solution                                                                                                   |
| -------------------------- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Retention disabled         | `enabled = false` in config                        | Set `enabled = true` (retention is enabled by default; `enabled = false` only appears when set explicitly) |
| Dry-run mode enabled       | `dry_run = true` in config                         | Set `dry_run = false` (the default is `false`)                                                             |
| Grace period too large     | Check `grace_period`                               | Reduce grace period                                                                                        |
| No data old enough         | Check partition timestamps                         | Wait for data to age                                                                                       |
| Retention check hasn't run | Check `compactor_retention_cutoffs_computed_total` | Restart compactor or wait for interval                                                                     |

**Example Fix:**

```toml
# Before (not working)
[compactor.retention]
enabled = false  # ← Problem: disabled

# After (working)
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval = "1h"
traces = "7d"
```

### Issue 2: Partitions Dropped Too Aggressively

**Symptoms:**

- More partitions dropped than expected
- Data deleted sooner than configured retention
- Unexpected partition drop logs

**Diagnostic Steps:**

```bash
# 1. Check effective retention configuration (compactor logs to stdout)
RUST_LOG=debug,compactor::retention=trace cargo run --bin signaldb-compactor 2>&1 | \
  grep "Retention cutoff computed"
# The source field shows which level applied: source=Global, source=Tenant,
# or source=Dataset

# 2. Check for configuration errors
grep -A 20 "compactor.retention" signaldb.toml

# 3. Verify grace period
grep "grace_period" signaldb.toml
```

**Common Causes:**

1. **Incorrect Override Hierarchy:**

```toml
# Problem: Dataset override shorter than intended
[compactor.retention.tenant_overrides.production]
traces = "30d"

[compactor.retention.tenant_overrides.production.dataset_overrides.critical]
traces = "3d"  # ← Accidentally 3d instead of 90d
```

**Solution:** Review and fix retention periods in configuration.

2. **Environment Variable Override:**

```bash
# Check for unexpected environment variables
env | grep SIGNALDB__COMPACTOR__RETENTION

# Example problem:
# SIGNALDB__COMPACTOR__RETENTION__TRACES=1d  ← Overriding config file
```

**Solution:** Remove or correct environment variable overrides.

3. **Zero Grace Period:**

```toml
grace_period = "0s"  # ← No safety margin
```

**Solution:** Use at least 1 hour grace period for production.

### Issue 3: Retention Check Not Running

**Symptoms:**

- `compactor_retention_cutoffs_computed_total` not increasing
- No retention logs in recent time window
- Partitions not being evaluated

**Diagnostic Steps:**

```bash
# 1. Check compactor process is running
ps aux | grep compactor

# 2. Check for fatal errors at startup (stdout/journalctl, or
#    .data/logs/monolithic.log when using ./scripts/run-dev.sh)
journalctl -u signaldb-compactor -n 100 | grep -E "(ERROR|FATAL)"

# 3. Check retention run logs
journalctl -u signaldb-compactor | grep "Retention enforcement run completed"

# 4. Check the retention-cycle counter (should increase every cycle)
curl -s localhost:9091/metrics | grep compactor_retention_cutoffs_computed_total
```

**Common Causes:**

1. **Compactor Not Running:**

```bash
# Check process
systemctl status signaldb-compactor
# or
pgrep -f compactor
```

**Solution:** Start the compactor.

2. **Configuration Validation Failed:**

```bash
# Check startup logs (stdout/journalctl)
journalctl -u signaldb-compactor -n 50 | grep -E "(validation|config|failed)"
```

**Solution:** Fix configuration errors and restart.

3. **Retention Check Interval Too Long:**

```toml
retention_check_interval = "24h"  # Won't run often
```

**Solution:** Reduce interval for more frequent checks or wait longer.

4. **Waiting Behind a Compaction Job on the Same Table:**

Compaction, retention drops, and snapshot expiration take turns per table
within a compactor process, so a rewrite in progress defers that table's
retention pass until it completes. Retention on _other_ tables is unaffected,
which is the signature to look for: some tables progress while one lags.

```bash
# Is a rewrite in flight on the LAGGING table specifically? Without the
# tenant/dataset/table filter this matches every concurrent rewrite, which
# cannot tell you whether this table is the one holding the lock.
TENANT=acme; DATASET=prod; TABLE=traces
journalctl -u signaldb-compactor \
  | grep -E "Starting compaction job|Rewrote table data" \
  | grep -E "$TENANT/$DATASET/$TABLE|table=$TABLE"
```

The compaction job log line carries `tenant/dataset/table`; the rewrite
completion line carries `table=`, so match either.

**Solution:** None needed — the pass is deferred, not skipped, and runs as soon
as the rewrite finishes. The wait is bounded by the compaction job, not by a
timer: a job that hits commit conflicts redoes the whole rewrite, up to three
attempts, so the worst case is three rewrite durations rather than one. If the
delay is persistent rather than occasional, the rewrite itself is the problem to
chase: see
[Issue 13](#issue-13-rewrites-fail-with-resources-exhausted-instead-of-spilling)
and the `max_partition_input_mb` guidance in
[configuration.md](configuration.md).

### Issue 4: Snapshot Expiration Not Working

**Symptoms:**

- Snapshot count keeps growing
- `compactor_snapshots_expired_total` is 0
- Metadata size increasing

**Diagnostic Steps:**

```bash
# 1. Check snapshot count from the Iceberg metadata on the object store
#    (there is no iceberg_snapshots table in the catalog database)
jq '.snapshots | length' \
  .data/storage/<tenant>/<dataset>/traces/metadata/<latest>.metadata.json

# 2. Check snapshots_to_keep config
grep "snapshots_to_keep" signaldb.toml

# 3. Check logs for snapshot expiration (stdout/journalctl/monolithic.log).
#    "Expired old snapshots" is info-level; the "No snapshots to expire" /
#    "Found snapshots to expire" checks are debug-level
#    (RUST_LOG=info,compactor::retention=debug)
journalctl -u signaldb-compactor | \
  grep -E "(Expired old snapshots|No snapshots to expire|Found snapshots to expire)" | tail -20
```

**Common Causes:**

1. **snapshots_to_keep Set Too High:**

```toml
snapshots_to_keep = 1000  # ← Never expires if < 1000 snapshots
```

**Solution:** Use reasonable value (5-10 for most use cases).

2. **Snapshot Expiration Not Implemented:**

Check if snapshot expiration is actually running:

```bash
journalctl -u signaldb-compactor | grep -iE "expire.*snapshot"
curl -s localhost:9091/metrics | grep compactor_snapshots_expired_total
```

**Solution:** Verify the compactor is fully deployed.

3. **The Table Is Not Recognized As a Signal Table:**

If a single table's snapshot count grows without bound while every other
table in the same dataset falls to `snapshots_to_keep`, the lifecycle is not
enumerating that table at all — it gets no retention, no snapshot expiration,
and no orphan cleanup. Table membership is decided by one predicate
(`SignalType::from_table_name`), listed under
[Signal Type Mapping](configuration.md#global-retention-periods); a table name
it does not classify is invisible to every lifecycle job.

Compare per-table snapshot counts to spot the outlier:

```bash
for t in traces logs metrics_gauge profiles; do
  echo -n "$t: "
  jq '.snapshots | length' \
    .data/storage/<tenant>/<dataset>/$t/metadata/*.metadata.json 2>/dev/null | tail -1
done
```

**Solution:** This is a bug, not a misconfiguration — file an issue naming the
table. `profiles` was affected until [#1014](https://github.com/cedricziel/signaldb/issues/1014).

## Orphan Cleanup Issues

### Issue 5: Orphan Files Not Being Deleted

**Symptoms:**

- `compactor_orphan_candidates_identified_total` > 0
- `compactor_files_deleted_total` = 0
- Orphan files identified but not removed

**Diagnostic Steps:**

```bash
# 1. Check if cleanup is enabled and not in dry-run
grep -A 5 "compactor.orphan_cleanup" signaldb.toml

# 2. Check for deletion errors (stdout/journalctl/monolithic.log)
journalctl -u signaldb-compactor | grep -E "(orphan|delete|failed)" | tail -20

# 3. Check revalidation logs
journalctl -u signaldb-compactor | grep -i "revalidation" | tail -10
```

**Common Causes:**

| Cause                           | Verification                             | Solution                                                            |
| ------------------------------- | ---------------------------------------- | ------------------------------------------------------------------- |
| Dry-run mode enabled            | `dry_run = true`                         | Set `dry_run = false`                                               |
| Revalidation finding files live | Check revalidation logs                  | Normal - files no longer orphaned                                   |
| Permission errors               | Check error logs for "Permission denied" | Fix object store permissions                                        |
| Grace period not met            | Check file ages                          | Wait for grace period to elapse                                     |
| Object store unavailable        | Check network/S3 connectivity            | Restore object store access                                         |
| Snapshots never expiring        | Retention logs show no expiration runs   | Enable `[compactor.retention]`; expiration shrinks the retained set |

Files stay protected while **any retained snapshot** references them, so
zero candidates on a table whose snapshots never expire is expected
behavior, not a detection failure.

**Example Fix:**

```toml
# Before (not deleting)
[compactor.orphan_cleanup]
enabled = true
dry_run = true  # ← Problem: still in dry-run

# After (deleting)
[compactor.orphan_cleanup]
enabled = true
dry_run = false
grace_period_hours = 24
```

### Issue 6: False Orphan Detection

**Symptoms:**

- High orphan count (> 10% of total files)
- Recently written files flagged as orphans
- Revalidation preventing most deletions

**Diagnostic Steps:**

```bash
# 1. Check grace period configuration
grep "grace_period_hours" signaldb.toml | grep orphan_cleanup

# 2. Check file ages in orphan logs (stdout/journalctl/monolithic.log)
journalctl -u signaldb-compactor | grep "DRY-RUN.*Would delete" | tail -10

# 3. Check how many snapshots are retained (the live set spans all of them)
grep "snapshots_to_keep" signaldb.toml
```

**Common Causes:**

1. **Grace Period Too Short:**

```toml
grace_period_hours = 1  # ← Too short for busy systems
```

**Solution:** Increase to 24 hours for safety.

2. **Compaction Creating New Files:**

Compaction writes new files and drops its input files from the current snapshot; the inputs become orphans once no retained snapshot references them.

**Solution:** This is expected. Ensure the grace period covers compaction duration. Compaction is partition-scoped, so the orphans produced by one job are bounded by that partition rather than by the whole table.

### Issue 7: Orphan Cleanup Taking Too Long

**Symptoms:**

- Cleanup runs for hours
- High memory usage during cleanup
- Cleanup skipped with `compactor_orphan_cleanup_skipped_total` increasing

**Diagnostic Steps:**

```bash
# 1. Check how many orphan candidates were identified
curl -s localhost:9091/metrics | grep compactor_orphan_candidates_identified_total

# 2. Check batch size and live-file threshold
grep -E "(batch_size|max_live_files_threshold)" signaldb.toml

# 3. Monitor memory usage
ps aux | grep compactor | awk '{print $4, $6}'
```

**Solutions:**

1. **Reduce Batch Size:**

```toml
[compactor.orphan_cleanup]
batch_size = 500  # Down from 1000
```

2. **Retain Fewer Snapshots:**

```toml
[compactor.retention]
snapshots_to_keep = 5  # Fewer retained snapshots to scan
```

3. **Run Less Frequently:**

```toml
cleanup_interval_hours = 48  # Every 2 days instead of daily
```

4. **Reduce Live Files First:**

If cleanup is being skipped because the estimated live file count exceeds
`max_live_files_threshold`, run snapshot expiration and compaction first to
reduce file counts before raising the threshold.

## Performance Issues

### Issue 8: High CPU Usage During Retention

**Symptoms:**

- CPU spikes during retention check
- Retention check duration > 5 minutes
- System slowdown during retention

**Diagnostic Steps:**

```bash
# 1. Check retention duration
curl -s localhost:9091/metrics | grep compactor_retention_duration_ms_total

# 2. Profile with CPU profiling
RUST_LOG=info cargo flamegraph --bin signaldb-compactor

# 3. Check data file counts per table on the object store
#    (partition metadata lives in Iceberg metadata files, not PostgreSQL)
for t in .data/storage/*/*/*; do
  echo "$t: $(find "$t" -name '*.parquet' | wc -l) files"
done
```

**Solutions:**

1. **Increase Check Interval:**

```toml
retention_check_interval = "2h"
```

2. **Reduce Snapshots to Keep:**

```toml
snapshots_to_keep = 3  # Fewer snapshots = less metadata
```

3. **Partition Pruning:**

If many partitions exist, consider implementing partition pruning in the query path.

### Issue 9: High Memory Usage

**Symptoms:**

- OOM errors during orphan cleanup
- Memory usage growing over time
- System swapping during cleanup

> **Rewrite memory is a separate story from cleanup memory.** A compaction
> rewrite streams its partition in two passes — an unsorted scan that gathers
> attribute statistics, then a sorted scan that feeds the writer — so its peak
> is roughly `memory_limit_mb` (the DataFusion pool, which spills past it) plus
> one `target_file_size_mb` of output accumulation. Neither term grows with the
> partition. If rewrite memory looks proportional to how much data an hour
> holds, that is a bug, not tuning. If a _sort_ fails outright with
> `Resources exhausted` instead of spilling, see
> [Issue 13](#issue-13-rewrites-fail-with-resources-exhausted-instead-of-spilling).

**Diagnostic Steps:**

```bash
# 1. Monitor memory usage
watch -n 5 'ps aux | grep compactor | awk "{print \$4, \$6}"'

# 2. Check reference set size (stdout/journalctl/monolithic.log)
journalctl -u signaldb-compactor | grep -i "reference set"

# 3. Check whether cleanup was skipped by the live-file threshold
curl -s localhost:9091/metrics | grep compactor_orphan_cleanup_skipped_total
```

**Solutions:**

1. **Reduce Batch Size:**

```toml
batch_size = 250  # Smaller batches
```

2. **Retain Fewer Snapshots:**

```toml
[compactor.retention]
snapshots_to_keep = 5
```

3. **Increase Container Memory:**

```yaml
# compose.yml
services:
  compactor:
    mem_limit: 4g # Increase from default
```

### Issue 13: Rewrites Fail With "Resources Exhausted" Instead of Spilling

**Symptoms:**

- `Failed to rewrite partition data: ... Not enough memory to continue external sort`
- The error names several `ExternalSorter[N]` consumers, each reporting `can spill: true`
- The job fails in `read_and_merge` — no commit is ever attempted, so nothing is corrupted; the partition is simply never compacted

**What it means:** the memory pool is being exhausted by the sort's own
_concurrency_, not by one oversized partition. Every DataFusion partition gets
its own sorter plus an unspillable merge reservation, and they all divide the
single `memory_limit_mb` budget.

**Diagnostic Steps:**

```bash
# 1. How many sorters is the plan creating? (should be target_partitions)
journalctl -u signaldb-compactor | grep -o 'ExternalSorter\[[0-9]*\]' | sort -u

# 2. What is each sorter's share?  memory_limit_mb / max(target_partitions, 1)
#    Below ~64 MB a spilling sort fails instead of spilling.
curl -s localhost:9091/status | jq '.compaction'

# 3. Are partitions being declined for size rather than attempted?
curl -s localhost:9091/metrics | grep compactor_oversized_partitions_skipped_total
```

**Solutions:**

1. **Lower the fan-out** so one sorter owns the whole budget (the default):

```toml
[compactor]
target_partitions = 1
```

2. **Raise the pool** if the per-sorter share is below ~64 MB:

```toml
[compactor]
memory_limit_mb = 1024
```

3. **Check the startup warnings.** The compactor logs an explicit warning when
   `target_file_size_mb` is at or above `memory_limit_mb`, or when the
   per-sorter share is below the spill floor. Both combinations produce this
   failure and neither is visible from any single setting.

## Data Integrity Issues

### Issue 10: Queries Failing After Retention

**Symptoms:**

- "Snapshot not found" errors
- "Partition not found" errors
- Query failures correlated with retention runs

**Diagnostic Steps:**

```bash
# 1. Check recent partition drops (stdout/journalctl/monolithic.log)
journalctl -u signaldb-compactor | grep "Dropped expired partitions" | tail -20

# 2. Check snapshot expiration
journalctl -u signaldb-compactor | grep "Expired old snapshots" | tail -20

# 3. Check query timestamps
# Queries using old snapshots may fail if snapshot expired
```

**Solutions:**

1. **Increase snapshots_to_keep:**

```toml
snapshots_to_keep = 10  # Keep more snapshots
```

2. **Increase Retention Check Interval:**

```toml
retention_check_interval = "2h"  # Less frequent
```

3. **Ensure Queries Use Recent Snapshots:**

Configure query service to refresh snapshot references more frequently.

### Issue 11: Accidental Data Deletion

**Symptoms:**

- More data deleted than expected
- Incorrect retention cutoff applied
- Production data missing

**Immediate Response:**

1. **Stop Retention Immediately:**

```bash
# Option 1: Disable in config
sed -i 's/enabled = true/enabled = false/' signaldb.toml
systemctl restart signaldb-compactor

# Option 2: Stop process
systemctl stop signaldb-compactor
```

2. **Identify Affected Data:**

```bash
# Check recent drops in logs (stdout/journalctl/monolithic.log)
journalctl -u signaldb-compactor | grep "Dropped expired partitions" | \
  grep "$(date +%Y-%m-%d)" > /tmp/dropped_today.txt

# Review what was dropped
cat /tmp/dropped_today.txt
```

3. **Attempt Recovery:**

Partition drops are Iceberg commits, so the pre-drop snapshot survives until snapshot expiration removes it. There is currently no supported way to query it from SignalDB: snapshot metadata is not in the catalog database, and time-travel SQL (`FOR SYSTEM_TIME AS OF`) is not supported by the query path. Instead, inspect the Iceberg metadata on the object store:

```bash
# List snapshots for the table (snapshot-id, timestamp-ms, summary)
jq '.snapshots[] | {snapshot_id: ."snapshot-id", timestamp_ms: ."timestamp-ms", summary}' \
  .data/storage/<tenant>/<dataset>/traces/metadata/<latest>.metadata.json
```

If a pre-drop snapshot still exists, rolling the table back to it requires manual Iceberg surgery with external Iceberg tooling — keep the compactor stopped so snapshot expiration and orphan cleanup cannot delete the referenced files in the meantime.

**Prevention:**

1. **Always Use Dry-Run First:**

```toml
[compactor.retention]
dry_run = true  # Test first
```

2. **Test on Non-Production Tenant:**

```toml
[compactor.retention.tenant_overrides.test]
traces = "1d"  # Test here first
```

3. **Monitor Metrics:**

Set up alerts for unexpected partition drops:

```promql
# Alert if > 100 partitions dropped in 5 minutes
rate(compactor_partitions_dropped_total[5m]) > 20
```

## Lease and Recovery Issues

### Issue 12: Partitions Never Compacted After an Instance Crash

**Symptoms:**

- One partition is planned as a candidate every cycle but never executed
- Logs repeat `lease held by another instance` for a holder that no longer exists
- `compactor_stale_leases_expired_total` is flat while the fleet has restarted

**Diagnostic Steps:**

```bash
# 1. Which leases are currently held, and by which instance? Active leases
#    come from the Flight admin action, not the HTTP endpoint — /status
#    carries instance metadata and counters only.
signaldb-cli ops compact status  # or the compactor Flight action compact_status

# 2. Is the sweep running at all? (increments only when it reclaims)
curl -s localhost:9091/metrics | grep compactor_stale_leases_expired_total

# 3. Compare each holder_id against the instances that are actually up —
#    every live compactor reports its own id and uptime here.
curl -s localhost:9091/status | jq '{instance_id, uptime_seconds}'
```

**How it should behave:** stale leases are swept every 30s on a dedicated task,
independent of the compaction cycle, so a partition orphaned by a crash becomes
claimable within roughly `lease_ttl_seconds + 30s` no matter how long the
current compaction pass takes.

**Common Causes:**

1. **Lease TTL Longer Than the Outage Window:**

```toml
lease_ttl_seconds = 3600  # An hour before a crashed holder's lease expires
```

**Solution:** Lower `lease_ttl_seconds` toward the default `300`, but keep
margin. Held leases are renewed every `ttl / 3`, and a renewal failure does not
stop the running job — there is no fencing token, so once the lease expires
another instance can claim the partition and start a duplicate rewrite. Iceberg
CAS keeps both from committing, so the cost is wasted work rather than
corruption, but the TTL still needs to cover process pauses and catalog or
network outages.

2. **The Holder Is Alive and Genuinely Slow:**

A lease that keeps being renewed is not stale. Check whether the holder is
still executing the job (`compactor_jobs_started_total` vs
`compactor_jobs_succeeded_total`) before assuming a crash.

## Debug Procedures

### Enable Debug Logging

**Temporary (current session):**

```bash
RUST_LOG=debug,compactor=trace cargo run --bin signaldb-compactor
```

**Persistent:**

The compactor initializes `tracing-subscriber` with a standard `RUST_LOG` env filter and writes to stdout — there is no logging config file. For persistent debug logging, set `RUST_LOG` in the process environment and capture stdout:

```bash
# Shell: redirect stdout to a file
RUST_LOG=info,compactor::retention=trace,compactor::orphan=trace \
  cargo run --bin signaldb-compactor 2>&1 | tee compactor-debug.log

# systemd: set the filter in the unit and read logs via journald
#   [Service]
#   Environment=RUST_LOG=info,compactor::retention=trace,compactor::orphan=trace
journalctl -u signaldb-compactor -f
```

### Trace Specific Operation

**Trace Retention Enforcement:**

```bash
# Enable trace logging for retention only
RUST_LOG=info,compactor::retention=trace cargo run --bin signaldb-compactor 2>&1 | \
  grep -E "(retention|cutoff|drop|partition)"
```

**Trace Orphan Cleanup:**

```bash
# Enable trace logging for orphan cleanup only
RUST_LOG=info,compactor::orphan=trace cargo run --bin signaldb-compactor 2>&1 | \
  grep -E "(orphan|cleanup|delete|reference)"
```

### Verify Iceberg Operations

The SQL catalog only stores the table registry (`iceberg_tables`, maintained by iceberg-rust's SQL catalog); snapshot, partition, and manifest metadata live in metadata files on the object store.

**Check the Table Registry (SQL catalog):**

```sql
-- PostgreSQL (or SQLite) catalog: maps table identifiers to metadata locations
SELECT * FROM iceberg_tables;
```

**Check Snapshots, Manifests, and Partitions (object store):**

```bash
# Table metadata JSON (local filesystem storage; adapt for S3/MinIO)
ls .data/storage/<tenant>/<dataset>/traces/metadata/

# List snapshots
jq '.snapshots[] | {snapshot_id: ."snapshot-id", timestamp_ms: ."timestamp-ms", summary}' \
  .data/storage/<tenant>/<dataset>/traces/metadata/<latest>.metadata.json

# Manifest lists and manifests are Avro files referenced from the
# snapshot's "manifest-list" entry
jq '.snapshots[-1]."manifest-list"' \
  .data/storage/<tenant>/<dataset>/traces/metadata/<latest>.metadata.json
```

### Inspect Object Store

**List Parquet Files:**

```bash
# Local filesystem
find .data/storage -name "*.parquet" -ls

# S3
aws s3 ls s3://signaldb-data/ --recursive | grep "\.parquet$"

# MinIO
mc ls minio/signaldb-data --recursive | grep "\.parquet$"
```

**Check File Ages:**

```bash
# Files older than 7 days (candidates for cleanup)
find .data/storage -name "*.parquet" -mtime +7 -ls

# Files modified in last 24 hours (grace period)
find .data/storage -name "*.parquet" -mtime -1 -ls
```

## Common Error Messages

### Error: "Job ... failed with non-conflict error"

**Full Message:**

```
ERROR compactor::executor: Job 6e30dfc6-0367-41a5-833d-2f075b0e40aa failed with non-conflict error: Failed to commit compaction: Failed to commit compaction delta snapshot: <underlying cause>
```

A compaction job failed for a reason the executor did **not** classify as a
snapshot conflict. Everything after the first colon is the `anyhow` cause
chain; the last segment is the real failure. The chain is what makes this
message actionable — read it to the end rather than stopping at "Failed to
commit compaction", which every commit failure shares.

**Causes:** the chain distinguishes them; the wrapper alone does not.

- `Failed to load table for commit` — the catalog could not serve the table
- `Failed to re-read manifests for delta commit` — manifest read failed against
  the object store
- `Failed to commit compaction delta snapshot` — Iceberg rejected the
  `overwrite`

Snapshot conflicts are a _different_ path: they are retried with exponential
backoff and reported as "Conflict", not here. Seeing this message means the
failure was not contention.

**Solutions:**

1. Read the last segment of the chain and treat it as the actual error —
   catalog connectivity, object store availability/permissions, or an Iceberg
   commit rejection.
2. Note that non-conflict failures are **not** retried; the job fails on its
   first attempt.
3. Check whether the same table/partition recurs across cycles. Planning has no
   failure memory, so a partition that cannot commit is re-selected every
   `tick_interval` and fails again, consuming compaction capacity. A persistent
   repeat is a stuck partition, not a transient error.

```bash
# Failure rate over recent cycles (counters are cumulative per process)
grep "Jobs:" /path/to/compactor.log | tail -5

# Which tables/partitions keep coming back
grep "Starting compaction job" /path/to/compactor.log | tail -20
```

### Error: "Table retention enforcement failed"

**Full Message:**

```
WARN compactor::retention::enforcer: Table retention enforcement failed signaldb.tenant.id=acme signaldb.dataset.id=prod signaldb.table=traces error=Failed to commit partition drop: ...
```

**Causes:**

- Catalog connection lost
- Concurrent modification conflict (snapshot conflicts are retried a few times first; look for "Partition drop hit a snapshot conflict; retrying against fresh metadata")

**Solutions:**

1. Check catalog connectivity:

   ```bash
   psql -h localhost -U signaldb -d signaldb -c "SELECT 1"
   ```

2. Retry - operations are idempotent; the next retention cycle will re-evaluate the same partitions.

### Error: "Failed to delete orphan file"

**Full Message:**

```
ERROR compactor::orphan::cleaner: Failed to delete orphan file path=acme/prod/traces/data/data-001.parquet error=Failed to delete file: ... table=acme/prod/traces
```

**Causes:**

- Insufficient object store permissions
- Object store credentials invalid
- Object store unavailable

**Solutions:**

1. Check object store credentials:

   ```bash
   env | grep AWS
   # Verify AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
   ```

2. Test object store access:

   ```bash
   aws s3 ls s3://signaldb-data/
   ```

3. Verify IAM permissions include `s3:DeleteObject`.

4. Monitor `compactor_deletion_failures_total` for recurring failures.

### Warning: "File no longer orphan after revalidation, skipping deletion"

**Full Message:**

```
WARN compactor::orphan::cleaner: File no longer orphan after revalidation, skipping deletion path=acme/prod/traces/data/data-001.parquet table=acme/prod/traces
```

**Cause:** A concurrent write referenced the file between detection and deletion; revalidation caught it (this is the safety mechanism working as intended).

**Solution:** No action needed. If this happens for most candidates, increase `grace_period_hours` so in-flight files stop being detected as candidates in the first place.

### Debug: "Skipping recent file (within grace period)"

**Full Message:**

```
DEBUG compactor::orphan::detector: Skipping recent file (within grace period) path=acme/prod/traces/data/data-001.parquet last_modified=2026-02-09T09:30:00Z cutoff_time=2026-02-08T10:00:00Z grace_period_hours=24
```

**Cause:** File is too recent to be cleaned up (expected behavior; only visible with `RUST_LOG=...,compactor::orphan=debug` or lower).

**Solution:** Nothing to fix. The file becomes eligible after the grace period elapses.

## Attribute Promotion

**A `label_<key>` column appeared that is not in `[schema.materialized_labels]`:** attribute auto-promotion added it. With `[compactor.attr_promotion].dry_run = false`, the compactor promotes frequently queried attribute keys to columns at rewrite (see the [operations guide](operations.md#attribute-promotion)).

**How to tell a promotion happened:** look for `Added materialized label columns via schema evolution` in the compactor logs (table, schema id, columns), or compare the table's current schema against your pinned config. The preceding `Attribute promotion decision` line shows why the key qualified.

**How to stop promotions:** set `[compactor.attr_promotion].dry_run = true` (decisions are still logged, nothing changes) or `enabled = false` (no decision pass at all). Columns already added stay in place; they are nullable and harmless to queries.

**Removing a promoted column:** demotion is acted on at rewrite: unpinned promoted columns with no recorded query demand are dropped from the schema at the next compaction cycle (the data remains queryable through the attributes map). To force-keep a column, pin it in `[schema.materialized_labels]`.

## Additional Resources

- [Operations Guide](operations.md)
- [Configuration Reference](configuration.md)
- [Compactor README](https://github.com/cedricziel/signaldb/blob/main/src/compactor/README.md)
- [Integration Test Examples](https://github.com/cedricziel/signaldb/tree/main/tests-integration/tests/compactor)

## Getting Help

If you encounter issues not covered in this guide:

1. **Enable Debug Logging:** `RUST_LOG=debug,compactor=trace`
2. **Collect Logs:** Last 500 lines of compactor logs
3. **Gather Metrics:** `curl localhost:9091/metrics | grep compactor > metrics.txt`
4. **Configuration:** Share `signaldb.toml` (redact sensitive values)
5. **Open Issue:** https://github.com/cedricziel/signaldb/issues with above information

> The `Attribute-stats analyzer` log line on each rewrite is advisory only (epic #737); it never blocks or alters compaction. Its statistics are persisted to the catalog's `attribute_stats` table; a `Failed to persist attribute scan stats` warning means the catalog write failed and is safe to ignore for compaction correctness. The `Attribute promotion decision` line (when `[compactor.attr_promotion]` is enabled) is advisory while `dry_run = true`; with `dry_run = false` the compactor acts on it — see [Attribute Promotion](#attribute-promotion).
