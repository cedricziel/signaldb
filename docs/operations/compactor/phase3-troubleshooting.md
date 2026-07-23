---
audience: operator
type: how-to
status: living
sources:
  - src/compactor/src/**
---

# Phase 3 Troubleshooting Guide

Comprehensive troubleshooting guide for SignalDB Compactor Phase 3: Retention Enforcement and Lifecycle Management.

## Table of Contents

- [Quick Diagnosis](#quick-diagnosis)
- [Retention Issues](#retention-issues)
- [Orphan Cleanup Issues](#orphan-cleanup-issues)
- [Performance Issues](#performance-issues)
- [Data Integrity Issues](#data-integrity-issues)
- [Debug Procedures](#debug-procedures)
- [Common Error Messages](#common-error-messages)

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

| Cause | Verification | Solution |
|-------|-------------|----------|
| Retention disabled | `enabled = false` in config | Set `enabled = true` |
| Dry-run mode enabled | `dry_run = true` in config | Set `dry_run = false` |
| Grace period too large | Check `grace_period` | Reduce grace period |
| No data old enough | Check partition timestamps | Wait for data to age |
| Retention check hasn't run | Check `compactor_retention_cutoffs_computed_total` | Restart compactor or wait for interval |

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

# 3. Check logs for snapshot expiration (stdout/journalctl/monolithic.log)
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

**Solution:** Verify Phase 3 is fully deployed.

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

| Cause | Verification | Solution |
|-------|-------------|----------|
| Dry-run mode enabled | `dry_run = true` | Set `dry_run = false` |
| Revalidation finding files live | Check revalidation logs | Normal - files no longer orphaned |
| Permission errors | Check error logs for "Permission denied" | Fix object store permissions |
| Grace period not met | Check file ages | Wait for grace period to elapse |
| Object store unavailable | Check network/S3 connectivity | Restore object store access |

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
revalidate_before_delete = true
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

# 3. Check max_snapshot_age_hours
grep "max_snapshot_age_hours" signaldb.toml
```

**Common Causes:**

1. **Grace Period Too Short:**

```toml
grace_period_hours = 1  # ← Too short for busy systems
```

**Solution:** Increase to 24 hours for safety.

2. **Snapshot Window Too Narrow:**

```toml
max_snapshot_age_hours = 1  # ← Only looking at recent snapshots
```

**Solution:** Increase to 720 hours (30 days).

3. **Compaction Creating New Files:**

Compaction creates new files that reference data from old files. The old files become orphans after compaction completes.

**Solution:** This is expected. Ensure grace period covers compaction duration.

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

2. **Reduce Snapshot Window:**

```toml
max_snapshot_age_hours = 168  # 7 days instead of 30
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

2. **Reduce Snapshot Window:**

```toml
max_snapshot_age_hours = 168  # 7 days
```

3. **Increase Container Memory:**

```yaml
# compose.yml
services:
  compactor:
    mem_limit: 4g  # Increase from default
```

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

### Error: "Table retention enforcement failed"

**Full Message:**
```
WARN compactor::retention::enforcer: Table retention enforcement failed tenant_id=acme dataset_id=prod table_name=traces error=Failed to commit partition drop: ...
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

## Additional Resources

- [Phase 3 Operations Guide](phase3-operations.md)
- [Phase 3 Configuration Reference](phase3-configuration.md)
- [Compactor README](../../../src/compactor/README.md)
- [Integration Test Examples](../../../tests-integration/tests/compactor/)

## Getting Help

If you encounter issues not covered in this guide:

1. **Enable Debug Logging:** `RUST_LOG=debug,compactor=trace`
2. **Collect Logs:** Last 500 lines of compactor logs
3. **Gather Metrics:** `curl localhost:9091/metrics | grep compactor > metrics.txt`
4. **Configuration:** Share `signaldb.toml` (redact sensitive values)
5. **Open Issue:** https://github.com/cedricziel/signaldb/issues with above information
