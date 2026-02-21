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

# Check logs for errors
tail -100 .data/logs/compactor.log | grep ERROR

# Check metrics endpoint
curl -s localhost:9091/metrics | grep compactor

# Check retention last run
curl -s localhost:9091/metrics | grep compactor_retention_last_run

# Check recent orphan cleanup
curl -s localhost:9091/metrics | grep compactor_orphan_cleanup_runs_total
```

### Quick Status Check

```bash
# All-in-one status check
cat << 'EOF' > /tmp/compactor_status.sh
#!/bin/bash
echo "=== Compactor Status ==="
echo "Process: $(pgrep -f compactor | wc -l) running"
echo ""
echo "=== Recent Errors ==="
tail -50 .data/logs/compactor.log | grep ERROR | tail -5
echo ""
echo "=== Retention Metrics ==="
curl -s localhost:9091/metrics | grep -E "compactor_(partitions_dropped|retention_enforcement)" | tail -3
echo ""
echo "=== Orphan Cleanup Metrics ==="
curl -s localhost:9091/metrics | grep -E "compactor_(files_deleted|orphans_identified)" | tail -3
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

# 3. Check computed cutoff in logs
RUST_LOG=debug,compactor::retention=trace cargo run --bin compactor &
sleep 60 && tail -50 .data/logs/compactor.log | grep "retention cutoff"

# 4. List actual partitions
psql -h localhost -U signaldb signaldb -c "
  SELECT table_name, hour, file_count, total_bytes
  FROM iceberg_partitions
  WHERE hour < NOW() - INTERVAL '7 days'
  ORDER BY hour
  LIMIT 10;
"
```

**Common Causes and Solutions:**

| Cause | Verification | Solution |
|-------|-------------|----------|
| Retention disabled | `enabled = false` in config | Set `enabled = true` |
| Dry-run mode enabled | `dry_run = true` in config | Set `dry_run = false` |
| Grace period too large | Check `grace_period_hours` | Reduce grace period |
| No data old enough | Check partition timestamps | Wait for data to age |
| Retention check hasn't run | Check `compactor_retention_last_run` | Restart compactor or wait for interval |

**Example Fix:**

```toml
# Before (not working)
[compactor.retention]
enabled = false  # ← Problem: disabled

# After (working)
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 3600
traces_retention_days = 7
```

### Issue 2: Partitions Dropped Too Aggressively

**Symptoms:**
- More partitions dropped than expected
- Data deleted sooner than configured retention
- Unexpected partition drop logs

**Diagnostic Steps:**

```bash
# 1. Check effective retention configuration
RUST_LOG=debug,compactor::retention=trace cargo run --bin compactor &
sleep 30 && tail -100 .data/logs/compactor.log | grep -E "(tenant_override|dataset_override|retention cutoff)"

# 2. Check for configuration errors
grep -A 20 "compactor.retention" signaldb.toml

# 3. Verify grace period
grep "grace_period_hours" signaldb.toml
```

**Common Causes:**

1. **Incorrect Override Hierarchy:**

```toml
# Problem: Dataset override shorter than intended
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 30

[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces_retention_days = 3  # ← Accidentally 3 instead of 90
```

**Solution:** Review and fix retention periods in configuration.

2. **Environment Variable Override:**

```bash
# Check for unexpected environment variables
env | grep SIGNALDB_COMPACTOR_RETENTION

# Example problem:
# SIGNALDB_COMPACTOR_RETENTION_TRACES_RETENTION_DAYS=1  ← Overriding config file
```

**Solution:** Remove or correct environment variable overrides.

3. **Zero Grace Period:**

```toml
grace_period_hours = 0  # ← No safety margin
```

**Solution:** Use at least 1 hour grace period for production.

### Issue 3: Retention Check Not Running

**Symptoms:**
- `compactor_retention_last_run_timestamp` not updating
- No retention logs in recent time window
- Partitions not being evaluated

**Diagnostic Steps:**

```bash
# 1. Check compactor process is running
ps aux | grep compactor

# 2. Check for fatal errors at startup
head -100 .data/logs/compactor.log | grep -E "(ERROR|FATAL)"

# 3. Check retention scheduler logs
grep "retention scheduler" .data/logs/compactor.log

# 4. Check metrics for last run time
curl -s localhost:9091/metrics | grep compactor_retention_last_run_timestamp
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
# Check startup logs
head -50 .data/logs/compactor.log | grep -E "(validation|config|failed)"
```

**Solution:** Fix configuration errors and restart.

3. **Retention Check Interval Too Long:**

```toml
retention_check_interval_secs = 86400  # 24 hours - won't run often
```

**Solution:** Reduce interval for more frequent checks or wait longer.

### Issue 4: Snapshot Expiration Not Working

**Symptoms:**
- Snapshot count keeps growing
- `compactor_snapshots_expired_total` is 0
- Metadata size increasing

**Diagnostic Steps:**

```bash
# 1. Check snapshot count
psql -h localhost -U signaldb signaldb -c "
  SELECT table_name, COUNT(*) as snapshot_count
  FROM iceberg_snapshots
  GROUP BY table_name;
"

# 2. Check snapshots_to_keep config
grep "snapshots_to_keep" signaldb.toml

# 3. Check logs for snapshot expiration
grep "snapshot expiration" .data/logs/compactor.log | tail -20
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
grep "expire.*snapshot" .data/logs/compactor.log
```

**Solution:** Verify Phase 3 is fully deployed.

## Orphan Cleanup Issues

### Issue 5: Orphan Files Not Being Deleted

**Symptoms:**
- `compactor_orphans_identified_total` > 0
- `compactor_files_deleted_total` = 0
- Orphan files identified but not removed

**Diagnostic Steps:**

```bash
# 1. Check if cleanup is enabled and not in dry-run
grep -A 5 "compactor.orphan_cleanup" signaldb.toml

# 2. Check for deletion errors
grep -E "(orphan|delete|failed)" .data/logs/compactor.log | tail -20

# 3. Check revalidation logs
grep "revalidation" .data/logs/compactor.log | tail -10
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

# 2. Check file ages in orphan logs
grep "DRY-RUN.*Would delete" .data/logs/compactor.log | tail -10

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
- `compactor_orphan_cleanup_duration_seconds` very high

**Diagnostic Steps:**

```bash
# 1. Check file count being scanned
curl -s localhost:9091/metrics | grep compactor_files_scanned_total

# 2. Check batch size and rate limiting
grep -E "(batch_size|rate_limit)" signaldb.toml

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

3. **Add Rate Limiting:**

```toml
rate_limit_delay_ms = 1000  # 1 second between batches
```

4. **Run Less Frequently:**

```toml
cleanup_interval_hours = 48  # Every 2 days instead of daily
```

## Performance Issues

### Issue 8: High CPU Usage During Retention

**Symptoms:**
- CPU spikes during retention check
- Retention check duration > 5 minutes
- System slowdown during retention

**Diagnostic Steps:**

```bash
# 1. Check retention duration
curl -s localhost:9091/metrics | grep compactor_retention_enforcement_duration

# 2. Profile with CPU profiling
RUST_LOG=info cargo flamegraph --bin compactor

# 3. Check partition counts
psql -h localhost -U signaldb signaldb -c "
  SELECT table_name, COUNT(*) as partition_count
  FROM iceberg_partitions
  GROUP BY table_name;
"
```

**Solutions:**

1. **Increase Check Interval:**

```toml
retention_check_interval_secs = 7200  # Every 2 hours
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

# 2. Check reference set size
grep "reference set" .data/logs/compactor.log

# 3. Check file count
curl -s localhost:9091/metrics | grep compactor_files_scanned_total
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
# docker-compose.yml
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
# 1. Check recent partition drops
grep "dropped partition" .data/logs/compactor.log | tail -20

# 2. Check snapshot expiration
grep "expired snapshot" .data/logs/compactor.log | tail -20

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
retention_check_interval_secs = 7200  # Less frequent
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
# Check recent drops in logs
grep "dropped partition" .data/logs/compactor.log | \
  grep "$(date +%Y-%m-%d)" > /tmp/dropped_today.txt

# Review what was dropped
cat /tmp/dropped_today.txt
```

3. **Attempt Recovery:**

```sql
-- Check for snapshots before deletion
SELECT snapshot_id, committed_at, summary
FROM iceberg_snapshots
WHERE table_name = 'traces'
  AND committed_at < '2026-02-09 10:00:00'
ORDER BY committed_at DESC
LIMIT 10;

-- Time-travel query to verify data exists
SELECT COUNT(*) FROM traces
FOR SYSTEM_TIME AS OF '2026-02-09 09:00:00'
WHERE hour = '2026-01-25-10';
```

**Prevention:**

1. **Always Use Dry-Run First:**

```toml
[compactor.retention]
dry_run = true  # Test first
```

2. **Test on Non-Production Tenant:**

```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "test"
traces_retention_days = 1  # Test here first
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
RUST_LOG=debug,compactor=trace cargo run --bin compactor
```

**Persistent (config file):**

```bash
# Create logging config
cat > /tmp/log4rs.yml << EOF
appenders:
  stdout:
    kind: console
  file:
    kind: file
    path: .data/logs/compactor.log
    encoder:
      pattern: "[{d}] {l} {t} - {m}{n}"

root:
  level: info
  appenders:
    - stdout
    - file

loggers:
  compactor:
    level: trace  # Debug logging for compactor
  compactor::retention:
    level: trace  # Detailed retention logs
  compactor::orphan:
    level: trace  # Detailed orphan cleanup logs
EOF

# Run with logging config
RUST_LOG_CONFIG=/tmp/log4rs.yml cargo run --bin compactor
```

### Trace Specific Operation

**Trace Retention Enforcement:**

```bash
# Enable trace logging for retention only
RUST_LOG=info,compactor::retention=trace cargo run --bin compactor 2>&1 | \
  grep -E "(retention|cutoff|drop|partition)"
```

**Trace Orphan Cleanup:**

```bash
# Enable trace logging for orphan cleanup only
RUST_LOG=info,compactor::orphan=trace cargo run --bin compactor 2>&1 | \
  grep -E "(orphan|cleanup|delete|reference)"
```

### Verify Iceberg Operations

**Check Iceberg Metadata:**

```sql
-- PostgreSQL catalog

-- List tables
SELECT * FROM iceberg_tables;

-- List snapshots
SELECT * FROM iceberg_snapshots
WHERE table_name = 'traces'
ORDER BY committed_at DESC
LIMIT 10;

-- List partitions
SELECT * FROM iceberg_partitions
WHERE table_name = 'traces'
ORDER BY hour DESC
LIMIT 10;

-- Check manifest files
SELECT * FROM iceberg_manifests
WHERE snapshot_id = 'YOUR_SNAPSHOT_ID';
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

### Error: "Failed to drop partition"

**Full Message:**
```
ERROR compactor::retention: Failed to drop partition: tenant=acme dataset=prod table=traces hour=2026-01-25-10 error="Catalog error: ..."
```

**Causes:**
- Catalog connection lost
- Invalid partition specification
- Concurrent modification conflict

**Solutions:**
1. Check catalog connectivity:
   ```bash
   psql -h localhost -U signaldb -d signaldb -c "SELECT 1"
   ```

2. Verify partition format:
   ```sql
   SELECT DISTINCT hour FROM traces ORDER BY hour LIMIT 5;
   ```

3. Retry - operations are idempotent.

### Error: "Permission denied deleting file"

**Full Message:**
```
ERROR compactor::orphan: Failed to delete file: path=data/acme/prod/traces/hour=2026-01-01-10/data-001.parquet error="Permission denied"
```

**Causes:**
- Insufficient object store permissions
- File locked by another process
- Object store credentials invalid

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

### Error: "Snapshot not found"

**Full Message:**
```
ERROR compactor::iceberg: Snapshot not found: snapshot_id=123456 table=traces
```

**Causes:**
- Snapshot already expired
- Catalog metadata out of sync
- Concurrent snapshot expiration

**Solutions:**
1. Reload table metadata:
   ```sql
   REFRESH TABLE traces;
   ```

2. Increase `snapshots_to_keep` to retain more snapshots.

### Error: "Grace period violation"

**Full Message:**
```
WARN compactor::orphan: File younger than grace period: path=data/acme/prod/traces/hour=2026-02-09-09/data-001.parquet age=30m grace_period=24h
```

**Cause:** File is too recent to be cleaned up (expected behavior).

**Solution:** This is a warning, not an error. File will be eligible after grace period elapses.

## Additional Resources

- [Phase 3 Operations Guide](phase3-operations.md)
- [Phase 3 Configuration Reference](phase3-configuration.md)
- [Compactor README](../../src/compactor/README.md)
- [Integration Test Examples](../../tests-integration/tests/compactor/)

## Getting Help

If you encounter issues not covered in this guide:

1. **Enable Debug Logging:** `RUST_LOG=debug,compactor=trace`
2. **Collect Logs:** Last 500 lines of compactor logs
3. **Gather Metrics:** `curl localhost:9091/metrics | grep compactor > metrics.txt`
4. **Configuration:** Share `signaldb.toml` (redact sensitive values)
5. **Open Issue:** https://github.com/cedricziel/signaldb/issues with above information
