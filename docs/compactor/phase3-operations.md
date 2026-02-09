# Phase 3 Operations Guide

This guide covers day-to-day operations for SignalDB Compactor Phase 3: Retention Enforcement and Lifecycle Management.

## Table of Contents

- [Overview](#overview)
- [Enabling Retention Enforcement](#enabling-retention-enforcement)
- [Enabling Orphan Cleanup](#enabling-orphan-cleanup)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Common Operations](#common-operations)
- [Emergency Procedures](#emergency-procedures)
- [Performance Tuning](#performance-tuning)

## Overview

Phase 3 provides automatic data lifecycle management through:

1. **Retention Enforcement**: Drops expired partitions based on configurable policies
2. **Snapshot Expiration**: Maintains bounded metadata by expiring old snapshots
3. **Orphan Cleanup**: Reclaims storage by deleting unreferenced files

All operations respect Iceberg's transactional guarantees and snapshot isolation.

## Enabling Retention Enforcement

### Step 1: Plan Your Retention Policies

Determine appropriate retention periods for each signal type:

| Signal Type | Typical Retention | Production Example |
|-------------|------------------|-------------------|
| Traces | 7-30 days | 7 days (dev), 30 days (prod) |
| Logs | 3-14 days | 3 days (dev), 7 days (prod) |
| Metrics | 30-90 days | 30 days (dev), 90 days (prod) |

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
retention_check_interval_secs = 3600  # Check every hour

# Global defaults
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30

# Safety settings
grace_period_hours = 1   # Safety margin
timezone = "UTC"         # For logging
snapshots_to_keep = 5    # Keep last 5 snapshots
```

### Step 3: Test with Dry-Run Mode

Start the compactor with dry-run enabled:

```bash
# Start compactor
cargo run --bin compactor

# Or in monolithic mode
cargo run --bin signaldb

# Monitor logs
tail -f .data/logs/compactor.log | grep "DRY-RUN"
```

Look for log entries like:

```
[2026-02-09T10:00:00Z INFO compactor::retention] [DRY-RUN] Would drop partition: tenant=acme dataset=prod table=traces hour=2026-01-25-10
[2026-02-09T10:00:00Z INFO compactor::retention] [DRY-RUN] Retention run complete: partitions_evaluated=720 partitions_to_drop=48
```

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
traces_retention_days = 1  # 1 day for fast testing

[[compactor.retention.tenant_overrides]]
tenant_id = "test"
traces_retention_days = 1
logs_retention_days = 1
metrics_retention_days = 1
```

Restart the compactor and verify:

```bash
# Check partitions before
echo "SELECT DISTINCT hour FROM traces ORDER BY hour DESC LIMIT 5;" | \
  psql -h localhost -U signaldb signaldb

# Wait for retention cycle (check interval + processing time)
# Typically 1-5 minutes

# Check partitions after
echo "SELECT DISTINCT hour FROM traces ORDER BY hour DESC LIMIT 5;" | \
  psql -h localhost -U signaldb signaldb

# Verify old partitions are gone
```

### Step 5: Rollout to Production

After successful test tenant validation:

```toml
[compactor.retention]
enabled = true
dry_run = false
retention_check_interval_secs = 3600

# Production retention periods
traces_retention_days = 7
logs_retention_days = 3
metrics_retention_days = 30

# Production tenant overrides
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 30  # Keep production traces longer
logs_retention_days = 7
metrics_retention_days = 90

# Critical dataset overrides
[[compactor.retention.tenant_overrides.dataset_overrides]]
dataset_id = "critical"
traces_retention_days = 90  # Critical data kept 90 days
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
revalidate_before_delete = true
```

Start the compactor and monitor logs:

```bash
tail -f .data/logs/compactor.log | grep -E "(orphan|cleanup)"
```

Look for:

```
[2026-02-09T10:00:00Z INFO compactor::orphan] Starting orphan cleanup scan: tenant=acme dataset=prod table=traces
[2026-02-09T10:00:15Z INFO compactor::orphan] [DRY-RUN] Identified 42 orphan files (total size: 2.1 GB)
[2026-02-09T10:00:15Z INFO compactor::orphan] [DRY-RUN] Would delete: data/acme/prod/traces/hour=2026-01-01-10/orphan-001.parquet (age=5d)
[2026-02-09T10:00:15Z INFO compactor::orphan] [DRY-RUN] Cleanup scan complete: files_scanned=1024 orphans_found=42
```

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
revalidate_before_delete = true  # Extra safety
```

Restart and monitor:

```bash
# Monitor deletion progress
watch -n 5 'curl -s localhost:9091/metrics | grep compactor_files_deleted_total'

# Check logs for errors
tail -f .data/logs/compactor.log | grep -E "(ERROR|deleted_total)"
```

### Step 3: Verify Storage Reclamation

After cleanup runs:

```bash
# Check metrics
curl -s localhost:9091/metrics | grep -E "compactor_(orphans_identified|files_deleted|bytes_freed)"

# Example output:
# compactor_orphans_identified_total{tenant="acme",dataset="prod",table="traces"} 42
# compactor_files_deleted_total{tenant="acme",dataset="prod",table="traces"} 42
# compactor_bytes_freed_total{tenant="acme",dataset="prod",table="traces"} 2147483648
```

**Validation:**
- `files_deleted_total` should equal `orphans_identified_total`
- `bytes_freed_total` shows actual storage reclaimed
- No deletion failures (`compactor_deletion_failures_total` = 0)

## Monitoring and Metrics

### Key Metrics to Monitor

#### Retention Enforcement

**Partitions Dropped:**
```promql
# Total partitions dropped per tenant (last 24h)
sum by (tenant) (
  increase(compactor_partitions_dropped_total[24h])
)

# Partitions dropped by signal type
sum by (signal) (
  increase(compactor_partitions_dropped_total[24h])
)
```

**Retention Duration:**
```promql
# Average retention enforcement duration
rate(compactor_retention_enforcement_duration_seconds_sum[5m])
/
rate(compactor_retention_enforcement_duration_seconds_count[5m])
```

#### Orphan Cleanup

**Storage Reclaimed:**
```promql
# Total storage freed per tenant (last 24h)
sum by (tenant) (
  increase(compactor_bytes_freed_total[24h])
)

# Storage reclaimed rate (bytes/second)
rate(compactor_bytes_freed_total[5m])
```

**Cleanup Success Rate:**
```promql
# Success rate (should be ~100%)
sum(rate(compactor_files_deleted_total[5m]))
/
sum(rate(compactor_orphans_identified_total[5m]))
```

**Deletion Failures:**
```promql
# Should be 0 or very low
sum by (tenant, table) (
  increase(compactor_deletion_failures_total[1h])
)
```

### Recommended Alerts

#### Critical Alerts

**High Deletion Failure Rate:**
```yaml
alert: CompactorHighDeletionFailureRate
expr: |
  sum by (tenant) (
    rate(compactor_deletion_failures_total[5m])
  ) > 0.01
for: 10m
labels:
  severity: critical
annotations:
  summary: "Compactor deletion failures on {{ $labels.tenant }}"
  description: "{{ $value }} deletion failures/sec"
```

**Retention Enforcement Stuck:**
```yaml
alert: CompactorRetentionStuck
expr: |
  time() - compactor_retention_last_run_timestamp > 7200
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
  sum by (tenant) (
    increase(compactor_orphans_identified_total[24h])
  )
  /
  sum by (tenant) (
    increase(compactor_files_scanned_total[24h])
  ) > 0.1
for: 1h
labels:
  severity: warning
annotations:
  summary: "High orphan file rate: {{ $labels.tenant }}"
  description: "{{ $value | humanizePercentage }} of files are orphaned"
```

### Grafana Dashboard

Example dashboard queries:

**Panel: Storage Reclaimed (Bytes)**
```promql
sum by (tenant) (
  increase(compactor_bytes_freed_total[24h])
)
```

**Panel: Partitions Dropped Over Time**
```promql
sum by (signal) (
  rate(compactor_partitions_dropped_total[5m])
) * 300
```

**Panel: Cleanup Duration**
```promql
histogram_quantile(0.95,
  rate(compactor_orphan_cleanup_duration_seconds_bucket[5m])
)
```

## Common Operations

### Adjusting Retention Periods

To change retention for a tenant:

1. **Update Configuration:**

```toml
[[compactor.retention.tenant_overrides]]
tenant_id = "production"
traces_retention_days = 14  # Changed from 30 to 14
```

2. **Reload Configuration:**

```bash
# Restart compactor (graceful)
pkill -TERM compactor
cargo run --bin compactor

# Or restart monolithic service
systemctl restart signaldb
```

3. **Monitor Next Retention Cycle:**

```bash
# Wait for next retention check (check interval)
# Monitor logs for new cutoff
tail -f .data/logs/compactor.log | grep "retention cutoff"

# Expected log:
# [INFO] Computed retention cutoff: tenant=production signal=traces cutoff=2026-01-26T10:00:00Z
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
# retention_check_interval_secs = 60  # 1 minute
# Then restart
```

### Force Immediate Orphan Cleanup

To trigger orphan cleanup immediately:

```bash
# Restart compactor (cleanup runs on startup)
systemctl restart signaldb-compactor

# Monitor progress
tail -f .data/logs/compactor.log | grep orphan
```

### Verify Retention Cutoff Computation

To check what the current retention cutoff would be:

```bash
# Enable debug logging
RUST_LOG=debug,compactor::retention=trace cargo run --bin compactor

# Look for cutoff computation logs
tail -f .data/logs/compactor.log | grep "Computed retention cutoff"

# Example output:
# [DEBUG] Computed retention cutoff: tenant=acme dataset=prod signal=traces cutoff=2026-01-25T09:00:00Z grace_period=1h
```

### Inspect Orphan Candidates

To see what files would be identified as orphans (without deleting):

```bash
# Enable dry-run mode
# In signaldb.toml:
[compactor.orphan_cleanup]
enabled = true
dry_run = true

# Restart and check logs
tail -f .data/logs/compactor.log | grep "DRY-RUN.*Would delete"

# Example output:
# [INFO] [DRY-RUN] Would delete: data/acme/prod/traces/hour=2026-01-01-10/orphan-001.parquet (age=5d size=10MB)
```

### Check Storage Utilization

To see how much storage is being used:

```bash
# PostgreSQL catalog query
psql -h localhost -U signaldb signaldb -c "
  SELECT
    tenant_id,
    dataset_id,
    table_name,
    COUNT(*) as partition_count,
    SUM(file_count) as total_files,
    pg_size_pretty(SUM(total_bytes)) as total_size
  FROM iceberg_tables
  GROUP BY tenant_id, dataset_id, table_name
  ORDER BY SUM(total_bytes) DESC;
"

# Check object store directly (S3 example)
aws s3 ls s3://signaldb-data/ --recursive --summarize | grep "Total Size"
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

**Investigate:**
```bash
# Check revalidation logs
grep "Revalidation" .data/logs/compactor.log

# Check for concurrent writes
grep "concurrent" .data/logs/compactor.log

# Verify grace period is adequate
grep "grace_period" .data/logs/compactor.log
```

### Emergency: Restore Accidentally Deleted Data

Retention enforcement drops partitions, but Iceberg keeps snapshots:

1. **Identify Snapshot Before Deletion:**

```sql
-- Query Iceberg metadata
SELECT snapshot_id, committed_at, summary
FROM iceberg_snapshots
WHERE table_name = 'traces'
  AND committed_at < '2026-02-09 10:00:00'
ORDER BY committed_at DESC
LIMIT 5;
```

2. **Time-Travel Query to Old Snapshot:**

```sql
-- Query data as of old snapshot
SELECT * FROM traces
FOR SYSTEM_TIME AS OF '2026-02-09 09:00:00'
WHERE hour = '2026-01-25-10';
```

3. **Restore Partition (requires manual Iceberg operation):**

Contact SignalDB support for partition restoration procedures.

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
retention_check_interval_secs = 7200  # Every 2 hours

# Reduce snapshots to keep (less metadata to process)
snapshots_to_keep = 3

# Schedule during low-traffic hours (requires cron-based execution)
# retention_check_cron = "0 2 * * *"  # 2 AM daily
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

# Reduce snapshot window (less history to scan)
max_snapshot_age_hours = 168  # 7 days instead of 30

# Run less frequently (lower peak load)
cleanup_interval_hours = 48  # Every 2 days

# Add rate limiting between batches
rate_limit_delay_ms = 1000  # 1 second between batches
```

**Memory Optimization:**
```rust
// For very large tables (millions of files), consider:
// - Incremental scanning (scan per partition)
// - Bloom filters for reference set
// - Database-backed reference set instead of in-memory HashSet
```

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
retention_check_interval_secs = 7200

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

## Additional Resources

- [Phase 3 Configuration Reference](phase3-configuration.md)
- [Phase 3 Troubleshooting Guide](phase3-troubleshooting.md)
- [Phase 3 Implementation Plan](phase3-implementation-plan.md)
- [Compactor README](../../src/compactor/README.md)
