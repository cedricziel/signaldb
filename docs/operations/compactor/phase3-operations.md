---
audience: operator
type: how-to
status: living
sources:
  - src/compactor/src/**
---

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
cargo run --bin signaldb-compactor 2>&1 | tee compactor.log

# Or in monolithic mode via the dev script (logs to .data/logs/monolithic.log)
./scripts/run-dev.sh

# Monitor logs. Note: retention logs use "[DRY RUN]" (space), orphan
# cleanup logs use "[DRY-RUN]" (hyphen) — match both:
tail -f .data/logs/monolithic.log | grep -E "DRY.RUN"
```

Look for log entries like:

```text
INFO compactor::retention::enforcer: [DRY RUN] Would drop expired partitions tenant_id=acme dataset_id=prod table_name=traces partitions_to_drop=48 bytes_to_reclaim=1073741824
INFO compactor::retention::enforcer: [DRY RUN] Would drop partition tenant_id=acme dataset_id=prod table_name=traces partition_hour=Some("492245") file_count=12 size_bytes=Some(10485760)
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
revalidate_before_delete = true
```

Start the compactor and monitor its stdout (or `.data/logs/monolithic.log` when using `./scripts/run-dev.sh`):

```bash
tail -f .data/logs/monolithic.log | grep -E "(orphan|cleanup)"
```

Look for:

```text
INFO compactor::orphan::detector: Starting orphan detection tenant_id=acme dataset_id=prod table_name=traces
INFO compactor::orphan::detector: Identified orphan candidates tenant_id=acme dataset_id=prod table_name=traces orphan_candidates=42
INFO compactor::orphan::cleaner: [DRY-RUN] Would delete orphan file path=acme/prod/traces/data/orphan-001.parquet size_bytes=10485760 last_modified=2026-02-04T10:00:00Z table=acme/prod/traces
INFO compactor::orphan::cleaner: Batch deletion complete would_delete=42 would_free_bytes=2147483648 failed=0 dry_run=true
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

All Phase 3 counters are exported at `localhost:9091/metrics` (see `src/compactor/src/http.rs` for the authoritative list). Counters are process-global — there are no per-tenant, per-dataset, or per-table labels. The only labelled metric is `compactor_orphan_cleanup_skipped_total{reason="live_files_threshold_exceeded"}`.

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
cargo run --bin signaldb-compactor

# Or restart monolithic service
systemctl restart signaldb
```

3. **Monitor Next Retention Cycle:**

```bash
# Wait for next retention check (check interval)
# Monitor logs for new cutoff (stdout, or monolithic.log with run-dev.sh)
tail -f .data/logs/monolithic.log | grep "Retention cutoff computed"

# Expected log:
# INFO compactor::retention::enforcer: Retention cutoff computed tenant_id=production dataset_id=default table_name=traces cutoff_timestamp=2026-01-26 10:00:00 UTC retention_period=14d source=Tenant
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
RUST_LOG=debug,compactor::retention=trace cargo run --bin signaldb-compactor 2>&1 | \
  grep "Retention cutoff computed"

# Example output:
# INFO compactor::retention::enforcer: Retention cutoff computed tenant_id=acme dataset_id=prod table_name=traces cutoff_timestamp=2026-01-25 09:00:00 UTC retention_period=7d source=Global
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

# Bytes reclaimed by Phase 3 so far
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

# Reduce snapshot window (less history to scan)
max_snapshot_age_hours = 168  # 7 days instead of 30

# Run less frequently (lower peak load)
cleanup_interval_hours = 48  # Every 2 days

# Bound memory by skipping tables with too many estimated live files
# (skips are counted in compactor_orphan_cleanup_skipped_total)
max_live_files_threshold = 500000
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

## Additional Resources

- [Phase 3 Configuration Reference](phase3-configuration.md)
- [Phase 3 Troubleshooting Guide](phase3-troubleshooting.md)
- [Compactor README](../../../src/compactor/README.md)
