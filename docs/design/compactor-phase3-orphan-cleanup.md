# Compactor Phase 3: Orphan File Cleanup System Design

## Overview

This document specifies the design for the orphan file cleanup mechanism in the Compactor Service Phase 3. The cleanup system identifies and removes Parquet data files that are no longer referenced by any live Iceberg snapshot, ensuring storage efficiency while maintaining strict safety guarantees.

## Safety-Critical Requirements

The orphan cleanup system is safety-critical and MUST NEVER delete files that are still referenced. Key safety principles:

1. **Conservative Detection**: When in doubt, retain the file
2. **Grace Period**: Apply time-based safety margins before deletion
3. **Multi-Phase Validation**: Verify orphan status multiple times before deletion
4. **Tenant Isolation**: Never scan or delete across tenant boundaries
5. **Atomic Operations**: Track deletion state to prevent partial cleanup
6. **Audit Trail**: Log all deletion decisions for post-mortem analysis

## System Architecture

### Component Structure

```text
OrphanCleanupService
├── OrphanDetector          // Identifies orphan candidates
│   ├── ManifestScanner     // Reads snapshot manifests
│   ├── ObjectStoreScanner  // Lists files in object store
│   └── ReferenceTracker    // Builds live file reference set
├── SafetyValidator         // Multi-phase safety checks
│   ├── GracePeriodChecker  // Time-based safety margin
│   ├── SnapshotVerifier    // Re-validates against current snapshots
│   └── ActiveWriteDetector // Ensures no concurrent writes
├── BatchDeleter            // Efficient batch deletion
│   ├── DeletionPlanner     // Groups files for batch operations
│   └── ProgressTracker     // Tracks deletion state
└── CleanupScheduler        // Manages cleanup intervals
```

### Configuration

Add to `CompactorConfig` in `src/common/src/config.rs`:

```rust
/// Orphan file cleanup configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrphanCleanupConfig {
    /// Enable orphan file cleanup (default: false)
    pub enabled: bool,

    /// Minimum age in hours for a file to be considered for cleanup (default: 24)
    /// This grace period prevents deletion of files from in-flight writes
    pub grace_period_hours: u64,

    /// Interval in hours between cleanup runs (default: 24)
    pub cleanup_interval_hours: u64,

    /// Maximum files to delete per batch (default: 1000)
    pub batch_size: usize,

    /// Maximum concurrent deletion operations (default: 10)
    pub max_concurrent_deletions: usize,

    /// Dry-run mode: identify orphans but don't delete (default: true)
    pub dry_run: bool,

    /// Re-validate orphan status before deletion (default: true)
    /// Adds extra safety by checking manifests again before deletion
    pub revalidate_before_delete: bool,

    /// Maximum age in hours for considering snapshots (default: 720 = 30 days)
    /// Files referenced by snapshots older than this are still protected
    pub max_snapshot_age_hours: u64,
}

impl Default for OrphanCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grace_period_hours: 24,
            cleanup_interval_hours: 24,
            batch_size: 1000,
            max_concurrent_deletions: 10,
            dry_run: true,
            revalidate_before_delete: true,
            max_snapshot_age_hours: 720, // 30 days
        }
    }
}
```

### TOML Configuration

Add to `signaldb.dist.toml`:

```toml
[compactor.orphan_cleanup]
# Orphan file cleanup removes data files no longer referenced by any snapshot
enabled = false                     # Must be explicitly enabled
grace_period_hours = 24            # Minimum age before cleanup (safety margin)
cleanup_interval_hours = 24        # How often to run cleanup
batch_size = 1000                  # Files per deletion batch
max_concurrent_deletions = 10      # Parallel deletion limit
dry_run = true                     # Test mode: log but don't delete
revalidate_before_delete = true    # Extra safety: check again before delete
max_snapshot_age_hours = 720       # Consider snapshots up to 30 days old
```

## Orphan Detection Algorithm

### Phase 1: Build Live File Reference Set

**Goal**: Create a complete set of all file paths referenced by live snapshots.

**Algorithm**:

```rust
async fn build_live_reference_set(
    &self,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<HashSet<String>> {
    let mut live_files = HashSet::new();

    // 1. Load table metadata
    let table = self.catalog_manager
        .get_table(tenant_id, dataset_id, table_name)
        .await?;

    let metadata = table.metadata();

    // 2. Get all snapshots within retention window
    let cutoff_time = Utc::now() - Duration::hours(self.config.max_snapshot_age_hours);
    let snapshots = metadata.snapshots()
        .iter()
        .filter(|s| {
            // Include snapshot if timestamp is within window
            s.timestamp_ms() > cutoff_time.timestamp_millis()
        })
        .collect::<Vec<_>>();

    log::info!(
        "Scanning {} snapshots for {}/{}/{} within {} hour window",
        snapshots.len(),
        tenant_id,
        dataset_id,
        table_name,
        self.config.max_snapshot_age_hours
    );

    // 3. For each snapshot, read manifest list and collect data file paths
    for snapshot in snapshots {
        let manifest_list_path = snapshot.manifest_list();

        // Read manifest list (contains manifest file paths)
        let manifest_files = self.read_manifest_list(manifest_list_path).await?;

        // Read each manifest file (contains data file paths)
        for manifest_path in manifest_files {
            let data_files = self.read_manifest(manifest_path).await?;

            for data_file in data_files {
                // Store full path from data file
                live_files.insert(data_file.file_path().to_string());
            }
        }
    }

    log::info!(
        "Found {} live data files referenced by snapshots for {}/{}/{}",
        live_files.len(),
        tenant_id,
        dataset_id,
        table_name
    );

    Ok(live_files)
}
```

**Key Implementation Details**:

1. **Snapshot Window**: Use `max_snapshot_age_hours` to limit how far back to scan
2. **Manifest Reading**: Use iceberg-rust's snapshot and manifest APIs
3. **Path Normalization**: Ensure consistent path format (absolute paths, normalized slashes)
4. **Memory Management**: For tables with millions of files, consider streaming or chunking

### Phase 2: Scan Object Store for Actual Files

**Goal**: List all data files in the table's object store location.

**Algorithm**:

```rust
async fn scan_object_store_files(
    &self,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<Vec<ObjectStoreFile>> {
    // Build table location path following Iceberg conventions
    // Format: /{tenant_slug}/{dataset_slug}/{table_name}/data/
    let table_location = self.catalog_manager
        .build_table_location(tenant_id, dataset_id, table_name);

    let data_path = format!("{}/data/", table_location);
    let path = object_store::path::Path::from(data_path.as_str());

    log::info!(
        "Scanning object store path {} for {}/{}/{}",
        data_path,
        tenant_id,
        dataset_id,
        table_name
    );

    let mut all_files = Vec::new();

    // List all objects under data/ path
    let list_stream = self.object_store
        .list(Some(&path))
        .await?;

    use futures::StreamExt;

    let mut stream = Box::pin(list_stream);
    while let Some(meta_result) = stream.next().await {
        let meta = meta_result?;

        // Only include .parquet files
        if meta.location.as_ref().ends_with(".parquet") {
            all_files.push(ObjectStoreFile {
                path: meta.location.to_string(),
                size_bytes: meta.size,
                last_modified: meta.last_modified,
            });
        }
    }

    log::info!(
        "Found {} parquet files in object store for {}/{}/{}",
        all_files.len(),
        tenant_id,
        dataset_id,
        table_name
    );

    Ok(all_files)
}

#[derive(Debug, Clone)]
struct ObjectStoreFile {
    path: String,
    size_bytes: usize,
    last_modified: DateTime<Utc>,
}
```

**Safety Considerations**:

1. **Tenant Isolation**: Path construction ensures we only scan within tenant/dataset boundaries
2. **Metadata Files**: Explicitly exclude manifest files, metadata.json, etc. (only .parquet)
3. **Error Handling**: Object store unavailable should abort cleanup, not assume no files exist

### Phase 3: Identify Orphan Candidates

**Goal**: Find files in object store NOT in live reference set.

**Algorithm**:

```rust
async fn identify_orphan_candidates(
    &self,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<Vec<OrphanCandidate>> {
    // Build live reference set from manifests
    let live_files = self.build_live_reference_set(
        tenant_id,
        dataset_id,
        table_name
    ).await?;

    // Scan object store
    let all_files = self.scan_object_store_files(
        tenant_id,
        dataset_id,
        table_name
    ).await?;

    let grace_period = Duration::hours(self.config.grace_period_hours as i64);
    let cutoff_time = Utc::now() - grace_period;

    let mut orphan_candidates = Vec::new();

    for file in all_files {
        // Safety check 1: Is file referenced by any snapshot?
        if live_files.contains(&file.path) {
            continue; // File is live, skip
        }

        // Safety check 2: Is file older than grace period?
        if file.last_modified > cutoff_time {
            log::debug!(
                "Skipping recent file {} (age < {} hours)",
                file.path,
                self.config.grace_period_hours
            );
            continue; // File too recent, might be in-flight write
        }

        // File is orphan candidate
        orphan_candidates.push(OrphanCandidate {
            path: file.path,
            size_bytes: file.size_bytes,
            last_modified: file.last_modified,
            table_identifier: format!("{}/{}/{}", tenant_id, dataset_id, table_name),
        });
    }

    log::info!(
        "Identified {} orphan candidates for {}/{}/{} (grace period: {} hours)",
        orphan_candidates.len(),
        tenant_id,
        dataset_id,
        table_name,
        self.config.grace_period_hours
    );

    Ok(orphan_candidates)
}

#[derive(Debug, Clone)]
struct OrphanCandidate {
    path: String,
    size_bytes: usize,
    last_modified: DateTime<Utc>,
    table_identifier: String,
}
```

### Phase 4: Safety Validation (Optional Re-validation)

**Goal**: Add extra safety by re-checking orphan status immediately before deletion.

**Algorithm**:

```rust
async fn validate_orphan_before_deletion(
    &self,
    candidate: &OrphanCandidate,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<bool> {
    if !self.config.revalidate_before_delete {
        return Ok(true); // Skip revalidation if disabled
    }

    // Reload table metadata (get latest snapshots)
    let table = self.catalog_manager
        .get_table(tenant_id, dataset_id, table_name)
        .await?;

    let metadata = table.metadata();

    // Check current snapshot only (most recent commit)
    if let Some(snapshot) = metadata.current_snapshot() {
        let manifest_list = self.read_manifest_list(snapshot.manifest_list()).await?;

        for manifest_path in manifest_list {
            let data_files = self.read_manifest(&manifest_path).await?;

            for data_file in data_files {
                if data_file.file_path() == candidate.path {
                    log::warn!(
                        "File {} is NOW referenced by current snapshot! Aborting deletion.",
                        candidate.path
                    );
                    return Ok(false); // File is no longer orphan!
                }
            }
        }
    }

    // File is still orphan
    Ok(true)
}
```

**When This Matters**:

- Time elapsed between identification and deletion
- Concurrent compaction or snapshot expiration operations
- Adds ~seconds of latency but significant safety improvement

## Batch Deletion Strategy

### Deletion Planner

**Goals**:
- Minimize object store API calls
- Track progress for resumability
- Handle partial failures gracefully

**Algorithm**:

```rust
async fn delete_orphans_batch(
    &self,
    candidates: Vec<OrphanCandidate>,
) -> Result<DeletionResult> {
    let mut deleted_count = 0;
    let mut failed_deletions = Vec::new();
    let mut total_bytes_freed = 0u64;

    // Process in batches for efficiency
    for batch in candidates.chunks(self.config.batch_size) {
        log::info!(
            "Processing deletion batch of {} files (dry_run={})",
            batch.len(),
            self.config.dry_run
        );

        // Apply safety validation if enabled
        let mut validated_batch = Vec::new();
        for candidate in batch {
            // Parse table identifier to re-validate
            let parts: Vec<&str> = candidate.table_identifier.split('/').collect();
            if parts.len() != 3 {
                log::error!("Invalid table identifier: {}", candidate.table_identifier);
                continue;
            }
            let (tenant_id, dataset_id, table_name) = (parts[0], parts[1], parts[2]);

            if self.validate_orphan_before_deletion(
                candidate,
                tenant_id,
                dataset_id,
                table_name
            ).await? {
                validated_batch.push(candidate);
            }
        }

        // Delete validated files
        for candidate in validated_batch {
            if self.config.dry_run {
                log::info!(
                    "[DRY-RUN] Would delete orphan file: {} ({} bytes, last_modified={})",
                    candidate.path,
                    candidate.size_bytes,
                    candidate.last_modified
                );
                deleted_count += 1;
                total_bytes_freed += candidate.size_bytes as u64;
            } else {
                match self.delete_file(&candidate.path).await {
                    Ok(_) => {
                        log::info!(
                            "Deleted orphan file: {} ({} bytes)",
                            candidate.path,
                            candidate.size_bytes
                        );
                        deleted_count += 1;
                        total_bytes_freed += candidate.size_bytes as u64;
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to delete orphan file {}: {}",
                            candidate.path,
                            e
                        );
                        failed_deletions.push((candidate.path.clone(), e.to_string()));
                    }
                }
            }
        }

        // Rate limiting between batches
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(DeletionResult {
        deleted_count,
        failed_count: failed_deletions.len(),
        total_bytes_freed,
        failed_deletions,
    })
}

#[derive(Debug)]
struct DeletionResult {
    deleted_count: usize,
    failed_count: usize,
    total_bytes_freed: u64,
    failed_deletions: Vec<(String, String)>,
}

async fn delete_file(&self, path: &str) -> Result<()> {
    let object_path = object_store::path::Path::from(path);
    self.object_store.delete(&object_path).await?;
    Ok(())
}
```

**Batch Size Considerations**:

- **Too small**: Excessive API calls, slower overall
- **Too large**: Risk of timeout, harder to resume on failure
- **Recommended**: 1000 files per batch (configurable)

### Progress Tracking

For long-running cleanup operations, optionally persist progress:

```rust
#[derive(Debug, Serialize, Deserialize)]
struct CleanupProgress {
    run_id: String,
    started_at: DateTime<Utc>,
    tenant_id: String,
    dataset_id: String,
    table_name: String,
    total_candidates: usize,
    processed_count: usize,
    deleted_count: usize,
    failed_count: usize,
    last_checkpoint_at: DateTime<Utc>,
}

// Store in catalog database for resumability
async fn checkpoint_progress(&self, progress: &CleanupProgress) -> Result<()> {
    // Store in a dedicated cleanup_progress table
    // Enables resuming if service restarts mid-cleanup
    sqlx::query(
        r#"
        INSERT INTO cleanup_progress (run_id, data, updated_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (run_id) DO UPDATE SET data = $2, updated_at = $3
        "#
    )
    .bind(&progress.run_id)
    .bind(serde_json::to_value(progress)?)
    .bind(Utc::now())
    .execute(&self.db_pool)
    .await?;

    Ok(())
}
```

## Integration with Snapshot Expiration

**Critical Ordering**: Orphan cleanup MUST run AFTER snapshot expiration.

### Why Order Matters

1. **Snapshot expiration** marks old snapshots for removal
2. This makes their data files **newly orphaned**
3. **Orphan cleanup** can then safely remove those files

**If run in wrong order**:
- Orphan cleanup scans before expiration → misses newly orphaned files
- Must wait for next cleanup cycle

### Coordination Strategy

```rust
impl CompactorService {
    async fn run_cleanup_cycle(&self) -> Result<()> {
        log::info!("Starting cleanup cycle");

        // Phase 1: Snapshot expiration (marks old snapshots for removal)
        if self.snapshot_expiration_config.enabled {
            log::info!("Running snapshot expiration");
            self.snapshot_expiration_service.run().await?;
        }

        // Phase 2: Orphan file cleanup (removes unreferenced files)
        if self.orphan_cleanup_config.enabled {
            log::info!("Running orphan file cleanup");
            // Small delay to ensure snapshot expiration commits are visible
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.orphan_cleanup_service.run().await?;
        }

        log::info!("Cleanup cycle completed");
        Ok(())
    }
}
```

## Multi-Tenant Isolation

### Tenant Boundary Enforcement

**Critical**: Never scan or delete files outside tenant/dataset boundaries.

**Implementation**:

1. **Iterate per tenant/dataset**:
```rust
async fn run_orphan_cleanup(&self) -> Result<()> {
    // Get all enabled tenants from config
    let tenants = self.catalog_manager.get_enabled_tenants().await?;

    for tenant in tenants {
        for dataset in &tenant.datasets {
            log::info!(
                "Running orphan cleanup for tenant={}, dataset={}",
                tenant.id,
                dataset.id
            );

            // Process each table type
            for table_name in &["traces", "logs", "metrics_gauge", /* ... */] {
                match self.cleanup_table(
                    &tenant.id,
                    &dataset.id,
                    table_name
                ).await {
                    Ok(result) => {
                        log::info!(
                            "Cleanup complete for {}/{}/{}: deleted {} files ({} bytes)",
                            tenant.id,
                            dataset.id,
                            table_name,
                            result.deleted_count,
                            result.total_bytes_freed
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Cleanup failed for {}/{}/{}: {}",
                            tenant.id,
                            dataset.id,
                            table_name,
                            e
                        );
                        // Continue with next table
                    }
                }
            }
        }
    }

    Ok(())
}
```

2. **Path Validation**: Ensure all operations stay within table boundaries
```rust
fn validate_path_within_table(
    &self,
    file_path: &str,
    table_location: &str,
) -> bool {
    // Ensure file path starts with table location
    file_path.starts_with(table_location)
}
```

3. **Object Store Prefix**: Use table-scoped listing
```rust
// Only list files under: /{tenant_slug}/{dataset_slug}/{table_name}/data/
let prefix = format!("{}/data/", table_location);
```

## Cleanup Scheduling

### Interval-Based Execution

```rust
pub struct CleanupScheduler {
    cleanup_service: Arc<OrphanCleanupService>,
    config: OrphanCleanupConfig,
    shutdown: CancellationToken,
}

impl CleanupScheduler {
    pub async fn run(&self) -> Result<()> {
        log::info!(
            "Starting orphan cleanup scheduler (interval: {} hours, dry_run: {})",
            self.config.cleanup_interval_hours,
            self.config.dry_run
        );

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    log::info!("Cleanup scheduler shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(
                    self.config.cleanup_interval_hours * 3600
                )) => {
                    log::info!("Starting scheduled orphan cleanup run");

                    match self.cleanup_service.run().await {
                        Ok(_) => {
                            log::info!("Scheduled orphan cleanup completed successfully");
                        }
                        Err(e) => {
                            log::error!("Scheduled orphan cleanup failed: {}", e);
                            // Continue running, retry next interval
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
```

**Scheduling Recommendations**:

- **Daily**: Cleanup interval = 24 hours (default)
- **Weekly**: Cleanup interval = 168 hours (lower storage pressure)
- **After compaction**: Run cleanup after major compaction cycles
- **Manual trigger**: Provide admin API endpoint for on-demand cleanup

## Error Handling

### Error Categories and Responses

1. **Object Store Unavailable**
   - **Error**: Can't list files or delete
   - **Response**: Abort cleanup, retry next cycle
   - **Rationale**: Better to skip than assume no files exist

2. **Catalog Unavailable**
   - **Error**: Can't read snapshots or manifests
   - **Response**: Abort cleanup, retry next cycle
   - **Rationale**: Can't build live reference set safely

3. **Manifest Read Failure**
   - **Error**: Corrupt or missing manifest
   - **Response**: Skip affected snapshot, log warning
   - **Rationale**: Other snapshots may still be valid

4. **Deletion Failure**
   - **Error**: Permission denied, file not found
   - **Response**: Log error, continue with remaining files
   - **Rationale**: Partial cleanup is better than no cleanup

5. **Concurrent Write Detected**
   - **Error**: New snapshot appeared during cleanup
   - **Response**: Abort current batch, restart from validation
   - **Rationale**: Ensure latest snapshot is considered

### Error Logging

```rust
log::error!(
    "Orphan cleanup failed for {}/{}/{}: {}",
    tenant_id,
    dataset_id,
    table_name,
    error
);

// Include context for debugging
log::error!(
    "Failed to read manifest {}: {} (snapshot_id={}, table={}/{}/{})",
    manifest_path,
    error,
    snapshot_id,
    tenant_id,
    dataset_id,
    table_name
);
```

## Metrics and Observability

### Prometheus Metrics

Add to `CompactionMetrics`:

```rust
pub struct OrphanCleanupMetrics {
    // Counters
    cleanup_runs_total: IntCounterVec,           // status={success,failed}
    files_scanned_total: IntCounterVec,          // tenant, dataset, table
    orphans_identified_total: IntCounterVec,     // tenant, dataset, table
    files_deleted_total: IntCounterVec,          // tenant, dataset, table
    deletion_failures_total: IntCounterVec,      // tenant, dataset, table, reason

    // Gauges
    last_cleanup_timestamp: IntGaugeVec,         // tenant, dataset, table
    bytes_freed_total: IntCounterVec,            // tenant, dataset, table

    // Histograms
    cleanup_duration_seconds: HistogramVec,      // tenant, dataset, table
    batch_size: HistogramVec,                    // number of files per batch
}
```

### Logging

**Info-level** (normal operations):
```rust
log::info!("Starting orphan cleanup for {}/{}/{}", tenant_id, dataset_id, table_name);
log::info!("Found {} orphan candidates (grace period: {} hours)", count, hours);
log::info!("Deleted {} orphan files, freed {} MB", count, mb);
```

**Debug-level** (detailed progress):
```rust
log::debug!("Scanning snapshot {} for live file references", snapshot_id);
log::debug!("File {} is orphan candidate (last_modified: {})", path, ts);
```

**Warn-level** (recoverable issues):
```rust
log::warn!("Skipping recent file {} (age {} hours < grace {} hours)", path, age, grace);
log::warn!("Manifest read failed for {}: {}", manifest_path, error);
```

**Error-level** (failures):
```rust
log::error!("Failed to delete orphan file {}: {}", path, error);
log::error!("Object store unavailable, aborting cleanup: {}", error);
```

## Testing Strategy

### Unit Tests

1. **Orphan Detection Logic**
   - Build reference set from mock snapshots
   - Identify orphans from mock object store listing
   - Grace period filtering

2. **Path Validation**
   - Tenant isolation checks
   - Path normalization
   - Boundary enforcement

3. **Batch Processing**
   - Batch size limits
   - Progress tracking
   - Partial failure handling

### Integration Tests

1. **End-to-End Cleanup**
   - Create orphan files (write then remove from snapshot)
   - Run cleanup
   - Verify files deleted

2. **Safety Tests**
   - Verify grace period prevents premature deletion
   - Ensure live files never deleted
   - Test tenant isolation (no cross-tenant cleanup)

3. **Snapshot Expiration Integration**
   - Expire snapshots
   - Run orphan cleanup
   - Verify newly orphaned files removed

4. **Concurrent Operations**
   - Run cleanup during active writes
   - Verify no interference
   - Check revalidation catches new references

### Test Fixtures

```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn create_orphan_scenario() -> (TestContext, Vec<String>) {
        let ctx = TestContext::new().await;

        // Write initial data
        let files_v1 = ctx.write_test_data("tenant1", "prod", "traces").await;

        // Compact (creates new files)
        ctx.run_compaction().await;

        // Old files from v1 are now orphans
        let orphan_files = files_v1;

        (ctx, orphan_files)
    }

    #[tokio::test]
    async fn test_identifies_orphans_after_compaction() {
        let (ctx, expected_orphans) = create_orphan_scenario().await;

        // Wait for grace period
        tokio::time::sleep(Duration::from_secs(ctx.grace_period_seconds + 1)).await;

        let candidates = ctx.orphan_service
            .identify_orphan_candidates("tenant1", "prod", "traces")
            .await
            .unwrap();

        let candidate_paths: HashSet<_> = candidates
            .iter()
            .map(|c| c.path.clone())
            .collect();

        for orphan in expected_orphans {
            assert!(
                candidate_paths.contains(&orphan),
                "Expected orphan {} not identified",
                orphan
            );
        }
    }

    #[tokio::test]
    async fn test_respects_grace_period() {
        let (ctx, _orphans) = create_orphan_scenario().await;

        // Don't wait for grace period
        let candidates = ctx.orphan_service
            .identify_orphan_candidates("tenant1", "prod", "traces")
            .await
            .unwrap();

        // Should find no candidates (too recent)
        assert_eq!(candidates.len(), 0, "Grace period not respected");
    }

    #[tokio::test]
    async fn test_never_deletes_live_files() {
        let ctx = TestContext::new().await;

        // Write data (all files are live)
        ctx.write_test_data("tenant1", "prod", "traces").await;

        // Fast-forward past grace period
        tokio::time::sleep(Duration::from_secs(ctx.grace_period_seconds + 1)).await;

        // Run cleanup
        let result = ctx.orphan_service
            .run_cleanup("tenant1", "prod", "traces")
            .await
            .unwrap();

        // Should delete nothing (all files live)
        assert_eq!(result.deleted_count, 0, "Live files were deleted!");
    }
}
```

## Implementation Phases

### Phase 3a: Detection Only (Week 1)

- Implement orphan detection algorithm
- Build live reference set from manifests
- Scan object store and compare
- Log orphan candidates (no deletion)
- Add metrics and observability

**Deliverable**: Dry-run mode that identifies orphans

### Phase 3b: Safe Deletion (Week 2)

- Implement batch deletion with grace period
- Add revalidation before deletion
- Implement progress tracking
- Add error handling and recovery
- Integration with snapshot expiration

**Deliverable**: Production-ready cleanup with safety guarantees

### Phase 3c: Testing & Validation (Week 3)

- Comprehensive integration tests
- Safety test suite
- Performance benchmarking
- Production rollout with dry-run enabled initially

**Deliverable**: Fully tested, production-ready feature

## Rollout Plan

1. **Deploy with `dry_run = true`** (default)
   - Monitor logs for orphan identification
   - Validate accuracy (no false positives)
   - Run for 1-2 weeks

2. **Enable for single test tenant**
   - Set `dry_run = false` for one tenant
   - Monitor deletion behavior
   - Verify no live files deleted

3. **Gradual rollout**
   - Enable for additional tenants incrementally
   - Monitor metrics and error rates
   - Keep grace period conservative (24+ hours)

4. **Production default**
   - Update default config to `enabled = true`
   - Document configuration options
   - Provide runbook for troubleshooting

## Security Considerations

1. **Authorization**: Only compactor service should run cleanup
2. **Audit Trail**: Log all deletion decisions with timestamps
3. **Rate Limiting**: Prevent accidental mass deletion
4. **Dry-run Default**: Require explicit opt-in for deletion
5. **Tenant Isolation**: Enforce strict boundaries

## Performance Considerations

1. **Object Store Listing**: Can be slow for large tables
   - Use streaming/pagination
   - Consider caching file lists

2. **Manifest Reading**: Can be I/O intensive
   - Reuse ParquetRewriter's manifest reading logic
   - Consider in-memory caching for recently read manifests

3. **Memory Usage**: Large reference sets
   - For tables with millions of files, use bloom filters or chunking

4. **Deletion Rate**: Avoid overwhelming object store
   - Batch deletions
   - Rate limit between batches

## Summary

This design provides a safety-critical orphan file cleanup system with:

- **Conservative detection** using manifest-based reference tracking
- **Multi-phase validation** with grace periods and optional revalidation
- **Tenant isolation** preventing cross-tenant operations
- **Batch deletion** for efficiency with progress tracking
- **Integration** with snapshot expiration ensuring correct ordering
- **Comprehensive error handling** for object store and catalog failures
- **Rich observability** through metrics and structured logging
- **Gradual rollout** with dry-run mode for safe deployment

The system prioritizes **safety over performance**, ensuring files are never deleted unless absolutely proven to be orphaned.
