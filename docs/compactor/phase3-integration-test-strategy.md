# Phase 3 Integration Test Strategy - Retention Enforcement

## Overview

This document defines the comprehensive integration test strategy for Phase 3 of the Compactor Service, focusing on retention enforcement including retention cutoff computation, partition drop, snapshot expiration, and orphan file cleanup.

## Testing Principles

- **TDD Approach**: Write tests before implementing features
- **Realistic Environment**: Use testcontainers for PostgreSQL and MinIO
- **Concurrent Testing**: Verify safety under concurrent queries and writes
- **Failure Scenarios**: Test crash recovery, network failures, object store unavailability
- **Edge Cases**: Empty partitions, single-file partitions, zero retention, configuration boundaries
- **Isolation**: Each test should be independent and repeatable
- **Observability**: Verify metrics and logging for operational insights

## Test Infrastructure

### Test Fixtures

#### 1. Catalog Test Context

```rust
pub struct CatalogTestContext {
    pub container: ContainerAsync<Postgres>,
    pub catalog_manager: Arc<CatalogManager>,
    pub dsn: String,
}

impl CatalogTestContext {
    /// Creates a PostgreSQL-backed catalog for realistic testing
    pub async fn new() -> Result<Self>;

    /// Creates an in-memory catalog for fast unit-style tests
    pub async fn new_in_memory() -> Result<Self>;
}
```

#### 2. Storage Test Context

```rust
pub struct StorageTestContext {
    pub container: ContainerAsync<MinIO>,
    pub object_store: Arc<dyn ObjectStore>,
    pub dsn: Url,
}

impl StorageTestContext {
    /// Creates MinIO container with S3-compatible storage
    pub async fn new() -> Result<Self>;

    /// Creates in-memory storage for fast tests
    pub async fn new_in_memory() -> Result<Arc<InMemory>>;

    /// Lists all objects in storage (for verification)
    pub async fn list_all_objects(&self) -> Result<Vec<String>>;

    /// Gets object count for a specific prefix
    pub async fn count_objects(&self, prefix: &str) -> Result<usize>;
}
```

#### 3. Complete System Context

```rust
pub struct RetentionTestContext {
    pub catalog: CatalogTestContext,
    pub storage: StorageTestContext,
    pub compactor_config: CompactorConfig,
    pub metrics: CompactionMetrics,
}

impl RetentionTestContext {
    /// Full setup with PostgreSQL + MinIO
    pub async fn new() -> Result<Self>;

    /// Fast setup with in-memory implementations
    pub async fn new_in_memory() -> Result<Self>;

    /// Creates a table and populates it with test data
    pub async fn create_table_with_data(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        signal_type: SignalType,
        config: &DataGeneratorConfig,
    ) -> Result<TableInfo>;
}
```

### Test Data Generators

#### 1. Time-Partitioned Data Generator

```rust
pub struct DataGeneratorConfig {
    /// Number of partitions (days/hours) to generate
    pub partition_count: usize,

    /// Files per partition
    pub files_per_partition: usize,

    /// Rows per file
    pub rows_per_file: usize,

    /// Base timestamp (oldest data)
    pub base_timestamp: i64,

    /// Partition granularity (hour/day)
    pub partition_granularity: PartitionGranularity,
}

pub enum PartitionGranularity {
    Hour,
    Day,
}

impl DataGeneratorConfig {
    /// Generate traces with specified time distribution
    pub async fn generate_traces(
        &self,
        writer: &mut IcebergTableWriter,
    ) -> Result<Vec<PartitionInfo>>;

    /// Generate logs with specified time distribution
    pub async fn generate_logs(
        &self,
        writer: &mut IcebergTableWriter,
    ) -> Result<Vec<PartitionInfo>>;

    /// Generate metrics with specified time distribution
    pub async fn generate_metrics(
        &self,
        writer: &mut IcebergTableWriter,
    ) -> Result<Vec<PartitionInfo>>;
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: String,
    pub timestamp_range: (i64, i64),
    pub file_count: usize,
    pub row_count: usize,
}
```

#### 2. Snapshot Generator

```rust
pub struct SnapshotGenerator {
    catalog_manager: Arc<CatalogManager>,
}

impl SnapshotGenerator {
    /// Creates multiple snapshots for a table by writing batches
    pub async fn create_snapshots(
        &self,
        table_id: &str,
        count: usize,
    ) -> Result<Vec<SnapshotInfo>>;

    /// Verifies snapshot count matches expected
    pub async fn verify_snapshot_count(
        &self,
        table_id: &str,
        expected: usize,
    ) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
    pub manifest_count: usize,
}
```

## Test Suites

### Suite 1: Retention Cutoff Computation

Location: `tests-integration/tests/compactor/retention_cutoff.rs`

#### Test 1.1: Basic Retention Cutoff Calculation

```rust
#[tokio::test]
async fn test_retention_cutoff_basic() -> Result<()>
```

**Setup:**
- Create table with 30 days of data
- Set retention policy to 14 days
- Current time: Day 30

**Expected:**
- Cutoff timestamp = Day 30 - 14 days = Day 16
- Partitions older than Day 16 should be marked for deletion

#### Test 1.2: Per-Tenant Retention Overrides

```rust
#[tokio::test]
async fn test_retention_per_tenant_override() -> Result<()>
```

**Setup:**
- Global retention: 7 days
- Tenant A: 30 days (override)
- Tenant B: Default (7 days)
- Create data for both tenants

**Verify:**
- Tenant A data older than 30 days is dropped
- Tenant B data older than 7 days is dropped
- Tenant A data between 7-30 days is retained

#### Test 1.3: Per-Dataset Retention Overrides

```rust
#[tokio::test]
async fn test_retention_per_dataset_override() -> Result<()>
```

**Setup:**
- Global retention: 14 days
- Tenant A, Dataset production: 90 days
- Tenant A, Dataset staging: 3 days

**Verify:**
- Production data follows 90-day policy
- Staging data follows 3-day policy

#### Test 1.4: Zero Retention Policy

```rust
#[tokio::test]
async fn test_retention_zero_days() -> Result<()>
```

**Setup:**
- Set retention to 0 days
- Create table with data

**Expected:**
- No data is retained (immediate deletion)
- System handles this gracefully without errors

#### Test 1.5: Retention with Clock Skew

```rust
#[tokio::test]
async fn test_retention_with_clock_skew() -> Result<()>
```

**Setup:**
- Create partitions with future timestamps
- Set retention policy

**Verify:**
- Future-dated partitions are not dropped
- System handles time anomalies safely

### Suite 2: Partition Drop with Concurrent Queries

Location: `tests-integration/tests/compactor/partition_drop.rs`

#### Test 2.1: Partition Drop Without Query Interference

```rust
#[tokio::test]
async fn test_partition_drop_no_query_interference() -> Result<()>
```

**Setup:**
1. Create table with 10 partitions
2. Start background query reading all partitions
3. Trigger retention to drop oldest 3 partitions
4. Query continues running

**Verify:**
- Query completes successfully (no errors)
- Query returns data from retained partitions
- Dropped partitions are removed from metadata
- Query uses snapshot isolation (sees consistent view)

#### Test 2.2: Multiple Concurrent Queries During Drop

```rust
#[tokio::test]
async fn test_partition_drop_multiple_concurrent_queries() -> Result<()>
```

**Setup:**
1. Create table with 20 partitions
2. Start 5 concurrent queries with different time ranges
3. Trigger partition drop mid-query
4. All queries continue to completion

**Verify:**
- All queries complete successfully
- No query sees partial data or corruption
- Metrics show no query failures

#### Test 2.3: Partition Drop with Write Concurrency

```rust
#[tokio::test]
async fn test_partition_drop_with_concurrent_writes() -> Result<()>
```

**Setup:**
1. Create table with expired partitions
2. Start continuous writes to new partitions
3. Trigger retention enforcement
4. Verify writes continue uninterrupted

**Verify:**
- Writes succeed during partition drop
- Old partitions are dropped
- New data is retained
- No conflicts or transaction failures

#### Test 2.4: Empty Partition Drop

```rust
#[tokio::test]
async fn test_drop_empty_partition() -> Result<()>
```

**Setup:**
- Create partition metadata but no data files
- Mark partition as expired

**Verify:**
- Empty partition is removed cleanly
- No errors attempting to delete non-existent files

#### Test 2.5: Single-File Partition Drop

```rust
#[tokio::test]
async fn test_drop_single_file_partition() -> Result<()>
```

**Setup:**
- Create partition with exactly one data file
- Mark as expired

**Verify:**
- Single file is deleted
- Metadata is cleaned up
- No edge case errors

### Suite 3: Snapshot Expiration

Location: `tests-integration/tests/compactor/snapshot_expiration.rs`

#### Test 3.1: Basic Snapshot Expiration

```rust
#[tokio::test]
async fn test_snapshot_expiration_basic() -> Result<()>
```

**Setup:**
1. Create table and generate 10 snapshots over time
2. Configure snapshot retention: keep last 5 snapshots
3. Run retention enforcement

**Verify:**
- Oldest 5 snapshots are expired
- Newest 5 snapshots remain
- Manifest files for expired snapshots are cleaned up
- Table metadata references only retained snapshots

#### Test 3.2: Snapshot Expiration with Concurrent Writes

```rust
#[tokio::test]
async fn test_snapshot_expiration_concurrent_writes() -> Result<()>
```

**Setup:**
1. Start continuous writes (creating new snapshots)
2. Run snapshot expiration in parallel
3. Continue for 30 seconds

**Verify:**
- New snapshots are created successfully
- Old snapshots are expired without conflicts
- No transaction conflicts or lost data

#### Test 3.3: Snapshot Expiration with Active Queries

```rust
#[tokio::test]
async fn test_snapshot_expiration_active_queries() -> Result<()>
```

**Setup:**
1. Create 20 snapshots
2. Start query pinned to snapshot 5 (old)
3. Expire snapshots while query runs

**Verify:**
- Query completes successfully reading from snapshot 5
- Iceberg's snapshot isolation protects the query
- Snapshot files remain until query completes

#### Test 3.4: Snapshot Expiration Edge Cases

```rust
#[tokio::test]
async fn test_snapshot_expiration_edge_cases() -> Result<()>
```

**Test cases:**
- Table with only 1 snapshot (should not be deleted)
- Table with 0 snapshots (handle gracefully)
- Snapshot retention count exceeds actual snapshots

**Verify:**
- System handles edge cases without errors
- At least one snapshot is always retained

#### Test 3.5: Manifest File Cleanup After Expiration

```rust
#[tokio::test]
async fn test_manifest_cleanup_after_expiration() -> Result<()>
```

**Setup:**
1. Create 10 snapshots, each with unique manifest files
2. Expire oldest 7 snapshots
3. Query object store

**Verify:**
- Manifest files for expired snapshots are deleted
- Manifest files for retained snapshots remain
- Orphan manifests are detected and cleaned

### Suite 4: Orphan File Cleanup

Location: `tests-integration/tests/compactor/orphan_cleanup.rs`

#### Test 4.1: Detect and Remove Orphan Data Files

```rust
#[tokio::test]
async fn test_orphan_data_file_cleanup() -> Result<()>
```

**Setup:**
1. Create table and write data
2. Manually add orphan Parquet files to object store
3. Files are older than grace period
4. Run orphan cleanup

**Verify:**
- Orphan files are detected
- Orphan files are deleted
- Referenced files remain intact
- Metrics show correct orphan count

#### Test 4.2: Orphan Manifest File Cleanup

```rust
#[tokio::test]
async fn test_orphan_manifest_cleanup() -> Result<()>
```

**Setup:**
1. Create snapshots with manifest files
2. Manually add unreferenced manifest files
3. Files are older than grace period

**Verify:**
- Orphan manifests are detected and removed
- Referenced manifests are preserved

#### Test 4.3: Grace Period Enforcement

```rust
#[tokio::test]
async fn test_orphan_cleanup_grace_period() -> Result<()>
```

**Setup:**
1. Create orphan files with various ages:
   - 1 hour old
   - 12 hours old
   - 25 hours old
2. Grace period: 24 hours
3. Run cleanup

**Verify:**
- Files older than 24 hours are deleted
- Files younger than 24 hours are preserved
- Prevents deletion of in-flight data

#### Test 4.4: Orphan Cleanup with Concurrent Writes

```rust
#[tokio::test]
async fn test_orphan_cleanup_concurrent_writes() -> Result<()>
```

**Setup:**
1. Start continuous writes
2. Run orphan cleanup simultaneously
3. Some files may temporarily appear orphaned

**Verify:**
- No active files are deleted
- Grace period prevents premature deletion
- Writes succeed without interruption

#### Test 4.5: Object Store Listing Pagination

```rust
#[tokio::test]
async fn test_orphan_cleanup_large_dataset() -> Result<()>
```

**Setup:**
- Create table with 10,000+ files
- Add orphan files throughout
- Object store requires pagination

**Verify:**
- All objects are scanned (pagination works)
- Orphans are detected across all pages
- Memory usage remains bounded

#### Test 4.6: Verify Files Are Actually Deleted

```rust
#[tokio::test]
async fn test_orphan_cleanup_verifies_deletion() -> Result<()>
```

**Setup:**
1. Create orphan files in MinIO
2. Record object paths before cleanup
3. Run cleanup
4. Query object store directly

**Verify:**
- Objects are actually deleted from storage
- `list_objects` confirms removal
- Storage space is reclaimed

### Suite 5: Failure Scenarios

Location: `tests-integration/tests/compactor/retention_failures.rs`

#### Test 5.1: Crash During Partition Drop

```rust
#[tokio::test]
async fn test_crash_during_partition_drop() -> Result<()>
```

**Simulation:**
1. Start partition drop
2. Cancel/abort task mid-operation
3. Restart system
4. Run retention again

**Verify:**
- System recovers gracefully
- Partially dropped partitions are completed
- No data corruption
- Idempotency: running again is safe

#### Test 5.2: Object Store Unavailable

```rust
#[tokio::test]
async fn test_retention_object_store_unavailable() -> Result<()>
```

**Simulation:**
1. Stop MinIO container mid-operation
2. Retention enforcement attempts to delete files

**Verify:**
- Errors are logged
- System retries with backoff
- Catalog updates are not committed (transactional)
- System remains in consistent state

#### Test 5.3: Catalog Database Connection Loss

```rust
#[tokio::test]
async fn test_retention_catalog_connection_loss() -> Result<()>
```

**Simulation:**
1. Stop PostgreSQL container during retention
2. Retention attempts to update metadata

**Verify:**
- Errors are logged
- Transactions are rolled back
- Object store deletions are not committed
- System retries on recovery

#### Test 5.4: Partial Snapshot Expiration Failure

```rust
#[tokio::test]
async fn test_partial_snapshot_expiration_failure() -> Result<()>
```

**Setup:**
1. Expire 5 snapshots
2. Fail after expiring 2 snapshots
3. Restart and run again

**Verify:**
- Remaining 3 snapshots are expired
- No duplicate deletions
- System tracks progress

#### Test 5.5: Orphan Cleanup Interrupted

```rust
#[tokio::test]
async fn test_orphan_cleanup_interrupted() -> Result<()>
```

**Simulation:**
1. Start orphan cleanup with 1000 orphans
2. Cancel after deleting 500
3. Run cleanup again

**Verify:**
- Remaining 500 orphans are deleted
- No double-deletion attempts
- Cleanup is resumable

### Suite 6: Cross-Feature Integration

Location: `tests-integration/tests/compactor/retention_integration.rs`

#### Test 6.1: Compaction + Retention Pipeline

```rust
#[tokio::test]
async fn test_compaction_then_retention() -> Result<()>
```

**Workflow:**
1. Write small files to old partitions
2. Run compaction (consolidates files)
3. Wait for retention period
4. Run retention enforcement

**Verify:**
- Compaction succeeds
- Compacted partitions are later dropped by retention
- Both operations work together seamlessly

#### Test 6.2: Retention Across All Signal Types

```rust
#[tokio::test]
async fn test_retention_all_signal_types() -> Result<()>
```

**Setup:**
- Create traces, logs, and metrics tables
- Each with different retention policies
- Run retention enforcement

**Verify:**
- Each signal type respects its policy
- No cross-table interference

#### Test 6.3: Multi-Tenant Retention Isolation

```rust
#[tokio::test]
async fn test_multi_tenant_retention_isolation() -> Result<()>
```

**Setup:**
- 3 tenants with different retention policies
- Each tenant has expired data
- Run retention for all tenants

**Verify:**
- Each tenant's data is dropped according to its policy
- No cross-tenant data deletion
- Tenant isolation is maintained

#### Test 6.4: Full Retention Cycle End-to-End

```rust
#[tokio::test]
async fn test_full_retention_cycle() -> Result<()>
```

**Workflow:**
1. Ingest data via Acceptor → Writer
2. Data is written to Iceberg tables
3. Compactor runs periodically
4. Retention enforcer runs
5. Orphan cleanup runs

**Verify:**
- Complete pipeline works end-to-end
- Data ages out correctly
- Storage is reclaimed
- Metrics reflect operations

## Test Execution Strategy

### Fast Tests (In-Memory)

Use in-memory implementations for fast feedback:

```bash
cargo test -p tests-integration retention -- --test-threads=1
```

**Target runtime:** < 10 seconds for unit-style tests

### Full Integration Tests (Containers)

Use testcontainers for realistic testing:

```bash
cargo test -p tests-integration --test retention_integration -- --ignored
```

**Target runtime:** < 5 minutes for full suite

### CI/CD Integration

```yaml
# .github/workflows/compactor-phase3-tests.yml
name: Compactor Phase 3 Tests

on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Install Docker
        run: # ...
      - name: Run Phase 3 Tests
        run: cargo test -p tests-integration --test retention_integration
```

## Metrics and Observability

Each test should verify that appropriate metrics are emitted:

### Retention Metrics

```rust
pub struct RetentionMetrics {
    pub partitions_evaluated: u64,
    pub partitions_dropped: u64,
    pub snapshots_expired: u64,
    pub orphan_files_found: u64,
    pub orphan_files_deleted: u64,
    pub bytes_reclaimed: u64,
    pub errors_encountered: u64,
}
```

### Test Assertions

```rust
// Verify metrics after retention run
let metrics = retention_service.metrics();
assert!(metrics.partitions_dropped > 0, "Expected partitions to be dropped");
assert_eq!(metrics.errors_encountered, 0, "Should have no errors");
assert!(metrics.bytes_reclaimed > 0, "Expected storage to be reclaimed");
```

## Performance Benchmarks

Include performance tests to ensure retention scales:

### Test 6.5: Large-Scale Retention

```rust
#[tokio::test]
#[ignore] // Run explicitly for performance testing
async fn bench_retention_10k_partitions() -> Result<()>
```

**Setup:**
- 10,000 partitions
- 50% expired

**Target:**
- Complete in < 60 seconds
- Memory usage < 500MB

### Test 6.6: Orphan Cleanup Scale

```rust
#[tokio::test]
#[ignore]
async fn bench_orphan_cleanup_100k_files() -> Result<()>
```

**Setup:**
- 100,000 files in object store
- 10% orphaned

**Target:**
- Complete in < 5 minutes
- Streaming/pagination prevents OOM

## Implementation Checklist

- [ ] Implement `CatalogTestContext` with PostgreSQL support
- [ ] Implement `StorageTestContext` with MinIO support
- [ ] Implement `DataGeneratorConfig` with time-partitioned generation
- [ ] Implement `SnapshotGenerator` helper
- [ ] Create retention_cutoff.rs test suite
- [ ] Create partition_drop.rs test suite
- [ ] Create snapshot_expiration.rs test suite
- [ ] Create orphan_cleanup.rs test suite
- [ ] Create retention_failures.rs test suite
- [ ] Create retention_integration.rs test suite
- [ ] Add CI/CD workflow for Phase 3 tests
- [ ] Document test execution in README
- [ ] Add performance benchmarks
- [ ] Verify all tests pass with both in-memory and container backends

## References

- Existing compactor tests: `tests-integration/tests/compactor/`
- TestContainers example: `tests-integration/src/test_helpers.rs`
- Iceberg snapshot management: Apache Iceberg spec
- TDD principles: `docs/ai/development.md`
