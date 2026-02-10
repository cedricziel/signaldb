# Changelog

All notable changes to the SignalDB Compactor Service will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-02-09

### Added - Phase 3: Comprehensive Retention & Lifecycle Management

#### Retention Enforcement
- **3-Tier Policy Hierarchy**: Global defaults → Tenant overrides → Dataset overrides for flexible retention configuration
- **Per-Signal Type Policies**: Separate retention settings for traces, logs, and metrics
- **Retention Cutoff Computation**: Timezone-aware cutoff calculation with grace period protection
- **Automatic Partition Dropping**: Identify and drop expired partitions based on retention policies
- **Dry-Run Mode**: Test retention policies without actual data deletion
- **Grace Period Protection**: Configurable safety margin to prevent premature deletion due to clock skew

#### Snapshot Expiration
- **Bounded Metadata**: Keep configurable number of recent snapshots (default: 5) to prevent metadata bloat
- **Automatic Expiration**: Expire old snapshots while ensuring at least one snapshot always remains
- **Coordinated Cleanup**: Snapshot expiration runs before orphan cleanup for efficient storage reclamation

#### Orphan File Cleanup
- **4-Phase Detection Algorithm**:
  1. Build live file reference set from all snapshots
  2. Scan object store for all Parquet files
  3. Identify orphan candidates (unreferenced + older than grace period)
  4. Optional revalidation before deletion (race condition protection)
- **Safety-First Design**:
  - 24-hour grace period default prevents deletion of in-flight writes
  - Multi-phase validation catches concurrent write races
  - Tenant isolation enforced through path validation
  - Dry-run mode for safe testing
- **Batch Processing**: Configurable batch sizes with progress tracking for resumability
- **Checkpoint-Based Progress**: Resume cleanup operations after crashes or interruptions

#### Configuration
- Added `[compactor.retention]` configuration section with:
  - Per-signal type retention days (`traces_retention_days`, `logs_retention_days`, `metrics_retention_days`)
  - Grace period (`grace_period_hours`)
  - Timezone configuration (`timezone`)
  - Snapshot retention (`snapshots_to_keep`)
  - Tenant and dataset override arrays
- Added `[compactor.orphan_cleanup]` configuration section with:
  - Grace period (`grace_period_hours`)
  - Cleanup interval (`cleanup_interval_hours`)
  - Batch size (`batch_size`)
  - Safety options (`revalidate_before_delete`, `dry_run`)

#### Metrics
- **Retention Metrics**:
  - `compactor_retention_cutoffs_computed_total` - Cutoffs computed per tenant/dataset/signal
  - `compactor_partitions_evaluated_total` - Partitions checked for expiration
  - `compactor_partitions_dropped_total` - Partitions successfully dropped
  - `compactor_snapshots_expired_total` - Snapshots expired per table
  - `compactor_retention_enforcement_duration_seconds` - Enforcement duration histogram
- **Orphan Cleanup Metrics**:
  - `compactor_orphan_cleanup_runs_total` - Cleanup runs executed
  - `compactor_files_scanned_total` - Files scanned in object store
  - `compactor_orphans_identified_total` - Orphan files identified
  - `compactor_files_deleted_total` - Files successfully deleted
  - `compactor_deletion_failures_total` - Deletion failures
  - `compactor_bytes_freed_total` - Storage reclaimed
  - `compactor_orphan_cleanup_duration_seconds` - Cleanup duration histogram

#### Testing
- **19 Integration Tests** covering:
  - Retention cutoff computation (5 tests)
  - Partition drop with isolation (5 tests)
  - Snapshot expiration (4 tests)
  - Orphan file cleanup (5 tests)
- Multi-tenant isolation verified
- Concurrent operation safety validated
- Dry-run mode fully tested

#### Documentation
- Added comprehensive `README.md` with:
  - Configuration examples
  - Architecture diagrams
  - Usage instructions
  - Troubleshooting guide
  - Metrics documentation
- Updated Phase 3 implementation plan with completion status
- Added retention policy hierarchy documentation

#### Internal Changes
- New modules: `retention/`, `orphan/`
- Extended Iceberg integration: `iceberg/snapshot.rs`, `iceberg/manifest.rs`, `iceberg/partition.rs`
- Integrated retention and cleanup schedulers into `CompactorService`
- Added catalog schema extensions for compactor run tracking
- Implemented coordinated cleanup cycle (snapshot expiration → orphan cleanup)

### Fixed
- DataGenerator schema alignment for all signal types in integration tests
- Partition timestamp extraction to properly handle hour-based partitioning

### Implementation Commits
- `eec5e56` - Phase 3 foundation: retention config and Iceberg extensions
- `3b64712` - Phase 3 retention enforcement engine with partition drop
- `f7d70cf` - Phase 3 orphan file cleanup system
- `005fce8` - CompactorService integration and test infrastructure
- `0aef22d` - Retention integration tests with partial schema alignment
- `a507fa2` - Complete DataGenerator schema alignment - all tests enabled

## [0.2.0] - 2026-01-XX

### Added - Phase 2: Compaction Execution Engine

- Active compaction execution for Parquet file consolidation
- Transactional file operations with Iceberg integration
- Compaction scheduler with configurable intervals
- Metrics for compaction runs, files processed, and bytes compacted
- Support for all table types (traces, logs, metrics_*)

### Implementation Commits
- `e58271d` - Phase 2: Compaction Execution Engine (#465)
- `55ab128` - Enable compaction for all table types (#466)

## [0.1.0] - 2025-12-XX

### Added - Phase 1: Dry-Run Compaction Planning

- Initial compactor service structure
- Dry-run compaction planning and validation
- Partition statistics analysis
- Integration with Iceberg catalog
- Basic metrics and observability

### Implementation Commits
- Initial compactor implementation commits

---

## Upgrade Guide

### Upgrading to 0.3.0 (Phase 3)

**Breaking Changes:** None - All Phase 3 features are opt-in and disabled by default.

**New Configuration Options:**

1. **Enable Retention (Optional):**
   ```toml
   [compactor.retention]
   enabled = true
   dry_run = true  # Start with dry-run
   traces_retention_days = 7
   logs_retention_days = 3
   metrics_retention_days = 30
   ```

2. **Enable Orphan Cleanup (Optional):**
   ```toml
   [compactor.orphan_cleanup]
   enabled = true
   dry_run = true  # Start with dry-run
   grace_period_hours = 24
   ```

**Recommended Rollout:**

1. Deploy with Phase 3 disabled (default)
2. Enable with `dry_run = true` and monitor logs
3. Enable for test tenant first
4. Gradually roll out to production tenants
5. Enable orphan cleanup after retention is stable

**New Metrics:**

Add the following to your Prometheus scrape config:
- All `compactor_retention_*` metrics
- All `compactor_orphan_*` metrics
- Recommended alerts: `compactor_deletion_failures_total > 0`

**New Dependencies:**

No new runtime dependencies. All features use existing Iceberg and object store integrations.

---

[Unreleased]: https://github.com/yourorg/signaldb/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/yourorg/signaldb/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yourorg/signaldb/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yourorg/signaldb/releases/tag/v0.1.0
