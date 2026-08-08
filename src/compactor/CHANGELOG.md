# Changelog

All notable changes to the SignalDB Compactor Service will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0](https://github.com/cedricziel/signaldb/compare/compactor-v0.2.1...compactor-v0.3.0) (2026-08-08)


### ⚠ BREAKING CHANGES

* **compactor:** [compactor.orphan_cleanup] revalidate_before_delete no longer exists. Note that a leftover key is silently ignored rather than rejected -- the design assumed unknown keys fail config parsing, but neither config struct sets serde(deny_unknown_fields), and adding it is not a safe drive-by because figment's env provider populates the same structs. Documented in the compactor configuration reference; tightening the structs deserves its own change.
* **compactor:** [compactor] min_input_file_size_kb is replaced by max_input_file_size_kb (semantics inverted) and max_files_per_job is removed. No backward-compat alias is provided.

### Features

* **compactor:** reclaim metadata backlog and enable orphan cleanup by default ([#1008](https://github.com/cedricziel/signaldb/issues/1008)) ([908ea79](https://github.com/cedricziel/signaldb/commit/908ea798e78a6d2dd90396f56e584275e9dfc9b3))
* **compactor:** warn on incoherent memory settings and document sizing ([#1081](https://github.com/cedricziel/signaldb/issues/1081)) ([b0a4bb0](https://github.com/cedricziel/signaldb/commit/b0a4bb0740430fad36129b2c40a5c0dc9c2f111d)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* DB client spans, query stage spans, compactor job spans ([#906](https://github.com/cedricziel/signaldb/issues/906)) ([04a4c4e](https://github.com/cedricziel/signaldb/commit/04a4c4e5788cf6531e0421b50b523b04ac4db38b))
* **iceberg:** tune the Parquet writer properties now that they are honored ([#1025](https://github.com/cedricziel/signaldb/issues/1025)) ([219132a](https://github.com/cedricziel/signaldb/commit/219132a3eb1bba1c15975245081ad4a2d54eb7d1))
* semconv RPC server spans on Flight boundaries ([#904](https://github.com/cedricziel/signaldb/issues/904)) ([a791f45](https://github.com/cedricziel/signaldb/commit/a791f45edf5b1650cc9091d1acf481175060628a))
* source-agnostic tenant registry (admin-API tenants queryable without restart) ([#853](https://github.com/cedricziel/signaldb/issues/853)) ([c685935](https://github.com/cedricziel/signaldb/commit/c6859353a739fefcdc45f56cc0c7899193a6086a))


### Bug Fixes

* address CodeRabbit review on the tenant registry ([#853](https://github.com/cedricziel/signaldb/issues/853) follow-up) ([#855](https://github.com/cedricziel/signaldb/issues/855)) ([d5011ec](https://github.com/cedricziel/signaldb/commit/d5011ecc4a6101c8a51d5944a9480dff8b19d6a8))
* **compactor:** bound the rewrite's DataFusion fan-out ([#1067](https://github.com/cedricziel/signaldb/issues/1067)) ([9fc7dde](https://github.com/cedricziel/signaldb/commit/9fc7ddeea7497ce4e63fac2f60b11d77d66c621c)), closes [#1064](https://github.com/cedricziel/signaldb/issues/1064)
* **compactor:** cover profiles in retention, snapshot expiration, and orphan cleanup ([#1021](https://github.com/cedricziel/signaldb/issues/1021)) ([3bcc644](https://github.com/cedricziel/signaldb/commit/3bcc644438874392d75e4f048fa6380614a4e935)), closes [#1014](https://github.com/cedricziel/signaldb/issues/1014)
* **compactor:** decline partitions whose inputs exceed the job budget ([#1069](https://github.com/cedricziel/signaldb/issues/1069)) ([8373ff7](https://github.com/cedricziel/signaldb/commit/8373ff71195a3dedcd11e650a39410bff4fdfe1e))
* **compactor:** derive orphan live-file set from retained snapshots, not snapshot age ([#1007](https://github.com/cedricziel/signaldb/issues/1007)) ([8835c71](https://github.com/cedricziel/signaldb/commit/8835c71335333247d7215f839f7c62d510c3453a))
* **compactor:** log commit failures with their full cause chain ([#1050](https://github.com/cedricziel/signaldb/issues/1050)) ([61704a0](https://github.com/cedricziel/signaldb/commit/61704a0f327eb20878c6a40c78a7aefee5462443))
* **compactor:** re-validate unconditionally before deleting orphans ([#1020](https://github.com/cedricziel/signaldb/issues/1020)) ([5634ab8](https://github.com/cedricziel/signaldb/commit/5634ab820f68d3ed8e24dc4e45ae120dadd15b3b))
* **compactor:** read partition values from manifest entries, not file paths ([#930](https://github.com/cedricziel/signaldb/issues/930)) ([#991](https://github.com/cedricziel/signaldb/issues/991)) ([2f7e79b](https://github.com/cedricziel/signaldb/commit/2f7e79b86bd5a1884604d9441692b92ac17e665f))
* **compactor:** select small files for compaction via max input size ([#934](https://github.com/cedricziel/signaldb/issues/934)) ([#975](https://github.com/cedricziel/signaldb/issues/975)) ([2ea86f8](https://github.com/cedricziel/signaldb/commit/2ea86f875d87be703d552844faaa9734ee0e7b2a))
* **compactor:** use a FairSpillPool for compaction and queries ([#1068](https://github.com/cedricziel/signaldb/issues/1068)) ([6b7bd13](https://github.com/cedricziel/signaldb/commit/6b7bd1368ac4444f785be14b8c29d92629295ee2))
* **monolith:** run the full compactor lifecycle loop, not just planning ([#1005](https://github.com/cedricziel/signaldb/issues/1005)) ([2e751fb](https://github.com/cedricziel/signaldb/commit/2e751fb5849ce596f3dca7366624ee65e4def3ac))
* provision signal tables for every registered dataset, and read an absent one as empty ([#1074](https://github.com/cedricziel/signaldb/issues/1074)) ([9a50ffa](https://github.com/cedricziel/signaldb/commit/9a50ffaa7e404a96cb80d7d3b0cc0850ede00f49))
* **telemetry:** emit int-typed registry attributes as i64 ([#1013](https://github.com/cedricziel/signaldb/issues/1013)) ([be67718](https://github.com/cedricziel/signaldb/commit/be677184819e5cbe700d253a03e59cd2bffa7ba8))
* **telemetry:** register retention span-event attributes and whitelist unremovable bridge attrs for weaver live-check ([#1009](https://github.com/cedricziel/signaldb/issues/1009)) ([da74098](https://github.com/cedricziel/signaldb/commit/da74098adf02b64500a032b860c0c5aad8af93ad))


### Performance Improvements

* **compactor:** stream the rewrite instead of collecting the partition ([#1080](https://github.com/cedricziel/signaldb/issues/1080)) ([da7fa82](https://github.com/cedricziel/signaldb/commit/da7fa82c0edc3832f2272b4f5fc3872c7b7d8476))
* CPU target features and jemalloc allocator for release builds ([#970](https://github.com/cedricziel/signaldb/issues/970)) ([766e2d1](https://github.com/cedricziel/signaldb/commit/766e2d1c82dad65a674184edaf2e8d67cb4083dd))
* **flight,wal:** compress Flight IPC payloads and WAL entries ([#945](https://github.com/cedricziel/signaldb/issues/945)) ([#998](https://github.com/cedricziel/signaldb/issues/998)) ([efb5ef4](https://github.com/cedricziel/signaldb/commit/efb5ef4bc85e2e77483f4546255b50c564015827))


### Documentation

* **compactor:** reframe phase-3 docs as retention & lifecycle ([#854](https://github.com/cedricziel/signaldb/issues/854)) ([6961887](https://github.com/cedricziel/signaldb/commit/6961887e5dce725744e4cdfb347ec7dbda7b252a))


### Code Refactoring

* **compactor:** detect self-authored commit conflicts via typed errors ([#951](https://github.com/cedricziel/signaldb/issues/951)) ([#996](https://github.com/cedricziel/signaldb/issues/996)) ([28bccd1](https://github.com/cedricziel/signaldb/commit/28bccd18d3fb1342389627e3f2608f5eb45533e1))
* **compactor:** partition-scoped compaction with delta commits ([#1017](https://github.com/cedricziel/signaldb/issues/1017)) ([52dc957](https://github.com/cedricziel/signaldb/commit/52dc9572a10378d6d69f653d1a78a4cf4d2f1407))
* **compactor:** run lifecycle cycles as independent tasks ([#1026](https://github.com/cedricziel/signaldb/issues/1026)) ([0b0f02a](https://github.com/cedricziel/signaldb/commit/0b0f02a6875b5dba5e853821a5e45319b92b8455))


### Tests

* delete tautological tests and rewrite salvageable ones as contract tests ([#961](https://github.com/cedricziel/signaldb/issues/961)) ([b3e884a](https://github.com/cedricziel/signaldb/commit/b3e884ad59b4df853429133d5eef2724a8adcada))
* exercise real implementations instead of test-local copies ([#964](https://github.com/cedricziel/signaldb/issues/964)) ([e142b3d](https://github.com/cedricziel/signaldb/commit/e142b3d006065205c7194fd22c4ca4e182402f55))
* polish medium/low audit findings across the workspace ([#969](https://github.com/cedricziel/signaldb/issues/969)) ([8962f6d](https://github.com/cedricziel/signaldb/commit/8962f6d1d22c8a176d4a1d99376d61b42b1da258))
* replace sleep-based synchronization with deterministic waits ([#968](https://github.com/cedricziel/signaldb/issues/968)) ([6391326](https://github.com/cedricziel/signaldb/commit/6391326013c8620f186e4a63c2cdf3bbdf9ee963))

## [0.2.1](https://github.com/cedricziel/signaldb/compare/compactor-v0.2.0...compactor-v0.2.1) (2026-07-30)

## [0.2.0](https://github.com/cedricziel/signaldb/compare/compactor-v0.1.0...compactor-v0.2.0) (2026-07-30)


### ⚠ BREAKING CHANGES

* **compactor:** upgraded deployments running with default configuration will start deleting data older than 30 days. Operators who want infinite retention must set [compactor.retention].enabled = false (or configure longer durations).

### Features

* **compactor, querier:** persist attribute stats and query demand ([#753](https://github.com/cedricziel/signaldb/issues/753)) ([3419bd9](https://github.com/cedricziel/signaldb/commit/3419bd98505c2e61c18991fac94965ac3425422c))
* **compactor:** act on attribute promotion decisions at rewrite ([#784](https://github.com/cedricziel/signaldb/issues/784)) ([68125f9](https://github.com/cedricziel/signaldb/commit/68125f9a1f0bf6e37b28ddcb2e329abbc168719e))
* **compactor:** advisory attribute-statistics analyzer ([#744](https://github.com/cedricziel/signaldb/issues/744)) ([37bf8ab](https://github.com/cedricziel/signaldb/commit/37bf8ab1844285bc6fb0ff96279606f888e7c548))
* **compactor:** attribute auto-promotion decision engine (dry-run) ([#756](https://github.com/cedricziel/signaldb/issues/756)) ([51c5411](https://github.com/cedricziel/signaldb/commit/51c5411fb16e92b384a347575c2a25849b189d8b))
* **compactor:** complete epic [#432](https://github.com/cedricziel/signaldb/issues/432) — real compaction, multi-instance tests, observability ([#540](https://github.com/cedricziel/signaldb/issues/540)) ([ed95e20](https://github.com/cedricziel/signaldb/commit/ed95e2062a05b7386d05188c89a754a3606fc428))
* **compactor:** demote unqueried label columns at rewrite ([#785](https://github.com/cedricziel/signaldb/issues/785)) ([d76c5eb](https://github.com/cedricziel/signaldb/commit/d76c5ebdc26d1217b9da6f2ca281a3c8ed96bae0))
* **compactor:** enable compaction and 30d retention by default ([#767](https://github.com/cedricziel/signaldb/issues/767)) ([77e2f81](https://github.com/cedricziel/signaldb/commit/77e2f81fa8aa58d0cfda3a2c06b99fceaeeffdc6))
* **compactor:** enable compaction for all table types ([#466](https://github.com/cedricziel/signaldb/issues/466)) ([55ab128](https://github.com/cedricziel/signaldb/commit/55ab12825f26c3d45c8c61859940f082421ffa98))
* **compactor:** enforce retention for real — partition drops and snapshot expiration ([#598](https://github.com/cedricziel/signaldb/issues/598)) ([106562d](https://github.com/cedricziel/signaldb/commit/106562de1208eebe0d0fefa30bbcf2e53087acbc))
* **compactor:** Phase 1 - Dry-run compaction planner ([#462](https://github.com/cedricziel/signaldb/issues/462)) ([a0ad75f](https://github.com/cedricziel/signaldb/commit/a0ad75f5478be94786d77e732a1b8db319ae8650))
* **compactor:** Phase 2 - Compaction Execution Engine ([#465](https://github.com/cedricziel/signaldb/issues/465)) ([e58271d](https://github.com/cedricziel/signaldb/commit/e58271d0d14f495290da4abe3d4ff3b9c185082b))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* **compactor:** Phase 4 — multi-instance safety (leases, round-robin, Flight endpoints) ([e9acbc2](https://github.com/cedricziel/signaldb/commit/e9acbc28ac75898fc1d9bd4fd866665b0ea076a5))
* **flight:** close out Flight port authentication ([#544](https://github.com/cedricziel/signaldb/issues/544)) ([#589](https://github.com/cedricziel/signaldb/issues/589)) ([f8a7b43](https://github.com/cedricziel/signaldb/commit/f8a7b43722fa0024e2b7c01b2243bb9329420f6c))


### Bug Fixes

* **ci:** resolve clippy 1.97 lints, security advisories, and ethnum build failure ([#516](https://github.com/cedricziel/signaldb/issues/516)) ([b21c459](https://github.com/cedricziel/signaldb/commit/b21c4596f361d14dad147447cc19da4156fb81da))
* **compactor:** derive orphan-cleanup tables from the catalog ([#604](https://github.com/cedricziel/signaldb/issues/604)) ([40dd6e2](https://github.com/cedricziel/signaldb/commit/40dd6e24e5a630b05dee73d1aa1f4cf97228affb))
* **compactor:** plan from real manifest data instead of synthetic files ([#602](https://github.com/cedricziel/signaldb/issues/602)) ([4e4702b](https://github.com/cedricziel/signaldb/commit/4e4702b9376e02a6b7894a7a7d2500f99f9ba7f8))
* **compactor:** renew leases during long compactions and use the DB clock ([#603](https://github.com/cedricziel/signaldb/issues/603)) ([4a1ead2](https://github.com/cedricziel/signaldb/commit/4a1ead2de48102f42d98f5cec289694b61fbf69e))
* **config:** refuse in-memory discovery/catalog in standalone services ([#599](https://github.com/cedricziel/signaldb/issues/599)) ([c8413ba](https://github.com/cedricziel/signaldb/commit/c8413babe5de5346477bf4d1ff26a7f2fef380bb))
* **iceberg:** load fresh table metadata in ensure_table instead of caching handles ([#606](https://github.com/cedricziel/signaldb/issues/606)) ([4539084](https://github.com/cedricziel/signaldb/commit/4539084cb5d1886edfacb000d3d93afbe584a67e)), closes [#537](https://github.com/cedricziel/signaldb/issues/537)


### Documentation

* audience-based taxonomy, doc-freshness enforcement, and Mermaid diagrams ([#607](https://github.com/cedricziel/signaldb/issues/607)) ([917709a](https://github.com/cedricziel/signaldb/commit/917709a5e765c7f93cdc4a56ae7842bd82d02e51))
* full staleness sweep — match all docs, skills, and READMEs to current code ([#611](https://github.com/cedricziel/signaldb/issues/611)) ([22247b0](https://github.com/cedricziel/signaldb/commit/22247b027d77820481d493c081e29f0df4efd6ed))


### Continuous Integration

* drop MSRV policy and fix security audit ignores ([#521](https://github.com/cedricziel/signaldb/issues/521)) ([7da71e3](https://github.com/cedricziel/signaldb/commit/7da71e3d78f593a4361f403e2d4be1e426fb8807))

## 0.1.0 (2026-03-02)


### Features

* **compactor:** enable compaction for all table types ([#466](https://github.com/cedricziel/signaldb/issues/466)) ([55ab128](https://github.com/cedricziel/signaldb/commit/55ab12825f26c3d45c8c61859940f082421ffa98))
* **compactor:** Phase 1 - Dry-run compaction planner ([#462](https://github.com/cedricziel/signaldb/issues/462)) ([a0ad75f](https://github.com/cedricziel/signaldb/commit/a0ad75f5478be94786d77e732a1b8db319ae8650))
* **compactor:** Phase 2 - Compaction Execution Engine ([#465](https://github.com/cedricziel/signaldb/issues/465)) ([e58271d](https://github.com/cedricziel/signaldb/commit/e58271d0d14f495290da4abe3d4ff3b9c185082b))
* **compactor:** Phase 3 - Retention & Lifecycle Management ([#467](https://github.com/cedricziel/signaldb/issues/467)) ([28acc8d](https://github.com/cedricziel/signaldb/commit/28acc8d215f029fe0b81dcd9b916f29ccdea60d6))
* **compactor:** Phase 4 — multi-instance safety (leases, round-robin, Flight endpoints) ([e9acbc2](https://github.com/cedricziel/signaldb/commit/e9acbc28ac75898fc1d9bd4fd866665b0ea076a5))

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
