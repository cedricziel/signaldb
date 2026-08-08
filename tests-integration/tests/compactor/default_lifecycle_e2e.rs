//! Default-configuration end-to-end lifecycle test (task 7.1, D7).
//!
//! Chains every lifecycle stage — compaction, retention (partition drop +
//! snapshot expiration), and orphan cleanup — starting from
//! `common::config::Configuration::default()`, to prove D7's central claim:
//! a deployment running *default* configuration both compacts small files
//! and physically reclaims the bytes retention/compaction free logically,
//! with no operator action.
//!
//! Unlike `retention_cutoff.rs`, `partition_drop.rs`, `snapshot_expiration.rs`,
//! `orphan_cleanup.rs`, `basic_compaction.rs`, and
//! `partition_scoped_compaction.rs` — which each pin one stage against a
//! hand-picked config — this test drives all of them back to back against
//! the compiled-in defaults, so a regression that silently reverts D5/D7
//! (e.g. orphan cleanup flipped back to disabled, or dry-run) or reintroduces
//! logical-only reclamation shows up here even if every single-stage test
//! still passes in isolation.
//!
//! Overrides are kept to the minimum needed for the test to terminate
//! deterministically against an in-memory backend; every override is called
//! out at its use site below. `enabled`/`dry_run` — the flags D7 actually
//! changed — are asserted at their compiled-in defaults *before* any
//! override is applied, so the test is meaningful about what it claims to
//! cover rather than just re-testing hand-set values.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use compactor::retention::enforcer::RetentionEnforcer;
use compactor::retention::metrics::RetentionMetrics;
use compactor::{
    CompactionExecutor, CompactionMetrics, CompactionPlanner, CompactionStatus, ExecutorConfig,
    ManifestReader, OrphanCleaner, OrphanCleanupConfig as CompactorOrphanConfig, OrphanDetector,
    PartitionManager, PlannerConfig, RetentionConfig as CompactorRetentionConfig,
};
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use std::collections::HashSet;
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

const MILLIS_PER_HOUR: i64 = 3_600 * 1_000;

/// Start of the hour, `hours_ago` hours before now, in epoch millis. Aligning
/// to the hour boundary keeps every generated batch inside exactly one
/// Iceberg `timestamp_hour` partition (mirrors `partition_scoped_compaction.rs`).
fn aligned_hour_start(hours_ago: i64) -> i64 {
    let now_ms = chrono::Utc::now().timestamp_millis();
    (now_ms / MILLIS_PER_HOUR - hours_ago) * MILLIS_PER_HOUR
}

async fn load_table(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    table_name: &str,
) -> Result<Table> {
    let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    match catalog_manager.catalog().load_tabular(&identifier).await? {
        Tabular::Table(table) => Ok(table),
        _ => anyhow::bail!("expected a table at {identifier}"),
    }
}

/// Chains compaction -> retention (partition drop + snapshot expiration) ->
/// orphan cleanup starting from `Configuration::default()`, and proves the
/// end state: small files were merged, the expired partition is gone and
/// unqueryable, and every file that is no longer live — the compaction
/// leftovers *and* the retention-dropped partition's data — is physically
/// gone from object storage, while everything the current snapshot still
/// references survives.
#[tokio::test]
async fn default_config_compacts_and_reclaims_end_to_end() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    // --- Start from the real defaults -----------------------------------
    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory() // unavoidable: storage/catalog must point somewhere for a test
        .with_tenant(tenant_id, dataset_id)
        .build();

    // The properties D7 actually flips must already be true here, straight
    // out of `Configuration::default()`, before any test override — this is
    // what makes the rest of the test meaningful rather than a test of
    // hand-set config.
    assert!(
        config.compactor.retention.enabled,
        "retention must be enabled by default"
    );
    assert!(
        !config.compactor.retention.dry_run,
        "retention must not be dry-run by default"
    );
    assert!(
        config.compactor.orphan_cleanup.enabled,
        "orphan cleanup must be enabled by default (D7)"
    );
    assert!(
        !config.compactor.orphan_cleanup.dry_run,
        "orphan cleanup must not be dry-run by default (D7)"
    );
    assert_eq!(
        config.compactor.orphan_cleanup.grace_period_hours, 24,
        "sanity check on the value this test overrides below"
    );
    assert_eq!(
        config.compactor.retention.snapshots_to_keep,
        Some(10),
        "sanity check on the value this test overrides below"
    );

    // --- Unavoidable overrides -------------------------------------------
    //
    // grace_period_hours: the in-memory object store does not carry
    // meaningful "age" for its objects the way a real filesystem/S3 would,
    // so a 24h grace period can never elapse in-process. Every other
    // orphan-cleanup integration test in this suite sets this to 0 for the
    // identical reason (see `orphan_cleanup.rs`). `enabled`/`dry_run` are
    // untouched.
    config.compactor.orphan_cleanup.grace_period_hours = 0;
    //
    // snapshots_to_keep: the default (10) protects a 10-snapshot time-travel
    // window. This test's hand-seeded commit sequence produces on the order
    // of two dozen snapshots total, nowhere near enough to age the
    // compaction/retention snapshots out of a 10-deep window without adding
    // dozens of unrelated filler commits to simulate hours of further
    // ingest. Shrinking this is the direct analogue of shortening a grace
    // period so the test can observe full physical reclamation without
    // manufacturing that filler traffic. `enabled`/`dry_run` are untouched.
    config.compactor.retention.snapshots_to_keep = Some(1);

    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);
    // `IcebergTableWriter::new` requires an `Arc<dyn ObjectStore>` argument,
    // but it is dead weight for the actual write path: `self.object_store`
    // is stored on the writer and never read again anywhere in
    // `src/writer/src/storage/iceberg.rs`. Real data/metadata I/O goes
    // through the `Table` handle the catalog resolves internally
    // (`catalog_manager.ensure_table(...)` -> `table.object_store()`), which
    // is a *different* `InMemory` instance from whatever is passed here.
    // Every other test in this suite that verifies physical file state
    // (`orphan_cleanup.rs`'s `idle_table_live_files_are_never_orphan_candidates`
    // and `files_replaced_by_compaction_stay_protected_while_snapshots_retained`)
    // re-derives the real store via `load_tabular(...)` ->
    // `Tabular::Table(t) => t.object_store()` rather than trusting this
    // constructor argument, and this test follows the same pattern below
    // once the table exists.
    let placeholder_object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        placeholder_object_store,
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("failed to create table writer")?;

    // --- Seed: two closed hour partitions of many small files ------------
    //
    // OLD: 35 days ago — older than the default 30-day traces retention, so
    // retention must drop it.
    let old_hour_ms = aligned_hour_start(35 * 24);
    // RECENT: 3 hours ago — closed (well past the default 10-minute
    // partition lateness) but inside the 30-day retention window, so it
    // survives retention while still being a compaction candidate.
    let recent_hour_ms = aligned_hour_start(3);
    let old_hour_key = old_hour_ms / MILLIS_PER_HOUR;
    let recent_hour_key = recent_hour_ms / MILLIS_PER_HOUR;

    // file_count_threshold defaults to 10; use a few more so candidacy is
    // unambiguous even with rounding in real-world variants of this config.
    let files_per_partition = 12;

    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition,
            rows_per_file: 20,
            base_timestamp: old_hour_ms,
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("failed to seed old partition")?;
    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition,
            rows_per_file: 20,
            base_timestamp: recent_hour_ms,
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("failed to seed recent partition")?;

    let manifest_reader = ManifestReader::new();
    let table = load_table(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    // The object store the catalog actually reads/writes table data
    // through — see the comment on `placeholder_object_store` above. Used
    // for orphan detection/cleanup and every physical-existence assertion
    // below.
    let table_store = table.object_store();
    let seeded_files = manifest_reader.get_snapshot_files(&table).await?;
    let rows_before: u64 = seeded_files.iter().map(|f| f.record_count).sum();

    let old_files_before: HashSet<String> = seeded_files
        .iter()
        .filter(|f| f.partition_hours == Some(old_hour_key))
        .map(|f| f.file_path.clone())
        .collect();
    let recent_files_before: HashSet<String> = seeded_files
        .iter()
        .filter(|f| f.partition_hours == Some(recent_hour_key))
        .map(|f| f.file_path.clone())
        .collect();
    assert_eq!(
        old_files_before.len(),
        files_per_partition,
        "seeding must produce one file per write for the old partition"
    );
    assert_eq!(
        recent_files_before.len(),
        files_per_partition,
        "seeding must produce one file per write for the recent partition"
    );

    // ==================== 1. Compaction ====================
    //
    // Derived from the real `common::config::CompactorConfig` defaults
    // (target_file_size_mb=128, file_count_threshold=10,
    // max_input_file_size_kb=65536, partition_lateness=10m) — not
    // hand-picked planner/executor config.
    let planner_config = PlannerConfig::from(&config.compactor);
    let executor_config = ExecutorConfig::from(&planner_config);
    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config);
    let candidates = planner.plan().await?;
    assert_eq!(
        candidates.len(),
        2,
        "both closed partitions must be compaction candidates under default thresholds, got {:?}",
        candidates
            .iter()
            .map(|c| &c.partition_id)
            .collect::<Vec<_>>()
    );

    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        executor_config,
        CompactionMetrics::new(),
    );
    for candidate in candidates {
        let result = executor
            .execute_candidate(candidate)
            .await
            .context("compaction execution returned a hard error")?;
        assert_eq!(
            result.status,
            CompactionStatus::Success,
            "compaction of a closed partition with no concurrent writer must succeed: {:?}",
            result.error
        );
    }

    let table = load_table(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let after_compaction = manifest_reader.get_snapshot_files(&table).await?;
    let rows_after_compaction: u64 = after_compaction.iter().map(|f| f.record_count).sum();
    assert_eq!(
        rows_after_compaction, rows_before,
        "compaction must not lose or duplicate rows"
    );

    let old_files_after_compaction: HashSet<String> = after_compaction
        .iter()
        .filter(|f| f.partition_hours == Some(old_hour_key))
        .map(|f| f.file_path.clone())
        .collect();
    let recent_files_after_compaction: HashSet<String> = after_compaction
        .iter()
        .filter(|f| f.partition_hours == Some(recent_hour_key))
        .map(|f| f.file_path.clone())
        .collect();
    assert!(
        !old_files_after_compaction.is_empty()
            && old_files_after_compaction.len() < files_per_partition,
        "old partition file count must drop after compaction: {} -> {}",
        files_per_partition,
        old_files_after_compaction.len()
    );
    assert!(
        !recent_files_after_compaction.is_empty()
            && recent_files_after_compaction.len() < files_per_partition,
        "recent partition file count must drop after compaction: {} -> {}",
        files_per_partition,
        recent_files_after_compaction.len()
    );
    assert!(
        old_files_after_compaction.is_disjoint(&old_files_before),
        "compacted old partition must reference only newly written files"
    );
    assert!(
        recent_files_after_compaction.is_disjoint(&recent_files_before),
        "compacted recent partition must reference only newly written files"
    );

    // ==================== 2/3. Retention: partition drop + snapshot
    // expiration ====================
    //
    // `RetentionEnforcer::enforce_retention` performs both steps in one
    // call (drop_expired_partitions, then expire_old_snapshots against the
    // post-drop table) — there is no separate public entry point for
    // "just" snapshot expiration in the production retention path, so one
    // call covers both halves of the task.
    let retention_config = CompactorRetentionConfig::from(config.compactor.retention.clone());
    let enforcer = RetentionEnforcer::new(
        catalog_manager.clone(),
        retention_config,
        RetentionMetrics::new(),
    )?;
    let retention_result = enforcer.enforce_retention(tenant_id, dataset_id).await?;
    assert!(
        retention_result.errors.is_empty(),
        "retention must not error: {:?}",
        retention_result.errors
    );
    assert_eq!(
        retention_result.total_partitions_dropped, 1,
        "exactly the old (35-day) partition must be dropped under the default 30-day retention"
    );
    assert!(
        retention_result.total_bytes_reclaimed > 0,
        "dropping the old partition must account reclaimed bytes"
    );
    assert!(
        retention_result.total_snapshots_expired > 0,
        "snapshot expiration must have run as part of the same enforcement pass"
    );

    // The dropped partition is gone from the live snapshot and therefore
    // unqueryable; the recent partition survives.
    let table = load_table(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let partition_manager = PartitionManager::new();
    let partitions_after = partition_manager.list_partitions(&table).await?;
    let remaining_hours: Vec<i64> = partitions_after
        .iter()
        .filter_map(|p| {
            p.partition_values
                .get(compactor::iceberg::partition::TIMESTAMP_HOUR_FIELD)
                .and_then(|h| h.parse::<i64>().ok())
        })
        .collect();
    assert!(
        !remaining_hours.contains(&old_hour_key),
        "the retention-dropped partition must no longer be present in the live snapshot: {remaining_hours:?}"
    );
    assert!(
        remaining_hours.contains(&recent_hour_key),
        "the recent partition must survive retention: {remaining_hours:?}"
    );

    let after_retention = manifest_reader.get_snapshot_files(&table).await?;
    assert!(
        after_retention
            .iter()
            .all(|f| f.partition_hours != Some(old_hour_key)),
        "no file of the dropped partition may remain in the current snapshot's manifests"
    );
    let live_recent_paths: HashSet<String> = after_retention
        .iter()
        .filter(|f| f.partition_hours == Some(recent_hour_key))
        .map(|f| f.file_path.clone())
        .collect();
    assert_eq!(
        live_recent_paths, recent_files_after_compaction,
        "the recent partition's compacted output must be exactly what the current snapshot references after retention"
    );

    // ==================== 4. Orphan cleanup ====================
    let orphan_config = CompactorOrphanConfig::from(config.compactor.orphan_cleanup.clone());
    let detector = Arc::new(OrphanDetector::new(
        orphan_config.clone(),
        catalog_manager.clone(),
        table_store.clone(),
    ));
    let cleaner =
        OrphanCleaner::with_detector(orphan_config, table_store.clone(), detector.clone());

    let orphan_candidates = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await
        .context("orphan detection failed")?;

    let candidate_paths: HashSet<String> =
        orphan_candidates.iter().map(|c| c.path.clone()).collect();

    // The point of this test: with `snapshots_to_keep = 1` collapsing the
    // retained-snapshot set down to just the current one, every file the
    // current snapshot no longer references — the small inputs superseded
    // by compaction in *both* partitions, and the old partition's
    // compacted output superseded by the retention drop — must be
    // detected as an orphan candidate. The recent partition's live,
    // current-snapshot files must not be.
    for expected in old_files_before
        .iter()
        .chain(old_files_after_compaction.iter())
        .chain(recent_files_before.iter())
    {
        assert!(
            candidate_paths.contains(expected),
            "expected {expected} to be detected as an orphan candidate"
        );
    }
    for live in &live_recent_paths {
        assert!(
            !candidate_paths.contains(live),
            "live file {live} must never be an orphan candidate"
        );
    }

    let deletion_result = cleaner
        .delete_orphans_batch(orphan_candidates)
        .await
        .context("orphan deletion failed")?;
    assert_eq!(
        deletion_result.failed_count, 0,
        "no orphan deletion may fail: {:?}",
        deletion_result.failed_deletions
    );
    assert!(
        deletion_result.deleted_count
            >= old_files_before.len()
                + old_files_after_compaction.len()
                + recent_files_before.len(),
        "expected at least {} files deleted, got {}",
        old_files_before.len() + old_files_after_compaction.len() + recent_files_before.len(),
        deletion_result.deleted_count
    );

    // The core assertion: logical deletion became *physical* deletion.
    for path in old_files_before
        .iter()
        .chain(old_files_after_compaction.iter())
        .chain(recent_files_before.iter())
    {
        let exists = table_store.get(&ObjectPath::from(path.as_str())).await;
        assert!(
            exists.is_err(),
            "superseded/dropped file must be physically deleted from object storage: {path}"
        );
    }

    // And the other half of the guarantee: nothing live was swept up in the
    // same pass.
    for path in &live_recent_paths {
        let exists = table_store.get(&ObjectPath::from(path.as_str())).await;
        assert!(
            exists.is_ok(),
            "live file referenced by the current snapshot must survive orphan cleanup: {path}"
        );
    }

    Ok(())
}
