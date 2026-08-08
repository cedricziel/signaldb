//! Default-configuration safety-window tests (CodeRabbit finding on #1091).
//!
//! `default_lifecycle_e2e.rs` proves that a default deployment eventually
//! reclaims everything retention/compaction free logically, but it does so
//! by overriding both of the safety windows the
//! `lifecycle-reclamation` spec's "Reclamation is safe against in-flight
//! work" requirement relies on: `orphan_cleanup.grace_period_hours` (24 -> 0)
//! and `retention.snapshots_to_keep` (10 -> 1). Those overrides are what let
//! that test observe full reclamation without simulating a day of wall-clock
//! time or dozens of unrelated commits — but they also mean the *actual*
//! default windows are never exercised anywhere: nothing proves a 24-hour
//! grace period really protects a recent orphan, or that the default
//! 10-snapshot retention window really protects a file an older, still-
//! retained snapshot references.
//!
//! Each test below runs its window at the real compiled-in default and
//! proves both halves of the guarantee: the file is protected while inside
//! the window, and the same file becomes reclaimable once it crosses the
//! boundary. A test that only checked "nothing was deleted" would pass just
//! as well with the whole cleanup path broken (see `identify_candidates`'s
//! grace-period and liveness checks in
//! `src/compactor/src/orphan/detector.rs`), so proving the boundary is the
//! point.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use common::testing::TestConfigBuilder;
use compactor::retention::enforcer::RetentionEnforcer;
use compactor::retention::metrics::RetentionMetrics;
use compactor::{
    CompactionExecutor, CompactionMetrics, CompactionPlanner, CompactionStatus, ExecutorConfig,
    ManifestReader, OrphanCleaner, OrphanCleanupConfig as CompactorOrphanConfig, OrphanDetector,
    PlannerConfig, RetentionConfig as CompactorRetentionConfig,
};
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use std::collections::HashSet;
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

const MILLIS_PER_HOUR: i64 = 3_600 * 1_000;

/// Start of the hour, `hours_ago` hours before now, in epoch millis. Mirrors
/// `default_lifecycle_e2e.rs`'s helper of the same name.
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

/// Proves the default 24-hour orphan-cleanup grace period at
/// `src/compactor/src/orphan/config.rs`'s `grace_period_hours` default.
///
/// Grace period is evaluated in `OrphanDetector::identify_candidates`
/// (`src/compactor/src/orphan/detector.rs`) purely from
/// `object_store::ObjectMeta::last_modified`, which for `object_store`
/// 0.13's `LocalFileSystem` is the real filesystem mtime
/// (`object_store::local::last_modified` reads `Metadata::modified()`). The
/// in-memory store used by every other test in this suite cannot backdate
/// that timestamp (see `orphan_cleanup.rs`'s
/// `test_orphan_detection_finds_unreferenced_files`, which overrides
/// `grace_period_hours` to 0 for exactly that reason), so this test is
/// unavoidably backed by a real `file://` temp directory: it writes a
/// genuinely unreferenced file, confirms the default grace period protects
/// it while fresh, then backdates the physical file's mtime with
/// `std::fs::File::set_modified` (stable since Rust 1.75, no new
/// dependency) to push it outside the window and confirms it becomes
/// reclaimable. No real sleeping and no detector-internal clock faking:
/// the file's age, as the detector actually reads it, really changes.
#[tokio::test]
async fn default_grace_period_protects_a_recent_orphan_then_reclaims_it_once_aged_out() -> Result<()>
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let temp_dir = tempfile::tempdir().context("failed to create temp dir for file:// storage")?;
    let storage_dsn = format!("file://{}", temp_dir.path().display());

    let config = TestConfigBuilder::new()
        .in_memory() // catalog/discovery/schema only; storage is overridden below
        .with_storage_dsn(&storage_dsn)
        .with_tenant(tenant_id, dataset_id)
        .build();

    // Sanity check on the value this test deliberately does NOT override —
    // this is what makes the rest of the test meaningful.
    assert_eq!(
        config.compactor.orphan_cleanup.grace_period_hours, 24,
        "this test exists to exercise the real default grace period"
    );
    assert!(
        config.compactor.orphan_cleanup.enabled && !config.compactor.orphan_cleanup.dry_run,
        "orphan cleanup must be enabled and deleting by default (D7)"
    );

    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);
    let table = catalog_manager
        .ensure_table(tenant_id, dataset_id, table_name)
        .await
        .context("failed to create table")?;
    let table_store = table.object_store();

    // A genuinely orphaned file: unreferenced by any snapshot, exactly what
    // a failed ingest or compaction commit would leave behind (spec
    // scenario "Genuinely orphaned files are found").
    let orphan_relpath =
        format!("{tenant_id}/{dataset_id}/{table_name}/data/orphan-recent.parquet");
    let orphan_path = ObjectPath::from(orphan_relpath.as_str());
    table_store
        .put(&orphan_path, bytes::Bytes::from(vec![0u8; 128]).into())
        .await
        .context("failed to write the orphan file")?;

    let orphan_config = CompactorOrphanConfig::from(config.compactor.orphan_cleanup.clone());
    let detector = Arc::new(OrphanDetector::new(
        orphan_config.clone(),
        catalog_manager.clone(),
        table_store.clone(),
    ));

    // --- Half 1: fresh file is protected by the default 24h grace period ---
    let candidates = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await
        .context("orphan detection failed (fresh file)")?;
    assert!(
        !candidates.iter().any(|c| c.path == orphan_relpath),
        "a freshly-written, genuinely unreferenced file must be protected by \
         the default 24h grace period, not just eventually deleted: {candidates:?}"
    );

    // --- Cross the boundary: backdate the real file mtime past 24h. ---
    let physical_path = temp_dir.path().join(&orphan_relpath);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&physical_path)
        .with_context(|| {
            format!(
                "failed to open {} for mtime backdating",
                physical_path.display()
            )
        })?;
    file.set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(25 * 3600))
        .context("failed to backdate the orphan file's mtime")?;

    // --- Half 2: the same file, now older than the grace period, is reclaimable ---
    let candidates = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await
        .context("orphan detection failed (aged file)")?;
    assert!(
        candidates.iter().any(|c| c.path == orphan_relpath),
        "the same file, once older than the grace period, must become a \
         reclaim candidate — otherwise the grace period never expires: {candidates:?}"
    );

    let cleaner =
        OrphanCleaner::with_detector(orphan_config, table_store.clone(), detector.clone());
    let deletion_result = cleaner
        .delete_orphans_batch(candidates)
        .await
        .context("orphan deletion failed")?;
    assert_eq!(
        deletion_result.failed_count, 0,
        "no orphan deletion may fail: {:?}",
        deletion_result.failed_deletions
    );
    assert!(
        deletion_result.deleted_count >= 1,
        "the aged orphan file must actually be deleted"
    );

    let exists = table_store.get(&orphan_path).await;
    assert!(
        exists.is_err(),
        "the aged orphan file must be physically gone from object storage"
    );

    Ok(())
}

/// Proves the default `snapshots_to_keep = 10` retention window at
/// `src/compactor/src/retention/config.rs`.
///
/// `OrphanDetector::build_live_reference_set` derives the live-file set from
/// `ManifestReader::collect_retained_manifests`
/// (`src/compactor/src/iceberg/manifest.rs`), which unions the manifests of
/// every snapshot *currently present in table metadata* — i.e. every
/// snapshot retention's `expire_old_snapshots` has not yet expired. This
/// test compacts a partition (so its small input files are replaced by a
/// merged output and the *current* snapshot stops referencing them), then
/// shows those input files are still protected — because the
/// pre-compaction snapshots that added them have not been expired yet — and
/// only become reclaimable once enough later commits push every one of
/// those snapshots outside the default 10-deep retained window and
/// `RetentionEnforcer::enforce_retention` (default `snapshots_to_keep =
/// Some(10)`) actually expires them.
///
/// No wall-clock or config trickery is needed for this window: snapshot
/// retention is a commit-count depth, not an age, so it is fully
/// controllable in-process. 12 seed commits (exceeds the default
/// `file_count_threshold = 10`, so the partition is an unambiguous
/// compaction candidate) + 1 compaction commit + 9 filler commits = 22
/// total snapshots; keeping the most recent 10 expires exactly the first 12
/// (every pre-compaction snapshot), which is the arithmetic this test
/// exercises and asserts on directly.
#[tokio::test]
async fn default_snapshots_to_keep_protects_a_referenced_file_then_reclaims_it_once_expired()
-> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut config = TestConfigBuilder::new()
        .in_memory()
        .with_tenant(tenant_id, dataset_id)
        .build();

    // Sanity check on the value this test deliberately does NOT override.
    assert_eq!(
        config.compactor.retention.snapshots_to_keep,
        Some(10),
        "this test exists to exercise the real default retained-snapshot window"
    );

    // grace_period_hours: unrelated to the window under test here (the
    // in-memory store can't carry real file ages anyway — see
    // `default_grace_period_protects_a_recent_orphan_then_reclaims_it_once_aged_out`
    // above, which is what actually exercises that window). Zeroing it
    // isolates this test to snapshot retention, matching the convention in
    // `default_lifecycle_e2e.rs` and `orphan_cleanup.rs`.
    config.compactor.orphan_cleanup.grace_period_hours = 0;

    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);
    let placeholder_object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        placeholder_object_store,
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("failed to create table writer")?;

    // Recent, closed hour partition; well within the default 30-day trace
    // retention so `drop_expired_partitions` never touches it in this test.
    let recent_hour_ms = aligned_hour_start(3);
    let files_per_partition = 12;

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
    .context("failed to seed partition")?;

    let manifest_reader = ManifestReader::new();
    let table = load_table(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let table_store = table.object_store();
    let seeded_files = manifest_reader.get_snapshot_files(&table).await?;
    let seed_paths: HashSet<String> = seeded_files.iter().map(|f| f.file_path.clone()).collect();
    assert_eq!(
        seed_paths.len(),
        files_per_partition,
        "seeding must produce one file per write"
    );

    // ==================== Compaction ====================
    // One commit: the current snapshot stops referencing the seed files,
    // but every pre-compaction snapshot (S1..S12) is still present in table
    // metadata (not yet expired) and still references them.
    let planner_config = PlannerConfig::from(&config.compactor);
    let executor_config = ExecutorConfig::from(&planner_config);
    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config);
    let candidates = planner.plan().await?;
    assert_eq!(
        candidates.len(),
        1,
        "the seeded partition must be the sole compaction candidate under default thresholds"
    );
    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        executor_config,
        CompactionMetrics::new(),
    );
    let result = executor
        .execute_candidate(candidates.into_iter().next().expect("checked len == 1"))
        .await
        .context("compaction execution returned a hard error")?;
    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "compaction must succeed: {:?}",
        result.error
    );

    let table = load_table(&catalog_manager, tenant_id, dataset_id, table_name).await?;
    let after_compaction = manifest_reader.get_snapshot_files(&table).await?;
    let live_paths_after_compaction: HashSet<String> = after_compaction
        .iter()
        .map(|f| f.file_path.clone())
        .collect();
    assert!(
        live_paths_after_compaction.is_disjoint(&seed_paths),
        "the current snapshot after compaction must reference only the newly merged output"
    );

    // ==================== Half 1: seed files are still protected ====================
    //
    // No snapshot has been expired yet, so every retained (= currently
    // present) snapshot is still unioned into the live set, including the
    // 12 pre-compaction snapshots that added the seed files.
    let orphan_config = CompactorOrphanConfig::from(config.compactor.orphan_cleanup.clone());
    let detector = Arc::new(OrphanDetector::new(
        orphan_config.clone(),
        catalog_manager.clone(),
        table_store.clone(),
    ));
    let candidates_before_expiry = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await
        .context("orphan detection failed (before snapshot expiration)")?;
    let candidate_paths_before: HashSet<String> = candidates_before_expiry
        .iter()
        .map(|c| c.path.clone())
        .collect();
    for seed_path in &seed_paths {
        assert!(
            !candidate_paths_before.contains(seed_path),
            "seed file {seed_path} is still referenced by a retained \
             pre-compaction snapshot and must not be a reclaim candidate yet: \
             {candidate_paths_before:?}"
        );
    }

    // ==================== Push every pre-compaction snapshot out of the
    // default 10-deep window ====================
    //
    // 9 unrelated filler commits to a distinct, recent (well within the 30d
    // retention window) hour: total snapshots become 12 (seed) + 1
    // (compaction) + 9 (filler) = 22. Keeping the 10 most recent expires
    // exactly the first 12 (every pre-compaction snapshot) and retains the
    // compaction commit onward.
    let filler_hour_ms = aligned_hour_start(5);
    let filler_commits = 9;
    generators::generate_traces(
        &mut writer,
        &DataGeneratorConfig {
            partition_count: 1,
            files_per_partition: filler_commits,
            rows_per_file: 1,
            base_timestamp: filler_hour_ms,
            partition_granularity: PartitionGranularity::Hour,
        },
    )
    .await
    .context("failed to write filler commits")?;

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
        retention_result.total_partitions_dropped, 0,
        "every partition in this test is well within the default 30-day \
         retention window; only snapshot expiration should act"
    );
    assert_eq!(
        retention_result.total_snapshots_expired, 12,
        "exactly the 12 pre-compaction snapshots must fall outside the \
         default 10-deep retained-snapshot window"
    );

    // ==================== Half 2: seed files are now reclaimable ====================
    let candidates_after_expiry = detector
        .identify_orphan_candidates(tenant_id, dataset_id, table_name)
        .await
        .context("orphan detection failed (after snapshot expiration)")?;
    let candidate_paths_after: HashSet<String> = candidates_after_expiry
        .iter()
        .map(|c| c.path.clone())
        .collect();
    for seed_path in &seed_paths {
        assert!(
            candidate_paths_after.contains(seed_path),
            "seed file {seed_path} must become a reclaim candidate once no \
             retained snapshot references it anymore: {candidate_paths_after:?}"
        );
    }

    let cleaner =
        OrphanCleaner::with_detector(orphan_config, table_store.clone(), detector.clone());
    let deletion_result = cleaner
        .delete_orphans_batch(candidates_after_expiry)
        .await
        .context("orphan deletion failed")?;
    assert_eq!(
        deletion_result.failed_count, 0,
        "no orphan deletion may fail: {:?}",
        deletion_result.failed_deletions
    );

    for seed_path in &seed_paths {
        let exists = table_store.get(&ObjectPath::from(seed_path.as_str())).await;
        assert!(
            exists.is_err(),
            "seed file {seed_path} must be physically deleted once reclaimed"
        );
    }

    // And the compacted output — still live in the current snapshot — must
    // never be touched.
    for live in &live_paths_after_compaction {
        let exists = table_store.get(&ObjectPath::from(live.as_str())).await;
        assert!(
            exists.is_ok(),
            "live compacted output {live} must survive orphan cleanup"
        );
    }

    Ok(())
}
