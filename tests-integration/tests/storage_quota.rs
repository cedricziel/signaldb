//! Per-tenant storage quota integration test (issue #610)
//!
//! Exercises the real accounting path: writes actual Parquet data files
//! into an Iceberg table, computes per-tenant usage from table metadata,
//! cross-checks it against the manifest reader, and verifies that the
//! quota gate rejects ingest for a tenant over its `max_storage_bytes`.

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use common::storage_usage::{StorageUsageTracker, compute_usage};
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

/// The tenant slug (== id here) whose usage the test tracks.
const TENANT: &str = "quota-tenant";
const DATASET: &str = "quota-dataset";

async fn write_traces(catalog_manager: &Arc<CatalogManager>, writes: usize) -> Result<()> {
    let object_store = Arc::new(InMemory::new());
    let mut writer = IcebergTableWriter::new(
        catalog_manager,
        object_store,
        TENANT.to_string(),
        DATASET.to_string(),
        "traces".to_string(),
    )
    .await
    .expect("Failed to create Iceberg writer");

    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 100,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    for _ in 0..writes {
        generators::generate_traces(&mut writer, &config).await?;
    }
    Ok(())
}

async fn write_profiles(catalog_manager: &Arc<CatalogManager>, writes: usize) -> Result<()> {
    let object_store = Arc::new(InMemory::new());
    let mut writer = IcebergTableWriter::new(
        catalog_manager,
        object_store,
        TENANT.to_string(),
        DATASET.to_string(),
        "profiles".to_string(),
    )
    .await
    .expect("Failed to create Iceberg writer");

    let config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: 100,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    for _ in 0..writes {
        generators::generate_profiles(&mut writer, &config).await?;
    }
    Ok(())
}

/// Sum of live data-file sizes in one table's current snapshot, per the
/// manifest reader (the accounting ground truth).
async fn manifest_live_bytes(
    catalog_manager: &Arc<CatalogManager>,
    table_name: &str,
) -> Result<u64> {
    let identifier = catalog_manager.build_table_identifier(TENANT, DATASET, table_name);
    let table = match catalog_manager
        .catalog()
        .load_tabular(&identifier)
        .await
        .expect("Failed to load table")
    {
        iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
        _ => panic!("Expected table"),
    };
    Ok(compactor::iceberg::ManifestReader::new()
        .get_snapshot_files(&table)
        .await?
        .iter()
        .map(|f| f.file_size_bytes)
        .sum())
}

/// A fresh in-memory catalog and a config with a 1-byte quota for `TENANT`
/// (any data at all is over quota). Shared arrange step for the tests below;
/// each test gets its own `CatalogManager` so writes in one don't leak into
/// another.
async fn setup_quota_tenant() -> Result<(common::config::Configuration, Arc<CatalogManager>)> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    config.auth.default_limits.max_storage_bytes = Some(1);
    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);
    Ok((config, catalog_manager))
}

#[tokio::test]
async fn storage_usage_accounting_matches_manifest_and_grows_monotonically() -> Result<()> {
    let (_config, catalog_manager) = setup_quota_tenant().await?;

    // Before any data lands, accounting reports nothing for the tenant.
    let usage_before = compute_usage(&catalog_manager).await?;
    assert!(
        !usage_before.contains_key(TENANT),
        "tenant with no data must not appear in the usage map"
    );

    // Write real Parquet data files into the tenant's traces table.
    write_traces(&catalog_manager, 3).await?;

    // The accounting must match the live data-file sizes in the manifests.
    let usage = compute_usage(&catalog_manager).await?;
    let counted = *usage
        .get(TENANT)
        .expect("tenant with data must appear in the usage map");
    let manifest_bytes = manifest_live_bytes(&catalog_manager, "traces").await?;
    assert!(counted > 0, "real data files must produce non-zero usage");
    assert_eq!(
        counted, manifest_bytes,
        "usage accounting must equal the sum of live data-file sizes"
    );

    // More data grows the count monotonically (no double-count assertions
    // here, just that the fresh snapshot supersedes the old one).
    write_traces(&catalog_manager, 2).await?;
    let usage_after = compute_usage(&catalog_manager).await?;
    assert!(
        usage_after[TENANT] > counted,
        "additional writes must increase accounted usage"
    );
    Ok(())
}

#[tokio::test]
async fn storage_quota_enforcement_rejects_over_quota_tenant_and_isolates_others() -> Result<()> {
    let (config, catalog_manager) = setup_quota_tenant().await?;
    let tracker = StorageUsageTracker::from_auth_config(&config.auth);
    assert!(tracker.quotas_configured());

    // Before any data lands, the tenant has no usage and passes the gate
    // even though a quota is configured (accounting lag must not block).
    tracker.replace_all(compute_usage(&catalog_manager).await?);
    assert!(tracker.check_ingest(TENANT).is_ok());

    // Write real Parquet data files, then publish the refreshed usage: the
    // tenant is now over its 1-byte quota and ingest is rejected with a
    // quota_exceeded error.
    write_traces(&catalog_manager, 3).await?;
    let usage = compute_usage(&catalog_manager).await?;
    let counted = usage[TENANT];
    tracker.replace_all(usage);
    let err = tracker
        .check_ingest(TENANT)
        .expect_err("tenant over quota must be rejected");
    assert!(err.to_string().contains("quota_exceeded"));
    assert_eq!(err.usage_bytes, counted);

    // A tenant without data stays unaffected by someone else's usage.
    assert!(tracker.check_ingest("other-tenant").is_ok());
    Ok(())
}

#[tokio::test]
async fn storage_usage_counts_profiles_toward_tenant_total() -> Result<()> {
    let (_config, catalog_manager) = setup_quota_tenant().await?;

    write_traces(&catalog_manager, 3).await?;
    let usage_traces_only = compute_usage(&catalog_manager).await?;

    // The profiles signal (issue #635) must count toward the same tenant
    // quota: accounting walks every table in the tenant namespace, so a
    // profiles table's live files show up byte-for-byte in the total.
    write_profiles(&catalog_manager, 2).await?;
    let usage_with_profiles = compute_usage(&catalog_manager).await?;
    let profile_bytes = manifest_live_bytes(&catalog_manager, "profiles").await?;
    assert!(
        profile_bytes > 0,
        "profile writes must produce live data files"
    );
    assert_eq!(
        usage_with_profiles[TENANT],
        usage_traces_only[TENANT] + profile_bytes,
        "profiles tables must count toward tenant storage usage"
    );
    Ok(())
}
