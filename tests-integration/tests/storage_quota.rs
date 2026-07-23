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

#[tokio::test]
async fn storage_usage_is_accounted_from_live_files_and_enforced() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    config.auth.default_limits.max_storage_bytes = Some(1); // 1 byte: any data is over quota
    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);

    // Before any data lands, the tenant has no usage and passes the gate
    // even though a quota is configured (accounting lag must not block).
    let tracker = StorageUsageTracker::from_auth_config(&config.auth);
    assert!(tracker.quotas_configured());
    tracker.replace_all(compute_usage(&catalog_manager).await?);
    assert!(tracker.check_ingest(TENANT).is_ok());

    // Write real Parquet data files into the tenant's traces table.
    write_traces(&catalog_manager, 3).await?;

    // The accounting must match the live data-file sizes in the manifests.
    let usage = compute_usage(&catalog_manager).await?;
    let counted = *usage
        .get(TENANT)
        .expect("tenant with data must appear in the usage map");
    let table_identifier = catalog_manager.build_table_identifier(TENANT, DATASET, "traces");
    let table = match catalog_manager
        .catalog()
        .load_tabular(&table_identifier)
        .await
        .expect("Failed to load table")
    {
        iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
        _ => panic!("Expected table"),
    };
    let manifest_bytes: u64 = compactor::iceberg::ManifestReader::new()
        .get_snapshot_files(&table)
        .await?
        .iter()
        .map(|f| f.file_size_bytes)
        .sum();
    assert!(counted > 0, "real data files must produce non-zero usage");
    assert_eq!(
        counted, manifest_bytes,
        "usage accounting must equal the sum of live data-file sizes"
    );

    // Publish the refreshed usage: the tenant is now over its 1-byte quota
    // and ingest is rejected with a quota_exceeded error.
    tracker.replace_all(usage);
    let err = tracker
        .check_ingest(TENANT)
        .expect_err("tenant over quota must be rejected");
    assert!(err.to_string().contains("quota_exceeded"));
    assert_eq!(err.usage_bytes, counted);

    // A tenant without data stays unaffected by someone else's usage.
    assert!(tracker.check_ingest("other-tenant").is_ok());

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
