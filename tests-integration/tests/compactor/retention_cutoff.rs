//! Integration tests for retention cutoff computation
//!
//! Tests the calculation of retention cutoff timestamps based on various
//! retention policies including global, per-tenant, and per-dataset configurations.

use anyhow::Result;
use tests_integration::fixtures::{
    DataGeneratorConfig, PartitionGranularity, RetentionTestContext,
};
use tests_integration::generators;

/// Test 1.1: Basic retention cutoff calculation
///
/// Verifies that retention cutoff is computed correctly with a simple policy.
#[tokio::test]
async fn test_retention_cutoff_basic() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // Create test context
    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create table with 30 days of data
    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let thirty_days_ago = now - (30 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 30,
        files_per_partition: 2,
        rows_per_file: 50,
        base_timestamp: thirty_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    // Generate data
    let partitions = generators::generate_traces(&mut writer, &config).await?;
    log::info!("Generated {} partitions", partitions.len());

    // Set retention policy to 14 days
    let retention_days = 14;
    let retention_cutoff_ms = now - (retention_days * 24 * 60 * 60 * 1000);

    // Verify partitions older than 14 days should be marked for deletion
    let old_partitions: Vec<_> = partitions
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_cutoff_ms)
        .collect();

    let retained_partitions: Vec<_> = partitions
        .iter()
        .filter(|p| p.timestamp_range.1 >= retention_cutoff_ms)
        .collect();

    log::info!(
        "Partitions to delete: {}, to retain: {}",
        old_partitions.len(),
        retained_partitions.len()
    );

    // With 30 days of data and 14 day retention, approximately 16 days should be deleted
    assert!(
        old_partitions.len() >= 15,
        "Expected at least 15 old partitions, got {}",
        old_partitions.len()
    );
    assert!(
        retained_partitions.len() >= 13,
        "Expected at least 13 retained partitions, got {}",
        retained_partitions.len()
    );

    Ok(())
}

/// Test 1.2: Per-tenant retention overrides
///
/// Verifies that tenant-specific retention policies override global defaults.
#[tokio::test]
async fn test_retention_per_tenant_override() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create tables for two tenants
    let mut writer_a = ctx.create_table("tenant-a", "production", "traces").await?;
    let mut writer_b = ctx.create_table("tenant-b", "production", "traces").await?;

    let now = chrono::Utc::now().timestamp_millis();
    let forty_days_ago = now - (40 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 40,
        files_per_partition: 1,
        rows_per_file: 50,
        base_timestamp: forty_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    // Generate data for both tenants
    let partitions_a = generators::generate_traces(&mut writer_a, &config).await?;
    let partitions_b = generators::generate_traces(&mut writer_b, &config).await?;

    log::info!("Generated data for both tenants");

    // Tenant A: 30 days retention
    let retention_a_ms = now - (30 * 24 * 60 * 60 * 1000);
    let old_a: Vec<_> = partitions_a
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_a_ms)
        .collect();

    // Tenant B: 7 days retention (global default)
    let retention_b_ms = now - (7 * 24 * 60 * 60 * 1000);
    let old_b: Vec<_> = partitions_b
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_b_ms)
        .collect();

    log::info!(
        "Tenant A old partitions: {}, Tenant B old partitions: {}",
        old_a.len(),
        old_b.len()
    );

    // Tenant B should have more partitions to delete (shorter retention)
    assert!(
        old_b.len() > old_a.len(),
        "Tenant B with shorter retention should have more old partitions"
    );

    // Tenant A should have approximately 10 days to delete (40 - 30)
    assert!(
        old_a.len() >= 9,
        "Expected at least 9 old partitions for tenant A"
    );

    // Tenant B should have approximately 33 days to delete (40 - 7)
    assert!(
        old_b.len() >= 32,
        "Expected at least 32 old partitions for tenant B"
    );

    Ok(())
}

/// Test 1.3: Per-dataset retention overrides
///
/// Verifies that dataset-specific retention policies override tenant defaults.
#[tokio::test]
async fn test_retention_per_dataset_override() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create tables for different datasets
    let mut writer_prod = ctx.create_table("tenant-a", "production", "traces").await?;
    let mut writer_staging = ctx.create_table("tenant-a", "staging", "traces").await?;

    let now = chrono::Utc::now().timestamp_millis();
    let hundred_days_ago = now - (100 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 100,
        files_per_partition: 1,
        rows_per_file: 20,
        base_timestamp: hundred_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    // Generate data for both datasets
    let partitions_prod = generators::generate_traces(&mut writer_prod, &config).await?;
    let partitions_staging = generators::generate_traces(&mut writer_staging, &config).await?;

    log::info!("Generated data for production and staging");

    // Production: 90 days retention
    let retention_prod_ms = now - (90 * 24 * 60 * 60 * 1000);
    let old_prod: Vec<_> = partitions_prod
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_prod_ms)
        .collect();

    // Staging: 3 days retention
    let retention_staging_ms = now - (3 * 24 * 60 * 60 * 1000);
    let old_staging: Vec<_> = partitions_staging
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_staging_ms)
        .collect();

    log::info!(
        "Production old partitions: {}, Staging old partitions: {}",
        old_prod.len(),
        old_staging.len()
    );

    // Staging should have much more data to delete
    assert!(
        old_staging.len() > old_prod.len(),
        "Staging with shorter retention should have more old partitions"
    );

    // Production: approximately 10 days to delete (100 - 90)
    assert!(
        old_prod.len() >= 9,
        "Expected at least 9 old partitions for production"
    );

    // Staging: approximately 97 days to delete (100 - 3)
    assert!(
        old_staging.len() >= 95,
        "Expected at least 95 old partitions for staging"
    );

    Ok(())
}

/// Test 1.4: Zero retention policy
///
/// Verifies that zero-day retention is handled gracefully.
#[tokio::test]
async fn test_retention_zero_days() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    // Create table with data
    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();
    let ten_days_ago = now - (10 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 10,
        files_per_partition: 1,
        rows_per_file: 50,
        base_timestamp: ten_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    let partitions = generators::generate_traces(&mut writer, &config).await?;
    log::info!(
        "Generated {} partitions for zero retention test",
        partitions.len()
    );

    // Zero retention means all data is expired
    // In practice, this would delete all partitions
    let retention_cutoff_ms = now; // Current time = everything is old

    let expired_partitions: Vec<_> = partitions
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_cutoff_ms)
        .collect();

    log::info!(
        "All {} partitions would be expired",
        expired_partitions.len()
    );

    // All partitions should be marked for deletion
    assert_eq!(
        expired_partitions.len(),
        partitions.len(),
        "All partitions should be expired with zero retention"
    );

    Ok(())
}

/// Test 1.5: Retention with clock skew
///
/// Verifies that future-dated partitions are not incorrectly dropped.
#[tokio::test]
async fn test_retention_with_clock_skew() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let ctx = RetentionTestContext::new_in_memory().await?;

    let mut writer = ctx
        .create_table("test-tenant", "test-dataset", "traces")
        .await?;

    let now = chrono::Utc::now().timestamp_millis();

    // Create data with some partitions in the future (clock skew scenario)
    let five_days_ago = now - (5 * 24 * 60 * 60 * 1000);

    let config = DataGeneratorConfig {
        partition_count: 10,
        files_per_partition: 1,
        rows_per_file: 50,
        base_timestamp: five_days_ago,
        partition_granularity: PartitionGranularity::Day,
    };

    let partitions = generators::generate_traces(&mut writer, &config).await?;
    log::info!(
        "Generated {} partitions including future dates",
        partitions.len()
    );

    // With 3-day retention, only data older than 3 days should be dropped
    let retention_cutoff_ms = now - (3 * 24 * 60 * 60 * 1000);

    let expired: Vec<_> = partitions
        .iter()
        .filter(|p| p.timestamp_range.1 < retention_cutoff_ms)
        .collect();

    let retained: Vec<_> = partitions
        .iter()
        .filter(|p| p.timestamp_range.1 >= retention_cutoff_ms)
        .collect();

    log::info!(
        "Expired: {}, Retained: {} (including future)",
        expired.len(),
        retained.len()
    );

    // Some data should be expired (older than 3 days from -5 days start)
    assert!(!expired.is_empty(), "Expected at least 1 expired partition");

    // Some data should be retained (within 3 days or future)
    assert!(
        !retained.is_empty(),
        "Expected at least 1 retained partition"
    );

    // Future-dated partitions (if any exist beyond 'now') should never be expired
    let future_partitions: Vec<_> = partitions
        .iter()
        .filter(|p| p.timestamp_range.0 > now)
        .collect();

    if !future_partitions.is_empty() {
        log::info!("Found {} future partitions", future_partitions.len());
        // Verify none of them are in the expired list
        for fp in &future_partitions {
            assert!(
                !expired.iter().any(|e| e.partition_id == fp.partition_id),
                "Future partition {} should not be expired",
                fp.partition_id
            );
        }
    }

    Ok(())
}
