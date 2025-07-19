use anyhow::Result;
use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use std::sync::Arc;
use writer::create_iceberg_writer;

#[tokio::test]
async fn test_transaction_basic_flow() -> Result<()> {
    // Setup configuration
    let mut config = Configuration::default();
    config.schema = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: DefaultSchemas {
            traces_enabled: true,
            logs_enabled: true,
            metrics_enabled: true,
            custom_schemas: Default::default(),
        },
    };
    config.storage = StorageConfig {
        dsn: "memory://".to_string(),
    };

    let object_store = Arc::new(InMemory::new());
    let mut writer = create_iceberg_writer(&config, object_store, "test_tenant", "metrics_gauge")
        .await
        .expect("Failed to create Iceberg writer");

    // Test 1: Begin transaction
    assert!(!writer.has_active_transaction());
    let txn_id = writer.begin_transaction().await?;
    assert!(writer.has_active_transaction());
    assert_eq!(writer.current_transaction_id(), Some(txn_id.clone()));

    // Test 2: Write batch within transaction (should be queued)
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
            Arc::new(TimestampNanosecondArray::from(vec![None])),
            Arc::new(StringArray::from(vec!["test-service"])),
            Arc::new(StringArray::from(vec!["test.metric"])),
            Arc::new(StringArray::from(vec![Some("Test metric")])),
            Arc::new(StringArray::from(vec![Some("count")])),
            Arc::new(Float64Array::from(vec![42.0])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("{\"host\":\"test\"}")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("{\"type\":\"test\"}")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Date32Array::from(vec![19000])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )?;

    // Write should succeed but be queued
    writer.write_batch(batch).await?;

    // Test 3: Commit transaction
    writer.commit_transaction(&txn_id).await?;
    assert!(!writer.has_active_transaction());
    assert_eq!(writer.current_transaction_id(), None);

    Ok(())
}

#[tokio::test]
async fn test_transaction_rollback() -> Result<()> {
    let mut config = Configuration::default();
    config.schema = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: DefaultSchemas {
            traces_enabled: true,
            logs_enabled: true,
            metrics_enabled: true,
            custom_schemas: Default::default(),
        },
    };
    config.storage = StorageConfig {
        dsn: "memory://".to_string(),
    };

    let object_store = Arc::new(InMemory::new());
    let mut writer =
        create_iceberg_writer(&config, object_store, "test_tenant", "metrics_gauge").await?;

    // Begin transaction
    let txn_id = writer.begin_transaction().await?;
    assert!(writer.has_active_transaction());

    // Create and queue a batch
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![2_000_000_000])),
            Arc::new(TimestampNanosecondArray::from(vec![None])),
            Arc::new(StringArray::from(vec!["rollback-service"])),
            Arc::new(StringArray::from(vec!["rollback.metric"])),
            Arc::new(StringArray::from(vec![Some("Should be rolled back")])),
            Arc::new(StringArray::from(vec![Some("count")])),
            Arc::new(Float64Array::from(vec![99.0])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("{\"host\":\"rollback\"}")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("{\"type\":\"rollback\"}")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Date32Array::from(vec![19000])),
            Arc::new(Int32Array::from(vec![11])),
        ],
    )?;

    writer.write_batch(batch).await?;

    // Rollback transaction
    writer.rollback_transaction(&txn_id).await?;
    assert!(!writer.has_active_transaction());

    // Operations should have been discarded
    // If we start a new transaction and commit immediately, it should succeed
    let new_txn_id = writer.begin_transaction().await?;
    writer.commit_transaction(&new_txn_id).await?;

    Ok(())
}

#[tokio::test]
async fn test_transaction_errors() -> Result<()> {
    let mut config = Configuration::default();
    config.schema = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: DefaultSchemas {
            traces_enabled: true,
            logs_enabled: true,
            metrics_enabled: true,
            custom_schemas: Default::default(),
        },
    };
    config.storage = StorageConfig {
        dsn: "memory://".to_string(),
    };

    let object_store = Arc::new(InMemory::new());
    let mut writer =
        create_iceberg_writer(&config, object_store, "test_tenant", "metrics_gauge").await?;

    // Test 1: Cannot begin transaction while one is active
    let txn_id1 = writer.begin_transaction().await?;
    let result = writer.begin_transaction().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already active"));

    // Test 2: Cannot commit non-existent transaction
    let result = writer.commit_transaction("non-existent-id").await;
    assert!(result.is_err());

    // Test 3: Cannot rollback non-existent transaction
    writer.commit_transaction(&txn_id1).await?; // Clean up first transaction
    let result = writer.rollback_transaction("non-existent-id").await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("No active transaction")
    );

    Ok(())
}

#[tokio::test]
async fn test_write_batches_creates_transaction() -> Result<()> {
    let mut config = Configuration::default();
    config.schema = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: DefaultSchemas {
            traces_enabled: true,
            logs_enabled: true,
            metrics_enabled: true,
            custom_schemas: Default::default(),
        },
    };
    config.storage = StorageConfig {
        dsn: "memory://".to_string(),
    };

    let object_store = Arc::new(InMemory::new());
    let mut writer =
        create_iceberg_writer(&config, object_store, "test_tenant", "metrics_gauge").await?;

    // Create test batches
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![3_000_000_000])),
            Arc::new(TimestampNanosecondArray::from(vec![None])),
            Arc::new(StringArray::from(vec!["batch-service"])),
            Arc::new(StringArray::from(vec!["batch1.metric"])),
            Arc::new(StringArray::from(vec![Some("First batch")])),
            Arc::new(StringArray::from(vec![Some("count")])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Date32Array::from(vec![19000])),
            Arc::new(Int32Array::from(vec![12])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![4_000_000_000])),
            Arc::new(TimestampNanosecondArray::from(vec![None])),
            Arc::new(StringArray::from(vec!["batch-service"])),
            Arc::new(StringArray::from(vec!["batch2.metric"])),
            Arc::new(StringArray::from(vec![Some("Second batch")])),
            Arc::new(StringArray::from(vec![Some("count")])),
            Arc::new(Float64Array::from(vec![2.0])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Date32Array::from(vec![19000])),
            Arc::new(Int32Array::from(vec![12])),
        ],
    )?;

    // write_batches should create a transaction automatically
    assert!(!writer.has_active_transaction());
    writer.write_batches(vec![batch1, batch2]).await?;
    assert!(!writer.has_active_transaction()); // Transaction should be committed

    Ok(())
}
