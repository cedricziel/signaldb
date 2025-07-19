use anyhow::Result;
use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use std::sync::Arc;
use writer::create_iceberg_writer;

/// Simple E2E test configuration
fn create_simple_test_config() -> Configuration {
    let config = Configuration {
        schema: SchemaConfig {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            default_schemas: DefaultSchemas {
                traces_enabled: true,
                logs_enabled: true,
                metrics_enabled: true,
                custom_schemas: Default::default(),
            },
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    };
    config
}

/// Create simple test data for metrics_gauge table
fn create_simple_test_data(num_rows: usize) -> Result<RecordBatch> {
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

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| 1_000_000_000 + (i as i64 * 1_000_000))
        .collect();
    let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec!["e2e-simple-service"; num_rows])),
            Arc::new(StringArray::from(vec!["simple.metric"; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("Simple test metric");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"host\":\"simple-test\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"type\":\"simple\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Date32Array::from(vec![19000; num_rows])),
            Arc::new(Int32Array::from(vec![10; num_rows])),
        ],
    )?;

    Ok(batch)
}

#[tokio::test]
async fn test_simple_e2e_write() -> Result<()> {
    let config = create_simple_test_config();
    let object_store = Arc::new(InMemory::new());

    // Create writer
    let mut writer = create_iceberg_writer(
        &config,
        object_store.clone(),
        "simple_tenant",
        "metrics_gauge",
    )
    .await?;

    // Create simple test data
    let test_data = create_simple_test_data(10)?;
    let expected_rows = test_data.num_rows();

    log::info!("Writing {expected_rows} rows to Iceberg table");

    // Write the data - this should succeed if our implementation is working
    writer.write_batch(test_data).await?;

    log::info!("Successfully wrote {expected_rows} rows");

    Ok(())
}

#[tokio::test]
async fn test_simple_e2e_multi_batch() -> Result<()> {
    let config = create_simple_test_config();
    let object_store = Arc::new(InMemory::new());

    let mut writer = create_iceberg_writer(
        &config,
        object_store.clone(),
        "simple_tenant",
        "metrics_gauge",
    )
    .await?;

    // Create multiple small batches
    let batch1 = create_simple_test_data(5)?;
    let batch2 = create_simple_test_data(7)?;
    let batch3 = create_simple_test_data(3)?;

    let total_expected_rows = batch1.num_rows() + batch2.num_rows() + batch3.num_rows();

    log::info!("Writing {total_expected_rows} total rows in 3 batches");

    // Write batches transactionally
    writer.write_batches(vec![batch1, batch2, batch3]).await?;

    log::info!("Successfully wrote {total_expected_rows} rows in transaction");

    Ok(())
}

#[tokio::test]
async fn test_simple_e2e_transaction() -> Result<()> {
    let config = create_simple_test_config();
    let object_store = Arc::new(InMemory::new());

    let mut writer = create_iceberg_writer(
        &config,
        object_store.clone(),
        "simple_tenant",
        "metrics_gauge",
    )
    .await?;

    // Test basic transaction flow
    let txn_id = writer.begin_transaction().await?;
    assert!(writer.has_active_transaction());

    let test_data = create_simple_test_data(5)?;
    writer.write_batch(test_data).await?;

    // Commit the transaction
    writer.commit_transaction(&txn_id).await?;
    assert!(!writer.has_active_transaction());

    log::info!("Transaction test completed successfully");

    Ok(())
}

#[tokio::test]
async fn test_simple_e2e_rollback() -> Result<()> {
    let config = create_simple_test_config();
    let object_store = Arc::new(InMemory::new());

    let mut writer = create_iceberg_writer(
        &config,
        object_store.clone(),
        "simple_tenant",
        "metrics_gauge",
    )
    .await?;

    // Test rollback
    let txn_id = writer.begin_transaction().await?;
    let test_data = create_simple_test_data(5)?;
    writer.write_batch(test_data).await?;

    // Rollback the transaction
    writer.rollback_transaction(&txn_id).await?;
    assert!(!writer.has_active_transaction());

    log::info!("Rollback test completed successfully");

    Ok(())
}
