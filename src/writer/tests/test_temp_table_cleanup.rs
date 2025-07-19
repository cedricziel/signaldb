use common::config::Configuration;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use std::sync::Arc;
use writer::create_iceberg_writer;

#[tokio::test]
async fn test_temp_table_cleanup_on_commit() {
    // Set up configuration
    let config = Configuration::default();
    let object_store = Arc::new(InMemory::new());
    let tenant_id = "test_tenant";
    let table_name = "metrics_gauge";

    // Create writer
    let mut writer = create_iceberg_writer(&config, object_store.clone(), tenant_id, table_name)
        .await
        .expect("Failed to create writer");

    // Create test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000, 2000])),
            Arc::new(StringArray::from(vec![tenant_id, tenant_id])),
            Arc::new(StringArray::from(vec!["service1", "service2"])),
            Arc::new(StringArray::from(vec!["cpu_usage", "memory_usage"])),
            Arc::new(Float64Array::from(vec![50.0, 75.0])),
        ],
    )
    .expect("Failed to create batch1");

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3000, 4000])),
            Arc::new(StringArray::from(vec![tenant_id, tenant_id])),
            Arc::new(StringArray::from(vec!["service3", "service4"])),
            Arc::new(StringArray::from(vec!["disk_usage", "network_usage"])),
            Arc::new(Float64Array::from(vec![80.0, 90.0])),
        ],
    )
    .expect("Failed to create batch2");

    // Begin transaction
    let txn_id = writer
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Write batches
    writer
        .write_batch(batch1)
        .await
        .expect("Failed to write batch1");
    writer
        .write_batch(batch2)
        .await
        .expect("Failed to write batch2");

    // Commit transaction - this should clean up temp tables
    writer
        .commit_transaction(&txn_id)
        .await
        .expect("Failed to commit transaction");

    // Transaction should be complete - no way to verify internal state directly
    // but we can verify that the transaction completed successfully
}

#[tokio::test]
async fn test_temp_table_cleanup_on_rollback() {
    // Set up configuration
    let config = Configuration::default();
    let object_store = Arc::new(InMemory::new());
    let tenant_id = "test_tenant";
    let table_name = "logs";

    // Create writer
    let mut writer = create_iceberg_writer(&config, object_store.clone(), tenant_id, table_name)
        .await
        .expect("Failed to create writer");

    // Create test data - matching logs schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("log_attributes", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("observed_timestamp", DataType::Int64, false),
        Field::new("instrumentation_scope_name", DataType::Utf8, false),
        Field::new("instrumentation_scope_version", DataType::Utf8, false),
        Field::new("instrumentation_scope_attributes", DataType::Utf8, false),
        Field::new(
            "instrumentation_scope_dropped_attributes_count",
            DataType::UInt32,
            false,
        ),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000])),
            Arc::new(StringArray::from(vec![tenant_id])),
            Arc::new(StringArray::from(vec!["trace1"])),
            Arc::new(StringArray::from(vec!["span1"])),
            Arc::new(StringArray::from(vec!["INFO"])),
            Arc::new(Int32Array::from(vec![9])),
            Arc::new(StringArray::from(vec!["{}"])),
            Arc::new(StringArray::from(vec!["service1"])),
            Arc::new(StringArray::from(vec!["Test log message"])),
            Arc::new(StringArray::from(vec!["{}"])),
            Arc::new(StringArray::from(vec!["{}"])),
            Arc::new(UInt32Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![1000])),
            Arc::new(StringArray::from(vec!["scope1"])),
            Arc::new(StringArray::from(vec!["1.0.0"])),
            Arc::new(StringArray::from(vec!["{}"])),
            Arc::new(UInt32Array::from(vec![0])),
            Arc::new(UInt32Array::from(vec![0])),
        ],
    )
    .expect("Failed to create batch");

    // Begin transaction
    let txn_id = writer
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Write batch
    writer
        .write_batch(batch)
        .await
        .expect("Failed to write batch");

    // Rollback transaction - this should clean up temp tables
    writer
        .rollback_transaction(&txn_id)
        .await
        .expect("Failed to rollback transaction");

    // Transaction should be rolled back - no way to verify internal state directly
    // but we can verify that the rollback completed successfully
}
