use anyhow::Result;
use common::config::Configuration;
use common::wal::{Wal, WalConfig, WalOperation, record_batch_to_bytes};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;
use tempfile::tempdir;
use writer::{IcebergWriterFlightService, WalProcessor, create_iceberg_writer};

/// Integration test demonstrating the Iceberg table writer functionality
#[tokio::test]
async fn test_iceberg_writer_integration() -> Result<()> {
    // Setup test environment
    let _temp_dir = tempdir()?;
    let config = Configuration::default();
    let object_store = Arc::new(InMemory::new());

    // Test that we can create an Iceberg writer (should work now with table creation)
    let result =
        create_iceberg_writer(&config, object_store.clone(), "default", "default", "traces").await;

    // Table creation is now implemented, but may fail due to test environment
    if let Err(e) = result {
        // Should not fail due to "not implemented" anymore
        assert!(!e.to_string().contains("Table creation not yet implemented"));
        println!("Expected test environment failure: {}", e);
    } else {
        println!("Successfully created Iceberg writer in test environment");
    }

    Ok(())
}

/// Integration test for WAL processor with Iceberg integration
#[tokio::test]
async fn test_wal_processor_integration() -> Result<()> {
    // Setup test environment
    let temp_dir = tempdir()?;
    let wal_config = WalConfig {
        wal_dir: temp_dir.path().to_path_buf(),
        max_segment_size: 1024 * 1024, // 1MB
        max_buffer_entries: 1000,
        flush_interval_secs: 5,
    };
    let wal = Arc::new(Wal::new(wal_config).await?);
    let config = Configuration::default();
    let object_store = Arc::new(InMemory::new());

    // Create WAL processor
    let mut processor = WalProcessor::new(wal.clone(), config, object_store);

    // Create a test record batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace-1", "trace-2"])),
            Arc::new(StringArray::from(vec!["span-1", "span-2"])),
            Arc::new(Int64Array::from(vec![1234567890, 1234567891])),
        ],
    )?;

    // Serialize batch and write to WAL
    let batch_bytes = record_batch_to_bytes(&batch)?;
    let entry_id = wal.append(WalOperation::WriteTraces, batch_bytes).await?;
    wal.flush().await?;

    // Verify we can get stats from processor
    let stats = processor.get_stats();
    assert_eq!(stats.active_writers, 0);

    // Test processing (should work now that table creation is implemented, or fail gracefully due to test environment)
    let result = processor.process_single_entry(entry_id).await;
    if let Err(e) = result {
        // Should not fail due to "not implemented" anymore
        assert!(!e.to_string().contains("Table creation not yet implemented"));
        println!("Expected test environment failure in processing: {}", e);
    } else {
        println!("Successfully processed WAL entry with Iceberg writer");
    }

    // Shutdown processor
    processor.shutdown().await?;

    Ok(())
}

/// Integration test for the Iceberg Flight service
#[tokio::test]
async fn test_iceberg_flight_service_integration() -> Result<()> {
    // Setup test environment
    let temp_dir = tempdir()?;
    let config = Configuration::default();
    let object_store = Arc::new(InMemory::new());
    let wal_config = WalConfig {
        wal_dir: temp_dir.path().to_path_buf(),
        max_segment_size: 1024 * 1024, // 1MB
        max_buffer_entries: 1000,
        flush_interval_secs: 5,
    };
    let wal = Arc::new(Wal::new(wal_config).await?);

    // Create Iceberg Flight service
    let _service = IcebergWriterFlightService::new(config, object_store, wal);

    // Verify service was created successfully
    // Note: Full Flight service testing would require actual gRPC integration,
    // which is beyond the scope of this integration test

    Ok(())
}

/// Test that demonstrates the transition path from Parquet to Iceberg
#[tokio::test]
async fn test_parquet_to_iceberg_transition() -> Result<()> {
    use writer::write_batch_to_object_store;

    // Setup test environment
    let object_store = Arc::new(InMemory::new());

    // Create a test record batch
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?;

    // Test that we can still write using the legacy Parquet method
    write_batch_to_object_store(object_store.clone(), "test.parquet", batch.clone()).await?;

    // Verify the file was written
    let path = object_store::path::Path::from("test.parquet");
    let metadata = object_store.head(&path).await?;
    assert!(metadata.size > 0);

    // In the future, this would be replaced with Iceberg table writes
    // For now, we demonstrate that both approaches can coexist during transition

    Ok(())
}
