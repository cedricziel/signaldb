// Integration test for writer storage functionality using an in-memory object store.
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use object_store::{memory::InMemory, ObjectStore};

use writer::{write_batch_to_object_store, WriterFlightService};

#[tokio::test]
async fn test_write_batch_to_object_store() -> anyhow::Result<()> {
    // Setup in-memory object store for testing
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::default());

    // Build a simple RecordBatch
    let schema = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;

    // Write the batch to object store
    let path = "test_batch.parquet";
    write_batch_to_object_store(object_store.clone(), path, batch).await?;

    // Verify that a Parquet file was written to the in-memory store
    let mut found = false;
    let mut list_stream = object_store.list(None);
    while let Some(meta) = list_stream.try_next().await? {
        if meta.location.as_ref() == path {
            found = true;
            println!(
                "Found parquet file: {} (size: {} bytes)",
                meta.location, meta.size
            );
            break;
        }
    }
    assert!(
        found,
        "No parquet file found in object store at path: {}",
        path
    );
    Ok(())
}

#[tokio::test]
async fn test_writer_flight_service_creation() -> anyhow::Result<()> {
    // Setup in-memory object store for testing
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::default());

    // Create the WriterFlightService with the in-memory store
    let _svc = WriterFlightService::new(object_store.clone());

    // If we get here, the service was created successfully
    Ok(())
}
