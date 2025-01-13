use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use common::queue::{memory::InMemoryQueue, Message, Queue, QueueConfig};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};

pub mod storage;
pub use storage::write_batch_to_object_store;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchWrapper {
    #[serde(skip)]
    pub batch: Option<RecordBatch>,
}

impl From<RecordBatch> for BatchWrapper {
    fn from(batch: RecordBatch) -> Self {
        Self { batch: Some(batch) }
    }
}

/// A trait for writing batches of data to storage
#[async_trait]
pub trait BatchWriter: Send + Sync {
    /// Start the writer service
    async fn start(&self) -> Result<()>;

    /// Process a single batch message
    async fn process_message(&self, message: Message<BatchWrapper>) -> Result<()>;

    /// Stop the writer service
    async fn stop(&self) -> Result<()>;
}

#[derive(thiserror::Error, Debug)]
pub enum WriterError {
    #[error("Failed to write batch: {0}")]
    WriteBatchError(String),
    #[error("Failed to receive message: {0}")]
    ReceiveError(String),
    #[error("Missing batch data")]
    MissingBatch,
}

/// A writer implementation that reads from a queue and writes to object storage
pub struct QueueBatchWriter {
    queue: Arc<Mutex<InMemoryQueue>>,
    object_store: Arc<dyn ObjectStore>,
    queue_config: QueueConfig,
    shutdown: broadcast::Sender<()>,
}

impl QueueBatchWriter {
    pub fn new(queue_config: QueueConfig, object_store: Arc<dyn ObjectStore>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            queue: Arc::new(Mutex::new(InMemoryQueue::default())),
            object_store,
            queue_config,
            shutdown: shutdown_tx,
        }
    }
}

#[async_trait]
impl BatchWriter for QueueBatchWriter {
    async fn start(&self) -> Result<()> {
        // Subscribe to messages
        self.queue
            .lock()
            .await
            .subscribe("batch".to_string())
            .await
            .map_err(|e| WriterError::ReceiveError(e.to_string()))?;

        let mut shutdown_rx = self.shutdown.subscribe();
        tokio::select! {
            _ = shutdown_rx.recv() => {
                Ok(())
            }
        }
    }

    async fn process_message(&self, message: Message<BatchWrapper>) -> Result<()> {
        let batch = message
            .payload()
            .batch
            .as_ref()
            .ok_or_else(|| WriterError::MissingBatch)?;

        // Generate path for the batch
        let path = format!(
            "batch/{}-{}.parquet",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos(),
            Uuid::new_v4()
        );

        // Write the batch to object store
        storage::write_batch_to_object_store(self.object_store.clone(), &path, batch.clone())
            .await
            .map_err(|e| WriterError::WriteBatchError(e.to_string()))?;

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // Send shutdown signal
        let _ = self.shutdown.send(());
        Ok(())
    }
}

/// A mock writer implementation for testing
#[cfg(test)]
pub struct MockBatchWriter {
    pub processed_messages: Arc<Mutex<Vec<Message<BatchWrapper>>>>,
}

#[cfg(test)]
impl MockBatchWriter {
    pub fn new() -> Self {
        Self {
            processed_messages: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl BatchWriter for MockBatchWriter {
    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn process_message(&self, message: Message<BatchWrapper>) -> Result<()> {
        self.processed_messages.lock().await.push(message);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch as ArrowRecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use std::{fs, path::Path, sync::Arc};

    fn setup_test_dir() -> Result<()> {
        let test_dir = Path::new("./test_data");
        if test_dir.exists() {
            fs::remove_dir_all(test_dir)?;
        }
        fs::create_dir_all(test_dir)?;
        Ok(())
    }

    fn cleanup_test_dir() -> Result<()> {
        let test_dir = Path::new("./test_data");
        if test_dir.exists() {
            fs::remove_dir_all(test_dir)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_queue_writer_basic() -> Result<()> {
        // Setup test directory
        setup_test_dir()?;

        // Create a test schema and batch
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = ArrowRecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;

        // Create writer
        let object_store = Arc::new(LocalFileSystem::new_with_prefix("./test_data")?);
        let writer = QueueBatchWriter::new(QueueConfig::default(), object_store);

        // Create and publish a message
        let message = Message::new_in_memory(BatchWrapper::from(batch));
        writer.queue.lock().await.publish(message).await?;

        // Cleanup test directory
        cleanup_test_dir()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_mock_writer() -> Result<()> {
        // Create mock writer
        let writer = MockBatchWriter::new();

        // Create a test message
        let message = Message::new_in_memory(BatchWrapper { batch: None });

        // Process message
        writer.process_message(message.clone()).await?;

        // Verify message was processed
        let processed = writer.processed_messages.lock().await;
        assert_eq!(processed.len(), 1);

        Ok(())
    }
}
