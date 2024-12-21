use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use common::{
    persistence::write_batch_to_object_store,
    queue::{memory::InMemoryQueue, Message, MessageType, Queue, QueueConfig},
};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchWrapper {
    #[serde(skip)]
    pub batch: Option<RecordBatch>,
}

impl From<RecordBatch> for BatchWrapper {
    fn from(batch: RecordBatch) -> Self {
        Self {
            batch: Some(batch),
        }
    }
}

pub struct Writer {
    queue: Arc<Mutex<InMemoryQueue>>,
    object_store: Arc<dyn ObjectStore>,
    queue_config: QueueConfig,
}

#[derive(thiserror::Error, Debug)]
pub enum WriterError {
    #[error("Failed to write batch: {0}")]
    WriteBatchError(String),
    #[error("Failed to receive message: {0}")]
    ReceiveError(String),
    #[error("Invalid message type: {0}")]
    InvalidMessageType(String),
    #[error("Missing batch data")]
    MissingBatch,
}

impl Writer {
    pub fn new(queue_config: QueueConfig, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            queue: Arc::new(Mutex::new(InMemoryQueue::default())),
            object_store,
            queue_config,
        }
    }

    pub async fn start(&self) -> Result<()> {
        // Connect to queue
        self.queue
            .lock()
            .await
            .connect(self.queue_config.clone())
            .await?;

        // Subscribe to signal messages
        self.queue
            .lock()
            .await
            .subscribe(MessageType::Signal, None)
            .await
            .map_err(|e| WriterError::ReceiveError(e.to_string()))?;

        loop {
            // Receive messages from the queue
            let message = self
                .queue
                .lock()
                .await
                .receive::<BatchWrapper>()
                .await
                .map_err(|e| WriterError::ReceiveError(e.to_string()))?;

            if let Some(message) = message {
                self.process_message(message).await?;
            }
        }
    }

    async fn process_message(&self, message: Message<BatchWrapper>) -> Result<()> {
        match message.message_type {
            MessageType::Signal => {
                let batch = message
                    .payload
                    .batch
                    .as_ref()
                    .ok_or_else(|| WriterError::MissingBatch)?;

                // Generate path for the batch
                let path = format!("{}/{}.parquet", message.subtype, message.timestamp.elapsed()?.as_nanos());
                
                // Write the batch to object store
                write_batch_to_object_store(self.object_store.clone(), &path, batch.clone())
                    .await
                    .map_err(|e| WriterError::WriteBatchError(e.to_string()))?;

                // Acknowledge the message
                self.queue
                    .lock()
                    .await
                    .ack(&message)
                    .await
                    .map_err(|e| WriterError::ReceiveError(e.to_string()))?;
            }
            _ => {
                return Err(WriterError::InvalidMessageType(format!(
                    "Unexpected message type: {:?}",
                    message.message_type
                ))
                .into());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch as ArrowRecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use std::{fs, path::Path, sync::Arc, time::SystemTime};

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
    async fn test_writer_basic() -> Result<()> {
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
        let writer = Writer::new(QueueConfig::default(), object_store);

        // Connect to queue
        writer
            .queue
            .lock()
            .await
            .connect(QueueConfig::default())
            .await?;

        // Create a message
        let message = Message {
            message_type: MessageType::Signal,
            subtype: "test".to_string(),
            payload: BatchWrapper::from(batch),
            metadata: Default::default(),
            timestamp: SystemTime::now(),
        };

        // Send message to queue
        writer.queue.lock().await.publish(message).await?;

        // Cleanup test directory
        cleanup_test_dir()?;

        Ok(())
    }
}
