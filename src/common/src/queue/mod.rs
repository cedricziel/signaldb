use async_nats::jetstream;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

/// Message variants for different queue implementations
pub trait Message: Serialize + for<'de> Deserialize<'de> {
    fn message_type(&self) -> &'static str;
}

/// Configuration for a queue implementation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Type of queue (e.g., "redis", "kafka")
    pub queue_type: String,
    /// Connection URL
    pub url: String,
    /// Additional options specific to the queue implementation
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            queue_type: "memory".to_string(),
            url: "memory://local".to_string(),
            options: HashMap::new(),
        }
    }
}

/// Error types that can occur during queue operations
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("Failed to connect to queue: {0}")]
    ConnectionError(String),

    #[error("Failed to publish message: {0}")]
    PublishError(String),

    #[error("Failed to subscribe: {0}")]
    SubscribeError(String),

    #[error("Failed to receive message: {0}")]
    ReceiveError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

/// Result type for queue operations
pub type QueueResult<T> = Result<T, QueueError>;

/// Trait that must be implemented by all queue implementations
#[async_trait]
pub trait MessagingBackend: std::fmt::Debug + Sync + Send + Clone + 'static {
    /// Create the queue, do initial setup
    async fn create(&mut self) -> QueueResult<()>;

    /// Publish a message to the queue
    async fn publish<T: Message>(&self, topic: String, message: T) -> QueueResult<()>;

    /// Subscribe to messages with a specific topic
    async fn consume(&mut self, topic: String) -> QueueResult<Arc<Receiver<dyn Message>>>;
}

pub struct Dispatcher<B: MessagingBackend> {
    backend: B,
}

impl<B: MessagingBackend> Dispatcher<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn publish<T: Message>(&self, topic: String, message: T) -> Result<(), String> {
        self.backend.publish(topic, message)
    }

    pub fn receive(&self) -> Result<Box<dyn Message>, String> {
        self.backend.receive_message()
    }
}

pub mod memory;
pub mod nats;
pub use memory::InMemoryQueue;
pub use nats::NatsQueue;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::memory::InMemoryQueue;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct TestPayload {
        data: String,
    }

    #[tokio::test]
    async fn test_basic_publish_subscribe() {
        let mut queue = InMemoryQueue::default();

        // Subscribe to test topic
        let rx = queue.consume("test".to_string()).await.unwrap();

        // Publish a message
        let test_msg = TestPayload {
            data: "test".to_string(),
        };
    }
}
