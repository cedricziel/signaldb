use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Message variants for different queue implementations
#[derive(Debug, Clone)]
pub enum Message<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Send + Sync + std::fmt::Debug,
{
    /// In-memory message
    InMemory(T),
    /// NATS JetStream message
    JetStream(T),
}

impl<T> Message<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Send + Sync + std::fmt::Debug,
{
    /// Create a new in-memory message
    pub fn new_in_memory(payload: T) -> Self {
        Self::InMemory(payload)
    }

    /// Create a new JetStream message
    pub fn new_jetstream(payload: T) -> Self {
        Self::JetStream(payload)
    }

    /// Get the message payload
    pub fn payload(&self) -> &T {
        match self {
            Self::InMemory(payload) => payload,
            Self::JetStream(payload) => payload,
        }
    }

    /// Serialize the message to JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, QueueError> {
        serde_json::to_vec(self.payload()).map_err(QueueError::SerializationError)
    }

    /// Deserialize the message from JSON bytes
    pub fn from_bytes(bytes: &[u8], is_jetstream: bool) -> Result<Self, QueueError> {
        let payload: T = serde_json::from_slice(bytes)?;
        Ok(if is_jetstream {
            Self::JetStream(payload)
        } else {
            Self::InMemory(payload)
        })
    }
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
pub trait Queue: std::fmt::Debug + Sync + Send + Clone + 'static {
    /// Publish a message to the queue
    async fn publish<T>(&self, message: Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync + std::fmt::Debug;

    /// Subscribe to messages with a specific topic
    async fn subscribe(&mut self, topic: String) -> QueueResult<()>;
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
        queue.subscribe("test".to_string()).await.unwrap();

        // Publish a message
        let message = Message::new_in_memory(TestPayload {
            data: "test".to_string(),
        });
        queue.publish(message).await.unwrap();
    }
}
