use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Message type to identify different kinds of messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    /// Signal data (traces, metrics, logs)
    Signal,
    /// System commands
    Command,
    /// Internal state changes
    State,
    /// Custom message types
    Custom(String),
}

/// Generic message envelope that can contain any serializable payload
#[derive(Debug, Clone)]
pub struct Message<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Send + Sync,
{
    /// Type of message for routing
    pub message_type: MessageType,

    /// Specific subtype (e.g., "trace", "metric", "log" for Signal type)
    pub subtype: String,

    /// The actual payload
    pub payload: T,

    /// Message metadata
    pub metadata: HashMap<String, String>,

    /// Timestamp when the message was created (in nanoseconds)
    pub timestamp: std::time::SystemTime,
}

impl<T> Message<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Send + Sync,
{
    /// Create a new signal message
    pub fn new_signal(subtype: impl Into<String>, payload: T) -> Self {
        Self {
            message_type: MessageType::Signal,
            subtype: subtype.into(),
            payload,
            metadata: HashMap::new(),
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Create a new command message
    pub fn new_command(subtype: impl Into<String>, payload: T) -> Self {
        Self {
            message_type: MessageType::Command,
            subtype: subtype.into(),
            payload,
            metadata: HashMap::new(),
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Add metadata to the message
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Serialize the message to JSON bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, QueueError> {
        #[derive(Serialize)]
        struct MessageWrapper<'a, T> {
            message_type: &'a MessageType,
            subtype: &'a str,
            payload: &'a T,
            metadata: &'a HashMap<String, String>,
            timestamp: &'a std::time::SystemTime,
        }

        let wrapper = MessageWrapper {
            message_type: &self.message_type,
            subtype: &self.subtype,
            payload: &self.payload,
            metadata: &self.metadata,
            timestamp: &self.timestamp,
        };

        serde_json::to_vec(&wrapper).map_err(QueueError::SerializationError)
    }

    /// Deserialize the message from JSON bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, QueueError> {
        #[derive(Deserialize)]
        struct MessageWrapper<T> {
            message_type: MessageType,
            subtype: String,
            payload: T,
            metadata: HashMap<String, String>,
            timestamp: std::time::SystemTime,
        }

        let wrapper: MessageWrapper<T> = serde_json::from_slice(bytes)?;

        Ok(Self {
            message_type: wrapper.message_type,
            subtype: wrapper.subtype,
            payload: wrapper.payload,
            metadata: wrapper.metadata,
            timestamp: wrapper.timestamp,
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
pub trait Queue: std::fmt::Debug + Sync + Send + 'static {
    /// Connect to the queue system
    async fn connect(&mut self, config: QueueConfig) -> QueueResult<()>;

    /// Publish a message to the queue
    async fn publish<T>(&self, message: Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync;

    /// Subscribe to messages of specific types
    async fn subscribe(
        &mut self,
        message_type: MessageType,
        subtype: Option<String>,
    ) -> QueueResult<()>;

    /// Receive the next message (if any)
    async fn receive<T>(&self) -> QueueResult<Option<Message<T>>>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync;

    /// Acknowledge that a message has been processed
    async fn ack<T>(&self, message: &Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync;

    /// Close the queue connection
    async fn close(&mut self) -> QueueResult<()>;
}

pub mod memory;
pub use memory::InMemoryQueue;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::memory::InMemoryQueue;
    use std::time::Duration;
    use tokio::time::timeout;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct TestPayload {
        data: String,
    }

    #[tokio::test]
    async fn test_in_memory_queue_basic() {
        let mut queue = InMemoryQueue::default();

        // Connect to queue
        queue
            .connect(QueueConfig {
                queue_type: "memory".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Subscribe to Signal/trace messages
        queue
            .subscribe(MessageType::Signal, Some("trace".to_string()))
            .await
            .unwrap();

        // Publish a message
        let message = Message::new_signal(
            "trace",
            TestPayload {
                data: "test".to_string(),
            },
        );
        queue.publish(message).await.unwrap();

        // Receive the message with timeout
        let received: Option<Message<TestPayload>> =
            timeout(Duration::from_secs(1), queue.receive())
                .await
                .unwrap()
                .unwrap();

        assert!(received.is_some());
        let received = received.unwrap();
        assert_eq!(received.message_type, MessageType::Signal);
        assert_eq!(received.subtype, "trace");
        assert_eq!(received.payload.data, "test");

        // Cleanup
        queue.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let mut queue1 = InMemoryQueue::default();
        let mut queue2 = InMemoryQueue::default();

        // Connect both queues with separate channels
        queue1
            .connect(QueueConfig {
                queue_type: "memory".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        queue2
            .connect(QueueConfig {
                queue_type: "memory".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Subscribe to different message types
        queue1
            .subscribe(MessageType::Signal, Some("trace".to_string()))
            .await
            .unwrap();
        queue2
            .subscribe(MessageType::Command, Some("process".to_string()))
            .await
            .unwrap();

        // Publish messages
        let trace_msg = Message::new_signal(
            "trace",
            TestPayload {
                data: "trace data".to_string(),
            },
        );
        let command_msg = Message::new_command(
            "process",
            TestPayload {
                data: "command data".to_string(),
            },
        );

        queue1.publish(trace_msg).await.unwrap();
        queue2.publish(command_msg).await.unwrap();

        // Queue1 should only receive trace message with timeout
        let received1: Option<Message<TestPayload>> =
            timeout(Duration::from_secs(1), queue1.receive())
                .await
                .unwrap()
                .unwrap();

        assert!(received1.is_some());
        let received1 = received1.unwrap();
        assert_eq!(received1.message_type, MessageType::Signal);
        assert_eq!(received1.payload.data, "trace data");

        // Queue2 should only receive command message with timeout
        let received2: Option<Message<TestPayload>> =
            timeout(Duration::from_secs(1), queue2.receive())
                .await
                .unwrap()
                .unwrap();

        assert!(received2.is_some());
        let received2 = received2.unwrap();
        assert_eq!(received2.message_type, MessageType::Command);
        assert_eq!(received2.payload.data, "command data");

        // Cleanup
        queue1.close().await.unwrap();
        queue2.close().await.unwrap();
    }
}
