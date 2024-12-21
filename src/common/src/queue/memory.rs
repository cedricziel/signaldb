use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use serde::{Deserialize, Serialize};

use super::{Message, MessageType, Queue, QueueConfig, QueueError, QueueResult};

const DEFAULT_CHANNEL_SIZE: usize = 1024;

/// In-memory queue implementation using tokio's broadcast channel
#[derive(Debug)]
pub struct InMemoryQueue {
    /// Sender for the broadcast channel
    sender: Option<broadcast::Sender<Vec<u8>>>,
    /// Receiver for the broadcast channel
    receiver: Arc<Mutex<Option<broadcast::Receiver<Vec<u8>>>>>,
    /// Subscribed message types
    subscriptions: Arc<Mutex<HashMap<MessageType, HashSet<String>>>>,
}

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self {
            sender: None,
            receiver: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl Queue for InMemoryQueue {
    async fn connect(&mut self, _config: QueueConfig) -> QueueResult<()> {
        let (tx, rx) = broadcast::channel(DEFAULT_CHANNEL_SIZE);
        self.sender = Some(tx);
        *self.receiver.lock().await = Some(rx);
        Ok(())
    }

    async fn publish<T>(&self, message: Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync,
    {
        let sender = self.sender.as_ref().ok_or_else(|| {
            QueueError::ConnectionError("Queue not connected".to_string())
        })?;

        // Serialize the message
        let bytes = message.to_bytes()?;

        // Send the serialized message
        sender.send(bytes).map_err(|e| {
            QueueError::PublishError(format!("Failed to publish message: {}", e))
        })?;

        Ok(())
    }

    async fn subscribe(
        &mut self,
        message_type: MessageType,
        subtype: Option<String>,
    ) -> QueueResult<()> {
        let mut subs = self.subscriptions.lock().await;
        let subtypes = subs.entry(message_type).or_default();
        
        if let Some(subtype) = subtype {
            subtypes.insert(subtype);
        }

        Ok(())
    }

    async fn receive<T>(&self) -> QueueResult<Option<Message<T>>>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync,
    {
        let mut receiver = self.receiver.lock().await;
        let receiver = receiver.as_mut().ok_or_else(|| {
            QueueError::ConnectionError("Queue not connected".to_string())
        })?;

        let subscriptions = self.subscriptions.lock().await;

        // Try to receive a message
        match receiver.recv().await {
            Ok(bytes) => {
                // Deserialize the message
                let message = Message::from_bytes(&bytes)?;

                // Check if we're subscribed to this message type
                if let Some(subtypes) = subscriptions.get(&message.message_type) {
                    if subtypes.is_empty() || subtypes.contains(&message.subtype) {
                        return Ok(Some(message));
                    }
                }
                // If we're not subscribed to this message type/subtype, return None
                Ok(None)
            }
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            Err(broadcast::error::RecvError::Lagged(_)) => Ok(None),
        }
    }

    async fn ack<T>(&self, _message: &Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync,
    {
        // In-memory implementation doesn't need acknowledgment
        Ok(())
    }

    async fn close(&mut self) -> QueueResult<()> {
        self.sender = None;
        *self.receiver.lock().await = None;
        Ok(())
    }
}
