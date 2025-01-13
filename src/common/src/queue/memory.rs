use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

use super::{Message, Queue, QueueError, QueueResult};

const DEFAULT_CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
struct TopicChannel {
    sender: broadcast::Sender<Vec<u8>>,
    _receiver: broadcast::Receiver<Vec<u8>>, // Keep a receiver alive to prevent channel closure
}

/// In-memory queue implementation using tokio's broadcast channel
#[derive(Debug, Clone)]
pub struct InMemoryQueue {
    /// Channels for each topic
    channels: Arc<Mutex<HashMap<String, TopicChannel>>>,
}

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl Queue for InMemoryQueue {
    async fn publish<T>(&self, message: Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync + std::fmt::Debug,
    {
        let channels = self.channels.lock().await;

        // Serialize the message
        let bytes = message.to_bytes()?;

        // Send to all subscribed topics
        if !channels.is_empty() {
            for channel in channels.values() {
                channel.sender.send(bytes.clone()).map_err(|e| {
                    QueueError::PublishError(format!("Failed to publish message: {}", e))
                })?;
            }
        }

        Ok(())
    }

    async fn subscribe(&mut self, topic: String) -> QueueResult<()> {
        let mut channels = self.channels.lock().await;

        if !channels.contains_key(&topic) {
            let (tx, rx) = broadcast::channel(DEFAULT_CHANNEL_SIZE);
            channels.insert(
                topic,
                TopicChannel {
                    sender: tx,
                    _receiver: rx,
                },
            );
        }

        Ok(())
    }
}
