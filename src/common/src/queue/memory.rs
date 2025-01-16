use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

use super::{Message, MessagingBackend, QueueError, QueueResult};

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
impl MessagingBackend for InMemoryQueue {
    async fn create(&mut self) -> QueueResult<()> {
        Ok(())
    }

    async fn publish<T: Message>(&self, topic: String, message: T) -> QueueResult<()> {
        let channels = self.channels.lock().await;

        // Serialize the message
        let bytes = message.to_bytes()?;

        // Send to the specified topic
        if let Some(channel) = channels.get(&topic) {
            channel.sender.send(bytes.clone()).map_err(|e| {
                QueueError::PublishError(format!("Failed to publish message: {}", e))
            })?;
        } else {
            return Err(QueueError::PublishError(format!(
                "Topic not found: {}",
                topic
            )));
        }

        Ok(())
    }

    async fn consume(&mut self, topic: String) -> QueueResult<Arc<Receiver<Message>>> {
        let mut channels = self.channels.lock().await;
        let (tx, rx) = channel();

        if !channels.contains_key(&topic) {
            let (broadcast_tx, broadcast_rx) = broadcast::channel(DEFAULT_CHANNEL_SIZE);
            channels.insert(
                topic.clone(),
                TopicChannel {
                    sender: broadcast_tx,
                    _receiver: broadcast_rx,
                },
            );
        }

        // Get the broadcast channel
        if let Some(channel) = channels.get(&topic) {
            let mut broadcast_rx = channel.sender.subscribe();

            // Spawn a task to forward messages
            tokio::spawn(async move {
                while let Ok(bytes) = broadcast_rx.recv().await {
                    if let Ok(message) = Message::from_bytes(&bytes, false) {
                        let _ = tx.send(message);
                    }
                }
            });
        }

        Ok(rx.into())
    }
}
