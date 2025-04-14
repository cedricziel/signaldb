use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::{Message, MessagingBackend};

#[derive(Clone, Debug)]
pub struct InMemoryStreamingBackend {
    // We store a broadcast::Sender for each topic
    topics: Arc<Mutex<HashMap<String, broadcast::Sender<Message>>>>,
    // Used to define the size of the broadcast channel
    capacity: usize,
}

impl InMemoryStreamingBackend {
    pub fn new(capacity: usize) -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            capacity,
        }
    }

    /// Get or create a broadcast sender for this topic
    fn get_or_create_topic_sender(&self, topic: &str) -> broadcast::Sender<Message> {
        let mut map = self.topics.lock().unwrap();

        map.entry(topic.to_string())
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .clone()
    }
}

#[async_trait]
impl MessagingBackend for InMemoryStreamingBackend {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String> {
        // Get the broadcast sender (creating it if needed) and send
        let sender = self.get_or_create_topic_sender(topic);
        sender.send(message).map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        // Subscribe to the broadcast channel for that topic
        let sender = self.get_or_create_topic_sender(topic);
        let rx = sender.subscribe();

        // Wrap rx in a BroadcastStream, which yields `Result<Message, RecvError>`
        let broadcast_stream = BroadcastStream::new(rx).filter_map(|res| async {
            match res {
                Ok(msg) => Some(msg),
                Err(_err) => {
                    // If a slow consumer falls behind, messages are dropped.
                    // We simply skip them here, but you could log it or handle otherwise.
                    None
                }
            }
        });

        // Pin the stream so we can return it
        Box::pin(broadcast_stream)
    }

    async fn ack(&self, _message: Message) -> Result<(), String> {
        // No-op for in-memory backend
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{messages::SimpleMessage, Message};
    use futures::StreamExt;

    #[tokio::test]
    async fn test_broadcast_in_memory_backend() {
        let backend = InMemoryStreamingBackend::new(10);

        // Create a first subscriber
        let mut stream1 = backend.stream("some_topic").await;

        // Send a message
        backend
            .send_message(
                "some_topic",
                Message::SimpleMessage(SimpleMessage {
                    id: "1".to_string(),
                    name: "hello".to_string(),
                }),
            )
            .await
            .unwrap();

        // The first subscriber should receive it
        if let Some(msg) = stream1.next().await {
            assert_eq!(
                msg,
                Message::SimpleMessage(SimpleMessage {
                    id: "1".to_string(),
                    name: "hello".to_string(),
                })
            );
        } else {
            panic!("stream1 did not receive a message");
        }

        // Create a second subscriber
        let mut stream2 = backend.stream("some_topic").await;

        // Send one more message
        backend
            .send_message(
                "some_topic",
                Message::SimpleMessage(SimpleMessage {
                    id: "1".to_string(),
                    name: "hello".to_string(),
                }),
            )
            .await
            .unwrap();

        // Both subscribers should see it (if they haven't lagged behind).
        let msg1 = stream1.next().await.expect("stream1 didn't get the second");
        let msg2 = stream2.next().await.expect("stream2 didn't get the second");

        assert_eq!(
            msg1,
            Message::SimpleMessage(SimpleMessage {
                id: "1".to_string(),
                name: "hello".to_string(),
            })
        );
        assert_eq!(
            msg2,
            Message::SimpleMessage(SimpleMessage {
                id: "1".to_string(),
                name: "hello".to_string(),
            })
        );
    }
}
