use async_trait::async_trait;
use futures::stream::Stream;
use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};
use tokio::sync::{mpsc, Mutex};

use crate::{Message, MessagingBackend};

struct TopicState {
    senders: Vec<mpsc::Sender<Message>>,
    buffer: Vec<Message>,
}

pub struct InMemoryStreamingBackend {
    topics: Arc<Mutex<HashMap<String, TopicState>>>,
    buffer_size: usize,
}

impl InMemoryStreamingBackend {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            buffer_size,
        }
    }

    // Get or create a topic state
    async fn get_or_create_topic(&self, topic: &str) -> mpsc::Receiver<Message> {
        let mut topics = self.topics.lock().await;
        let state = topics.entry(topic.to_string()).or_insert_with(|| TopicState {
            senders: Vec::new(),
            buffer: Vec::new(),
        });

        let (sender, receiver) = mpsc::channel(self.buffer_size);

        // Replay buffered messages
        let sender_clone = sender.clone();
        let buffered_messages = state.buffer.clone();
        tokio::spawn(async move {
            for msg in buffered_messages {
                if sender_clone.send(msg).await.is_err() {
                    break;
                }
            }
        });

        state.senders.push(sender);

        // Clean up closed senders
        state.senders.retain(|s| !s.is_closed());

        receiver
    }

    // Store message in buffer and forward to all senders
    async fn broadcast_message(&self, topic: &str, message: Message) -> Result<(), String> {
        let mut topics = self.topics.lock().await;
        let state = topics.entry(topic.to_string()).or_insert_with(|| TopicState {
            senders: Vec::new(),
            buffer: Vec::new(),
        });

        // Update buffer
        if state.buffer.len() >= self.buffer_size {
            state.buffer.remove(0);
        }
        state.buffer.push(message.clone());

        // Clean up closed senders
        state.senders.retain(|s| !s.is_closed());

        // Forward message to all senders
        let mut futures = Vec::new();
        for sender in &state.senders {
            futures.push(sender.send(message.clone()));
        }

        // Wait for all sends to complete
        for result in futures::future::join_all(futures).await {
            if let Err(e) = result {
                return Err(format!("Failed to send message: {}", e));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl MessagingBackend for InMemoryStreamingBackend {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String> {
        self.broadcast_message(topic, message).await
    }

    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        let receiver = self.get_or_create_topic(topic).await;
        Box::pin(futures::stream::unfold(receiver, |mut rx| async move {
            match tokio::time::timeout(
                tokio::time::Duration::from_millis(2000),
                rx.recv()
            ).await {
                Ok(Some(msg)) => Some((msg, rx)),
                _ => None,
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{messages::SimpleMessage, Dispatcher};

    use super::*;
    use futures::StreamExt;
    use ntest::timeout;

    #[tokio::test]
    #[timeout(10000)]
    async fn test_send_and_receive_single_message() {
        let backend = InMemoryStreamingBackend::new(10);
        let dispatcher = Dispatcher::new(backend);

        let message = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Test Thing".to_string(),
        });

        // Send the message
        dispatcher.send("topic_a", message.clone()).await.unwrap();

        // Consume the message
        let mut stream = dispatcher.stream("topic_a").await;
        if let Some(received_message) = stream.next().await {
            assert_eq!(received_message, message);
        } else {
            panic!("No message received");
        }
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_multiple_messages_in_single_topic() {
        let backend = InMemoryStreamingBackend::new(10);
        let dispatcher = Dispatcher::new(backend);

        let messages = vec![
            Message::SimpleMessage(SimpleMessage {
                id: "1".to_string(),
                name: "Thing 1".to_string(),
            }),
            Message::SimpleMessage(SimpleMessage {
                id: "1".to_string(),
                name: "Updated Thing 1".to_string(),
            }),
        ];

        for message in messages.clone() {
            dispatcher.send("topic_a", message).await.unwrap();
        }

        // Consume messages
        let mut stream = dispatcher.stream("topic_a").await;
        for expected_message in messages {
            if let Some(received_message) = stream.next().await {
                assert_eq!(received_message, expected_message);
            } else {
                panic!("Expected a message but got none");
            }
        }
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_messages_across_multiple_topics() {
        let backend = InMemoryStreamingBackend::new(10);
        let dispatcher = Dispatcher::new(backend);

        let message_topic_a = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Thing in Topic A".to_string(),
        });
        let message_topic_b = Message::SimpleMessage(SimpleMessage {
            id: "2".to_string(),
            name: "Thing in Topic B".to_string(),
        });

        // Send messages to different topics
        dispatcher
            .send("topic_a", message_topic_a.clone())
            .await
            .unwrap();
        dispatcher
            .send("topic_b", message_topic_b.clone())
            .await
            .unwrap();

        // Consume messages from topic_a
        let mut stream_a = dispatcher.stream("topic_a").await;
        if let Some(received_message) = stream_a.next().await {
            assert_eq!(received_message, message_topic_a);
        } else {
            panic!("No message received in topic_a");
        }

        // Consume messages from topic_b
        let mut stream_b = dispatcher.stream("topic_b").await;
        if let Some(received_message) = stream_b.next().await {
            assert_eq!(received_message, message_topic_b);
        } else {
            panic!("No message received in topic_b");
        }
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_stream_closes_gracefully_when_no_more_messages() {
        let backend = InMemoryStreamingBackend::new(10);
        let dispatcher = Dispatcher::new(backend);

        // Send a single message
        let message = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Test Thing".to_string(),
        });
        dispatcher.send("topic_a", message.clone()).await.unwrap();

        // Consume the single message
        let mut stream = dispatcher.stream("topic_a").await;
        if let Some(received_message) = stream.next().await {
            assert_eq!(received_message, message);
        } else {
            panic!("No message received");
        }

        // The stream should now be empty
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_backpressure_behavior() {
        let backend = InMemoryStreamingBackend::new(2); // Buffer size of 2
        let dispatcher = Dispatcher::new(backend);

        // Create stream before sending messages
        let mut stream = dispatcher.stream("topic_a").await;

        let messages = vec![
            Message::SimpleMessage(SimpleMessage {
                id: "1".to_string(),
                name: "Thing 1".to_string(),
            }),
            Message::SimpleMessage(SimpleMessage {
                id: "2".to_string(),
                name: "Thing 2".to_string(),
            }),
            Message::SimpleMessage(SimpleMessage {
                id: "3".to_string(),
                name: "Thing 3".to_string(),
            }),
        ];

        // Send messages (the buffer can only hold 2 at a time)
        for message in messages.clone() {
            dispatcher.send("topic_a", message).await.unwrap();
        }

        // Consume the messages
        for expected_message in messages {
            if let Some(received_message) = stream.next().await {
                assert_eq!(received_message, expected_message);
            } else {
                panic!("Expected a message but got none");
            }
        }
    }
}
