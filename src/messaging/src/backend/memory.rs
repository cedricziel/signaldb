use async_trait::async_trait;
use futures::stream::Stream;
use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};
use tokio::sync::{mpsc, Mutex};

use crate::{Message, MessagingBackend};

pub struct InMemoryStreamingBackend {
    topics: Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>, // Topic-based senders
    buffer_size: usize,
}

impl InMemoryStreamingBackend {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            buffer_size,
        }
    }

    // Ensure a topic exists, creating it if necessary
    async fn ensure_topic(&self, topic: &str) -> Arc<Mutex<mpsc::Receiver<Message>>> {
        let mut topics = self.topics.lock().await;

        if let Some(_sender) = topics.get(topic) {
            // Create a new receiver for the existing sender
            let (_, receiver) = mpsc::channel(self.buffer_size);
            return Arc::new(Mutex::new(receiver));
        }

        // Create a new channel for the topic
        let (sender, receiver) = mpsc::channel(self.buffer_size);
        topics.insert(topic.to_string(), sender);
        Arc::new(Mutex::new(receiver))
    }
}

#[async_trait]
impl MessagingBackend for InMemoryStreamingBackend {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String> {
        let mut topics = self.topics.lock().await;

        if let Some(sender) = topics.get(topic) {
            sender.send(message).await.map_err(|e| e.to_string())
        } else {
            let (sender, _) = mpsc::channel(self.buffer_size);
            topics.insert(topic.to_string(), sender.clone());
            sender.send(message).await.map_err(|e| e.to_string())
        }
    }

    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        let receiver = self.ensure_topic(topic).await.clone();

        // Wrap the receiver in a stream
        let receiver_clone = receiver.clone();
        Box::pin(futures::stream::unfold(receiver, move |rx| {
            let receiver_clone = receiver_clone.clone();
            async move {
                let mut rx = rx.lock().await;
                rx.recv().await.map(|msg| (msg, receiver_clone))
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{messages::SimpleMessage, Dispatcher};

    use super::*;
    use futures::StreamExt;

    #[tokio::test]
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
    async fn test_backpressure_behavior() {
        let backend = InMemoryStreamingBackend::new(2); // Buffer size of 2
        let dispatcher = Dispatcher::new(backend);

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
        let mut stream = dispatcher.stream("topic_a").await;
        for expected_message in messages {
            if let Some(received_message) = stream.next().await {
                assert_eq!(received_message, expected_message);
            } else {
                panic!("Expected a message but got none");
            }
        }
    }
}
