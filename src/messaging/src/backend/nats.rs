use async_nats::Client;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::pin::Pin;

use std::sync::Arc;

use crate::{Message, MessagingBackend};

#[derive(Debug, Clone)]
pub struct NatsBackend {
    client: Arc<Client>, // Shared NATS client
}

impl NatsBackend {
    /// Creates a new NATS backend instance.
    pub async fn new(server_url: &str) -> Result<Self, String> {
        let client = async_nats::connect(server_url)
            .await
            .map_err(|e| format!("Failed to connect to NATS: {}", e))?;
        Ok(Self {
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl MessagingBackend for NatsBackend {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String> {
        let serialized =
            serde_json::to_string(&message).map_err(|e| format!("Serialization error: {}", e))?;
        self.client
            .publish(topic.to_string(), serialized.into())
            .await
            .map_err(|e| format!("Publish error: {}", e))?;
        Ok(())
    }

    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        // Subscribe to the topic
        let subscription = self
            .client
            .subscribe(topic.to_string())
            .await
            .expect("Failed to subscribe to topic");

        // Convert the subscription into a stream
        let stream = futures::stream::unfold(subscription, |mut subscription| async {
            match subscription.next().await {
                Some(message) => {
                    let payload =
                        String::from_utf8(message.payload.to_vec()).expect("Invalid UTF-8 message");
                    match serde_json::from_str::<Message>(&payload) {
                        Ok(message) => Some((message, subscription)),
                        Err(_) => None, // Discard invalid messages
                    }
                }
                None => None, // No more messages
            }
        });

        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {
    use crate::{messages::SimpleMessage, Dispatcher};

    use super::*;
    use futures::StreamExt;
    use ntest::timeout;
    use testcontainers_modules::{
        nats::{Nats, NatsServerCmd},
        testcontainers::{runners::AsyncRunner, ImageExt},
    };

    async fn provide_nats() -> Result<
        testcontainers_modules::testcontainers::ContainerAsync<Nats>,
        testcontainers_modules::testcontainers::TestcontainersError,
    > {
        let cmd = NatsServerCmd::default().with_jetstream();
        Nats::default().with_cmd(&cmd).start().await
    }

    #[tokio::test]
    #[timeout(100)]
    async fn test_send_and_receive_single_message() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = NatsBackend::new(url.as_ref()).await.unwrap();
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
    #[timeout(100)]
    async fn test_multiple_messages_in_single_topic() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = NatsBackend::new(url.as_ref()).await.unwrap();
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
    #[timeout(100)]
    async fn test_messages_across_multiple_topics() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = NatsBackend::new(url.as_ref()).await.unwrap();
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
    #[timeout(100)]
    async fn test_stream_closes_gracefully_when_no_more_messages() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = NatsBackend::new(url.as_ref()).await.unwrap();
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
    #[timeout(100)]
    async fn test_no_messages_in_empty_topic() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = NatsBackend::new(url.as_ref()).await.unwrap();
        let dispatcher = Dispatcher::new(backend);

        // Start a stream for a topic with no messages
        let mut stream = dispatcher.stream("empty_topic").await;

        // Ensure the stream has no messages
        assert!(stream.next().await.is_none());
    }
}
