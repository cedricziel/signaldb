use async_nats::jetstream::{
    self, consumer,
    context::Context,
    stream::{Config, DiscardPolicy},
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::pin::Pin;

use crate::{Message, MessagingBackend};

#[derive(Debug, Clone)]
pub struct JetStreamBackend {
    context: Context, // JetStream context
}

impl JetStreamBackend {
    /// Creates a new JetStream backend
    pub async fn new(server_url: &str) -> Result<Self, String> {
        let client = async_nats::connect(server_url)
            .await
            .map_err(|e| format!("Failed to connect to NATS: {}", e))?;
        let context = jetstream::new(client);

        Ok(Self { context })
    }

    /// Ensures a stream exists for the specified topic
    async fn ensure_stream(&self, topic: &str) -> Result<(), String> {
        self.context
            .create_stream(Config {
                name: topic.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("Failed to create stream: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl MessagingBackend for JetStreamBackend {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String> {
        let serialized =
            serde_json::to_string(&message).map_err(|e| format!("Serialization error: {}", e))?;
        self.context
            .publish(topic.to_string(), serialized.into())
            .await
            .map_err(|e| format!("Publish error: {}", e))?;
        Ok(())
    }

    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        self.ensure_stream(topic)
            .await
            .expect("Failed to create stream");

        // Create a durable consumer
        let consumer: consumer::PullConsumer = self
            .context
            .create_consumer_on_stream(
                consumer::pull::Config {
                    durable_name: Some(topic.to_string()),
                    ..Default::default()
                },
                topic,
            )
            .await
            .expect("Failed to create consumer");

        // Convert the consumer's messages into a stream
        let stream = futures::stream::unfold(consumer, |consumer| async {
            let timeout = tokio::time::Duration::from_millis(2000);
            match tokio::time::timeout(timeout, async {
                if let Ok(mut messages) = consumer.fetch().max_messages(10).messages().await {
                    while let Some(msg) = messages.next().await {
                        if let Ok(msg) = msg {
                            return Some(msg);
                        }
                    }
                }
                None
            })
            .await
            {
                Ok(Some(msg)) => {
                    let payload =
                        String::from_utf8(msg.payload.to_vec()).expect("Invalid UTF-8 message");
                    if let Ok(message) = serde_json::from_str::<Message>(&payload) {
                        msg.ack().await.expect("Failed to ack message");
                        Some((message, consumer))
                    } else {
                        None
                    }
                }
                _ => None,
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
        let container = Nats::default().with_cmd(&cmd).start().await?;
        // Give the container a moment to fully start up
        tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
        Ok(container)
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_send_and_receive_single_message() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = JetStreamBackend::new(url.as_ref()).await.unwrap();
        backend.ensure_stream("topic_a").await.unwrap();

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
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = JetStreamBackend::new(&url).await.unwrap();
        backend.ensure_stream("topic_a").await.unwrap();

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
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = JetStreamBackend::new(&url).await.unwrap();
        backend.ensure_stream("topic_a").await.unwrap();

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
    async fn test_message_acknowledgment() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );
        let backend = JetStreamBackend::new(&url).await.unwrap();
        backend.ensure_stream("topic_ack").await.unwrap();

        let dispatcher = Dispatcher::new(backend);

        let message = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Acknowledged Thing".to_string(),
        });

        // Send the message
        dispatcher.send("topic_ack", message.clone()).await.unwrap();

        // Consume the message and ensure it's acknowledged
        let mut stream = dispatcher.stream("topic_ack").await;
        if let Some(received_message) = stream.next().await {
            assert_eq!(received_message, message);
        } else {
            panic!("No message received for acknowledgment test");
        }

        // Ensure the message is not replayed
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    #[timeout(10000)]
    async fn test_durable_consumer_persistence() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let backend = JetStreamBackend::new(&url).await.unwrap();
        backend.ensure_stream("topic_durable").await.unwrap();

        let dispatcher = Dispatcher::new(backend);

        let message = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Durable Thing".to_string(),
        });

        // Send the message
        dispatcher
            .send("topic_durable", message.clone())
            .await
            .unwrap();

        // Create a durable consumer and fetch the message
        let mut stream = dispatcher.stream("topic_durable").await;
        if let Some(received_message) = stream.next().await {
            assert_eq!(received_message, message);
        } else {
            panic!("No message received for durable consumer test");
        }

        // Recreate the consumer and ensure no messages are replayed
        let mut stream = dispatcher.stream("topic_durable").await;
        assert!(stream.next().await.is_none());
    }
}
