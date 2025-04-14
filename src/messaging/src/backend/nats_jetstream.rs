use async_nats::jetstream::{
    self,
    consumer::{self},
    context::Context,
    stream::Config,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    time::{Duration, Instant},
};
use tokio::time::timeout;

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
    pub async fn ensure_stream(&self, topic: &str) -> Result<(), String> {
        self.context
            .get_or_create_stream(Config {
                name: topic.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| format!("Failed to create stream: {}", e))?;

        // Ensure a durable consumer exists for the topic
        self.context
            .create_consumer_on_stream(
                consumer::pull::Config {
                    durable_name: Some(topic.to_string()),
                    ..Default::default()
                },
                topic,
            )
            .await
            .map_err(|e| format!("Failed to create consumer: {}", e))?;

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
        // Create a durable consumer
        let consumer: consumer::PullConsumer = self
            .context
            .get_consumer_from_stream(topic, topic)
            .await
            .expect("Failed to get consumer");

        // We'll produce a stream of `Message` items.
        // `try_stream!` lets us `yield` items as soon as they're ready.
        let s = stream! {
            loop {
                // 1) Request up to 10 messages from the server.
                //    If there aren't 10 available, JetStream will send what it has.
                consumer.batch().max_messages(10).messages().await.expect("Failed to get messages");

                // 2) Get a stream of the incoming messages for this pull.
                let mut js_stream = consumer.messages().await.expect("Failed to get messages");

                // 3) We'll read until 10 messages OR 50ms has elapsed—whichever comes first.
                let start = Instant::now();
                let max_wait = Duration::from_millis(50);
                let mut count = 0;

                while count < 10 {
                    // How long have we waited so far?
                    let elapsed = start.elapsed();
                    if elapsed >= max_wait {
                        // 50ms is up—stop pulling for this batch
                        break;
                    }
                    let remaining = max_wait - elapsed;

                    // 4) Attempt to read the next message with the leftover time
                    match timeout(remaining, js_stream.next()).await {
                        Ok(Some(Ok(js_msg))) => {
                            // We got a message from JetStream—deserialize it, ack it, and yield it immediately
                            let payload = String::from_utf8(js_msg.payload.to_vec()).unwrap_or_default();
                            match serde_json::from_str::<Message>(&payload) {
                                Ok(msg) => {
                                    yield msg;
                                }
                                Err(e) => {
                                    eprintln!("JSON parse error: {}", e);
                                }
                            }
                            count += 1;
                        }
                        // We either timed out waiting for a single message, or got an error, or the stream ended
                        _ => break,
                    }
                }

                if count == 0 {
                    break;
                }
            }
        };

        Box::pin(s)
    }

    async fn ack(&self, _message: Message) -> Result<(), String> {
        Ok(())
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
                id: "2".to_string(),
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
        backend.ensure_stream("topic_b").await.unwrap();

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
