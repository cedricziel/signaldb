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

                // 3) We'll read until 10 messages OR 500ms has elapsed—whichever comes first.
                // Increased timeout from 50ms to 500ms to give more time for messages to be processed
                let start = Instant::now();
                let max_wait = Duration::from_millis(500);
                let mut count = 0;

                while count < 10 {
                    // How long have we waited so far?
                    let elapsed = start.elapsed();
                    if elapsed >= max_wait {
                        // 500ms is up—stop pulling for this batch
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
                                    // Acknowledge the message to prevent redelivery
                                    if let Err(e) = js_msg.ack().await {
                                        eprintln!("Failed to acknowledge message: {}", e);
                                    }
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
                    // If we didn't get any messages in this batch, wait a bit before trying again
                    // This helps in test scenarios where messages might not be immediately available
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // In tests, we might want to break after a certain number of empty batches
                    // For now, we'll continue looping to keep trying
                }
            }
        };

        Box::pin(s)
    }

    async fn ack(&self, _message: Message) -> Result<(), String> {
        // Messages are already acknowledged in the stream method
        // when they are received, so this is a no-op
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
        // Give the container more time to fully start up
        tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
        Ok(container)
    }

    #[tokio::test]
    #[timeout(15000)]
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

        // Create stream first to ensure the consumer is ready
        let mut stream = dispatcher.stream("topic_a").await;

        // Give consumers more time to be fully ready
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Send the message
        dispatcher.send("topic_a", message.clone()).await.unwrap();

        // Give JetStream more time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Consume the message with timeout
        match tokio::time::timeout(tokio::time::Duration::from_millis(2000), stream.next()).await {
            Ok(Some(received_message)) => {
                assert_eq!(received_message, message);
            }
            Ok(None) => {
                panic!("No message received");
            }
            Err(_) => {
                panic!("Timeout waiting for message");
            }
        }
    }

    #[tokio::test]
    #[timeout(15000)]
    async fn test_multiple_messages_in_single_topic() {
        let nats = match provide_nats().await {
            Ok(n) => n,
            Err(e) => panic!("Failed to start NATS container: {}", e),
        };

        let host = match nats.get_host().await {
            Ok(h) => h,
            Err(e) => panic!("Failed to get host: {}", e),
        };

        let port = match nats.get_host_port_ipv4(4222).await {
            Ok(p) => p,
            Err(e) => panic!("Failed to get port: {}", e),
        };

        let url = format!("nats://{}:{}", host, port);

        let backend = match JetStreamBackend::new(&url).await {
            Ok(b) => b,
            Err(e) => panic!("Failed to create JetStream backend: {}", e),
        };

        match backend.ensure_stream("topic_a").await {
            Ok(_) => {}
            Err(e) => panic!("Failed to ensure stream: {}", e),
        };

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

        // Create stream first to ensure the consumer is ready
        let mut stream = dispatcher.stream("topic_a").await;

        // Give consumers more time to be fully ready
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Send messages
        for message in messages.clone() {
            match dispatcher.send("topic_a", message).await {
                Ok(_) => {}
                Err(e) => panic!("Failed to send message: {}", e),
            }
        }

        // Give JetStream more time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Consume messages with timeout
        for expected_message in messages {
            match tokio::time::timeout(tokio::time::Duration::from_millis(5000), stream.next())
                .await
            {
                Ok(Some(received_message)) => {
                    assert_eq!(received_message, expected_message);
                }
                Ok(None) => {
                    panic!("Expected a message but got none");
                }
                Err(_) => {
                    panic!("Timeout waiting for message");
                }
            }
        }
    }

    #[tokio::test]
    #[timeout(20000)]
    async fn test_messages_across_multiple_topics() {
        // Use the same error handling approach as in test_multiple_messages_in_single_topic
        let nats = match provide_nats().await {
            Ok(n) => n,
            Err(e) => panic!("Failed to start NATS container: {}", e),
        };

        let host = match nats.get_host().await {
            Ok(h) => h,
            Err(e) => panic!("Failed to get host: {}", e),
        };

        let port = match nats.get_host_port_ipv4(4222).await {
            Ok(p) => p,
            Err(e) => panic!("Failed to get port: {}", e),
        };

        let url = format!("nats://{}:{}", host, port);

        let backend = match JetStreamBackend::new(&url).await {
            Ok(b) => b,
            Err(e) => panic!("Failed to create JetStream backend: {}", e),
        };

        // Ensure streams exist with explicit error handling
        match backend.ensure_stream("topic_a").await {
            Ok(_) => println!("Stream topic_a created successfully"),
            Err(e) => panic!("Failed to ensure stream topic_a: {}", e),
        };

        match backend.ensure_stream("topic_b").await {
            Ok(_) => println!("Stream topic_b created successfully"),
            Err(e) => panic!("Failed to ensure stream topic_b: {}", e),
        };

        let dispatcher = Dispatcher::new(backend);

        let message_topic_a = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Thing in Topic A".to_string(),
        });
        let message_topic_b = Message::SimpleMessage(SimpleMessage {
            id: "2".to_string(),
            name: "Thing in Topic B".to_string(),
        });

        // Create streams first to ensure consumers are ready
        println!("Creating stream for topic_a");
        let mut stream_a = dispatcher.stream("topic_a").await;

        println!("Creating stream for topic_b");
        let mut stream_b = dispatcher.stream("topic_b").await;

        // Give consumers more time to be fully ready
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Send messages to different topics with explicit error handling
        println!("Sending message to topic_a");
        match dispatcher.send("topic_a", message_topic_a.clone()).await {
            Ok(_) => println!("Message sent to topic_a successfully"),
            Err(e) => panic!("Failed to send message to topic_a: {}", e),
        }

        println!("Sending message to topic_b");
        match dispatcher.send("topic_b", message_topic_b.clone()).await {
            Ok(_) => println!("Message sent to topic_b successfully"),
            Err(e) => panic!("Failed to send message to topic_b: {}", e),
        }

        // Give JetStream more time to process
        println!("Waiting for messages to be processed");
        tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

        // Consume messages from topic_a with timeout
        println!("Consuming message from topic_a");
        match tokio::time::timeout(tokio::time::Duration::from_millis(3000), stream_a.next()).await
        {
            Ok(Some(received_message)) => {
                println!("Message received from topic_a");
                assert_eq!(received_message, message_topic_a);
            }
            Ok(None) => {
                panic!("No message received in topic_a");
            }
            Err(_) => {
                panic!("Timeout waiting for message in topic_a");
            }
        }

        // Consume messages from topic_b with timeout
        println!("Consuming message from topic_b");
        match tokio::time::timeout(tokio::time::Duration::from_millis(5000), stream_b.next()).await
        {
            Ok(Some(received_message)) => {
                println!("Message received from topic_b");
                assert_eq!(received_message, message_topic_b);
            }
            Ok(None) => {
                panic!("No message received in topic_b");
            }
            Err(_) => {
                panic!("Timeout waiting for message in topic_b");
            }
        }
    }

    #[tokio::test]
    #[timeout(15000)]
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

        // Create stream first to ensure the consumer is ready
        let mut stream = dispatcher.stream("topic_ack").await;

        // Give consumers more time to be fully ready
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Send the message
        dispatcher.send("topic_ack", message.clone()).await.unwrap();

        // Give JetStream more time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Consume the message and ensure it's acknowledged with timeout
        match tokio::time::timeout(tokio::time::Duration::from_millis(5000), stream.next()).await {
            Ok(Some(received_message)) => {
                assert_eq!(received_message, message);
            }
            Ok(None) => {
                panic!("No message received for acknowledgment test");
            }
            Err(_) => {
                panic!("Timeout waiting for message in acknowledgment test");
            }
        }

        // Wait a bit to ensure message processing is complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Create a new stream to verify the message was acknowledged
        let mut new_stream = dispatcher.stream("topic_ack").await;

        // Set a timeout for the next() call to avoid hanging
        // We expect this to timeout or return None, as the message should have been acknowledged
        let result =
            tokio::time::timeout(tokio::time::Duration::from_millis(1000), new_stream.next()).await;

        match result {
            Ok(None) => {
                // This is the expected case - no message should be received
            }
            Ok(Some(_)) => {
                panic!("Message was replayed despite being acknowledged");
            }
            Err(_) => {
                // Timeout occurred, which is fine - it means no message was available
            }
        }
    }

    #[tokio::test]
    #[timeout(15000)]
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

        // Create a durable consumer first
        let mut stream = dispatcher.stream("topic_durable").await;

        // Give consumers more time to be fully ready
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Send the message
        dispatcher
            .send("topic_durable", message.clone())
            .await
            .unwrap();

        // Give JetStream more time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // Fetch the message with timeout
        match tokio::time::timeout(tokio::time::Duration::from_millis(5000), stream.next()).await {
            Ok(Some(received_message)) => {
                assert_eq!(received_message, message);
            }
            Ok(None) => {
                panic!("No message received for durable consumer test");
            }
            Err(_) => {
                panic!("Timeout waiting for message in durable consumer test");
            }
        }

        // Wait a bit to ensure message processing is complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Recreate the consumer and ensure no messages are replayed
        let mut new_stream = dispatcher.stream("topic_durable").await;

        // Set a timeout for the next() call to avoid hanging
        // We expect this to timeout or return None, as the message should have been acknowledged
        let result =
            tokio::time::timeout(tokio::time::Duration::from_millis(1000), new_stream.next()).await;

        match result {
            Ok(None) => {
                // This is the expected case - no message should be received
            }
            Ok(Some(_)) => {
                panic!("Message was replayed despite being acknowledged");
            }
            Err(_) => {
                // Timeout occurred, which is fine - it means no message was available
            }
        }
    }
}
