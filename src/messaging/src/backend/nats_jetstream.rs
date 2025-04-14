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

// Default configuration values
const DEFAULT_MIN_BACKOFF: Duration = Duration::from_millis(100);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_BATCH_SIZE: usize = 10;
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_millis(500);

/// Configuration for JetStream message streams
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Maximum time to wait for a batch of messages
    pub batch_timeout: Duration,
    /// Maximum number of messages to request in a batch
    pub batch_size: usize,
    /// Minimum backoff time when no messages are received
    pub min_backoff: Duration,
    /// Maximum backoff time when no messages are received
    pub max_backoff: Duration,
    /// Maximum time to wait for a message before closing the stream
    /// Set to None to wait indefinitely
    pub idle_timeout: Option<Duration>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            batch_timeout: DEFAULT_BATCH_TIMEOUT,
            batch_size: DEFAULT_BATCH_SIZE,
            min_backoff: DEFAULT_MIN_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            idle_timeout: None, // No idle timeout by default for better test compatibility
        }
    }
}

#[derive(Debug, Clone)]
pub struct JetStreamBackend {
    context: Context, // JetStream context
    config: StreamConfig,
}

impl JetStreamBackend {
    /// Creates a new JetStream backend with default configuration
    pub async fn new(server_url: &str) -> Result<Self, String> {
        let client = async_nats::connect(server_url)
            .await
            .map_err(|e| format!("Failed to connect to NATS: {}", e))?;
        let context = jetstream::new(client);

        Ok(Self {
            context,
            config: StreamConfig::default(),
        })
    }

    /// Creates a new JetStream backend with custom configuration
    pub async fn with_config(server_url: &str, config: StreamConfig) -> Result<Self, String> {
        let client = async_nats::connect(server_url)
            .await
            .map_err(|e| format!("Failed to connect to NATS: {}", e))?;
        let context = jetstream::new(client);

        Ok(Self { context, config })
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

        // Clone the config for use in the stream
        let config = self.config.clone();

        // We'll produce a stream of `Message` items.
        let s = stream! {
            // Track the last time we received a message for idle timeout
            let mut last_message_time = Instant::now();

            // Current backoff duration for empty batches
            let mut current_backoff = config.min_backoff;

            // Main message polling loop
            loop {
                // Check if we should shutdown due to idle timeout
                if let Some(idle_timeout) = config.idle_timeout {
                    if last_message_time.elapsed() > idle_timeout {
                        eprintln!("Stream shutting down due to idle timeout");
                        break;
                    }
                }

                // Request a batch of messages
                match consumer.batch().max_messages(config.batch_size).messages().await {
                    Ok(_) => {
                        // Successfully requested messages
                    }
                    Err(e) => {
                        eprintln!("Failed to request messages: {}", e);

                        // Sleep with backoff before trying again
                        tokio::time::sleep(current_backoff).await;

                        // Increase backoff for next attempt (exponential backoff)
                        current_backoff = std::cmp::min(current_backoff * 2, config.max_backoff);

                        continue;
                    }
                }

                // Get a stream of the incoming messages for this pull
                let js_stream = match consumer.messages().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("Failed to get message stream: {}", e);

                        // Sleep with backoff before trying again
                        tokio::time::sleep(current_backoff).await;

                        // Increase backoff for next attempt (exponential backoff)
                        current_backoff = std::cmp::min(current_backoff * 2, config.max_backoff);

                        continue;
                    }
                };

                // Process the batch of messages
                let mut js_stream = js_stream;
                let start = Instant::now();
                let mut count = 0;

                // Process messages until we reach batch_size or batch_timeout
                'batch: while count < config.batch_size {
                    // Check if we've exceeded the batch timeout
                    let elapsed = start.elapsed();
                    if elapsed >= config.batch_timeout {
                        break 'batch;
                    }

                    // Calculate remaining time for this batch
                    let remaining = config.batch_timeout - elapsed;

                    // Attempt to read the next message with the leftover time
                    match timeout(remaining, js_stream.next()).await {
                        Ok(Some(Ok(js_msg))) => {
                            // We got a message from JetStream
                            let payload = String::from_utf8(js_msg.payload.to_vec()).unwrap_or_default();
                            match serde_json::from_str::<Message>(&payload) {
                                Ok(msg) => {
                                    // Acknowledge the message to prevent redelivery
                                    if let Err(e) = js_msg.ack().await {
                                        eprintln!("Failed to acknowledge message: {}", e);
                                    }

                                    // Update last message time
                                    last_message_time = Instant::now();

                                    // Reset backoff since we got a message
                                    current_backoff = config.min_backoff;

                                    // Yield the message
                                    count += 1;
                                    yield msg;
                                }
                                Err(e) => {
                                    eprintln!("JSON parse error: {}", e);
                                    // Still acknowledge the message to prevent redelivery of invalid messages
                                    if let Err(e) = js_msg.ack().await {
                                        eprintln!("Failed to acknowledge invalid message: {}", e);
                                    }
                                }
                            }
                        }
                        // We either timed out waiting for a message, got an error, or the stream ended
                        _ => break 'batch,
                    }
                }

                // If we didn't get any messages in this batch, apply backoff
                if count == 0 {
                    // Sleep with backoff before trying again
                    tokio::time::sleep(current_backoff).await;

                    // Increase backoff for next attempt (exponential backoff)
                    current_backoff = std::cmp::min(current_backoff * 2, config.max_backoff);
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

        // Send the message
        dispatcher.send("topic_a", message.clone()).await.unwrap();

        // Give the system a moment to process
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Consume the message with timeout
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), stream.next()).await {
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

        // Send messages
        for message in messages.clone() {
            match dispatcher.send("topic_a", message).await {
                Ok(_) => {}
                Err(e) => panic!("Failed to send message: {}", e),
            }
        }

        // Consume messages with timeout
        for expected_message in messages {
            match tokio::time::timeout(tokio::time::Duration::from_secs(5), stream.next()).await {
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
    #[timeout(30000)]
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

        // Give the system a moment to set up the consumer
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Send message to topic_a
        println!("Sending message to topic_a");
        match dispatcher.send("topic_a", message_topic_a.clone()).await {
            Ok(_) => println!("Message sent to topic_a successfully"),
            Err(e) => panic!("Failed to send message to topic_a: {}", e),
        }

        // Give the system a moment to process
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Consume message from topic_a with timeout
        println!("Consuming message from topic_a");
        match tokio::time::timeout(tokio::time::Duration::from_secs(15), stream_a.next()).await {
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

        // Now create a stream for topic_b
        println!("Creating stream for topic_b");
        let mut stream_b = dispatcher.stream("topic_b").await;

        // Send message to topic_b
        println!("Sending message to topic_b");
        match dispatcher.send("topic_b", message_topic_b.clone()).await {
            Ok(_) => println!("Message sent to topic_b successfully"),
            Err(e) => panic!("Failed to send message to topic_b: {}", e),
        }

        // Give the system a moment to process
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Consume message from topic_b with timeout
        println!("Consuming message from topic_b");
        match tokio::time::timeout(tokio::time::Duration::from_secs(15), stream_b.next()).await {
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

        // Send the message
        dispatcher.send("topic_ack", message.clone()).await.unwrap();

        // Give the system a moment to process
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Consume the message and ensure it's acknowledged with timeout
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), stream.next()).await {
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

        // Create a new stream to verify the message was acknowledged
        let mut new_stream = dispatcher.stream("topic_ack").await;

        // Set a timeout for the next() call to avoid hanging
        // We expect this to timeout or return None, as the message should have been acknowledged
        let result =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), new_stream.next()).await;

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

        // Send the message
        dispatcher
            .send("topic_durable", message.clone())
            .await
            .unwrap();

        // Fetch the message with timeout
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), stream.next()).await {
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

        // Recreate the consumer and ensure no messages are replayed
        let mut new_stream = dispatcher.stream("topic_durable").await;

        // Set a timeout for the next() call to avoid hanging
        // We expect this to timeout or return None, as the message should have been acknowledged
        let result =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), new_stream.next()).await;

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
    async fn test_idle_timeout_shutdown() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        // Create a backend with a very short idle timeout
        let config = StreamConfig {
            idle_timeout: Some(Duration::from_millis(500)), // 500ms idle timeout
            ..Default::default()
        };
        let backend = JetStreamBackend::with_config(&url, config).await.unwrap();
        backend.ensure_stream("topic_idle").await.unwrap();

        let dispatcher = Dispatcher::new(backend);

        // Create a stream and don't send any messages
        let mut stream = dispatcher.stream("topic_idle").await;

        // The stream should complete on its own after the idle timeout
        // We'll wait a bit longer than the idle timeout to be sure
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(None) => {
                // This is the expected case - stream should complete with None
                println!("Stream completed as expected due to idle timeout");
            }
            Ok(Some(_)) => {
                panic!("Received unexpected message");
            }
            Err(_) => {
                panic!("Timeout waiting for stream to complete");
            }
        }
    }

    #[tokio::test]
    #[timeout(15000)]
    async fn test_exponential_backoff() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        // Create a backend with custom backoff settings for testing
        let config = StreamConfig {
            min_backoff: Duration::from_millis(50),     // Start with 50ms
            max_backoff: Duration::from_millis(200),    // Cap at 200ms
            idle_timeout: Some(Duration::from_secs(2)), // 2s idle timeout
            ..Default::default()
        };
        let backend = JetStreamBackend::with_config(&url, config).await.unwrap();
        backend.ensure_stream("topic_backoff").await.unwrap();

        let dispatcher = Dispatcher::new(backend);

        // Create a stream
        let mut stream = dispatcher.stream("topic_backoff").await;

        // Wait a bit to allow several backoff cycles
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now send a message
        let message = Message::SimpleMessage(SimpleMessage {
            id: "1".to_string(),
            name: "Backoff Test".to_string(),
        });
        dispatcher
            .send("topic_backoff", message.clone())
            .await
            .unwrap();

        // We should still receive the message despite the backoff
        match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(received_message)) => {
                assert_eq!(received_message, message);
                println!("Message received successfully after backoff");
            }
            Ok(None) => {
                panic!("No message received after backoff");
            }
            Err(_) => {
                panic!("Timeout waiting for message after backoff");
            }
        }
    }
}
