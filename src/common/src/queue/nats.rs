use std::collections::HashMap;
use std::sync::Arc;

use async_nats::jetstream::{
    self,
    consumer::{self, PullConsumer},
    stream::{self},
    Context,
};
use async_nats::Client;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::RwLock;

use super::{Message, MessageType, Queue, QueueConfig, QueueError, QueueResult};

/// NATS Queue implementation using JetStream
#[derive(Debug)]
pub struct NatsQueue {
    client: Option<Client>,
    js_context: Option<Context>,
    consumers: Arc<RwLock<HashMap<String, PullConsumer>>>,
    stream_name: String,
}

impl Default for NatsQueue {
    fn default() -> Self {
        Self {
            client: None,
            js_context: None,
            consumers: Arc::new(RwLock::new(HashMap::new())),
            stream_name: "signaldb".to_string(),
        }
    }
}

impl NatsQueue {
    /// Create a new consumer key from message type and subtype
    fn consumer_key(message_type: &MessageType, subtype: Option<&str>) -> String {
        match (message_type, subtype) {
            (MessageType::Custom(custom), Some(sub)) => format!("{}_{}", custom, sub),
            (MessageType::Custom(custom), None) => custom.clone(),
            (msg_type, Some(sub)) => {
                format!("{}_{}", format!("{:?}", msg_type).to_lowercase(), sub)
            }
            (msg_type, None) => format!("{:?}", msg_type).to_lowercase(),
        }
    }

    /// Get subject for message type and subtype
    fn get_subject(message_type: &MessageType, subtype: Option<&str>) -> String {
        match (message_type, subtype) {
            (MessageType::Custom(custom), Some(sub)) => format!("{}.{}", custom, sub),
            (MessageType::Custom(custom), None) => custom.clone(),
            (msg_type, Some(sub)) => {
                format!("{}.{}", format!("{:?}", msg_type).to_lowercase(), sub)
            }
            (msg_type, None) => format!("{:?}", msg_type).to_lowercase(),
        }
    }
}

#[async_trait]
impl Queue for NatsQueue {
    async fn connect(&mut self, config: QueueConfig) -> QueueResult<()> {
        // Connect to NATS server
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| QueueError::ConnectionError(e.to_string()))?;

        // Create JetStream context
        let js_context = jetstream::new(client.clone());

        // Create stream if it doesn't exist
        js_context
            .create_stream(stream::Config {
                name: self.stream_name.clone(),
                subjects: vec![format!("{}.>", self.stream_name)],
                ..Default::default()
            })
            .await
            .map_err(|e| QueueError::ConnectionError(e.to_string()))?;

        self.client = Some(client);
        self.js_context = Some(js_context);

        Ok(())
    }

    async fn publish<T>(&self, message: Message<T>) -> QueueResult<()>
    where
        T: serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync,
    {
        let js = self
            .js_context
            .as_ref()
            .ok_or_else(|| QueueError::ConnectionError("Not connected".to_string()))?;

        let subject = format!(
            "{}.{}",
            self.stream_name,
            Self::get_subject(&message.message_type, Some(&message.subtype))
        );

        let payload = message
            .to_bytes()
            .map_err(|e| QueueError::PublishError(e.to_string()))?;

        js.publish(subject, payload.into())
            .await
            .map_err(|e| QueueError::PublishError(e.to_string()))?;

        Ok(())
    }

    async fn subscribe(
        &mut self,
        message_type: MessageType,
        subtype: Option<String>,
    ) -> QueueResult<()> {
        let js = self
            .js_context
            .as_ref()
            .ok_or_else(|| QueueError::ConnectionError("Not connected".to_string()))?;

        let consumer_key = Self::consumer_key(&message_type, subtype.as_deref());
        let subject = format!(
            "{}.{}",
            self.stream_name,
            Self::get_subject(&message_type, subtype.as_deref())
        );

        // Get the stream first
        let stream = js
            .get_stream(&self.stream_name)
            .await
            .map_err(|e| QueueError::SubscribeError(e.to_string()))?;

        // Create the consumer on the stream
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some(consumer_key.clone()),
                filter_subject: subject,
                ..Default::default()
            })
            .await
            .map_err(|e| QueueError::SubscribeError(e.to_string()))?;

        // Store consumer
        self.consumers.write().await.insert(consumer_key, consumer);

        Ok(())
    }

    async fn receive<T>(&self) -> QueueResult<Option<Message<T>>>
    where
        T: serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync,
    {
        let consumers = self.consumers.read().await;
        if consumers.is_empty() {
            return Ok(None);
        }

        // Try to receive from any consumer
        for consumer in consumers.values() {
            if let Ok(mut messages) = consumer.messages().await {
                while let Some(Ok(message)) = messages.next().await {
                    if let Ok(msg) = Message::from_bytes(&message.payload) {
                        // Acknowledge message
                        if let Err(e) = message.ack().await {
                            return Err(QueueError::ReceiveError(e.to_string()));
                        }
                        return Ok(Some(msg));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn ack<T>(&self, _message: &Message<T>) -> QueueResult<()>
    where
        T: serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync,
    {
        // Messages are auto-acknowledged in receive()
        Ok(())
    }

    async fn close(&mut self) -> QueueResult<()> {
        // Drop the client which will close the connection
        self.client = None;
        self.js_context = None;
        self.consumers.write().await.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::time::timeout;

    use testcontainers_modules::{
        nats::{Nats, NatsServerCmd},
        testcontainers::{runners::AsyncRunner, ImageExt},
    };

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    struct TestPayload {
        data: String,
    }

    #[tokio::test]
    async fn test_nats_queue_basic() {
        // Start a NATS server for testing
        let nats_cmd = NatsServerCmd::default().with_jetstream();
        let container = Nats::default().with_cmd(&nats_cmd).start().await.unwrap();

        let default_username = "ruser";
        let default_password = "T0pS3cr3t";
        let _topic = "foo";
        let port = container.get_host_port_ipv4(4222).await.unwrap();

        // prepare connection string
        let connection_url = &format!(
            "nats://{}:{}@127.0.0.1:{}",
            default_username, default_password, port,
        );

        let mut queue = NatsQueue::default();

        // Connect to queue
        queue
            .connect(QueueConfig {
                queue_type: "nats".to_string(),
                url: connection_url.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Subscribe to Signal/trace messages
        queue
            .subscribe(MessageType::Signal, Some("trace".to_string()))
            .await
            .unwrap();

        // Publish a message
        let message = Message::new_signal(
            "trace",
            TestPayload {
                data: "test".to_string(),
            },
        );
        queue.publish(message).await.unwrap();

        // Receive the message with timeout
        let received: Option<Message<TestPayload>> =
            timeout(Duration::from_secs(1), queue.receive())
                .await
                .unwrap()
                .unwrap();

        assert!(received.is_some());
        let received = received.unwrap();
        assert_eq!(received.message_type, MessageType::Signal);
        assert_eq!(received.subtype, "trace");
        assert_eq!(received.payload.data, "test");

        // Cleanup
        queue.close().await.unwrap();
    }
}
