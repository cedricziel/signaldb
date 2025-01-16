use async_nats::jetstream::{self};
use async_nats::Client;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;

use super::{Message, MessagingBackend, QueueError, QueueResult};

const DEFAULT_STREAM_NAME: &str = "signaldb";

/// NATS JetStream queue implementation
#[derive(Debug, Clone)]
pub struct NatsQueue {
    /// The NATS client
    client: Option<Client>,
}

impl NatsQueue {
    pub fn new(client: Client) -> Self {
        Self {
            client: Some(client.clone()),
        }
    }
}

#[async_trait::async_trait]
impl MessagingBackend for NatsQueue {
    async fn create(&mut self) -> QueueResult<()> {
        Ok(())
    }

    async fn publish(&self, topic: String, message: Bytes) -> QueueResult<()> {
        // Publish to JetStream
        match &self.client {
            Some(client) => {
                let js = jetstream::new(client.clone());

                js.publish(topic, message)
                    .await
                    .map_err(|e| QueueError::PublishError(e.to_string()))?;
            }
            None => {
                return Err(QueueError::ConnectionError(
                    "Queue not connected".to_string(),
                ))
            }
        }

        Ok(())
    }

    async fn consume(&mut self, topic: String) -> QueueResult<Arc<Receiver<super::Message>>> {
        // Create a sync channel
        let (tx, rx) = channel();

        if let Some(client) = &self.client {
            let context = jetstream::new(client.clone());

            // Spawn task to forward messages
            tokio::task::spawn(async move {
                // Get or create stream
                let stream = context
                    .get_or_create_stream(jetstream::stream::Config {
                        name: DEFAULT_STREAM_NAME.to_string(),
                        subjects: vec![format!("{}.>", DEFAULT_STREAM_NAME)],
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| QueueError::SubscribeError(e.to_string()))
                    .unwrap();

                // Create pull consumer
                let consumer = stream
                    .create_consumer(jetstream::consumer::pull::Config {
                        durable_name: Some(format!("{}_consumer", topic)),
                        filter_subject: topic.clone(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| QueueError::SubscribeError(e.to_string()))
                    .unwrap();

                // Get messages
                let mut messages = consumer
                    .messages()
                    .await
                    .map_err(|e| QueueError::SubscribeError(e.to_string()))
                    .unwrap();

                while let Some(msg) = messages.next().await {
                    if let Ok(msg) = msg {
                        if let Ok(message) = Message::from_bytes(&msg.payload, true) {
                            let _ = tx.send(message);
                        }
                    }
                }
            });

            Ok(Arc::new(rx))
        } else {
            return Err(QueueError::ConnectionError(
                "Queue not connected".to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use testcontainers_modules::{
        nats::{Nats, NatsServerCmd},
        testcontainers::{runners::AsyncRunner, ImageExt},
    };

    #[derive(Debug, Serialize, Deserialize)]
    struct TestMessage {
        content: String,
    }

    #[tokio::test]
    async fn test_connect() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let nats_client = async_nats::connect(url).await.unwrap();

        let queue = NatsQueue::new(nats_client);
        assert!(queue.client.is_some());
    }

    #[tokio::test]
    async fn test_subscribe() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let nats_client = async_nats::connect(url).await.unwrap();

        let mut queue = NatsQueue::new(nats_client);

        let rx = queue.consume("test.topic".to_string()).await;
        assert!(rx.is_ok());
    }

    #[tokio::test]
    async fn test_publish_and_connect() {
        let nats = provide_nats().await.unwrap();
        let url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );

        let nats_client = async_nats::connect(url).await.unwrap();

        let mut queue = NatsQueue::new(nats_client);

        let publish_result = queue.publish("test.topic".to_string(), msg).await;
        assert!(publish_result.is_ok());
    }
}
