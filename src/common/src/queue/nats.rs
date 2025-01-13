use async_nats::jetstream::{self, Context};
use async_nats::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::{Message, Queue, QueueError, QueueResult};

const DEFAULT_STREAM_NAME: &str = "signaldb";

/// NATS JetStream queue implementation
#[derive(Debug, Clone)]
pub struct NatsQueue {
    /// NATS client
    client: Option<Arc<Client>>,
    /// JetStream context
    context: Option<Arc<Context>>,
    /// Stream name
    stream_name: String,
    /// Subscribed topics
    topics: Arc<Mutex<HashSet<String>>>,
}

impl Default for NatsQueue {
    fn default() -> Self {
        let mut queue = Self {
            client: None,
            context: None,
            stream_name: DEFAULT_STREAM_NAME.to_string(),
            topics: Arc::new(Mutex::new(HashSet::new())),
        };

        // Initialize connection in a blocking way
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let client = async_nats::connect("nats://localhost:4222")
                .await
                .expect("Failed to connect to NATS");

            let context = jetstream::new(client.clone());

            let stream = context
                .get_or_create_stream(async_nats::jetstream::stream::Config {
                    name: queue.stream_name.clone(),
                    subjects: vec!["signaldb.>".to_string()],
                    ..Default::default()
                })
                .await
                .expect("Failed to create stream");

            queue.client = Some(Arc::new(client));
            queue.context = Some(Arc::new(context));
        });

        queue
    }
}

#[async_trait::async_trait]
impl Queue for NatsQueue {
    async fn publish<T>(&self, message: Message<T>) -> QueueResult<()>
    where
        T: Serialize + for<'a> Deserialize<'a> + Send + Sync + std::fmt::Debug,
    {
        let context = self
            .context
            .as_ref()
            .ok_or_else(|| QueueError::ConnectionError("Queue not connected".to_string()))?;

        // Serialize the message
        let bytes = message.to_bytes()?;

        // Publish to JetStream
        context
            .publish("signaldb.message".to_string(), bytes.into())
            .await
            .map_err(|e| QueueError::PublishError(e.to_string()))?;

        Ok(())
    }

    async fn subscribe(&mut self, topic: String) -> QueueResult<()> {
        let mut topics = self.topics.lock().await;
        topics.insert(topic);
        Ok(())
    }
}
