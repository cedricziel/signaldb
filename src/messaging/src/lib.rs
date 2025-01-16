use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use messages::SimpleMessage;
use serde::{Deserialize, Serialize};

pub mod backend;
pub mod messages;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "message_type", content = "data")]
pub enum Message {
    SimpleMessage(SimpleMessage),
}

// Messaging backend trait
#[async_trait]
pub trait MessagingBackend: Send + Sync {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String>;

    // Return a stream of messages for a specific topic
    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>>;
}

pub struct Dispatcher<B: MessagingBackend> {
    backend: B,
}

impl<B: MessagingBackend> Dispatcher<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub async fn send(&self, topic: &str, message: Message) -> Result<(), String> {
        self.backend.send_message(topic, message).await
    }

    pub async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        self.backend.stream(topic).await
    }
}
