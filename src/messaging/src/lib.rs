use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use messages::{span::SpanBatch, trace::Trace, SimpleMessage};
use serde::{Deserialize, Serialize};

pub mod backend;
pub mod config;
pub mod messages;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "message_type", content = "data")]
pub enum Message {
    SimpleMessage(SimpleMessage),
    SpanBatch(SpanBatch),
    Trace(Trace),
}

// Messaging backend trait
#[async_trait]
pub trait MessagingBackend: Send + Sync {
    async fn send_message(&self, topic: &str, message: Message) -> Result<(), String>;

    // Return a stream of messages for a specific topic
    async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>>;

    async fn ack(&self, message: Message) -> Result<(), String>;
}

pub struct Dispatcher<B> {
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

    pub async fn ack(&self, message: Message) -> Result<(), String> {
        self.backend.ack(message).await
    }
}

// Add implementation for Arc<dyn MessagingBackend>
impl Dispatcher<std::sync::Arc<dyn MessagingBackend>> {
    pub fn new_with_arc(backend: std::sync::Arc<dyn MessagingBackend>) -> Self {
        Self { backend }
    }

    pub async fn send(&self, topic: &str, message: Message) -> Result<(), String> {
        self.backend.send_message(topic, message).await
    }

    pub async fn stream(&self, topic: &str) -> Pin<Box<dyn Stream<Item = Message> + Send>> {
        self.backend.stream(topic).await
    }

    pub async fn ack(&self, message: Message) -> Result<(), String> {
        self.backend.ack(message).await
    }
}
