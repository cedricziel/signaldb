use std::sync::Arc;

use common::{
    model::span::SpanBatch,
    queue::{Message, MessagingBackend},
};
use tokio::sync::Mutex;

pub struct QueueHandler<Q: MessagingBackend + 'static> {
    queue: Arc<Mutex<Q>>,
}

impl<Q: MessagingBackend + 'static> QueueHandler<Q> {
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self { queue }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        // Subscribe to trace messages
        self.queue
            .lock()
            .await
            .consume("trace".to_string())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subscribe to trace messages: {}", e))?;

        // Start the message processing loop
        tokio::spawn(async move {
            self.process_messages().await;
        });

        Ok(())
    }

    async fn process_messages(&self) {
        // In the simplified queue interface, message processing would be handled
        // by the subscriber directly through callbacks or channels, rather than
        // polling for messages. For now, this is a placeholder.
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    async fn handle_message(&self, span_batch: &SpanBatch) -> anyhow::Result<()> {
        log::info!(
            "Processing trace batch with {} spans",
            span_batch.spans.len()
        );

        // TODO: Store the spans in the database
        // This is where we'll integrate with DataFusion or another storage backend

        Ok(())
    }

    pub async fn publish_trace(&self, span_batch: SpanBatch) -> anyhow::Result<()> {
        let message = Message::new_in_memory(span_batch);
        self.queue
            .lock()
            .await
            .publish("traces".to_string(), message)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to publish trace message: {}", e))?;
        Ok(())
    }
}
