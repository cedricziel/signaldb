use std::sync::Arc;

use messaging::{messages::span::SpanBatch, Message, MessagingBackend};
use tokio::sync::Mutex;

pub struct QueueHandler<Q: MessagingBackend + 'static> {
    queue: Arc<Mutex<Q>>,
}

impl<Q: MessagingBackend + 'static> QueueHandler<Q> {
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self { queue }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        // Create a stream for the trace topic
        let mut stream = self.queue.lock().await.stream("trace").await;
        let _ = stream; // Use the stream to avoid unused variable warning

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
        let message = Message::SpanBatch(span_batch);
        let queue = self.queue.lock().await;
        queue
            .send_message("traces", message)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to publish trace message: {}", e))?;
        Ok(())
    }
}
