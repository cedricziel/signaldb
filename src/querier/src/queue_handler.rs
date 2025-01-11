use std::sync::Arc;

use common::{
    model::span::SpanBatch,
    queue::{Message, MessageType, Queue, QueueConfig},
};
use tokio::sync::Mutex;

pub struct QueueHandler<Q: Queue + 'static> {
    queue: Arc<Mutex<Q>>,
}

impl<Q: Queue + 'static> QueueHandler<Q> {
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self { queue }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let mut queue_lock = self.queue.lock().await;

        // Connect to the queue
        queue_lock
            .connect(QueueConfig::default())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to queue: {}", e))?;

        // Subscribe to trace messages
        queue_lock
            .subscribe(MessageType::Signal, Some("trace".to_string()))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subscribe to trace messages: {}", e))?;

        drop(queue_lock);

        // Start the message processing loop
        tokio::spawn(async move {
            self.process_messages().await;
        });

        Ok(())
    }

    async fn process_messages(&self) {
        loop {
            let message = {
                let queue = self.queue.lock().await;
                match queue.receive::<SpanBatch>().await {
                    Ok(Some(message)) => message,
                    Ok(None) => {
                        // No message available, wait a bit before trying again
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(e) => {
                        log::error!("Error receiving message: {}", e);
                        continue;
                    }
                }
            };

            // Process the message
            match self.handle_message(&message).await {
                Ok(_) => {
                    let queue = self.queue.lock().await;
                    if let Err(e) = queue.ack(&message).await {
                        log::error!("Failed to acknowledge message: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("Failed to handle message: {}", e);
                }
            }
        }
    }

    async fn handle_message(&self, message: &Message<SpanBatch>) -> anyhow::Result<()> {
        match message.message_type {
            MessageType::Signal => {
                match message.subtype.as_str() {
                    "trace" => {
                        // Process the trace data
                        let span_batch = &message.payload;
                        log::info!(
                            "Processing trace batch with {} spans",
                            span_batch.spans.len()
                        );

                        // TODO: Store the spans in the database
                        // This is where we'll integrate with DataFusion or another storage backend
                    }
                    _ => {
                        log::warn!("Unknown signal subtype: {}", message.subtype);
                    }
                }
            }
            _ => {
                log::warn!("Unexpected message type: {:?}", message.message_type);
            }
        }

        Ok(())
    }
}
