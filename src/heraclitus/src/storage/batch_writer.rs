use crate::{
    config::BatchingConfig,
    error::Result,
    metrics::Metrics,
    storage::{KafkaMessage, KafkaMessageBatch, ObjectStorageLayout},
};
use chrono::{DateTime, Utc};
use datafusion::parquet::arrow::ArrowWriter;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};
use tracing::{error, info};

pub struct BatchWriter {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
    config: BatchingConfig,
    batches: Arc<Mutex<HashMap<(String, i32), PendingBatch>>>,
    metrics: Arc<Metrics>,
}

struct PendingBatch {
    batch: KafkaMessageBatch,
    size_bytes: usize,
    created_at: DateTime<Utc>,
}

impl BatchWriter {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        layout: ObjectStorageLayout,
        config: BatchingConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            object_store,
            layout,
            config,
            batches: Arc::new(Mutex::new(HashMap::new())),
            metrics,
        }
    }

    pub async fn start_flush_timer(&self) {
        let batches = self.batches.clone();
        let config = self.config.clone();
        let object_store = self.object_store.clone();
        let layout = self.layout.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(config.max_batch_delay_ms));

            loop {
                interval.tick().await;

                let mut batches_guard = batches.lock().await;
                let now = Utc::now();

                let mut to_flush = Vec::new();
                for ((topic, partition), batch) in batches_guard.iter() {
                    let age = now.signed_duration_since(batch.created_at);
                    if age.num_milliseconds() >= config.max_batch_delay_ms as i64 {
                        to_flush.push((topic.clone(), *partition));
                    }
                }

                for (topic, partition) in to_flush {
                    if let Some(batch) = batches_guard.remove(&(topic.clone(), partition)) {
                        drop(batches_guard); // Release lock before I/O

                        if let Err(e) = Self::flush_batch(
                            &object_store,
                            &layout,
                            &topic,
                            partition,
                            batch.batch,
                            &metrics,
                        )
                        .await
                        {
                            error!("Failed to flush batch: {e}");
                            metrics.storage.flush_errors.inc();
                        }

                        batches_guard = batches.lock().await;
                    }
                }
            }
        });
    }

    /// Flush all pending batches to storage - used during shutdown
    pub async fn flush_all_pending(&self) -> Result<()> {
        info!("Flushing all pending batches before shutdown");

        let batches_to_flush = {
            let mut batches = self.batches.lock().await;
            let pending: Vec<_> = batches
                .drain()
                .map(|((topic, partition), batch)| {
                    info!(
                        "Flushing batch for topic '{}' partition {} with {} messages",
                        topic,
                        partition,
                        batch.batch.messages.len()
                    );
                    (topic, partition, batch)
                })
                .collect();
            pending
        };

        let mut flush_errors = 0;
        for (topic, partition, batch) in batches_to_flush {
            if let Err(e) = Self::flush_batch(
                &self.object_store,
                &self.layout,
                &topic,
                partition,
                batch.batch,
                &self.metrics,
            )
            .await
            {
                error!("Failed to flush batch during shutdown: {e}");
                flush_errors += 1;
                self.metrics.storage.flush_errors.inc();
            }
        }

        // Reset pending messages metric
        self.metrics.storage.pending_messages.set(0);

        if flush_errors > 0 {
            Err(crate::error::HeraclitusError::Storage(format!(
                "Failed to flush {flush_errors} batches during shutdown"
            )))
        } else {
            info!("Successfully flushed all pending batches");
            Ok(())
        }
    }

    pub async fn write(&self, message: KafkaMessage) -> Result<()> {
        let topic = message.topic.clone();
        let partition = message.partition;
        let message_size = Self::estimate_message_size(&message);

        // Update metrics
        self.metrics.storage.messages_written.inc();
        self.metrics
            .storage
            .bytes_written
            .inc_by(message_size as u64);

        let mut batches = self.batches.lock().await;

        let batch = batches
            .entry((topic.clone(), partition))
            .or_insert_with(|| PendingBatch {
                batch: KafkaMessageBatch::new(),
                size_bytes: 0,
                created_at: Utc::now(),
            });

        batch.batch.add(message);
        batch.size_bytes += message_size;

        // Check if we should flush
        let should_flush = batch.batch.messages.len() >= self.config.max_batch_size
            || batch.size_bytes >= self.config.max_batch_bytes;

        // Update pending messages metric
        let pending_count = batches
            .values()
            .map(|b| b.batch.messages.len())
            .sum::<usize>() as i64;
        self.metrics.storage.pending_messages.set(pending_count);

        if should_flush {
            let pending_batch = batches.remove(&(topic.clone(), partition)).unwrap();
            drop(batches); // Release lock before I/O

            Self::flush_batch(
                &self.object_store,
                &self.layout,
                &topic,
                partition,
                pending_batch.batch,
                &self.metrics,
            )
            .await?;
        }

        Ok(())
    }

    async fn flush_batch(
        object_store: &Arc<dyn ObjectStore>,
        layout: &ObjectStorageLayout,
        topic: &str,
        partition: i32,
        batch: KafkaMessageBatch,
        metrics: &Arc<Metrics>,
    ) -> Result<()> {
        if batch.messages.is_empty() {
            return Ok(());
        }

        let len = batch.messages.len();
        info!("Flushing batch for topic {topic} partition {partition} with {len} messages");

        let now = Utc::now();
        let hour = now.format("%Y-%m-%d-%H").to_string();
        let segment_id = uuid::Uuid::new_v4().to_string();

        let path = layout
            .messages_path(topic, partition, &hour)
            .child(format!("segment-{segment_id}.parquet"));

        // Convert to Arrow RecordBatch
        let record_batch = batch
            .to_record_batch()
            .map_err(|e| crate::error::HeraclitusError::Serialization(e.to_string()))?;

        // Write as Parquet
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)
                .map_err(|e| crate::error::HeraclitusError::Storage(e.to_string()))?;

            writer
                .write(&record_batch)
                .map_err(|e| crate::error::HeraclitusError::Storage(e.to_string()))?;

            writer
                .close()
                .map_err(|e| crate::error::HeraclitusError::Storage(e.to_string()))?;
        }

        // Upload to object storage
        info!("Writing segment to path: {}", path);
        object_store.put(&path, buffer.into()).await?;

        info!(
            "Successfully wrote segment {segment_id} for topic {topic} partition {partition} to {path}"
        );

        // Update metrics
        metrics.storage.batches_flushed.inc();

        Ok(())
    }

    fn estimate_message_size(message: &KafkaMessage) -> usize {
        let mut size = 0;
        size += message.topic.len();
        size += 4 + 8 + 8; // partition, offset, timestamp
        size += message.key.as_ref().map(|k| k.len()).unwrap_or(0);
        size += message.value.len();
        size += message
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
        size += 8 + 2 + 4; // producer fields
        size
    }

    /// Get a reference to the object store
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// Get a reference to the storage layout
    pub fn layout(&self) -> &ObjectStorageLayout {
        &self.layout
    }
}
