use crate::{
    error::{HeraclitusError, Result},
    storage::{KafkaMessage, ObjectStorageLayout},
};
use datafusion::arrow::array::{
    Array, BinaryArray, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use object_store::{ObjectMeta, ObjectStore, path::Path as ObjectPath};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

pub struct MessageReader {
    object_store: Arc<dyn ObjectStore>,
    layout: ObjectStorageLayout,
}

impl MessageReader {
    pub fn new(object_store: Arc<dyn ObjectStore>, layout: ObjectStorageLayout) -> Self {
        Self {
            object_store,
            layout,
        }
    }

    pub async fn read_messages(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_messages: usize,
    ) -> Result<Vec<KafkaMessage>> {
        info!(
            "Reading messages from topic {} partition {} starting at offset {}",
            topic, partition, start_offset
        );

        // List all segments for this partition
        let segments = self.list_segments(topic, partition).await?;
        info!(
            "Found {} segments for topic {} partition {}",
            segments.len(),
            topic,
            partition
        );

        let mut messages = Vec::new();
        let mut current_offset = start_offset;

        for segment in segments {
            if messages.len() >= max_messages {
                break;
            }

            let segment_messages = self
                .read_segment(
                    &segment,
                    topic,
                    partition,
                    current_offset,
                    max_messages - messages.len(),
                )
                .await?;

            if !segment_messages.is_empty() {
                current_offset = segment_messages.last().unwrap().offset + 1;
                messages.extend(segment_messages);
            }
        }

        Ok(messages)
    }

    async fn list_segments(&self, topic: &str, partition: i32) -> Result<Vec<ObjectMeta>> {
        let mut segments = Vec::new();

        // Build the base path without the hour component to list all hour directories
        let base_path = ObjectPath::from(format!(
            "{}/messages/topic={}/partition={}/",
            self.layout.prefix, topic, partition
        ));

        info!("Listing segments from path: {}", base_path);

        let mut stream = self.object_store.list(Some(&base_path));

        while let Some(result) = futures::StreamExt::next(&mut stream).await {
            match result {
                Ok(meta) => {
                    if meta.location.as_ref().ends_with(".parquet") {
                        segments.push(meta);
                    }
                }
                Err(e) => {
                    tracing::error!("Error listing segments: {}", e);
                    return Err(e.into());
                }
            }
        }

        // Sort by path to ensure chronological order
        segments.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

        Ok(segments)
    }

    async fn read_segment(
        &self,
        segment: &ObjectMeta,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_messages: usize,
    ) -> Result<Vec<KafkaMessage>> {
        debug!("Reading segment: {}", segment.location);

        let get_result = self.object_store.get(&segment.location).await?;
        let bytes = get_result.bytes().await?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| HeraclitusError::Storage(e.to_string()))?
            .build()
            .map_err(|e| HeraclitusError::Storage(e.to_string()))?;

        let mut messages = Vec::new();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| HeraclitusError::Storage(e.to_string()))?;

            let batch_messages = Self::record_batch_to_messages(&batch)?;

            for msg in batch_messages {
                if msg.topic == topic && msg.partition == partition && msg.offset >= start_offset {
                    messages.push(msg);
                    if messages.len() >= max_messages {
                        return Ok(messages);
                    }
                }
            }
        }

        Ok(messages)
    }

    fn record_batch_to_messages(batch: &RecordBatch) -> Result<Vec<KafkaMessage>> {
        let num_rows = batch.num_rows();
        let mut messages = Vec::with_capacity(num_rows);

        let topic_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| HeraclitusError::Serialization("Invalid topic column".to_string()))?;

        let partition_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                HeraclitusError::Serialization("Invalid partition column".to_string())
            })?;

        let offset_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| HeraclitusError::Serialization("Invalid offset column".to_string()))?;

        let timestamp_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| {
                HeraclitusError::Serialization("Invalid timestamp column".to_string())
            })?;

        let key_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| HeraclitusError::Serialization("Invalid key column".to_string()))?;

        let value_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| HeraclitusError::Serialization("Invalid value column".to_string()))?;

        let headers_array = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| HeraclitusError::Serialization("Invalid headers column".to_string()))?;

        let producer_id_array = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                HeraclitusError::Serialization("Invalid producer_id column".to_string())
            })?;

        let producer_epoch_array = batch
            .column(8)
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| {
                HeraclitusError::Serialization("Invalid producer_epoch column".to_string())
            })?;

        let sequence_array = batch
            .column(9)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| HeraclitusError::Serialization("Invalid sequence column".to_string()))?;

        for i in 0..num_rows {
            let headers: HashMap<String, Vec<u8>> = if headers_array.is_valid(i) {
                serde_json::from_str(headers_array.value(i)).unwrap_or_else(|_| HashMap::new())
            } else {
                HashMap::new()
            };

            messages.push(KafkaMessage {
                topic: topic_array.value(i).to_string(),
                partition: partition_array.value(i),
                offset: offset_array.value(i),
                timestamp: timestamp_array.value(i),
                key: if key_array.is_valid(i) {
                    Some(key_array.value(i).to_vec())
                } else {
                    None
                },
                value: value_array.value(i).to_vec(),
                headers,
                producer_id: if producer_id_array.is_valid(i) {
                    Some(producer_id_array.value(i))
                } else {
                    None
                },
                producer_epoch: if producer_epoch_array.is_valid(i) {
                    Some(producer_epoch_array.value(i))
                } else {
                    None
                },
                sequence: if sequence_array.is_valid(i) {
                    Some(sequence_array.value(i))
                } else {
                    None
                },
            });
        }

        Ok(messages)
    }
}
