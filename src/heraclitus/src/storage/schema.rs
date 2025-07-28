use arrow::array::{
    BinaryBuilder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: HashMap<String, Vec<u8>>,
    pub producer_id: Option<i64>,
    pub producer_epoch: Option<i16>,
    pub sequence: Option<i32>,
}

impl KafkaMessage {
    /// Create a KafkaMessage from a kafka-protocol Record with topic and partition info
    pub fn from_record(
        record: &kafka_protocol::records::Record,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Self {
        use kafka_protocol::records::{NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE};

        Self {
            topic,
            partition,
            offset,
            timestamp: record.timestamp,
            key: record.key.as_ref().map(|k| k.to_vec()),
            value: record
                .value
                .as_ref()
                .map(|v| v.to_vec())
                .unwrap_or_default(),
            headers: record
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_string(),
                        v.as_ref().map(|v| v.to_vec()).unwrap_or_default(),
                    )
                })
                .collect(),
            producer_id: if record.producer_id == NO_PRODUCER_ID {
                None
            } else {
                Some(record.producer_id)
            },
            producer_epoch: if record.producer_epoch == NO_PRODUCER_EPOCH {
                None
            } else {
                Some(record.producer_epoch)
            },
            sequence: if record.sequence == NO_SEQUENCE {
                None
            } else {
                Some(record.sequence)
            },
        }
    }

    /// Convert KafkaMessage to kafka-protocol Record for network transmission
    pub fn to_record(&self) -> kafka_protocol::records::Record {
        use kafka_protocol::indexmap::IndexMap;
        use kafka_protocol::records::{
            NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
            TimestampType,
        };

        kafka_protocol::records::Record {
            // Batch properties - using defaults for non-transactional records
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: self.producer_id.unwrap_or(NO_PRODUCER_ID),
            producer_epoch: self.producer_epoch.unwrap_or(NO_PRODUCER_EPOCH),

            // Record properties
            timestamp_type: TimestampType::Creation,
            offset: self.offset,
            sequence: self.sequence.unwrap_or(NO_SEQUENCE),
            timestamp: self.timestamp,
            key: self.key.as_ref().map(|k| bytes::Bytes::copy_from_slice(k)),
            value: Some(bytes::Bytes::copy_from_slice(&self.value)),
            headers: {
                let mut header_map = IndexMap::new();
                for (k, v) in &self.headers {
                    header_map.insert(k.clone().into(), Some(bytes::Bytes::copy_from_slice(v)));
                }
                header_map
            },
        }
    }
}

#[derive(Default)]
pub struct KafkaMessageBatch {
    pub messages: Vec<KafkaMessage>,
}

impl KafkaMessageBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, message: KafkaMessage) {
        self.messages.push(message);
    }

    pub fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("topic", DataType::Utf8, false),
            Field::new("partition", DataType::Int32, false),
            Field::new("offset", DataType::Int64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("key", DataType::Binary, true),
            Field::new("value", DataType::Binary, false),
            Field::new("headers", DataType::Utf8, false), // JSON encoded
            Field::new("producer_id", DataType::Int64, true),
            Field::new("producer_epoch", DataType::Int16, true),
            Field::new("sequence", DataType::Int32, true),
        ]))
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let schema = Self::arrow_schema();
        let num_rows = self.messages.len();

        let mut topic_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut partition_builder = Int32Builder::with_capacity(num_rows);
        let mut offset_builder = Int64Builder::with_capacity(num_rows);
        let mut timestamp_builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
        let mut key_builder = BinaryBuilder::with_capacity(num_rows, num_rows * 64);
        let mut value_builder = BinaryBuilder::with_capacity(num_rows, num_rows * 256);
        let mut headers_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut producer_id_builder = Int64Builder::with_capacity(num_rows);
        let mut producer_epoch_builder = Int16Builder::with_capacity(num_rows);
        let mut sequence_builder = Int32Builder::with_capacity(num_rows);

        for msg in &self.messages {
            topic_builder.append_value(&msg.topic);
            partition_builder.append_value(msg.partition);
            offset_builder.append_value(msg.offset);
            timestamp_builder.append_value(msg.timestamp);

            match &msg.key {
                Some(key) => key_builder.append_value(key),
                None => key_builder.append_null(),
            }

            value_builder.append_value(&msg.value);

            // Encode headers as JSON
            let headers_json = serde_json::to_string(&msg.headers).unwrap_or_default();
            headers_builder.append_value(&headers_json);

            match msg.producer_id {
                Some(id) => producer_id_builder.append_value(id),
                None => producer_id_builder.append_null(),
            }

            match msg.producer_epoch {
                Some(epoch) => producer_epoch_builder.append_value(epoch),
                None => producer_epoch_builder.append_null(),
            }

            match msg.sequence {
                Some(seq) => sequence_builder.append_value(seq),
                None => sequence_builder.append_null(),
            }
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(topic_builder.finish()),
                Arc::new(partition_builder.finish()),
                Arc::new(offset_builder.finish()),
                Arc::new(timestamp_builder.finish()),
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
                Arc::new(headers_builder.finish()),
                Arc::new(producer_id_builder.finish()),
                Arc::new(producer_epoch_builder.finish()),
                Arc::new(sequence_builder.finish()),
            ],
        )
    }
}
