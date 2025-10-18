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

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::{NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE};
    use std::collections::HashMap;

    #[test]
    fn test_kafka_message_conversion_roundtrip() {
        // Create a test KafkaMessage with all fields
        let mut headers = HashMap::new();
        headers.insert("test-header".to_string(), b"test-value".to_vec());
        headers.insert("another-header".to_string(), b"another-value".to_vec());

        let kafka_msg = KafkaMessage {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            timestamp: 1234567890,
            key: Some(b"test-key".to_vec()),
            value: b"test-value".to_vec(),
            headers,
            producer_id: Some(123),
            producer_epoch: Some(5),
            sequence: Some(10),
        };

        // Convert to Record and back to verify consistency
        let record = kafka_msg.to_record();
        let converted_back = KafkaMessage::from_record(&record, "test-topic".to_string(), 0, 42);

        assert_eq!(converted_back.topic, "test-topic");
        assert_eq!(converted_back.partition, 0);
        assert_eq!(converted_back.offset, 42);
        assert_eq!(converted_back.timestamp, 1234567890);
        assert_eq!(converted_back.key, Some(b"test-key".to_vec()));
        assert_eq!(converted_back.value, b"test-value");
        assert_eq!(converted_back.producer_id, Some(123));
        assert_eq!(converted_back.producer_epoch, Some(5));
        assert_eq!(converted_back.sequence, Some(10));

        // Check headers were preserved
        assert_eq!(converted_back.headers.len(), 2);
        assert_eq!(
            converted_back.headers.get("test-header"),
            Some(&b"test-value".to_vec())
        );
        assert_eq!(
            converted_back.headers.get("another-header"),
            Some(&b"another-value".to_vec())
        );
    }

    #[test]
    fn test_kafka_message_minimal_fields() {
        // Create a minimal KafkaMessage with only required fields
        let kafka_msg = KafkaMessage {
            topic: "minimal-topic".to_string(),
            partition: 1,
            offset: 0,
            timestamp: 0,
            key: None,
            value: b"minimal-value".to_vec(),
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        };

        // Convert to Record
        let record = kafka_msg.to_record();

        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp, 0);
        assert!(record.key.is_none());
        assert_eq!(record.value.as_ref().unwrap().as_ref(), b"minimal-value");
        assert_eq!(record.producer_id, NO_PRODUCER_ID);
        assert_eq!(record.producer_epoch, NO_PRODUCER_EPOCH);
        assert_eq!(record.sequence, NO_SEQUENCE);
        assert!(record.headers.is_empty());

        // Convert back to KafkaMessage
        let converted_back = KafkaMessage::from_record(&record, "minimal-topic".to_string(), 1, 0);

        assert_eq!(converted_back.topic, "minimal-topic");
        assert_eq!(converted_back.partition, 1);
        assert_eq!(converted_back.offset, 0);
        assert!(converted_back.key.is_none());
        assert_eq!(converted_back.value, b"minimal-value");
        assert!(converted_back.producer_id.is_none());
        assert!(converted_back.producer_epoch.is_none());
        assert!(converted_back.sequence.is_none());
        assert!(converted_back.headers.is_empty());
    }

    #[test]
    fn test_kafka_message_empty_value() {
        let kafka_msg = KafkaMessage {
            topic: "empty-value-topic".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1234567890,
            key: Some(b"has-key".to_vec()),
            value: b"".to_vec(), // Empty value
            headers: HashMap::new(),
            producer_id: None,
            producer_epoch: None,
            sequence: None,
        };

        let record = kafka_msg.to_record();
        assert!(record.value.is_some());
        assert_eq!(record.value.as_ref().unwrap().as_ref(), b"");

        let converted_back =
            KafkaMessage::from_record(&record, "empty-value-topic".to_string(), 0, 100);
        assert_eq!(converted_back.value, b"");
        assert_eq!(converted_back.key, Some(b"has-key".to_vec()));
    }

    #[test]
    fn test_kafka_message_large_headers() {
        let mut headers = HashMap::new();
        for i in 0..10 {
            headers.insert(
                format!("header-{i}"),
                format!("value-{i}-with-some-longer-content").into_bytes(),
            );
        }

        let kafka_msg = KafkaMessage {
            topic: "headers-topic".to_string(),
            partition: 2,
            offset: 200,
            timestamp: 9876543210,
            key: Some(b"key-with-headers".to_vec()),
            value: b"value-with-many-headers".to_vec(),
            headers: headers.clone(),
            producer_id: Some(999),
            producer_epoch: Some(3),
            sequence: Some(555),
        };

        let record = kafka_msg.to_record();
        let converted_back =
            KafkaMessage::from_record(&record, "headers-topic".to_string(), 2, 200);

        // Verify all headers were preserved
        assert_eq!(converted_back.headers.len(), 10);
        for i in 0..10 {
            let key = format!("header-{i}");
            let expected_value = format!("value-{i}-with-some-longer-content").into_bytes();
            assert_eq!(converted_back.headers.get(&key), Some(&expected_value));
        }
    }
}
