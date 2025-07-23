use crate::error::{HeraclitusError, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka Produce Request (API Key 0)
#[derive(Debug)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicData>,
}

#[derive(Debug)]
pub struct ProduceTopicData {
    pub name: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug)]
pub struct ProducePartitionData {
    pub partition_index: i32,
    pub records: Vec<u8>, // Raw record batch bytes
}

/// Kafka Produce Response
#[derive(Debug)]
pub struct ProduceResponse {
    pub responses: Vec<ProduceTopicResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug)]
pub struct ProduceTopicResponse {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponse>,
}

#[derive(Debug)]
pub struct ProducePartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

impl ProduceRequest {
    /// Parse a Produce request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // For API versions 3-8, we have:
        // transactional_id: nullable string
        // acks: int16
        // timeout_ms: int32
        // topics: array

        let transactional_id = if api_version >= 3 {
            crate::protocol::kafka_protocol::read_nullable_string(cursor)?
        } else {
            None
        };

        if cursor.remaining() < 6 {
            return Err(HeraclitusError::Protocol(
                "Insufficient data for produce request".to_string(),
            ));
        }

        let acks = cursor.get_i16();
        let timeout_ms = cursor.get_i32();

        // Read topics array
        let topics = Self::read_topics(cursor, api_version)?;

        Ok(ProduceRequest {
            transactional_id,
            acks,
            timeout_ms,
            topics,
        })
    }

    fn read_topics(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Vec<ProduceTopicData>> {
        if cursor.remaining() < 4 {
            return Err(HeraclitusError::Protocol(
                "Insufficient data for topics array".to_string(),
            ));
        }

        let num_topics = cursor.get_i32();
        if num_topics < 0 {
            return Ok(vec![]);
        }

        let mut topics = Vec::with_capacity(num_topics as usize);

        for _ in 0..num_topics {
            let name = crate::protocol::kafka_protocol::read_string(cursor)?;
            let partitions = Self::read_partitions(cursor, api_version)?;

            topics.push(ProduceTopicData { name, partitions });
        }

        Ok(topics)
    }

    fn read_partitions(
        cursor: &mut Cursor<&[u8]>,
        _api_version: i16,
    ) -> Result<Vec<ProducePartitionData>> {
        if cursor.remaining() < 4 {
            return Err(HeraclitusError::Protocol(
                "Insufficient data for partitions array".to_string(),
            ));
        }

        let num_partitions = cursor.get_i32();
        if num_partitions < 0 {
            return Ok(vec![]);
        }

        let mut partitions = Vec::with_capacity(num_partitions as usize);

        for _ in 0..num_partitions {
            if cursor.remaining() < 4 {
                return Err(HeraclitusError::Protocol(
                    "Insufficient data for partition index".to_string(),
                ));
            }

            let partition_index = cursor.get_i32();

            // Read record batch size
            if cursor.remaining() < 4 {
                return Err(HeraclitusError::Protocol(
                    "Insufficient data for record batch size".to_string(),
                ));
            }

            let record_batch_size = cursor.get_i32();
            if record_batch_size < 0 {
                // Null record batch
                partitions.push(ProducePartitionData {
                    partition_index,
                    records: vec![],
                });
                continue;
            }

            if cursor.remaining() < record_batch_size as usize {
                return Err(HeraclitusError::Protocol(
                    "Insufficient data for record batch".to_string(),
                ));
            }

            // Read the raw record batch bytes
            let mut records = vec![0u8; record_batch_size as usize];
            cursor.copy_to_slice(&mut records);

            partitions.push(ProducePartitionData {
                partition_index,
                records,
            });
        }

        Ok(partitions)
    }
}

impl ProduceResponse {
    /// Encode a Produce response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // Write topics array
        buf.put_i32(self.responses.len() as i32);

        for topic_response in &self.responses {
            // Write topic name
            crate::protocol::kafka_protocol::write_string(&mut buf, &topic_response.name);

            // Write partitions array
            buf.put_i32(topic_response.partitions.len() as i32);

            for partition in &topic_response.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i16(partition.error_code);
                buf.put_i64(partition.base_offset);

                // API version 2+ includes log_append_time
                if api_version >= 2 {
                    buf.put_i64(partition.log_append_time_ms);
                }

                // API version 5+ includes log_start_offset
                if api_version >= 5 {
                    buf.put_i64(partition.log_start_offset);
                }
            }
        }

        // API version 1+ includes throttle_time_ms
        if api_version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_request_parse_v3() {
        let mut buf = BytesMut::new();

        // transactional_id: null
        buf.put_i16(-1);

        // acks: 1
        buf.put_i16(1);

        // timeout_ms: 30000
        buf.put_i32(30000);

        // topics array with 1 topic
        buf.put_i32(1);

        // topic name
        buf.put_i16(10);
        buf.put_slice(b"test-topic");

        // partitions array with 1 partition
        buf.put_i32(1);

        // partition index: 0
        buf.put_i32(0);

        // record batch: null for now
        buf.put_i32(-1);

        let mut cursor = Cursor::new(&buf[..]);
        let request = ProduceRequest::parse(&mut cursor, 3).unwrap();

        assert_eq!(request.transactional_id, None);
        assert_eq!(request.acks, 1);
        assert_eq!(request.timeout_ms, 30000);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, "test-topic");
        assert_eq!(request.topics[0].partitions.len(), 1);
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].records.len(), 0);
    }

    #[test]
    fn test_produce_response_encode_v3() {
        let response = ProduceResponse {
            responses: vec![ProduceTopicResponse {
                name: "test-topic".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    base_offset: 100,
                    log_append_time_ms: 1234567890,
                    log_start_offset: 0,
                }],
            }],
            throttle_time_ms: 0,
        };

        let encoded = response.encode(3).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // Check topics array length
        assert_eq!(cursor.get_i32(), 1);

        // Check topic name
        let name_len = cursor.get_i16();
        assert_eq!(name_len, 10);
        let mut name_bytes = vec![0u8; name_len as usize];
        cursor.copy_to_slice(&mut name_bytes);
        assert_eq!(String::from_utf8(name_bytes).unwrap(), "test-topic");

        // Check partitions array length
        assert_eq!(cursor.get_i32(), 1);

        // Check partition response
        assert_eq!(cursor.get_i32(), 0); // partition_index
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i64(), 100); // base_offset
        assert_eq!(cursor.get_i64(), 1234567890); // log_append_time_ms

        // Check throttle_time_ms
        assert_eq!(cursor.get_i32(), 0);
    }

    #[test]
    fn test_produce_request_with_transactional_id() {
        let mut buf = BytesMut::new();

        // transactional_id: "txn-123"
        buf.put_i16(7);
        buf.put_slice(b"txn-123");

        // acks: -1 (all)
        buf.put_i16(-1);

        // timeout_ms: 5000
        buf.put_i32(5000);

        // topics array: empty
        buf.put_i32(0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = ProduceRequest::parse(&mut cursor, 3).unwrap();

        assert_eq!(request.transactional_id, Some("txn-123".to_string()));
        assert_eq!(request.acks, -1);
        assert_eq!(request.timeout_ms, 5000);
        assert_eq!(request.topics.len(), 0);
    }

    #[test]
    fn test_produce_response_encode_v5() {
        let response = ProduceResponse {
            responses: vec![ProduceTopicResponse {
                name: "test".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 1,
                    error_code: 0,
                    base_offset: 200,
                    log_append_time_ms: 9999,
                    log_start_offset: 50,
                }],
            }],
            throttle_time_ms: 100,
        };

        let encoded = response.encode(5).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // Skip to partition response fields
        cursor.advance(2 + 4 + 4 + 4); // array len + name len + name + partitions array len

        assert_eq!(cursor.get_i32(), 1); // partition_index
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i64(), 200); // base_offset
        assert_eq!(cursor.get_i64(), 9999); // log_append_time_ms
        assert_eq!(cursor.get_i64(), 50); // log_start_offset

        assert_eq!(cursor.get_i32(), 100); // throttle_time_ms
    }
}
