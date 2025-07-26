use crate::error::{HeraclitusError, Result};
use crate::protocol::encoder::{KafkaResponse, ProtocolEncoder};
use bytes::Buf;
use std::io::Cursor;

/// Fetch API Request (API Key = 1)
#[derive(Debug)]
pub struct FetchRequest {
    /// The broker ID of the follower, or -1 if this request is from a consumer
    pub replica_id: i32,

    /// Maximum time in milliseconds to wait for the response
    pub max_wait_ms: i32,

    /// Minimum bytes to accumulate in the response
    pub min_bytes: i32,

    /// Maximum bytes to return in the response (v3+)
    pub max_bytes: i32,

    /// Controls visibility of transactional records (v4+)
    /// 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED
    pub isolation_level: i8,

    /// The fetch session ID (v7+)
    pub session_id: i32,

    /// The fetch session epoch (v7+)
    pub session_epoch: i32,

    /// Topics to fetch
    pub topics: Vec<FetchTopic>,

    /// Topics to remove from the fetch session (v7+)
    pub forgotten_topics: Vec<ForgottenTopic>,

    /// Rack ID of the consumer (v11+)
    pub rack_id: Option<String>,
}

#[derive(Debug)]
pub struct FetchTopic {
    /// Topic name
    pub topic: String,

    /// Partitions to fetch from this topic
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug)]
pub struct FetchPartition {
    /// Partition index
    pub partition: i32,

    /// Current leader epoch (v9+)
    pub current_leader_epoch: i32,

    /// Offset to fetch from
    pub fetch_offset: i64,

    /// Earliest available offset of the follower (v5+)
    pub log_start_offset: i64,

    /// Maximum bytes to fetch from this partition (v3+)
    pub partition_max_bytes: i32,
}

#[derive(Debug)]
pub struct ForgottenTopic {
    /// Topic name
    pub topic: String,

    /// Partitions to remove from session
    pub partitions: Vec<i32>,
}

impl FetchRequest {
    /// Parse a FetchRequest from wire format
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        use tracing::debug;

        debug!(
            "Parsing Fetch request v{}, buffer has {} bytes",
            api_version,
            cursor.remaining()
        );

        // Check minimum version
        if api_version < 0 {
            return Err(HeraclitusError::Protocol(format!(
                "Unsupported Fetch API version: {api_version}"
            )));
        }

        let replica_id = cursor.get_i32();
        let max_wait_ms = cursor.get_i32();
        let min_bytes = cursor.get_i32();

        // max_bytes added in v3
        let max_bytes = if api_version >= 3 {
            cursor.get_i32()
        } else {
            i32::MAX // Default to max value for older versions
        };

        // isolation_level added in v4
        let isolation_level = if api_version >= 4 {
            cursor.get_i8()
        } else {
            0 // READ_UNCOMMITTED by default
        };

        // session_id and session_epoch added in v7
        let (session_id, session_epoch) = if api_version >= 7 {
            (cursor.get_i32(), cursor.get_i32())
        } else {
            (0, -1)
        };

        // Read topics
        let topic_count = cursor.get_i32();
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let topic = crate::protocol::kafka_protocol::read_string(cursor).map_err(|e| {
                HeraclitusError::Protocol(format!("Failed to read topic name: {e}"))
            })?;

            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                let partition = cursor.get_i32();

                // current_leader_epoch added in v9
                let current_leader_epoch = if api_version >= 9 {
                    cursor.get_i32()
                } else {
                    -1
                };

                let fetch_offset = cursor.get_i64();

                // log_start_offset added in v5
                let log_start_offset = if api_version >= 5 {
                    cursor.get_i64()
                } else {
                    -1
                };

                // partition_max_bytes was in v0 already
                let partition_max_bytes = cursor.get_i32();

                partitions.push(FetchPartition {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                    log_start_offset,
                    partition_max_bytes,
                });
            }

            topics.push(FetchTopic { topic, partitions });
        }

        // forgotten_topics added in v7
        let forgotten_topics = if api_version >= 7 {
            let forgotten_count = cursor.get_i32();
            let mut forgotten = Vec::with_capacity(forgotten_count as usize);

            for _ in 0..forgotten_count {
                let topic = crate::protocol::kafka_protocol::read_string(cursor).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to read forgotten topic: {e}"))
                })?;

                let partition_count = cursor.get_i32();
                let mut partitions = Vec::with_capacity(partition_count as usize);

                for _ in 0..partition_count {
                    partitions.push(cursor.get_i32());
                }

                forgotten.push(ForgottenTopic { topic, partitions });
            }
            forgotten
        } else {
            Vec::new()
        };

        // rack_id added in v11
        let rack_id = if api_version >= 11 {
            crate::protocol::kafka_protocol::read_nullable_string(cursor)
                .map_err(|e| HeraclitusError::Protocol(format!("Failed to read rack_id: {e}")))?
        } else {
            None
        };

        Ok(FetchRequest {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        })
    }
}

/// Fetch API Response
#[derive(Debug)]
pub struct FetchResponse {
    /// Duration in milliseconds for which the request was throttled
    pub throttle_time_ms: i32,

    /// The top-level error code (v7+)
    pub error_code: i16,

    /// The fetch session ID (v7+)
    pub session_id: i32,

    /// Topic responses
    pub responses: Vec<FetchTopicResponse>,
}

#[derive(Debug)]
pub struct FetchTopicResponse {
    /// Topic name
    pub topic: String,

    /// Partition responses
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug)]
pub struct FetchPartitionResponse {
    /// Partition index
    pub partition: i32,

    /// Error code
    pub error_code: i16,

    /// High water mark offset
    pub high_watermark: i64,

    /// Last stable offset (v4+)
    pub last_stable_offset: i64,

    /// Log start offset (v5+)
    pub log_start_offset: i64,

    /// Aborted transactions (v4+) - for now we'll skip this
    pub aborted_transactions: Vec<AbortedTransaction>,

    /// Preferred read replica (v11+)
    pub preferred_read_replica: i32,

    /// Record data in RecordBatch format (None for empty/null)
    pub records: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

impl FetchResponse {
    /// Legacy encode method - delegates to centralized encoder
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let encoder = ProtocolEncoder::new(1, api_version); // Fetch API key = 1
        self.encode_with_encoder(&encoder)
    }

    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        let mut buf = encoder.create_buffer();

        // Throttle time (v1+)
        if encoder.api_version() >= 1 {
            encoder.write_i32(&mut buf, self.throttle_time_ms);
        }

        // Error code (v7+)
        if encoder.api_version() >= 7 {
            encoder.write_i16(&mut buf, self.error_code);
            encoder.write_i32(&mut buf, self.session_id);
        }

        // Topics array
        encoder.write_array_len(&mut buf, self.responses.len());

        for topic_response in &self.responses {
            encoder.write_string(&mut buf, &topic_response.topic);

            // Partitions array
            encoder.write_array_len(&mut buf, topic_response.partitions.len());

            for partition_response in &topic_response.partitions {
                encoder.write_i32(&mut buf, partition_response.partition);
                encoder.write_i16(&mut buf, partition_response.error_code);
                encoder.write_i64(&mut buf, partition_response.high_watermark);

                // Last stable offset (v4+)
                if encoder.api_version() >= 4 {
                    encoder.write_i64(&mut buf, partition_response.last_stable_offset);
                }

                // Log start offset (v5+)
                if encoder.api_version() >= 5 {
                    encoder.write_i64(&mut buf, partition_response.log_start_offset);
                }

                // Aborted transactions (v4+)
                if encoder.api_version() >= 4 {
                    encoder
                        .write_array_len(&mut buf, partition_response.aborted_transactions.len());
                    for aborted in &partition_response.aborted_transactions {
                        encoder.write_i64(&mut buf, aborted.producer_id);
                        encoder.write_i64(&mut buf, aborted.first_offset);
                    }
                }

                // Preferred read replica (v11+)
                if encoder.api_version() >= 11 {
                    encoder.write_i32(&mut buf, partition_response.preferred_read_replica);
                }

                // Record data (nullable bytes)
                encoder.write_nullable_bytes(&mut buf, partition_response.records.as_deref());

                // Tagged fields for partitions in flexible versions
                encoder.write_tagged_fields(&mut buf);
            }

            // Tagged fields for topics in flexible versions
            encoder.write_tagged_fields(&mut buf);
        }

        // Top-level tagged fields for flexible versions
        encoder.write_tagged_fields(&mut buf);

        Ok(buf.to_vec())
    }
}

impl KafkaResponse for FetchResponse {
    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        self.encode_with_encoder(encoder)
    }

    fn api_key(&self) -> i16 {
        1 // Fetch API key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_fetch_request_parse_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // replica_id
        buf.put_i32(100); // max_wait_ms
        buf.put_i32(1024); // min_bytes
        buf.put_i32(1); // topic count

        // Topic
        buf.put_i16(10); // topic name length
        buf.put_slice(b"test-topic");
        buf.put_i32(1); // partition count

        // Partition
        buf.put_i32(0); // partition
        buf.put_i64(100); // fetch_offset
        buf.put_i32(1048576); // partition_max_bytes

        let mut cursor = Cursor::new(&buf[..]);
        let request = FetchRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.replica_id, -1);
        assert_eq!(request.max_wait_ms, 100);
        assert_eq!(request.min_bytes, 1024);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].topic, "test-topic");
        assert_eq!(request.topics[0].partitions.len(), 1);
        assert_eq!(request.topics[0].partitions[0].partition, 0);
        assert_eq!(request.topics[0].partitions[0].fetch_offset, 100);
    }

    #[test]
    fn test_fetch_response_encode_v0() {
        let response = FetchResponse {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0,
            responses: vec![FetchTopicResponse {
                topic: "test-topic".to_string(),
                partitions: vec![FetchPartitionResponse {
                    partition: 0,
                    error_code: 0,
                    high_watermark: 100,
                    last_stable_offset: 100,
                    log_start_offset: 0,
                    aborted_transactions: vec![],
                    preferred_read_replica: -1,
                    records: Some(vec![1, 2, 3, 4]), // Dummy data
                }],
            }],
        };

        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // Verify structure
        assert_eq!(cursor.get_i32(), 1); // topic count
        assert_eq!(cursor.get_i16(), 10); // topic name length
        let mut topic_bytes = vec![0u8; 10];
        cursor.copy_to_slice(&mut topic_bytes);
        assert_eq!(&topic_bytes, b"test-topic");

        assert_eq!(cursor.get_i32(), 1); // partition count
        assert_eq!(cursor.get_i32(), 0); // partition
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i64(), 100); // high_watermark
        assert_eq!(cursor.get_i32(), 4); // records length
        let mut records = vec![0u8; 4];
        cursor.copy_to_slice(&mut records);
        assert_eq!(records, vec![1, 2, 3, 4]);
    }
}
