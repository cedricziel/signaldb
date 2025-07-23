use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use tracing::debug;

use crate::error::Result;
use crate::protocol::kafka_protocol::*;

#[derive(Debug)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>, // None means all topics
}

#[derive(Debug)]
pub struct MetadataResponse {
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>, // In-sync replicas
}

impl MetadataRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        debug!("Parsing metadata request v{}", api_version);

        // For version 0, read array of topic names
        // Null array means all topics
        if api_version >= 0 {
            let topics = read_nullable_string_array(buf)?;
            Ok(Self { topics })
        } else {
            Err(crate::error::HeraclitusError::Protocol(format!(
                "Unsupported metadata request version: {api_version}"
            )))
        }
    }
}

impl MetadataResponse {
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        if api_version >= 0 {
            // Write brokers array
            buf.put_i32(self.brokers.len() as i32);
            for broker in &self.brokers {
                buf.put_i32(broker.node_id);
                write_string(&mut buf, &broker.host);
                buf.put_i32(broker.port);
            }

            // Write topics array
            buf.put_i32(self.topics.len() as i32);
            for topic in &self.topics {
                buf.put_i16(topic.error_code);
                write_string(&mut buf, &topic.name);

                // Write partitions array
                buf.put_i32(topic.partitions.len() as i32);
                for partition in &topic.partitions {
                    buf.put_i16(partition.error_code);
                    buf.put_i32(partition.partition_id);
                    buf.put_i32(partition.leader);

                    // Write replicas array
                    buf.put_i32(partition.replicas.len() as i32);
                    for replica in &partition.replicas {
                        buf.put_i32(*replica);
                    }

                    // Write ISR array
                    buf.put_i32(partition.isr.len() as i32);
                    for isr in &partition.isr {
                        buf.put_i32(*isr);
                    }
                }
            }

            Ok(buf.to_vec())
        } else {
            Err(crate::error::HeraclitusError::Protocol(format!(
                "Unsupported metadata response version: {api_version}"
            )))
        }
    }
}

fn read_nullable_string_array(buf: &mut Cursor<&[u8]>) -> Result<Option<Vec<String>>> {
    if buf.remaining() < 4 {
        return Err(crate::error::HeraclitusError::Protocol(
            "Not enough bytes for array length".to_string(),
        ));
    }

    let len = buf.get_i32();
    if len < 0 {
        return Ok(None);
    }

    let len = len as usize;
    let mut strings = Vec::with_capacity(len);

    for _ in 0..len {
        let string = read_string(buf).map_err(|e| {
            crate::error::HeraclitusError::Protocol(format!("Failed to read string: {e}"))
        })?;
        strings.push(string);
    }

    Ok(Some(strings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::io::Cursor;

    #[test]
    fn test_metadata_request_parse_all_topics() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // Null array = all topics

        let mut cursor = Cursor::new(&buf[..]);
        let request = MetadataRequest::parse(&mut cursor, 0).unwrap();

        assert!(request.topics.is_none());
    }

    #[test]
    fn test_metadata_request_parse_specific_topics() {
        let mut buf = BytesMut::new();
        buf.put_i32(2); // Array with 2 topics

        // First topic
        let topic1 = "events";
        buf.put_i16(topic1.len() as i16);
        buf.put_slice(topic1.as_bytes());

        // Second topic
        let topic2 = "logs";
        buf.put_i16(topic2.len() as i16);
        buf.put_slice(topic2.as_bytes());

        let mut cursor = Cursor::new(&buf[..]);
        let request = MetadataRequest::parse(&mut cursor, 0).unwrap();

        assert!(request.topics.is_some());
        let topics = request.topics.unwrap();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0], "events");
        assert_eq!(topics[1], "logs");
    }

    #[test]
    fn test_metadata_request_parse_empty_topics() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // Empty array

        let mut cursor = Cursor::new(&buf[..]);
        let request = MetadataRequest::parse(&mut cursor, 0).unwrap();

        assert!(request.topics.is_some());
        let topics = request.topics.unwrap();
        assert_eq!(topics.len(), 0);
    }

    #[test]
    fn test_metadata_response_encode_no_topics() {
        let response = MetadataResponse {
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
            }],
            topics: vec![],
        };

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // Check brokers
        assert_eq!(cursor.get_i32(), 1); // 1 broker
        assert_eq!(cursor.get_i32(), 1); // node_id
        let host_len = cursor.get_i16() as usize;
        let mut host_bytes = vec![0u8; host_len];
        cursor.copy_to_slice(&mut host_bytes);
        assert_eq!(String::from_utf8(host_bytes).unwrap(), "localhost");
        assert_eq!(cursor.get_i32(), 9092);

        // Check topics
        assert_eq!(cursor.get_i32(), 0); // 0 topics
    }

    #[test]
    fn test_metadata_response_encode_with_topics() {
        let response = MetadataResponse {
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: "broker1".to_string(),
                port: 9092,
            }],
            topics: vec![TopicMetadata {
                error_code: 0,
                name: "test-topic".to_string(),
                partitions: vec![
                    PartitionMetadata {
                        error_code: 0,
                        partition_id: 0,
                        leader: 0,
                        replicas: vec![0],
                        isr: vec![0],
                    },
                    PartitionMetadata {
                        error_code: 0,
                        partition_id: 1,
                        leader: 0,
                        replicas: vec![0],
                        isr: vec![0],
                    },
                ],
            }],
        };

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // Check brokers
        assert_eq!(cursor.get_i32(), 1); // 1 broker
        assert_eq!(cursor.get_i32(), 0); // node_id
        let host_len = cursor.get_i16() as usize;
        let mut host_bytes = vec![0u8; host_len];
        cursor.copy_to_slice(&mut host_bytes);
        assert_eq!(String::from_utf8(host_bytes).unwrap(), "broker1");
        assert_eq!(cursor.get_i32(), 9092);

        // Check topics
        assert_eq!(cursor.get_i32(), 1); // 1 topic
        assert_eq!(cursor.get_i16(), 0); // error_code
        let topic_len = cursor.get_i16() as usize;
        let mut topic_bytes = vec![0u8; topic_len];
        cursor.copy_to_slice(&mut topic_bytes);
        assert_eq!(String::from_utf8(topic_bytes).unwrap(), "test-topic");

        // Check partitions
        assert_eq!(cursor.get_i32(), 2); // 2 partitions

        // First partition
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i32(), 0); // partition_id
        assert_eq!(cursor.get_i32(), 0); // leader
        assert_eq!(cursor.get_i32(), 1); // replicas count
        assert_eq!(cursor.get_i32(), 0); // replica id
        assert_eq!(cursor.get_i32(), 1); // isr count
        assert_eq!(cursor.get_i32(), 0); // isr id

        // Second partition
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i32(), 1); // partition_id
        assert_eq!(cursor.get_i32(), 0); // leader
        assert_eq!(cursor.get_i32(), 1); // replicas count
        assert_eq!(cursor.get_i32(), 0); // replica id
        assert_eq!(cursor.get_i32(), 1); // isr count
        assert_eq!(cursor.get_i32(), 0); // isr id
    }

    #[test]
    fn test_metadata_response_encode_with_error() {
        let response = MetadataResponse {
            brokers: vec![],
            topics: vec![TopicMetadata {
                error_code: ERROR_TOPIC_NOT_FOUND,
                name: "missing-topic".to_string(),
                partitions: vec![],
            }],
        };

        let encoded = response.encode(0).unwrap();
        let mut cursor = Cursor::new(&encoded[..]);

        // Check brokers
        assert_eq!(cursor.get_i32(), 0); // 0 brokers

        // Check topics
        assert_eq!(cursor.get_i32(), 1); // 1 topic
        assert_eq!(cursor.get_i16(), ERROR_TOPIC_NOT_FOUND); // error_code
        let topic_len = cursor.get_i16() as usize;
        let mut topic_bytes = vec![0u8; topic_len];
        cursor.copy_to_slice(&mut topic_bytes);
        assert_eq!(String::from_utf8(topic_bytes).unwrap(), "missing-topic");
        assert_eq!(cursor.get_i32(), 0); // 0 partitions
    }
}
