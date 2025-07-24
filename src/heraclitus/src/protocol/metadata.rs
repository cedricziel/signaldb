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
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
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
    pub topic_id: Option<[u8; 16]>, // UUID field for v10+
    pub is_internal: bool,          // v10+
    pub partitions: Vec<PartitionMetadata>,
    pub topic_authorized_operations: i32, // v10+
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
        debug!(
            "Parsing metadata request v{}, buffer remaining: {} bytes",
            api_version,
            buf.remaining()
        );

        let is_compact = api_version >= 9;

        // Read array of topic names based on protocol version
        let topics = match is_compact {
            true => {
                debug!(
                    "TAKING COMPACT PATH: Using compact protocol for Metadata v{}",
                    api_version
                );
                // v9+ uses compact protocol
                let result = read_compact_nullable_string_array(buf);
                debug!("COMPACT PATH RESULT: {:?}", result);
                result?
            }
            false => {
                debug!(
                    "TAKING NON-COMPACT PATH: Using regular protocol for Metadata v{}",
                    api_version
                );
                // v0-v8 use regular protocol
                let result = read_nullable_string_array(buf);
                debug!("NON-COMPACT PATH RESULT: {:?}", result);
                result?
            }
        };

        debug!("Successfully parsed topics: {:?}", topics);

        // For compact versions, handle tagged fields
        if is_compact {
            debug!("Reading tagged fields for compact protocol");
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(buf)?;
            debug!("Read {} tagged fields", _num_tagged_fields);
        }

        debug!("Metadata request parsing completed successfully");
        Ok(Self { topics })
    }
}

impl MetadataResponse {
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        let is_compact = api_version >= 9;

        if api_version >= 0 {
            // Write throttle_time_ms (v1+)
            if api_version >= 1 {
                buf.put_i32(self.throttle_time_ms);
            }

            // Write brokers array
            if is_compact {
                write_unsigned_varint(&mut buf, (self.brokers.len() + 1) as u32);
            } else {
                buf.put_i32(self.brokers.len() as i32);
            }

            for broker in &self.brokers {
                buf.put_i32(broker.node_id);
                if is_compact {
                    write_compact_string(&mut buf, &broker.host);
                } else {
                    write_string(&mut buf, &broker.host);
                }
                buf.put_i32(broker.port);

                // Tagged fields for brokers in compact versions
                if is_compact {
                    write_unsigned_varint(&mut buf, 0);
                }
            }

            // Write cluster_id (v2+)
            if api_version >= 2 {
                if is_compact {
                    if let Some(ref cluster_id) = self.cluster_id {
                        write_compact_string(&mut buf, cluster_id);
                    } else {
                        // Compact nullable string: 0 = null
                        write_unsigned_varint(&mut buf, 0);
                    }
                } else {
                    crate::protocol::kafka_protocol::write_nullable_string(
                        &mut buf,
                        self.cluster_id.as_deref(),
                    );
                }
            }

            // Write controller_id (v1+)
            if api_version >= 1 {
                buf.put_i32(self.controller_id);
            }

            // Write topics array
            if is_compact {
                write_unsigned_varint(&mut buf, (self.topics.len() + 1) as u32);
            } else {
                buf.put_i32(self.topics.len() as i32);
            }

            for topic in &self.topics {
                buf.put_i16(topic.error_code);
                if is_compact {
                    write_compact_string(&mut buf, &topic.name);
                } else {
                    write_string(&mut buf, &topic.name);
                }

                // Write topic_id (UUID) for v10+
                if api_version >= 10 {
                    if let Some(topic_id) = topic.topic_id {
                        buf.put_slice(&topic_id);
                    } else {
                        // Write null UUID (16 zero bytes)
                        buf.put_slice(&[0u8; 16]);
                    }
                }

                // Write is_internal for v10+
                if api_version >= 10 {
                    buf.put_u8(if topic.is_internal { 1 } else { 0 });
                }

                // Write partitions array
                if is_compact {
                    write_unsigned_varint(&mut buf, (topic.partitions.len() + 1) as u32);
                } else {
                    buf.put_i32(topic.partitions.len() as i32);
                }

                for partition in &topic.partitions {
                    buf.put_i16(partition.error_code);
                    buf.put_i32(partition.partition_id);
                    buf.put_i32(partition.leader);

                    // Write replicas array
                    if is_compact {
                        write_unsigned_varint(&mut buf, (partition.replicas.len() + 1) as u32);
                    } else {
                        buf.put_i32(partition.replicas.len() as i32);
                    }
                    for replica in &partition.replicas {
                        buf.put_i32(*replica);
                    }

                    // Write ISR array
                    if is_compact {
                        write_unsigned_varint(&mut buf, (partition.isr.len() + 1) as u32);
                    } else {
                        buf.put_i32(partition.isr.len() as i32);
                    }
                    for isr in &partition.isr {
                        buf.put_i32(*isr);
                    }

                    // Tagged fields for partitions in compact versions
                    if is_compact {
                        write_unsigned_varint(&mut buf, 0);
                    }
                }

                // Write topic_authorized_operations for v10+
                if api_version >= 10 {
                    buf.put_i32(topic.topic_authorized_operations);
                }

                // Tagged fields for topics in compact versions
                if is_compact {
                    write_unsigned_varint(&mut buf, 0);
                }
            }

            // Top-level tagged fields for compact versions
            if is_compact {
                write_unsigned_varint(&mut buf, 0);
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

fn read_compact_nullable_string_array(buf: &mut Cursor<&[u8]>) -> Result<Option<Vec<String>>> {
    debug!(
        "read_compact_nullable_string_array: starting with {} bytes remaining",
        buf.remaining()
    );
    let len = read_unsigned_varint(buf)? as i32;
    debug!(
        "read_compact_nullable_string_array: varint length = {}",
        len
    );

    if len == 0 {
        debug!("read_compact_nullable_string_array: null array (len=0)");
        return Ok(None); // Null array
    }

    let actual_len = len - 1;
    debug!(
        "read_compact_nullable_string_array: actual array length = {}",
        actual_len
    );

    if actual_len == 0 {
        debug!("read_compact_nullable_string_array: empty array");
        return Ok(Some(Vec::new())); // Empty array
    }

    let mut strings = Vec::with_capacity(actual_len as usize);

    for i in 0..actual_len {
        debug!(
            "read_compact_nullable_string_array: reading string {} of {}, {} bytes remaining",
            i + 1,
            actual_len,
            buf.remaining()
        );
        let string = read_compact_string(buf)?;
        debug!(
            "read_compact_nullable_string_array: read string '{}', {} bytes remaining",
            string,
            buf.remaining()
        );
        strings.push(string);
    }

    debug!(
        "read_compact_nullable_string_array: successfully read {} strings",
        strings.len()
    );
    Ok(Some(strings))
}

fn read_unsigned_varint(buf: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = 0u32;
    let mut i = 0;

    loop {
        if i > 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Varint is too long".to_string(),
            ));
        }

        if buf.remaining() < 1 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Not enough bytes for varint".to_string(),
            ));
        }

        let b = buf.get_u8();
        value |= ((b & 0x7F) as u32) << (i * 7);

        if (b & 0x80) == 0 {
            break;
        }

        i += 1;
    }

    Ok(value)
}

fn read_compact_string(buf: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_unsigned_varint(buf)? as usize;
    if len == 0 {
        return Ok(String::new()); // Null or empty string
    }

    let actual_len = len - 1;
    if actual_len == 0 {
        return Ok(String::new()); // Empty string
    }

    if buf.remaining() < actual_len {
        return Err(crate::error::HeraclitusError::Protocol(format!(
            "Not enough bytes for compact string: needed {}, got {}",
            actual_len,
            buf.remaining()
        )));
    }

    let mut bytes = vec![0u8; actual_len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes).map_err(|e| {
        crate::error::HeraclitusError::Protocol(format!("Invalid UTF-8 in compact string: {e}"))
    })
}

fn write_unsigned_varint(buffer: &mut BytesMut, mut value: u32) {
    while (value & 0xFFFFFF80) != 0 {
        buffer.put_u8(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buffer.put_u8((value & 0x7F) as u8);
}

fn write_compact_string(buffer: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    write_unsigned_varint(buffer, (bytes.len() + 1) as u32);
    buffer.put_slice(bytes);
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
            throttle_time_ms: 0,
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
            }],
            cluster_id: None,
            controller_id: 0,
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
            throttle_time_ms: 0,
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: "broker1".to_string(),
                port: 9092,
            }],
            cluster_id: None,
            controller_id: 0,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: "test-topic".to_string(),
                topic_id: None,
                is_internal: false,
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
                topic_authorized_operations: -1,
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
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: None,
            controller_id: 0,
            topics: vec![TopicMetadata {
                error_code: ERROR_TOPIC_NOT_FOUND,
                name: "missing-topic".to_string(),
                topic_id: None,
                is_internal: false,
                partitions: vec![],
                topic_authorized_operations: -1,
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
