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
