use crate::error::Result;
use crate::protocol::kafka_protocol::*;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[derive(Debug)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    #[allow(dead_code)]
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsTopic>,
}

#[derive(Debug)]
pub struct ListOffsetsTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

#[derive(Debug)]
pub struct ListOffsetsPartition {
    pub partition_index: i32,
    pub timestamp: i64,
    #[allow(dead_code)]
    pub max_num_offsets: i32, // Only used in v0
}

impl ListOffsetsRequest {
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        let replica_id = cursor.get_i32();

        let isolation_level = if api_version >= 2 {
            cursor.get_i8()
        } else {
            0 // Default to read_uncommitted
        };

        let topic_count = cursor.get_i32();
        let mut topics = Vec::with_capacity(topic_count as usize);

        for _ in 0..topic_count {
            let name_len = cursor.get_i16();
            let mut name_bytes = vec![0u8; name_len as usize];
            cursor.copy_to_slice(&mut name_bytes);
            let name = String::from_utf8(name_bytes).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Invalid UTF-8 in topic name: {e}"))
            })?;

            let partition_count = cursor.get_i32();
            let mut partitions = Vec::with_capacity(partition_count as usize);

            for _ in 0..partition_count {
                let partition_index = cursor.get_i32();
                let timestamp = cursor.get_i64();
                let max_num_offsets = if api_version == 0 {
                    cursor.get_i32()
                } else {
                    1 // Later versions only return one offset
                };

                partitions.push(ListOffsetsPartition {
                    partition_index,
                    timestamp,
                    max_num_offsets,
                });
            }

            topics.push(ListOffsetsTopic { name, partitions });
        }

        Ok(ListOffsetsRequest {
            replica_id,
            isolation_level,
            topics,
        })
    }
}

#[derive(Debug)]
pub struct ListOffsetsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<ListOffsetsTopicResponse>,
}

#[derive(Debug)]
pub struct ListOffsetsTopicResponse {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

#[derive(Debug)]
pub struct ListOffsetsPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
    #[allow(dead_code)]
    pub old_style_offsets: Vec<i64>, // Only used in v0
}

impl ListOffsetsResponse {
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        if api_version >= 2 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Topics
        buf.put_i32(self.topics.len() as i32);
        for topic in &self.topics {
            // Topic name
            buf.put_i16(topic.name.len() as i16);
            buf.extend_from_slice(topic.name.as_bytes());

            // Partitions
            buf.put_i32(topic.partitions.len() as i32);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i16(partition.error_code);

                if api_version == 0 {
                    // Old style: array of offsets
                    if partition.error_code == ERROR_NONE {
                        buf.put_i32(1); // Number of offsets
                        buf.put_i64(partition.offset);
                    } else {
                        buf.put_i32(0); // No offsets on error
                    }
                } else {
                    // New style: timestamp and offset
                    buf.put_i64(partition.timestamp);
                    buf.put_i64(partition.offset);
                }
            }
        }

        Ok(buf.to_vec())
    }
}

// Special timestamp values
pub const EARLIEST_TIMESTAMP: i64 = -2;
pub const LATEST_TIMESTAMP: i64 = -1;
