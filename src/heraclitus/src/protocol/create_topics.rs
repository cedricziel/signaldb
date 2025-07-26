use crate::error::{HeraclitusError, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[derive(Debug)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreateTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool, // v1+
}

#[derive(Debug)]
pub struct CreateTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub _assignments: Vec<CreateTopicAssignment>,
    pub _configs: Vec<CreateTopicConfig>,
}

#[derive(Debug)]
pub struct CreateTopicAssignment {
    pub _partition_index: i32,
    pub _broker_ids: Vec<i32>,
}

#[derive(Debug)]
pub struct CreateTopicConfig {
    pub _name: String,
    pub _value: Option<String>,
}

#[derive(Debug)]
pub struct CreateTopicsResponse {
    pub throttle_time_ms: i32, // v2+
    pub topics: Vec<CreateTopicResponse>,
}

#[derive(Debug)]
pub struct CreateTopicResponse {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,           // v1+
    pub num_partitions: i32,                     // v5+
    pub replication_factor: i16,                 // v5+
    pub configs: Vec<CreateTopicConfigResponse>, // v5+
}

#[derive(Debug)]
pub struct CreateTopicConfigResponse {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
}

impl CreateTopicsRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        // Topics array
        let topic_count = if version >= 5 {
            // Compact array
            let count = parse_unsigned_varint(buf)?;
            if count == 0 { 0 } else { (count - 1) as usize }
        } else {
            buf.get_i32() as usize
        };

        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            topics.push(CreateTopic::parse(buf, version)?);
        }

        // Timeout
        let timeout_ms = buf.get_i32();

        // Validate only (v1+)
        let validate_only = if version >= 1 {
            buf.get_i8() != 0
        } else {
            false
        };

        // Tagged fields (v5+)
        if version >= 5 {
            let _num_tagged_fields = parse_unsigned_varint(buf)?;
            // Skip tagged fields for now
        }

        Ok(CreateTopicsRequest {
            topics,
            timeout_ms,
            validate_only,
        })
    }
}

impl CreateTopic {
    fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        // Topic name
        let name = if version >= 5 {
            parse_compact_string(buf)?
        } else {
            parse_string(buf)?
        };

        // Num partitions
        let num_partitions = buf.get_i32();

        // Replication factor
        let replication_factor = buf.get_i16();

        // Assignments
        let assignment_count = if version >= 5 {
            let count = parse_unsigned_varint(buf)?;
            if count == 0 { 0 } else { (count - 1) as usize }
        } else {
            buf.get_i32() as usize
        };

        let mut assignments = Vec::with_capacity(assignment_count);
        for _ in 0..assignment_count {
            let partition_index = buf.get_i32();

            let broker_count = if version >= 5 {
                let count = parse_unsigned_varint(buf)?;
                if count == 0 { 0 } else { (count - 1) as usize }
            } else {
                buf.get_i32() as usize
            };

            let mut broker_ids = Vec::with_capacity(broker_count);
            for _ in 0..broker_count {
                broker_ids.push(buf.get_i32());
            }

            assignments.push(CreateTopicAssignment {
                _partition_index: partition_index,
                _broker_ids: broker_ids,
            });
        }

        // Configs
        let config_count = if version >= 5 {
            let count = parse_unsigned_varint(buf)?;
            if count == 0 { 0 } else { (count - 1) as usize }
        } else {
            buf.get_i32() as usize
        };

        let mut configs = Vec::with_capacity(config_count);
        for _ in 0..config_count {
            let name = if version >= 5 {
                parse_compact_string(buf)?
            } else {
                parse_string(buf)?
            };

            let value = if version >= 5 {
                parse_compact_nullable_string(buf)?
            } else {
                parse_nullable_string(buf)?
            };

            configs.push(CreateTopicConfig {
                _name: name,
                _value: value,
            });
        }

        // Tagged fields (v5+)
        if version >= 5 {
            let _num_tagged_fields = parse_unsigned_varint(buf)?;
            // Skip tagged fields for now
        }

        Ok(CreateTopic {
            name,
            num_partitions,
            replication_factor,
            _assignments: assignments,
            _configs: configs,
        })
    }
}

impl CreateTopicsResponse {
    pub fn encode(&self, version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // Throttle time (v2+)
        if version >= 2 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Topics array
        if version >= 5 {
            // Compact array
            write_unsigned_varint(&mut buf, self.topics.len() as u32 + 1);
        } else {
            buf.put_i32(self.topics.len() as i32);
        }

        for topic in &self.topics {
            topic.encode(&mut buf, version)?;
        }

        // Tagged fields (v5+)
        if version >= 5 {
            write_unsigned_varint(&mut buf, 0); // No tagged fields
        }

        Ok(buf.to_vec())
    }
}

impl CreateTopicResponse {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        // Topic name
        if version >= 5 {
            write_compact_string(buf, &self.name);
        } else {
            write_string(buf, &self.name);
        }

        // Error code
        buf.put_i16(self.error_code);

        // Error message (v1+)
        if version >= 1 {
            if version >= 5 {
                write_compact_nullable_string(buf, &self.error_message);
            } else {
                write_nullable_string(buf, &self.error_message);
            }
        }

        // Additional fields (v5+)
        if version >= 5 {
            buf.put_i32(self.num_partitions);
            buf.put_i16(self.replication_factor);

            // Configs array
            write_unsigned_varint(buf, self.configs.len() as u32 + 1);
            for config in &self.configs {
                write_compact_string(buf, &config.name);
                write_compact_nullable_string(buf, &config.value);
                buf.put_i8(if config.read_only { 1 } else { 0 });
                buf.put_i8(config.config_source);
                buf.put_i8(if config.is_sensitive { 1 } else { 0 });
            }

            // Tagged fields
            write_unsigned_varint(buf, 0); // No tagged fields
        }

        Ok(())
    }
}

// Helper functions
fn parse_string(buf: &mut Cursor<&[u8]>) -> Result<String> {
    let len = buf.get_i16();
    if len < 0 {
        return Err(HeraclitusError::Protocol(
            "Invalid string length".to_string(),
        ));
    }
    let mut bytes = vec![0u8; len as usize];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|e| HeraclitusError::Protocol(format!("Invalid UTF-8: {e}")))
}

fn parse_nullable_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>> {
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let mut bytes = vec![0u8; len as usize];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(String::from_utf8(bytes).map_err(|e| {
        HeraclitusError::Protocol(format!("Invalid UTF-8: {e}"))
    })?))
}

fn parse_compact_string(buf: &mut Cursor<&[u8]>) -> Result<String> {
    let len = parse_unsigned_varint(buf)?;
    if len == 0 {
        return Err(HeraclitusError::Protocol("Null compact string".to_string()));
    }
    let len = len - 1;
    let mut bytes = vec![0u8; len as usize];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|e| HeraclitusError::Protocol(format!("Invalid UTF-8: {e}")))
}

fn parse_compact_nullable_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>> {
    let len = parse_unsigned_varint(buf)?;
    if len == 0 {
        return Ok(None);
    }
    let len = len - 1;
    let mut bytes = vec![0u8; len as usize];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(String::from_utf8(bytes).map_err(|e| {
        HeraclitusError::Protocol(format!("Invalid UTF-8: {e}"))
    })?))
}

fn parse_unsigned_varint(buf: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = 0u32;
    let mut i = 0;
    loop {
        if i > 4 {
            return Err(HeraclitusError::Protocol("Varint too long".to_string()));
        }
        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u32) << (i * 7);
        if byte & 0x80 == 0 {
            break;
        }
        i += 1;
    }
    Ok(value)
}

fn write_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

fn write_nullable_string(buf: &mut BytesMut, s: &Option<String>) {
    match s {
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => buf.put_i16(-1),
    }
}

fn write_compact_string(buf: &mut BytesMut, s: &str) {
    write_unsigned_varint(buf, s.len() as u32 + 1);
    buf.put_slice(s.as_bytes());
}

fn write_compact_nullable_string(buf: &mut BytesMut, s: &Option<String>) {
    match s {
        Some(s) => {
            write_unsigned_varint(buf, s.len() as u32 + 1);
            buf.put_slice(s.as_bytes());
        }
        None => write_unsigned_varint(buf, 0),
    }
}

fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}
