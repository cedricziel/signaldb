use crate::error::Result;
use crate::protocol::kafka_protocol::*;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        // Topic names array
        let topic_count = if buf.remaining() < 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Not enough bytes for topic count".to_string(),
            ));
        } else {
            buf.get_i32()
        };

        let mut topic_names = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            topic_names.push(read_string(buf)?);
        }

        // Timeout
        let timeout_ms = if buf.remaining() < 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Not enough bytes for timeout".to_string(),
            ));
        } else {
            buf.get_i32()
        };

        // Validate timeout (v1+ allows -1 for no timeout)
        if version == 0 && timeout_ms < 0 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Invalid timeout for v0".to_string(),
            ));
        }

        Ok(Self {
            topic_names,
            timeout_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    pub throttle_time_ms: i32, // v2+
    pub topic_errors: Vec<TopicDeletionError>,
}

#[derive(Debug, Clone)]
pub struct TopicDeletionError {
    pub topic_name: String,
    pub error_code: i16,
    pub error_message: Option<String>, // v5+
}

impl DeleteTopicsResponse {
    pub fn encode(&self, version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // Throttle time (v2+)
        if version >= 2 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Topic errors array
        buf.put_i32(self.topic_errors.len() as i32);

        for topic_error in &self.topic_errors {
            // Topic name
            write_string(&mut buf, &topic_error.topic_name);

            // Error code
            buf.put_i16(topic_error.error_code);

            // Error message (v5+)
            if version >= 5 {
                write_nullable_string(&mut buf, topic_error.error_message.as_deref());
            }
        }

        Ok(buf.to_vec())
    }
}
