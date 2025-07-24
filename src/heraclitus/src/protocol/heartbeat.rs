use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka Heartbeat Request (API Key 12)
#[derive(Debug)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    #[allow(dead_code)]
    pub group_instance_id: Option<String>, // v3+
}

/// Kafka Heartbeat Response
#[derive(Debug)]
pub struct HeartbeatResponse {
    pub throttle_time_ms: i32, // v1+
    pub error_code: i16,
}

impl HeartbeatRequest {
    /// Parse a Heartbeat request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // group_id: string
        let group_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        // generation_id: int32
        let generation_id = cursor.get_i32();

        // member_id: string
        let member_id = crate::protocol::kafka_protocol::read_string(cursor)?;

        // group_instance_id: nullable string (v3+)
        let group_instance_id = if api_version >= 3 {
            crate::protocol::kafka_protocol::read_nullable_string(cursor)?
        } else {
            None
        };

        // Handle tagged fields for newer versions
        if api_version >= 4 {
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(cursor)?;
        }

        Ok(HeartbeatRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        })
    }
}

impl HeartbeatResponse {
    /// Create a successful Heartbeat response
    pub fn success() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0, // NONE
        }
    }

    /// Create an error response
    pub fn error(error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
        }
    }

    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // throttle_time_ms: int32 (v1+)
        if api_version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // error_code: int16
        buf.put_i16(self.error_code);

        // Handle tagged fields for newer versions
        if api_version >= 4 {
            // Write empty tagged fields
            write_unsigned_varint(&mut buf, 0);
        }

        Ok(buf.to_vec())
    }
}

fn read_unsigned_varint(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = 0u32;
    let mut i = 0;

    loop {
        if i > 4 {
            return Err(crate::error::HeraclitusError::Protocol(
                "Varint is too long".to_string(),
            ));
        }

        let b = cursor.get_u8();
        value |= ((b & 0x7F) as u32) << (i * 7);

        if (b & 0x80) == 0 {
            break;
        }

        i += 1;
    }

    Ok(value)
}

fn write_unsigned_varint(buffer: &mut BytesMut, mut value: u32) {
    while (value & 0xFFFFFF80) != 0 {
        buffer.put_u8(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buffer.put_u8((value & 0x7F) as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_request_parse_v0() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // generation_id: 1
        buf.put_i32(1);

        // member_id: "member-1"
        buf.put_i16(8);
        buf.put_slice(b"member-1");

        let mut cursor = Cursor::new(&buf[..]);
        let request = HeartbeatRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, 1);
        assert_eq!(request.member_id, "member-1");
        assert_eq!(request.group_instance_id, None);
    }

    #[test]
    fn test_heartbeat_request_parse_v3() {
        let mut buf = BytesMut::new();

        // group_id: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        // generation_id: 2
        buf.put_i32(2);

        // member_id: "member-2"
        buf.put_i16(8);
        buf.put_slice(b"member-2");

        // group_instance_id: "instance-1"
        buf.put_i16(10);
        buf.put_slice(b"instance-1");

        let mut cursor = Cursor::new(&buf[..]);
        let request = HeartbeatRequest::parse(&mut cursor, 3).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, 2);
        assert_eq!(request.member_id, "member-2");
        assert_eq!(request.group_instance_id, Some("instance-1".to_string()));
    }

    #[test]
    fn test_heartbeat_response_encode_v0() {
        let response = HeartbeatResponse::success();
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);
        // No throttle_time_ms in v0
        assert_eq!(cursor.get_i16(), 0); // error_code
    }

    #[test]
    fn test_heartbeat_response_encode_v1() {
        let response = HeartbeatResponse::error(27); // REBALANCE_IN_PROGRESS
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);
        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 27); // error_code
    }
}
