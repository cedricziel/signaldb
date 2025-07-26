use crate::error::Result;
use crate::protocol::{
    encoder::{KafkaResponse, ProtocolEncoder},
    kafka_protocol::*,
};
use bytes::Buf;
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
        let is_compact = api_version >= 4; // Heartbeat uses flexible versions from v4+

        // group_id: string
        let group_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read group_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        // generation_id: int32
        let generation_id = cursor.get_i32();

        // member_id: string
        let member_id = if is_compact {
            read_compact_string(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!("Failed to read member_id: {e}"))
            })?
        } else {
            read_string(cursor)?
        };

        // group_instance_id: nullable string (v3+)
        let group_instance_id = if api_version >= 3 {
            if is_compact {
                read_compact_nullable_string(cursor).map_err(|e| {
                    crate::error::HeraclitusError::Protocol(format!(
                        "Failed to read group_instance_id: {e}"
                    ))
                })?
            } else {
                read_nullable_string(cursor)?
            }
        } else {
            None
        };

        // Handle tagged fields for newer versions
        if is_compact {
            // Read tagged fields (empty for now)
            let _num_tagged_fields = read_unsigned_varint(cursor).map_err(|e| {
                crate::error::HeraclitusError::Protocol(format!(
                    "Failed to read tagged fields: {e}"
                ))
            })?;
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
    /// Legacy encode method - delegates to centralized encoder
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let encoder = ProtocolEncoder::new(12, api_version); // Heartbeat API key = 12
        self.encode_with_encoder(&encoder)
    }

    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        let mut buf = encoder.create_buffer();

        // throttle_time_ms: int32 (v1+)
        if encoder.api_version() >= 1 {
            encoder.write_i32(&mut buf, self.throttle_time_ms);
        }

        // error_code: int16
        encoder.write_i16(&mut buf, self.error_code);

        // Tagged fields for flexible versions
        encoder.write_tagged_fields(&mut buf);

        Ok(buf.to_vec())
    }
}

impl KafkaResponse for HeartbeatResponse {
    fn encode_with_encoder(&self, encoder: &ProtocolEncoder) -> Result<Vec<u8>> {
        self.encode_with_encoder(encoder)
    }

    fn api_key(&self) -> i16 {
        12 // Heartbeat API key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

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
        let response = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
        };
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);
        // No throttle_time_ms in v0
        assert_eq!(cursor.get_i16(), 0); // error_code
    }

    #[test]
    fn test_heartbeat_response_encode_v1() {
        let response = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 27, // REBALANCE_IN_PROGRESS
        };
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);
        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 27); // error_code
    }

    #[test]
    fn test_heartbeat_request_parse_v4_compact() {
        let mut buf = BytesMut::new();

        // group_id: "test-group" (compact string)
        write_compact_string(&mut buf, "test-group");

        // generation_id: 3
        buf.put_i32(3);

        // member_id: "member-3" (compact string)
        write_compact_string(&mut buf, "member-3");

        // group_instance_id: None (compact nullable string)
        write_compact_nullable_string(&mut buf, None);

        // Tagged fields (empty)
        write_unsigned_varint(&mut buf, 0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = HeartbeatRequest::parse(&mut cursor, 4).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, 3);
        assert_eq!(request.member_id, "member-3");
        assert_eq!(request.group_instance_id, None);
    }

    #[test]
    fn test_heartbeat_response_encode_v4_compact() {
        let response = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
        };
        let encoded = response.encode(4).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);
        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 0); // error_code

        // Tagged fields
        let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(tagged_fields_len, 0);
    }
}
