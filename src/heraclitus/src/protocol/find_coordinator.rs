use crate::error::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

/// Kafka FindCoordinator Request (API Key 10)
#[derive(Debug)]
pub struct FindCoordinatorRequest {
    pub key: String,
    pub key_type: i8, // 0 = group, 1 = transaction
}

/// Kafka FindCoordinator Response
#[derive(Debug)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl FindCoordinatorRequest {
    /// Parse a FindCoordinator request from bytes
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // For API versions 0-3:
        // key: string
        // key_type: int8 (v1+)

        let key = crate::protocol::kafka_protocol::read_string(cursor)?;

        let key_type = if api_version >= 1 {
            cursor.get_i8()
        } else {
            0 // Default to group coordinator for v0
        };

        Ok(FindCoordinatorRequest { key, key_type })
    }
}

impl FindCoordinatorResponse {
    /// Create a new FindCoordinator response
    pub fn new(node_id: i32, host: String, port: i32) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0, // NONE
            error_message: None,
            node_id,
            host,
            port,
        }
    }

    /// Create an error response
    pub fn error(error_code: i16, error_message: String) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            error_message: Some(error_message),
            node_id: -1,
            host: String::new(),
            port: -1,
        }
    }

    /// Encode the response to bytes
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // API version 1+ includes throttle_time_ms
        if api_version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Error code
        buf.put_i16(self.error_code);

        // API version 1+ includes error_message
        if api_version >= 1 {
            crate::protocol::kafka_protocol::write_nullable_string(
                &mut buf,
                self.error_message.as_deref(),
            );
        }

        // Coordinator info
        buf.put_i32(self.node_id);
        crate::protocol::kafka_protocol::write_string(&mut buf, &self.host);
        buf.put_i32(self.port);

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_coordinator_request_parse_v0() {
        let mut buf = BytesMut::new();

        // key: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");

        let mut cursor = Cursor::new(&buf[..]);
        let request = FindCoordinatorRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.key, "test-group");
        assert_eq!(request.key_type, 0); // Default to group
    }

    #[test]
    fn test_find_coordinator_request_parse_v1() {
        let mut buf = BytesMut::new();

        // key: "test-group"
        buf.put_i16(10);
        buf.put_slice(b"test-group");
        // key_type: 0 (group)
        buf.put_i8(0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = FindCoordinatorRequest::parse(&mut cursor, 1).unwrap();

        assert_eq!(request.key, "test-group");
        assert_eq!(request.key_type, 0);
    }

    #[test]
    fn test_find_coordinator_response_encode_v0() {
        let response = FindCoordinatorResponse::new(0, "localhost".to_string(), 9092);
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v0
        assert_eq!(cursor.get_i16(), 0); // error_code
        // No error_message in v0
        assert_eq!(cursor.get_i32(), 0); // node_id

        // Read host string
        let host_len = cursor.get_i16() as usize;
        let mut host_bytes = vec![0u8; host_len];
        cursor.copy_to_slice(&mut host_bytes);
        assert_eq!(String::from_utf8(host_bytes).unwrap(), "localhost");

        assert_eq!(cursor.get_i32(), 9092); // port
    }

    #[test]
    fn test_find_coordinator_response_encode_v1() {
        let response = FindCoordinatorResponse::new(0, "localhost".to_string(), 9092);
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i16(), -1); // error_message (null)
        assert_eq!(cursor.get_i32(), 0); // node_id

        // Read host string
        let host_len = cursor.get_i16() as usize;
        let mut host_bytes = vec![0u8; host_len];
        cursor.copy_to_slice(&mut host_bytes);
        assert_eq!(String::from_utf8(host_bytes).unwrap(), "localhost");

        assert_eq!(cursor.get_i32(), 9092); // port
    }

    #[test]
    fn test_find_coordinator_error_response() {
        let response =
            FindCoordinatorResponse::error(15, "Group coordinator not available".to_string());
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 15); // error_code

        // Read error_message
        let msg_len = cursor.get_i16() as usize;
        let mut msg_bytes = vec![0u8; msg_len];
        cursor.copy_to_slice(&mut msg_bytes);
        assert_eq!(
            String::from_utf8(msg_bytes).unwrap(),
            "Group coordinator not available"
        );

        assert_eq!(cursor.get_i32(), -1); // node_id
    }
}
