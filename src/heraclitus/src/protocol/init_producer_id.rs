use crate::error::{HeraclitusError, Result};
use crate::protocol::kafka_protocol::*;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[derive(Debug)]
pub struct InitProducerIdRequest {
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
    #[allow(dead_code)]
    pub producer_id: i64, // v3+
    #[allow(dead_code)]
    pub producer_epoch: i16, // v3+
}

#[derive(Debug)]
pub struct InitProducerIdResponse {
    pub throttle_time_ms: i32, // v1+
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdRequest {
    pub fn parse(buf: &mut Cursor<&[u8]>, version: i16) -> Result<Self> {
        let is_compact = version >= 2; // InitProducerId uses flexible versions from v2+

        // Transactional ID (nullable string)
        let transactional_id = if is_compact {
            read_compact_nullable_string(buf).map_err(|e| {
                HeraclitusError::Protocol(format!("Failed to read transactional_id: {e}"))
            })?
        } else {
            read_nullable_string(buf)?
        };

        // Transaction timeout
        let transaction_timeout_ms = buf.get_i32();

        // Producer ID and epoch (v3+)
        let (producer_id, producer_epoch) = if version >= 3 {
            let id = buf.get_i64();
            let epoch = buf.get_i16();
            (id, epoch)
        } else {
            (-1, -1) // Default values for older versions
        };

        // Tagged fields (v2+)
        if is_compact {
            let _num_tagged_fields = read_unsigned_varint(buf).map_err(|e| {
                HeraclitusError::Protocol(format!("Failed to read tagged fields: {e}"))
            })?;
            // Skip tagged fields for now
        }

        Ok(InitProducerIdRequest {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        })
    }
}

impl InitProducerIdResponse {
    pub fn new(producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            producer_id,
            producer_epoch,
        }
    }

    pub fn error(error_code: i16) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            producer_id: -1,
            producer_epoch: -1,
        }
    }

    pub fn encode(&self, version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();
        let is_compact = version >= 2; // InitProducerId uses flexible versions from v2+

        // Throttle time (v1+)
        if version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }

        // Error code
        buf.put_i16(self.error_code);

        // Producer ID
        buf.put_i64(self.producer_id);

        // Producer epoch
        buf.put_i16(self.producer_epoch);

        // Tagged fields (v2+)
        if is_compact {
            write_unsigned_varint(&mut buf, 0); // No tagged fields
        }

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_producer_id_request_parse_v0() {
        let mut buf = BytesMut::new();

        // transactional_id: null
        buf.put_i16(-1);

        // transaction_timeout_ms: 30000
        buf.put_i32(30000);

        let mut cursor = Cursor::new(&buf[..]);
        let request = InitProducerIdRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.transactional_id, None);
        assert_eq!(request.transaction_timeout_ms, 30000);
        assert_eq!(request.producer_id, -1); // Default for v0
        assert_eq!(request.producer_epoch, -1); // Default for v0
    }

    #[test]
    fn test_init_producer_id_request_parse_v1_with_transactional_id() {
        let mut buf = BytesMut::new();

        // transactional_id: "test-tx-id"
        buf.put_i16(10);
        buf.put_slice(b"test-tx-id");

        // transaction_timeout_ms: 60000
        buf.put_i32(60000);

        let mut cursor = Cursor::new(&buf[..]);
        let request = InitProducerIdRequest::parse(&mut cursor, 1).unwrap();

        assert_eq!(request.transactional_id, Some("test-tx-id".to_string()));
        assert_eq!(request.transaction_timeout_ms, 60000);
        assert_eq!(request.producer_id, -1); // Default for v1
        assert_eq!(request.producer_epoch, -1); // Default for v1
    }

    #[test]
    fn test_init_producer_id_request_parse_v2_compact() {
        let mut buf = BytesMut::new();

        // transactional_id: "test-tx-id" (compact nullable string)
        write_compact_nullable_string(&mut buf, Some("test-tx-id"));

        // transaction_timeout_ms: 45000
        buf.put_i32(45000);

        // Tagged fields (empty)
        write_unsigned_varint(&mut buf, 0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = InitProducerIdRequest::parse(&mut cursor, 2).unwrap();

        assert_eq!(request.transactional_id, Some("test-tx-id".to_string()));
        assert_eq!(request.transaction_timeout_ms, 45000);
        assert_eq!(request.producer_id, -1); // Default for v2
        assert_eq!(request.producer_epoch, -1); // Default for v2
    }

    #[test]
    fn test_init_producer_id_request_parse_v3_with_producer_info() {
        let mut buf = BytesMut::new();

        // transactional_id: null (compact nullable string)
        write_compact_nullable_string(&mut buf, None);

        // transaction_timeout_ms: 30000
        buf.put_i32(30000);

        // producer_id: 12345
        buf.put_i64(12345);

        // producer_epoch: 5
        buf.put_i16(5);

        // Tagged fields (empty)
        write_unsigned_varint(&mut buf, 0);

        let mut cursor = Cursor::new(&buf[..]);
        let request = InitProducerIdRequest::parse(&mut cursor, 3).unwrap();

        assert_eq!(request.transactional_id, None);
        assert_eq!(request.transaction_timeout_ms, 30000);
        assert_eq!(request.producer_id, 12345);
        assert_eq!(request.producer_epoch, 5);
    }

    #[test]
    fn test_init_producer_id_response_encode_v0() {
        let response = InitProducerIdResponse::new(1000, 0);
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // No throttle_time_ms in v0
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i64(), 1000); // producer_id
        assert_eq!(cursor.get_i16(), 0); // producer_epoch
    }

    #[test]
    fn test_init_producer_id_response_encode_v1() {
        let response = InitProducerIdResponse::new(2000, 1);
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i64(), 2000); // producer_id
        assert_eq!(cursor.get_i16(), 1); // producer_epoch
    }

    #[test]
    fn test_init_producer_id_response_encode_v2_compact() {
        let response = InitProducerIdResponse::new(3000, 2);
        let encoded = response.encode(2).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 0); // error_code
        assert_eq!(cursor.get_i64(), 3000); // producer_id
        assert_eq!(cursor.get_i16(), 2); // producer_epoch

        // Tagged fields
        let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(tagged_fields_len, 0);
    }

    #[test]
    fn test_init_producer_id_response_error() {
        let response = InitProducerIdResponse::error(35); // UNSUPPORTED_VERSION
        let encoded = response.encode(2).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        assert_eq!(cursor.get_i32(), 0); // throttle_time_ms
        assert_eq!(cursor.get_i16(), 35); // error_code
        assert_eq!(cursor.get_i64(), -1); // producer_id
        assert_eq!(cursor.get_i16(), -1); // producer_epoch

        // Tagged fields
        let tagged_fields_len = read_unsigned_varint(&mut cursor).unwrap();
        assert_eq!(tagged_fields_len, 0);
    }
}
