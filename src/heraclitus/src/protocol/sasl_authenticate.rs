use crate::error::Result;
use crate::protocol::kafka_protocol::{read_bytes, write_bytes, write_nullable_string};
use bytes::{BufMut, BytesMut};
use std::io::Cursor;

/// SaslAuthenticate Request (API key 36)
#[derive(Debug)]
pub struct SaslAuthenticateRequest {
    pub auth_bytes: Vec<u8>,
}

impl SaslAuthenticateRequest {
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // auth_bytes: bytes
        let auth_bytes = read_bytes(cursor)?;

        // Validate API version
        if api_version > 2 {
            return Err(crate::error::HeraclitusError::InvalidRequest(format!(
                "Unsupported SaslAuthenticate API version: {api_version}"
            )));
        }

        Ok(SaslAuthenticateRequest { auth_bytes })
    }
}

/// SaslAuthenticate Response
#[derive(Debug)]
pub struct SaslAuthenticateResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
    pub session_lifetime_ms: i64,
}

impl SaslAuthenticateResponse {
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // error_code: int16
        buf.put_i16(self.error_code);

        // error_message: nullable_string
        write_nullable_string(&mut buf, self.error_message.as_deref());

        // auth_bytes: bytes
        write_bytes(&mut buf, &self.auth_bytes);

        // session_lifetime_ms: int64 (v1+)
        if api_version >= 1 {
            buf.put_i64(self.session_lifetime_ms);
        }

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn test_parse_sasl_authenticate_request() {
        let mut buf = BytesMut::new();

        // auth_bytes: bytes
        let auth_data = b"test auth data";
        write_bytes(&mut buf, auth_data);

        let mut cursor = Cursor::new(&buf[..]);
        let request = SaslAuthenticateRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.auth_bytes, auth_data);
    }

    #[test]
    fn test_encode_success_response() {
        let response = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        };
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 0);

        // error_message length: int16 (-1 for null)
        assert_eq!(cursor.get_i16(), -1);

        // auth_bytes length: int32
        assert_eq!(cursor.get_i32(), 0);
    }

    #[test]
    fn test_encode_error_response() {
        let response = SaslAuthenticateResponse {
            error_code: 58,
            error_message: Some("Authentication failed".to_string()),
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        };
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 58);

        // error_message length: int16
        assert_eq!(cursor.get_i16(), 21);
        let mut msg_bytes = vec![0u8; 21];
        cursor.copy_to_slice(&mut msg_bytes);
        assert_eq!(&msg_bytes, b"Authentication failed");

        // auth_bytes length: int32
        assert_eq!(cursor.get_i32(), 0);
    }

    #[test]
    fn test_encode_response_v1() {
        let response = SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: vec![],
            session_lifetime_ms: 3600000, // 1 hour
        };
        let encoded = response.encode(1).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 0);

        // error_message length: int16 (-1 for null)
        assert_eq!(cursor.get_i16(), -1);

        // auth_bytes length: int32
        assert_eq!(cursor.get_i32(), 0);

        // session_lifetime_ms: int64
        assert_eq!(cursor.get_i64(), 3600000);
    }
}
