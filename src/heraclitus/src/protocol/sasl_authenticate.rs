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
            return Err(crate::error::HeraclitusError::InvalidRequest(
                format!("Unsupported SaslAuthenticate API version: {}", api_version),
            ));
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
    pub fn success() -> Self {
        Self {
            error_code: 0, // ERROR_NONE
            error_message: None,
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        }
    }

    pub fn error(error_code: i16, error_message: String) -> Self {
        Self {
            error_code,
            error_message: Some(error_message),
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        }
    }

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

/// Parse SASL/PLAIN mechanism auth bytes
/// Format: [authzid] UTF8NUL authcid UTF8NUL passwd
/// We ignore authzid (authorization identity) and use authcid (authentication identity)
pub fn parse_plain_auth_bytes(auth_bytes: &[u8]) -> Result<(String, String)> {
    // Split by null bytes
    let parts: Vec<&[u8]> = auth_bytes.split(|&b| b == 0).collect();
    
    if parts.len() != 3 {
        return Err(crate::error::HeraclitusError::InvalidRequest(
            "Invalid SASL/PLAIN auth format".to_string(),
        ));
    }

    // parts[0] is authzid (ignored)
    // parts[1] is username (authcid)
    // parts[2] is password
    
    let username = String::from_utf8(parts[1].to_vec()).map_err(|_| {
        crate::error::HeraclitusError::InvalidRequest("Invalid UTF-8 in username".to_string())
    })?;
    
    let password = String::from_utf8(parts[2].to_vec()).map_err(|_| {
        crate::error::HeraclitusError::InvalidRequest("Invalid UTF-8 in password".to_string())
    })?;

    Ok((username, password))
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
        let response = SaslAuthenticateResponse::success();
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
        let response = SaslAuthenticateResponse::error(58, "Authentication failed".to_string());
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

    #[test]
    fn test_parse_plain_auth_bytes() {
        // Format: [authzid] UTF8NUL authcid UTF8NUL passwd
        let mut auth_bytes = Vec::new();
        auth_bytes.extend_from_slice(b""); // empty authzid
        auth_bytes.push(0); // null separator
        auth_bytes.extend_from_slice(b"alice");
        auth_bytes.push(0); // null separator
        auth_bytes.extend_from_slice(b"secret123");

        let (username, password) = parse_plain_auth_bytes(&auth_bytes).unwrap();
        assert_eq!(username, "alice");
        assert_eq!(password, "secret123");
    }

    #[test]
    fn test_parse_plain_auth_bytes_with_authzid() {
        // With authzid (which we ignore)
        let mut auth_bytes = Vec::new();
        auth_bytes.extend_from_slice(b"admin"); // authzid (ignored)
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"alice");
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"secret123");

        let (username, password) = parse_plain_auth_bytes(&auth_bytes).unwrap();
        assert_eq!(username, "alice");
        assert_eq!(password, "secret123");
    }

    #[test]
    fn test_parse_plain_auth_bytes_invalid() {
        // Missing null separators
        let auth_bytes = b"alicesecret123";
        assert!(parse_plain_auth_bytes(auth_bytes).is_err());

        // Only one null separator
        let mut auth_bytes = Vec::new();
        auth_bytes.extend_from_slice(b"alice");
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(b"secret123");
        assert!(parse_plain_auth_bytes(&auth_bytes).is_err());
    }
}