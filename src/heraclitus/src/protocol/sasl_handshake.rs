use crate::error::Result;
use crate::protocol::kafka_protocol::{read_string, write_string};
use bytes::{BufMut, BytesMut};
use std::io::Cursor;

/// SaslHandshake Request (API key 17)
#[derive(Debug)]
pub struct SaslHandshakeRequest {
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    pub fn parse(cursor: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self> {
        // mechanism: string
        let mechanism = read_string(cursor)?;

        // Validate API version
        if api_version > 1 {
            return Err(crate::error::HeraclitusError::InvalidRequest(format!(
                "Unsupported SaslHandshake API version: {api_version}"
            )));
        }

        Ok(SaslHandshakeRequest { mechanism })
    }
}

/// SaslHandshake Response
#[derive(Debug)]
pub struct SaslHandshakeResponse {
    pub error_code: i16,
    pub enabled_mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    pub fn new(enabled_mechanisms: Vec<String>) -> Self {
        Self {
            error_code: 0, // ERROR_NONE
            enabled_mechanisms,
        }
    }

    pub fn error(error_code: i16) -> Self {
        Self {
            error_code,
            enabled_mechanisms: vec![],
        }
    }

    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // error_code: int16
        buf.put_i16(self.error_code);

        // enabled_mechanisms: [string]
        if api_version >= 0 {
            buf.put_i32(self.enabled_mechanisms.len() as i32);
            for mechanism in &self.enabled_mechanisms {
                write_string(&mut buf, mechanism);
            }
        }

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn test_parse_sasl_handshake_request() {
        let mut buf = BytesMut::new();

        // mechanism: string
        write_string(&mut buf, "PLAIN");

        let mut cursor = Cursor::new(&buf[..]);
        let request = SaslHandshakeRequest::parse(&mut cursor, 0).unwrap();

        assert_eq!(request.mechanism, "PLAIN");
    }

    #[test]
    fn test_encode_sasl_handshake_response() {
        let response =
            SaslHandshakeResponse::new(vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()]);
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 0);

        // mechanisms count: int32
        assert_eq!(cursor.get_i32(), 2);

        // mechanism 1 length: int16
        assert_eq!(cursor.get_i16(), 5);
        let mut mechanism_bytes = vec![0u8; 5];
        cursor.copy_to_slice(&mut mechanism_bytes);
        assert_eq!(&mechanism_bytes, b"PLAIN");

        // mechanism 2 length: int16
        assert_eq!(cursor.get_i16(), 13);
        let mut mechanism_bytes = vec![0u8; 13];
        cursor.copy_to_slice(&mut mechanism_bytes);
        assert_eq!(&mechanism_bytes, b"SCRAM-SHA-256");
    }

    #[test]
    fn test_encode_error_response() {
        let response = SaslHandshakeResponse::error(33); // ERROR_UNSUPPORTED_SASL_MECHANISM
        let encoded = response.encode(0).unwrap();

        let mut cursor = Cursor::new(&encoded[..]);

        // error_code: int16
        assert_eq!(cursor.get_i16(), 33);

        // mechanisms count: int32
        assert_eq!(cursor.get_i32(), 0);
    }
}
