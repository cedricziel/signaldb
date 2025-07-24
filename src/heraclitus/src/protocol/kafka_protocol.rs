use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

pub fn read_request_header(buf: &mut Cursor<&[u8]>) -> Result<RequestHeader, std::io::Error> {
    if buf.remaining() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for request header",
        ));
    }

    let api_key = buf.get_i16();
    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    // Read client_id (nullable string)
    let client_id = read_nullable_string(buf)?;

    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

pub fn write_response_header(buf: &mut BytesMut, correlation_id: i32) {
    buf.put_i32(correlation_id);
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn read_string(buf: &mut Cursor<&[u8]>) -> Result<String, std::io::Error> {
    if buf.remaining() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string length",
        ));
    }

    let len = buf.get_i16();
    if len < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Negative string length",
        ));
    }

    let len = len as usize;
    if buf.remaining() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string data",
        ));
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 string"))
}

pub fn read_nullable_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>, std::io::Error> {
    if buf.remaining() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string length",
        ));
    }

    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }

    let len = len as usize;
    if buf.remaining() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string data",
        ));
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);

    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 string"))
}

/// Read bytes from buffer
pub fn read_bytes(buf: &mut Cursor<&[u8]>) -> Result<Vec<u8>, std::io::Error> {
    if buf.remaining() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for byte array length",
        ));
    }

    let len = buf.get_i32();
    if len < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Negative byte array length",
        ));
    }

    let len = len as usize;
    if buf.remaining() < len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Not enough bytes for byte array data",
        ));
    }

    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_i16(bytes.len() as i16);
    buf.put_slice(bytes);
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => write_string(buf, s),
        None => buf.put_i16(-1),
    }
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    buf.put_i32(bytes.len() as i32);
    buf.put_slice(bytes);
}

#[allow(dead_code)] // Will be used when protocol is fully implemented
pub fn write_nullable_bytes(buf: &mut BytesMut, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => write_bytes(buf, bytes),
        None => buf.put_i32(-1),
    }
}

// API Keys
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_PRODUCE: i16 = 0;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_FETCH: i16 = 1;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_LIST_OFFSETS: i16 = 2;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_METADATA: i16 = 3;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_OFFSET_COMMIT: i16 = 8;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_OFFSET_FETCH: i16 = 9;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_FIND_COORDINATOR: i16 = 10;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_JOIN_GROUP: i16 = 11;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_HEARTBEAT: i16 = 12;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_LEAVE_GROUP: i16 = 13;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const API_KEY_SYNC_GROUP: i16 = 14;

// Error codes
pub const ERROR_NONE: i16 = 0;
pub const ERROR_UNKNOWN: i16 = -1;
pub const ERROR_INVALID_REQUEST: i16 = 42;
#[allow(dead_code)]
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
pub const ERROR_TOPIC_NOT_FOUND: i16 = 3;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3; // Same as TOPIC_NOT_FOUND
pub const ERROR_INVALID_REQUIRED_ACKS: i16 = 21;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const ERROR_OFFSET_NOT_AVAILABLE: i16 = 78;
pub const ERROR_UNKNOWN_MEMBER_ID: i16 = 25;
pub const ERROR_ILLEGAL_GENERATION: i16 = 22;
#[allow(dead_code)]
pub const ERROR_REBALANCE_IN_PROGRESS: i16 = 27;
#[allow(dead_code)]
pub const ERROR_SESSION_TIMEOUT: i16 = 10;
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const ERROR_NOT_COORDINATOR: i16 = 16;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::io::Cursor;

    #[test]
    fn test_read_request_header() {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // API key (metadata)
        buf.put_i16(0); // API version
        buf.put_i32(123); // Correlation ID
        buf.put_i16(11); // Client ID length
        buf.put_slice(b"test-client"); // Client ID

        let mut cursor = Cursor::new(&buf[..]);
        let header = read_request_header(&mut cursor).unwrap();

        assert_eq!(header.api_key, 3);
        assert_eq!(header.api_version, 0);
        assert_eq!(header.correlation_id, 123);
        assert_eq!(header.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_read_request_header_null_client_id() {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // API key
        buf.put_i16(0); // API version
        buf.put_i32(456); // Correlation ID
        buf.put_i16(-1); // Null client ID

        let mut cursor = Cursor::new(&buf[..]);
        let header = read_request_header(&mut cursor).unwrap();

        assert_eq!(header.api_key, 1);
        assert_eq!(header.api_version, 0);
        assert_eq!(header.correlation_id, 456);
        assert_eq!(header.client_id, None);
    }

    #[test]
    fn test_read_request_header_insufficient_data() {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // API key
        buf.put_i16(0); // API version
        // Missing correlation ID and client ID

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_request_header(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_response_header() {
        let mut buf = BytesMut::new();
        write_response_header(&mut buf, 789);

        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(cursor.get_i32(), 789);
    }

    #[test]
    fn test_read_write_string() {
        let test_str = "hello kafka";
        let mut buf = BytesMut::new();
        write_string(&mut buf, test_str);

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_string(&mut cursor).unwrap();
        assert_eq!(result, test_str);
    }

    #[test]
    fn test_read_write_nullable_string() {
        // Test with Some value
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, Some("test"));

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, Some("test".to_string()));

        // Test with None
        let mut buf = BytesMut::new();
        write_nullable_string(&mut buf, None);

        let mut cursor = Cursor::new(&buf[..]);
        let result = read_nullable_string(&mut cursor).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_write_bytes() {
        let test_bytes = b"binary data";
        let mut buf = BytesMut::new();
        write_bytes(&mut buf, test_bytes);

        let mut cursor = Cursor::new(&buf[..]);
        let len = cursor.get_i32();
        assert_eq!(len, test_bytes.len() as i32);

        let mut data = vec![0u8; len as usize];
        cursor.copy_to_slice(&mut data);
        assert_eq!(&data[..], test_bytes);
    }

    #[test]
    fn test_write_nullable_bytes() {
        // Test with Some value
        let mut buf = BytesMut::new();
        write_nullable_bytes(&mut buf, Some(b"data"));

        let mut cursor = Cursor::new(&buf[..]);
        let len = cursor.get_i32();
        assert_eq!(len, 4);

        // Test with None
        let mut buf = BytesMut::new();
        write_nullable_bytes(&mut buf, None);

        let mut cursor = Cursor::new(&buf[..]);
        let len = cursor.get_i32();
        assert_eq!(len, -1);
    }
}
