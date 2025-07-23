use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

#[allow(dead_code)] // Will be used when protocol is fully implemented
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

#[allow(dead_code)] // Will be used when protocol is fully implemented
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

#[allow(dead_code)] // Will be used when protocol is fully implemented
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

#[allow(dead_code)] // Will be used when protocol is fully implemented
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
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_NONE: i16 = 0;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_UNKNOWN: i16 = -1;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_INVALID_REQUEST: i16 = 42;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_UNSUPPORTED_VERSION: i16 = 35;
#[allow(dead_code)] // Will be used when protocol is fully implemented
pub const ERROR_TOPIC_NOT_FOUND: i16 = 3;
