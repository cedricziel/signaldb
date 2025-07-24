use crate::error::{HeraclitusError, Result};
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
        // Transactional ID (nullable string)
        let transactional_id = parse_nullable_string(buf)?;

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
        if version >= 2 {
            let _num_tagged_fields = parse_unsigned_varint(buf)?;
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
        if version >= 2 {
            write_unsigned_varint(&mut buf, 0); // No tagged fields
        }

        Ok(buf.to_vec())
    }
}

// Helper functions
fn parse_nullable_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>> {
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let mut bytes = vec![0u8; len as usize];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(String::from_utf8(bytes).map_err(|e| {
        HeraclitusError::Protocol(format!("Invalid UTF-8: {e}"))
    })?))
}

fn parse_unsigned_varint(buf: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = 0u32;
    let mut i = 0;
    loop {
        if i > 4 {
            return Err(HeraclitusError::Protocol("Varint too long".to_string()));
        }
        let byte = buf.get_u8();
        value |= ((byte & 0x7F) as u32) << (i * 7);
        if byte & 0x80 == 0 {
            break;
        }
        i += 1;
    }
    Ok(value)
}

fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}
