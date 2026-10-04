//! # IPC stream passthrough
//!
//! Re-frames an Arrow IPC *stream* buffer as Flight `FlightData` messages
//! without decoding or re-encoding any record batch.
//!
//! The acceptor already encodes every ingest batch once, zstd-compressed,
//! for its WAL ([`crate::wal::record_batch_to_bytes`]). The Flight `DoPut`
//! to the writer carries the same logical messages (schema, then record
//! batch), only framed differently, so [`ipc_stream_to_flight_data`] reuses
//! those bytes instead of encoding the batch a second time (#942).

use arrow_flight::FlightData;
use bytes::Bytes;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::root_as_message;

/// Marker that precedes every encapsulated message in the post-0.15 IPC
/// stream format.
const CONTINUATION_MARKER: [u8; 4] = [0xFF; 4];

/// Split an Arrow IPC stream buffer into one `FlightData` per encapsulated
/// message (schema, dictionary and record-batch messages), stopping at the
/// end-of-stream marker.
///
/// Each message's flatbuffer metadata becomes `data_header` and its body
/// `data_body`, both zero-copy slices of `stream`. Buffer compression is
/// recorded in the metadata, so a zstd stream stays zstd on the wire and
/// Flight readers decode it as usual. `app_metadata` and
/// `flight_descriptor` are left empty for the caller to fill.
///
/// # Errors
///
/// Returns an error for a truncated stream, an unparseable message, a body
/// length that overruns the buffer, a missing end-of-stream marker, or the
/// legacy pre-0.15 framing (no continuation marker), which is not
/// supported.
pub fn ipc_stream_to_flight_data(stream: &Bytes) -> Result<Vec<FlightData>, ArrowError> {
    let mut messages = Vec::new();
    let mut offset = 0usize;
    loop {
        let prefix = read_slice(stream, offset, 8, "message prefix")?;
        if prefix[..4] != CONTINUATION_MARKER {
            return Err(ArrowError::IpcError(
                "IPC stream without continuation markers (legacy format) is not supported"
                    .to_string(),
            ));
        }
        let metadata_len = i32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]);
        offset += 8;
        if metadata_len == 0 {
            return Ok(messages);
        }
        let metadata_len = usize::try_from(metadata_len).map_err(|_| {
            ArrowError::IpcError(format!("negative IPC metadata length {metadata_len}"))
        })?;

        let metadata = read_slice(stream, offset, metadata_len, "message metadata")?;
        let message = root_as_message(metadata)
            .map_err(|e| ArrowError::IpcError(format!("invalid IPC message metadata: {e}")))?;
        let body_len = usize::try_from(message.bodyLength())
            .map_err(|_| ArrowError::IpcError("invalid IPC message body length".to_string()))?;
        let body_start = offset + metadata_len;
        read_slice(stream, body_start, body_len, "message body")?;

        messages.push(FlightData {
            data_header: stream.slice(offset..body_start),
            data_body: stream.slice(body_start..body_start + body_len),
            ..Default::default()
        });
        offset = body_start + body_len;
    }
}

fn read_slice<'a>(
    stream: &'a Bytes,
    start: usize,
    len: usize,
    what: &str,
) -> Result<&'a [u8], ArrowError> {
    start
        .checked_add(len)
        .and_then(|end| stream.get(start..end))
        .ok_or_else(|| ArrowError::IpcError(format!("truncated IPC stream: incomplete {what}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flight::decode::flight_data_vec_to_batches;
    use crate::flight::test_support::batch_with_nulls;
    use crate::wal::record_batch_to_bytes;
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    #[tokio::test]
    async fn wal_bytes_round_trip_through_flight_data_to_the_same_batch() {
        let batch = batch_with_nulls();
        let wal_bytes = record_batch_to_bytes(&batch).unwrap();

        let flight_data = ipc_stream_to_flight_data(&wal_bytes).unwrap();
        assert_eq!(flight_data.len(), 2, "schema message + one batch message");
        let schema = Arc::new(Schema::try_from(&flight_data[0]).unwrap());
        assert_eq!(schema, batch.schema());

        // Both the crate's decoder and arrow-flight's own reader accept it.
        let decoded = flight_data_vec_to_batches(flight_data.clone())
            .await
            .unwrap();
        assert_eq!(decoded, vec![batch.clone()]);
        let decoded = arrow_flight::utils::flight_data_to_batches(&flight_data).unwrap();
        assert_eq!(decoded, vec![batch]);
    }

    #[test]
    fn bodies_are_the_wal_buffer_bytes_not_a_re_encode() {
        let wal_bytes = record_batch_to_bytes(&batch_with_nulls()).unwrap();

        let flight_data = ipc_stream_to_flight_data(&wal_bytes).unwrap();

        let range = wal_bytes.as_ptr_range();
        for message in &flight_data {
            for part in [&message.data_header, &message.data_body] {
                assert!(
                    range.contains(&part.as_ptr()) || part.is_empty(),
                    "FlightData part must be a slice of the WAL buffer"
                );
            }
        }
        assert!(!flight_data[1].data_body.is_empty());
    }

    #[test]
    fn truncated_streams_error_instead_of_panicking() {
        let wal_bytes = record_batch_to_bytes(&batch_with_nulls()).unwrap();

        for cut in [0, 3, 7, 8, 20, wal_bytes.len() / 2, wal_bytes.len() - 9] {
            let truncated = Bytes::copy_from_slice(&wal_bytes[..cut]);
            assert!(
                ipc_stream_to_flight_data(&truncated).is_err(),
                "cut at {cut} must be rejected"
            );
        }
    }

    #[test]
    fn garbage_and_legacy_framing_are_rejected() {
        let garbage = Bytes::from_static(b"definitely not an arrow stream");
        assert!(ipc_stream_to_flight_data(&garbage).is_err());

        // Legacy framing: a bare length prefix, no continuation marker.
        let legacy = Bytes::from(vec![16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let err = ipc_stream_to_flight_data(&legacy).unwrap_err();
        assert!(err.to_string().contains("legacy"), "{err}");

        // A valid continuation marker with a corrupt flatbuffer.
        let mut corrupt = vec![0xFF; 4];
        corrupt.extend_from_slice(&8i32.to_le_bytes());
        corrupt.extend_from_slice(&[0xAB; 8]);
        assert!(ipc_stream_to_flight_data(&Bytes::from(corrupt)).is_err());
    }
}
