//! # Dictionary-aware Flight decode helpers
//!
//! [`arrow_flight::utils::flight_data_to_batches`] decodes a `&[FlightData]`
//! against a `dictionaries_by_id` map that is constructed empty and never
//! populated (see its source in `arrow-flight`): a `DictionaryBatch` message
//! anywhere in the slice makes every following `RecordBatch` message fail to
//! decode, because [`arrow_flight::decode`]'s own encoder
//! ([`super::batches_to_compressed_flight_data`],
//! `arrow_flight::utils::batches_to_flight_data`) emits dictionary batches
//! ahead of each data batch whenever a column is dictionary-encoded.
//!
//! [`arrow_flight::decode::FlightRecordBatchStream`] /
//! [`arrow_flight::decode::FlightDataDecoder`] track dictionaries correctly
//! (updating a per-field dictionary map as `DictionaryBatch` messages
//! arrive, then hydrating each `RecordBatch`), but only consume a `Stream`.
//! This module bridges that gap for call sites that buffer a complete
//! `Vec<FlightData>` before decoding — typically because they need custom
//! per-message error handling on the way in (e.g. mapping a querier's
//! terminal gRPC status) that decoding-while-streaming would entangle with
//! decode errors.
//!
//! Call sites that already hold a `Stream<Item = Result<FlightData, ...>>`
//! and don't need that separation should prefer decoding directly through
//! `FlightRecordBatchStream::new_from_flight_data` instead of collecting a
//! `Vec` first.

use arrow_flight::FlightData;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::channel::mpsc;
use futures::{FutureExt, StreamExt};

/// Decodes Flight messages one at a time as a caller receives them, so the
/// caller never holds the whole encoded response alongside the decoded
/// batches (#938).
///
/// For call sites that read a querier stream message by message because
/// they inspect each one on the way in (a trailer, a byte budget, a
/// terminal status) and so cannot hand the stream to
/// [`FlightRecordBatchStream`] directly. Dictionary batches are tracked
/// the same way, since this feeds that decoder.
pub struct IncrementalFlightDecoder {
    frames: mpsc::UnboundedSender<Result<FlightData, FlightError>>,
    batches: FlightRecordBatchStream,
}

impl IncrementalFlightDecoder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // Unbounded, but `push` drains after every send, so at most one
        // frame is ever queued.
        let (frames, rx) = mpsc::unbounded();
        Self {
            frames,
            batches: FlightRecordBatchStream::new_from_flight_data(rx),
        }
    }

    /// Decode one message, returning the record batches it completes:
    /// none for a schema or dictionary message, one for a data message.
    pub fn push(&mut self, frame: FlightData) -> Result<Vec<RecordBatch>, FlightError> {
        self.frames
            .unbounded_send(Ok(frame))
            .map_err(|e| FlightError::ProtocolError(format!("decoder closed: {e}")))?;
        let mut decoded = Vec::new();
        // Everything pushed so far is already queued, so the decoder never
        // waits on I/O here; it reports pending once the queue is drained.
        // It never ends, since `self` holds the sender.
        while let Some(Some(batch)) = self.batches.next().now_or_never() {
            decoded.push(batch?);
        }
        Ok(decoded)
    }
}

/// Decode a complete `Vec<FlightData>` into `RecordBatch`es, honoring any
/// dictionary batches present in the sequence.
///
/// Dictionary-safe drop-in replacement for
/// `arrow_flight::utils::flight_data_to_batches` for call sites that have
/// already buffered the full `FlightData` sequence. An empty `flight_data`
/// decodes to an empty `Vec` rather than erroring (unlike
/// `arrow_flight::utils::flight_data_to_batches`, which requires at least a
/// schema message).
pub async fn flight_data_vec_to_batches(
    flight_data: Vec<FlightData>,
) -> Result<Vec<RecordBatch>, FlightError> {
    let mut decoder = IncrementalFlightDecoder::new();
    let mut batches = Vec::new();
    for frame in flight_data {
        batches.extend(decoder.push(frame)?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int32DictionaryArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// A batch with a dictionary-encoded column, shaped like a
    /// low-cardinality attribute (e.g. `service.name`) that would benefit
    /// from dictionary encoding.
    fn dictionary_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "service_name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        let ids = Int32Array::from(vec![1, 2, 3, 4]);
        let keys = Int32Array::from(vec![0, 1, 0, 1]);
        let values = StringArray::from(vec!["checkout", "cart"]);
        let dict =
            Int32DictionaryArray::try_new(keys, Arc::new(values)).expect("valid dictionary array");
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(dict)])
            .expect("valid dictionary batch")
    }

    #[tokio::test]
    async fn dictionary_column_round_trips_through_encode_and_decode() {
        let batch = dictionary_batch();
        let schema = batch.schema();

        let flight_data =
            crate::flight::batches_to_compressed_flight_data(&schema, vec![batch.clone()])
                .expect("encode batch with dictionary column");

        // A dictionary batch must precede the data batch: schema, dictionary,
        // record batch.
        assert_eq!(
            flight_data.len(),
            3,
            "expected schema + dictionary + record batch messages"
        );

        let decoded = flight_data_vec_to_batches(flight_data)
            .await
            .expect("decode batch with dictionary column");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], batch);
    }

    #[tokio::test]
    async fn plain_batch_round_trips_without_dictionaries() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("valid batch");

        let flight_data =
            crate::flight::batches_to_compressed_flight_data(&schema, vec![batch.clone()])
                .expect("encode plain batch");

        let decoded = flight_data_vec_to_batches(flight_data)
            .await
            .expect("decode plain batch");

        assert_eq!(decoded, vec![batch]);
    }

    #[test]
    fn incremental_decoder_yields_each_batch_as_its_message_arrives() {
        let batch = dictionary_batch();
        let schema = batch.schema();
        let frames = crate::flight::batches_to_compressed_flight_data(
            &schema,
            vec![batch.clone(), batch.clone()],
        )
        .expect("encode");

        let mut decoder = IncrementalFlightDecoder::new();
        let per_frame: Vec<usize> = frames
            .into_iter()
            .map(|frame| decoder.push(frame).expect("decode").len())
            .collect();
        // schema, dictionary, batch, then the second batch reusing the
        // already-sent dictionary.
        assert_eq!(per_frame, vec![0, 0, 1, 1]);
    }

    #[test]
    fn incremental_decoder_rejects_garbage() {
        let mut decoder = IncrementalFlightDecoder::new();
        let bogus = FlightData {
            data_body: vec![1, 2, 3].into(),
            ..Default::default()
        };
        assert!(decoder.push(bogus).is_err());
    }

    #[tokio::test]
    async fn empty_input_decodes_to_no_batches() {
        let decoded = flight_data_vec_to_batches(vec![])
            .await
            .expect("decode empty input");
        assert!(decoded.is_empty());
    }
}
