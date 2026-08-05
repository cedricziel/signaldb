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
use futures::{StreamExt, stream};

/// Decode a complete `Vec<FlightData>` into `RecordBatch`es, honoring any
/// dictionary batches present in the sequence.
///
/// Dictionary-safe drop-in replacement for
/// `arrow_flight::utils::flight_data_to_batches` for call sites that have
/// already buffered the full `FlightData` sequence rather than decoding
/// incrementally from a stream. Feeds the vector through
/// [`FlightRecordBatchStream`], which maintains the same per-field
/// dictionary state a streaming client would.
///
/// An empty `flight_data` decodes to an empty `Vec` rather than erroring
/// (unlike `arrow_flight::utils::flight_data_to_batches`, which requires at
/// least a schema message) — callers that need a schema message to be
/// present should check `flight_data.is_empty()` themselves, as all current
/// call sites already do before decoding.
pub async fn flight_data_vec_to_batches(
    flight_data: Vec<FlightData>,
) -> Result<Vec<RecordBatch>, FlightError> {
    let stream = stream::iter(flight_data.into_iter().map(Ok::<_, FlightError>));
    let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(stream);

    let mut batches = Vec::new();
    while let Some(batch) = record_batch_stream.next().await {
        batches.push(batch?);
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

    #[tokio::test]
    async fn empty_input_decodes_to_no_batches() {
        let decoded = flight_data_vec_to_batches(vec![])
            .await
            .expect("decode empty input");
        assert!(decoded.is_empty());
    }
}
