//! Shared decoding of querier Flight responses for the HTTP query endpoints.
//!
//! A query whose result set is empty produces a well-formed empty Flight
//! stream: zero messages, not even a schema frame. `flight_data_to_batches`
//! errors on that ("Need at least one FlightData for schema"), so every
//! endpoint must treat no-frames as "no rows" rather than a decode failure.

use arrow_flight::FlightData;
use arrow_flight::utils::flight_data_to_batches;
use axum::http::StatusCode;
use datafusion::arrow::record_batch::RecordBatch;

/// Decode collected Flight frames into record batches. Zero frames is a
/// valid empty result. `what` names the query kind in the error log.
pub(crate) fn decode_flight_batches(
    data: &[FlightData],
    what: &str,
) -> Result<Vec<RecordBatch>, StatusCode> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    flight_data_to_batches(data).map_err(|e| {
        tracing::error!(error = %e, query_kind = what, "Failed to decode Flight data");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::utils::batches_to_flight_data;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        (schema, batch)
    }

    #[test]
    fn empty_stream_decodes_to_no_batches() {
        let batches = decode_flight_batches(&[], "logs").expect("empty is valid");
        assert!(batches.is_empty());
    }

    #[test]
    fn frames_decode_to_batches() {
        let (schema, batch) = sample_batch();
        let data = batches_to_flight_data(&schema, vec![batch]).expect("encode");
        let batches = decode_flight_batches(&data, "logs").expect("decode");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn garbage_frames_are_a_500() {
        let bogus = FlightData {
            data_body: vec![1, 2, 3].into(),
            ..Default::default()
        };
        let err = decode_flight_batches(&[bogus], "logs").expect_err("must fail");
        assert_eq!(err, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
