//! Shared decoding of querier Flight responses for the HTTP query endpoints.
//!
//! Endpoints decode each message as it arrives rather than collecting the
//! whole encoded response first, so the router never holds the encoded and
//! decoded result at once (#938). A query whose result set is empty
//! produces a well-formed empty Flight stream: zero messages, not even a
//! schema frame; that decodes to "no rows" rather than a decode failure.
//! Decoding tracks dictionary batches, so a querier response containing
//! dictionary-encoded columns decodes correctly instead of erroring (#951).

use arrow_flight::FlightData;
use axum::http::StatusCode;
use common::flight::decode::IncrementalFlightDecoder;
use datafusion::arrow::record_batch::RecordBatch;

/// Record batches decoded from a querier response, one message at a time.
pub(crate) struct DecodedBatches {
    decoder: IncrementalFlightDecoder,
    batches: Vec<RecordBatch>,
    what: &'static str,
}

impl DecodedBatches {
    /// `what` names the query kind in the error log.
    pub(crate) fn new(what: &'static str) -> Self {
        Self {
            decoder: IncrementalFlightDecoder::new(),
            batches: Vec::new(),
            what,
        }
    }

    pub(crate) fn push(&mut self, frame: FlightData) -> Result<(), StatusCode> {
        let decoded = self.decoder.push(frame).map_err(|e| {
            tracing::error!(error = %e, query_kind = self.what, "Failed to decode Flight data");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        self.batches.extend(decoded);
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<RecordBatch> {
        self.batches
    }
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
        assert!(DecodedBatches::new("logs").finish().is_empty());
    }

    #[test]
    fn frames_decode_to_batches() {
        let (schema, batch) = sample_batch();
        let mut decoded = DecodedBatches::new("logs");
        for frame in batches_to_flight_data(&schema, vec![batch]).expect("encode") {
            decoded.push(frame).expect("decode");
        }
        let batches = decoded.finish();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn garbage_frames_are_a_500() {
        let bogus = FlightData {
            data_body: vec![1, 2, 3].into(),
            ..Default::default()
        };
        let err = DecodedBatches::new("logs")
            .push(bogus)
            .expect_err("must fail");
        assert_eq!(err, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
