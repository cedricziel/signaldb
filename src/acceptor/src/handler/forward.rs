//! # Batch Forwarding
//!
//! Shared helper for forwarding Arrow RecordBatches from the acceptor to a
//! writer service via Flight. Used by the OTLP/Prometheus handlers on the
//! hot path and by the WAL retry consumer when replaying entries whose
//! initial forward failed.

use anyhow::Context;
use arrow_flight::utils::batches_to_flight_data;
use bytes::Bytes;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream};

/// Forward a RecordBatch to a writer service with Storage capability.
///
/// `metadata_json` is attached as `app_metadata` on the first FlightData
/// message (the schema message) so the writer can route the batch to the
/// right table.
///
/// Returns an error if no storage service is discoverable, the batch cannot
/// be encoded, or the Flight put fails. The caller decides whether the data
/// stays in the WAL for retry.
pub async fn forward_batch_to_writer(
    flight_transport: &InMemoryFlightTransport,
    record_batch: RecordBatch,
    metadata_json: Option<&str>,
) -> anyhow::Result<()> {
    let mut client = flight_transport
        .get_client_for_capability(ServiceCapability::Storage)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Flight client for storage service: {e}"))?;

    let schema = record_batch.schema();
    let mut flight_data = batches_to_flight_data(&schema, vec![record_batch])
        .context("Failed to convert batch to flight data")?;

    // Add metadata to the first FlightData message (which contains the schema)
    if let Some(metadata_json) = metadata_json
        && let Some(first) = flight_data.first_mut()
    {
        first.app_metadata = Bytes::from(metadata_json.as_bytes().to_vec());
    }

    let mut request = tonic::Request::new(stream::iter(flight_data));
    // Authenticate to the writer when service-to-service auth is configured
    if let Some(key) = flight_transport.internal_service_key() {
        common::flight::auth::attach_internal_auth(&mut request, key);
    }

    let response = client
        .do_put(request)
        .await
        .context("Flight do_put failed")?;

    let mut response_stream = response.into_inner();
    while let Some(result) = response_stream.next().await {
        let put_result = result.context("Flight put error")?;
        tracing::debug!(response = ?put_result, "Flight put response");
    }

    Ok(())
}
