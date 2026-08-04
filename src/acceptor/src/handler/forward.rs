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
use tracing::Instrument;

/// Overwrite the `traceparent`/`tracestate` fields in the metadata JSON with
/// the current span's context. Returns the input unchanged when it is not a
/// JSON object or no context is active.
fn restamp_trace_context(metadata_json: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(metadata_json) else {
        return metadata_json.to_owned();
    };
    let Some(obj) = value.as_object_mut() else {
        return metadata_json.to_owned();
    };
    if let Some((traceparent, tracestate)) =
        common::flight::trace_context::current_trace_context_fields()
    {
        obj.insert("traceparent".to_string(), traceparent.into());
        match tracestate {
            Some(ts) => obj.insert("tracestate".to_string(), ts.into()),
            None => obj.remove("tracestate"),
        };
        serde_json::to_string(&value).unwrap_or_else(|_| metadata_json.to_owned())
    } else {
        metadata_json.to_owned()
    }
}

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
    // The whole logical DoPut is a semconv RPC CLIENT span; the writer's
    // server span becomes its child via the trace context stamped into the
    // app_metadata below.
    let rpc_span = common::self_monitoring::spans::rpc_client_span(
        common::self_monitoring::spans::FLIGHT_DO_PUT,
        None,
        None,
    );
    let record_span = rpc_span.clone();
    let result = forward_batch_to_writer_inner(flight_transport, record_batch, metadata_json)
        .instrument(rpc_span)
        .await;
    // Best-effort status: the underlying tonic code survives anyhow's
    // context chain via the root cause; anything else is UNKNOWN.
    let code = match &result {
        Ok(()) => tonic::Code::Ok,
        Err(e) => e
            .root_cause()
            .downcast_ref::<tonic::Status>()
            .map(|s| s.code())
            .unwrap_or(tonic::Code::Unknown),
    };
    common::self_monitoring::spans::record_rpc_result(
        &record_span,
        common::self_monitoring::spans::RpcBoundary::Client,
        code,
    );
    result
}

async fn forward_batch_to_writer_inner(
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

    // Add metadata to the first FlightData message (which contains the
    // schema), re-stamping the trace context with the CLIENT span's own
    // (we run instrumented, so the current span is the rpc.client span) —
    // the handler-captured traceparent would skip this span otherwise.
    if let Some(metadata_json) = metadata_json
        && let Some(first) = flight_data.first_mut()
    {
        let restamped = restamp_trace_context(metadata_json);
        first.app_metadata = Bytes::from(restamped.into_bytes());
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
