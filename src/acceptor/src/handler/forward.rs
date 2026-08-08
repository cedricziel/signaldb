//! # Batch Forwarding
//!
//! Shared helper for forwarding Arrow RecordBatches from the acceptor to a
//! writer service via Flight. Used by the OTLP/Prometheus handlers on the
//! hot path and by the WAL retry consumer when replaying entries whose
//! initial forward failed.

use anyhow::Context;
use bytes::Bytes;
use common::flight::batches_to_compressed_flight_data;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream};
use tracing::Instrument;

/// What a failed forward implies about retrying the same batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardFailureKind {
    /// The writer could not be reached, or failed for a reason of its own.
    /// The same batch may well be accepted on a later attempt.
    Transient,
    /// The writer received the batch and refused it. The batch is the
    /// problem, so every retry fails identically.
    Permanent,
}

/// Classify a [`forward_batch_to_writer`] error by whether retrying can help.
///
/// Defaults to [`ForwardFailureKind::Transient`] whenever the failure cannot
/// be positively identified as a rejection. The two misjudgements are not
/// symmetric: calling a rejection transient costs retries, while calling a
/// transient failure permanent dead-letters data the writer would have
/// accepted. Ambiguous cases therefore fall on the retry side.
pub fn classify_forward_failure(error: &anyhow::Error) -> ForwardFailureKind {
    // The tonic status survives anyhow's context chain via the root cause;
    // a failure that never reached the writer has no status at all.
    let Some(status) = error.root_cause().downcast_ref::<tonic::Status>() else {
        return ForwardFailureKind::Transient;
    };

    match status.code() {
        // The writer inspected the batch and refused it.
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::OutOfRange
        | tonic::Code::Unimplemented => ForwardFailureKind::Permanent,
        // Everything else may clear on its own. `Internal` in particular must
        // stay here: the writer returns it both for a batch it cannot
        // transform and for its own WAL write/flush failures, so it does not
        // identify the batch as the culprit.
        _ => ForwardFailureKind::Transient,
    }
}

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
    // One RecordBatch encodes into one FlightData message; a batch whose
    // encoded size exceeds the receiver's gRPC limit fails do_put on every
    // retry and wedges its WAL entry forever (#944). Chunk oversized
    // batches so each message stays well below the shared limit — the
    // budget is measured on in-memory size, so lz4 IPC compression (#945)
    // only adds headroom on top.
    let batches = common::flight::chunk::split_batch_for_grpc(
        &record_batch,
        common::flight::chunk::MAX_ENCODED_BATCH_SIZE,
    )
    .context("Failed to split batch for transport")?;
    let mut flight_data = batches_to_compressed_flight_data(&schema, batches)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap a status the way `forward_batch_to_writer_inner` does, so the
    /// classifier is exercised against the real shape of the error.
    fn do_put_error(status: tonic::Status) -> anyhow::Error {
        anyhow::Error::new(status).context("Flight do_put failed")
    }

    #[test]
    fn writer_rejecting_the_batch_is_permanent() {
        // The hive wedge (#1060): the writer refuses a metrics_sum batch whose
        // non-nullable `value` column contains nulls. No retry can change it.
        let error = do_put_error(tonic::Status::invalid_argument(
            "Schema transformation failed: Column 'value' is declared as non-nullable",
        ));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Permanent
        );
    }

    #[test]
    fn unreachable_writer_is_transient() {
        let error = do_put_error(tonic::Status::unavailable("connection refused"));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }

    #[test]
    fn internal_is_transient_because_the_writer_uses_it_for_its_own_failures() {
        // `Status::internal` covers "Failed to write to WAL" and "Failed to
        // flush WAL" on the writer, which are writer-side and recoverable.
        // Treating it as permanent would dead-letter perfectly good batches
        // whenever the writer's own storage hiccups.
        let error = do_put_error(tonic::Status::internal("Failed to write to WAL: disk full"));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }

    #[test]
    fn failure_that_never_reached_the_writer_is_transient() {
        // No storage service discoverable — there is no status to inspect,
        // and the batch itself has not been judged by anything.
        let error = anyhow::anyhow!("Failed to get Flight client for storage service: none found");

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }

    #[test]
    fn deadline_exceeded_is_transient() {
        let error = do_put_error(tonic::Status::deadline_exceeded("timed out"));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }
}
