//! # Forwarding batches to a writer
//!
//! [`forward_batch_to_writer`] sends one Arrow `RecordBatch` to a writer
//! (`Storage` capability) with a Flight `DoPut`, the writer making it
//! durable in its own WAL before it acks. Shared by the acceptor's ingest
//! handlers and WAL retry consumer, and by the router's eval-results upload
//! (change: agent-offline-evals), so every ingest path sends batches the
//! same way.

use anyhow::Context;
use bytes::Bytes;
use datafusion::arrow::record_batch::RecordBatch;
use futures::{StreamExt, stream};
use tracing::Instrument;
use uuid::Uuid;

use super::batches_to_compressed_flight_data;
use super::transport::{InMemoryFlightTransport, ServiceCapability};

/// Overwrite the `traceparent`/`tracestate` fields in the metadata JSON with
/// the current span's context, and stamp `ingest_id` (the batch fingerprint
/// this DoPut carries). Returns the input with only `ingest_id` added when it is
/// not a JSON object (falls back to a bare object) so every DoPut can be
/// deduplicated on the writer side.
fn stamp_ingest_metadata(metadata_json: &str, ingest_id: Uuid) -> String {
    let mut value = serde_json::from_str::<serde_json::Value>(metadata_json)
        .unwrap_or_else(|_| serde_json::json!({}));
    let Some(obj) = value.as_object_mut() else {
        return metadata_json.to_owned();
    };
    obj.insert("ingest_id".to_string(), ingest_id.to_string().into());
    if let Some((traceparent, tracestate)) = super::trace_context::current_trace_context_fields() {
        obj.insert("traceparent".to_string(), traceparent.into());
        match tracestate {
            Some(ts) => obj.insert("tracestate".to_string(), ts.into()),
            None => obj.remove("tracestate"),
        };
    }
    serde_json::to_string(&value).unwrap_or_else(|_| metadata_json.to_owned())
}

/// Build the app_metadata bytes for the first FlightData message of a DoPut:
/// the caller's metadata JSON (or an empty object when none is given) with
/// `ingest_id` and the current trace context stamped in. Pure so it can be
/// unit tested without a Flight transport.
fn build_first_message_metadata(metadata_json: Option<&str>, ingest_id: Uuid) -> Vec<u8> {
    let base = metadata_json.unwrap_or("{}");
    stamp_ingest_metadata(base, ingest_id).into_bytes()
}

/// Forward a RecordBatch to a writer service with Storage capability.
///
/// `metadata_json` is attached as `app_metadata` on the first FlightData
/// message (the schema message) so the writer can route the batch to the
/// right table. `ingest_id` is the batch's content fingerprint; it is stamped
/// into that same metadata as `"ingest_id"` so the writer can deduplicate a
/// resend of the same batch, whichever acceptor WAL entry carries it, and it
/// also selects which writer receives the batch (see
/// [`InMemoryFlightTransport::get_client_for_capability_keyed`]), so every
/// resend is pinned to the same writer.
///
/// `ipc_stream` is the batch already encoded as an Arrow IPC stream (the
/// acceptor's WAL bytes); when given and the batch fits one message it is
/// re-framed instead of re-encoded (see [`super::ipc_stream`], #942).
/// `ipc_stream` must encode `record_batch`; the batch is not compared
/// against it.
///
/// Returns an error if no storage service is discoverable, the batch cannot
/// be encoded, or the Flight put fails. The caller decides whether the data
/// stays in the WAL for retry.
pub async fn forward_batch_to_writer(
    flight_transport: &InMemoryFlightTransport,
    record_batch: RecordBatch,
    ipc_stream: Option<Bytes>,
    metadata_json: Option<&str>,
    ingest_id: Uuid,
) -> anyhow::Result<()> {
    // Resolve writer address up-front so the CLIENT span carries
    // server.address per gRPC semconv (required on client call sites).
    let server_address = flight_transport
        .get_client_and_address_for_capability_keyed(ServiceCapability::Storage, ingest_id)
        .await
        .ok()
        .map(|(_, addr)| addr);
    // The whole logical DoPut is a semconv RPC CLIENT span; the writer's
    // server span becomes its child via the trace context stamped into the
    // app_metadata below.
    let rpc_span = crate::self_monitoring::spans::rpc_client_span(
        crate::self_monitoring::spans::FLIGHT_DO_PUT,
        None,
        server_address.as_deref(),
    );
    let record_span = rpc_span.clone();
    let result = forward_batch_to_writer_inner(
        flight_transport,
        record_batch,
        ipc_stream,
        metadata_json,
        ingest_id,
    )
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
    crate::self_monitoring::spans::record_rpc_result(
        &record_span,
        crate::self_monitoring::spans::RpcBoundary::Client,
        code,
    );
    result
}

/// The `DoPut` payload for one batch: the pre-encoded IPC stream re-framed
/// when it is supplied and the batch needs no splitting, otherwise a fresh
/// lz4 encoding.
fn flight_data_for_batch(
    record_batch: &RecordBatch,
    ipc_stream: Option<Bytes>,
    max_message_size: usize,
) -> anyhow::Result<Vec<arrow_flight::FlightData>> {
    if let Some(ipc_stream) = ipc_stream
        && super::chunk::fits_single_message(record_batch, max_message_size)
    {
        return super::ipc_stream::ipc_stream_to_flight_data(&ipc_stream)
            .context("Failed to frame pre-encoded IPC stream as flight data");
    }
    // One RecordBatch encodes into one FlightData message; a batch whose
    // encoded size exceeds the receiver's gRPC limit fails do_put on every
    // retry and wedges its WAL entry forever (#944). Chunk oversized
    // batches so each message stays well below the shared limit — the
    // budget is measured on in-memory size, so IPC compression (#945)
    // only adds headroom on top.
    let batches = super::chunk::split_batch_for_grpc(record_batch, max_message_size)
        .context("Failed to split batch for transport")?;
    batches_to_compressed_flight_data(&record_batch.schema(), batches)
        .context("Failed to convert batch to flight data")
}

async fn forward_batch_to_writer_inner(
    flight_transport: &InMemoryFlightTransport,
    record_batch: RecordBatch,
    ipc_stream: Option<Bytes>,
    metadata_json: Option<&str>,
    ingest_id: Uuid,
) -> anyhow::Result<()> {
    let mut client = flight_transport
        .get_client_for_capability_keyed(ServiceCapability::Storage, ingest_id)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get Flight client for storage service: {e}"))?;

    let mut flight_data = flight_data_for_batch(
        &record_batch,
        ipc_stream,
        super::chunk::MAX_ENCODED_BATCH_SIZE,
    )?;

    // Add metadata to the first FlightData message (which contains the
    // schema): the ingest id for writer-side dedup, and the trace context
    // re-stamped with the CLIENT span's own (we run instrumented, so the
    // current span is the rpc.client span) — the handler-captured
    // traceparent would skip this span otherwise.
    if let Some(first) = flight_data.first_mut() {
        first.app_metadata = build_first_message_metadata(metadata_json, ingest_id).into();
    }

    let mut request = tonic::Request::new(stream::iter(flight_data));
    // Authenticate to the writer when service-to-service auth is configured
    if let Some(key) = flight_transport.internal_service_key() {
        super::auth::attach_internal_auth(&mut request, key);
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
    use crate::catalog::Catalog;
    use crate::flight::test_support::{NoopFlightService, batch_with_nulls};
    use crate::service_bootstrap::{ServiceBootstrap, ServiceType};
    use crate::wal::record_batch_to_bytes;
    use arrow_flight::FlightData;
    use std::sync::{Arc, Mutex};

    /// Run a writer that records DoPut payloads and a transport that finds it.
    async fn transport_with_recording_writer()
    -> (InMemoryFlightTransport, Arc<Mutex<Vec<FlightData>>>) {
        let catalog = Catalog::new_in_memory().await.unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let writer = NoopFlightService {
            received: Some(received.clone()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(crate::flight::flight_service_server(writer))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        ServiceBootstrap::new_for_test_with_catalog(
            catalog.clone(),
            ServiceType::Writer,
            &addr.to_string(),
        )
        .await
        .unwrap();
        let acceptor = ServiceBootstrap::new_for_test_with_catalog(
            catalog,
            ServiceType::Acceptor,
            "127.0.0.1:0",
        )
        .await
        .unwrap();
        (InMemoryFlightTransport::new(acceptor), received)
    }

    #[tokio::test]
    async fn pre_encoded_bytes_reach_the_writer_as_the_same_rows() {
        let (transport, received) = transport_with_recording_writer().await;
        let batch = batch_with_nulls();
        let wal_bytes = record_batch_to_bytes(&batch).unwrap();
        let ingest_id = Uuid::new_v4();

        forward_batch_to_writer(
            &transport,
            batch.clone(),
            Some(wal_bytes.clone()),
            Some(r#"{"signal_type":"logs"}"#),
            ingest_id,
        )
        .await
        .unwrap();

        let received = received.lock().unwrap().clone();
        let first_metadata: serde_json::Value =
            serde_json::from_slice(&received[0].app_metadata).unwrap();
        assert_eq!(first_metadata["ingest_id"], ingest_id.to_string());
        assert_eq!(first_metadata["signal_type"], "logs");
        // The batch message is the WAL's zstd body, untouched.
        let wal_messages =
            crate::flight::ipc_stream::ipc_stream_to_flight_data(&wal_bytes).unwrap();
        assert_eq!(received[1].data_body, wal_messages[1].data_body);

        let decoded = arrow_flight::utils::flight_data_to_batches(&received).unwrap();
        assert_eq!(decoded, vec![batch]);
    }

    #[tokio::test]
    async fn without_pre_encoded_bytes_the_batch_is_encoded_with_lz4() {
        let (transport, received) = transport_with_recording_writer().await;
        let batch = batch_with_nulls();

        forward_batch_to_writer(&transport, batch.clone(), None, None, Uuid::new_v4())
            .await
            .unwrap();

        let received = received.lock().unwrap().clone();
        let decoded = arrow_flight::utils::flight_data_to_batches(&received).unwrap();
        assert_eq!(decoded, vec![batch]);
    }

    #[test]
    fn oversized_batches_ignore_pre_encoded_bytes_and_are_chunked() {
        let batch = batch_with_nulls();
        let wal_bytes = record_batch_to_bytes(&batch).unwrap();
        let limit = 512;
        assert!(!super::super::chunk::fits_single_message(&batch, limit));

        let flight_data = flight_data_for_batch(&batch, Some(wal_bytes), limit).unwrap();

        assert!(
            flight_data.len() > 2,
            "oversized batch must be split into several batch messages"
        );
        let decoded = arrow_flight::utils::flight_data_to_batches(&flight_data).unwrap();
        let rows: usize = decoded.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, batch.num_rows());
    }

    #[test]
    fn first_message_metadata_carries_ingest_id() {
        let ingest_id = Uuid::new_v4();
        let metadata = build_first_message_metadata(
            Some(r#"{"schema_version":"v1","signal_type":"traces"}"#),
            ingest_id,
        );
        let value: serde_json::Value = serde_json::from_slice(&metadata).unwrap();

        assert_eq!(value["ingest_id"], ingest_id.to_string());
        assert_eq!(value["signal_type"], "traces");
    }

    #[test]
    fn first_message_metadata_carries_ingest_id_with_no_caller_metadata() {
        let ingest_id = Uuid::new_v4();
        let metadata = build_first_message_metadata(None, ingest_id);
        let value: serde_json::Value = serde_json::from_slice(&metadata).unwrap();

        assert_eq!(value["ingest_id"], ingest_id.to_string());
    }
}
