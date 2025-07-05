use std::sync::Arc;

use common::flight::conversion::otlp_traces_to_arrow;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::wal::{record_batch_to_bytes, Wal, WalOperation};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
// Flight protocol imports
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt};

pub struct TraceHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL for durability
    wal: Arc<Wal>,
}

#[cfg(any(test, feature = "testing"))]
pub struct MockTraceHandler {
    pub handle_grpc_otlp_traces_calls: tokio::sync::Mutex<Vec<ExportTraceServiceRequest>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockTraceHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockTraceHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_traces_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        self.handle_grpc_otlp_traces_calls
            .lock()
            .await
            .push(request);
    }

    pub fn expect_handle_grpc_otlp_traces(&mut self) -> &mut Self {
        self
    }
}

impl TraceHandler {
    /// Create a new handler with Flight transport and WAL
    pub fn new(flight_transport: Arc<InMemoryFlightTransport>, wal: Arc<Wal>) -> Self {
        Self {
            flight_transport,
            wal,
        }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        log::info!("Handling OTLP trace request");

        // Convert OTLP traces to Arrow RecordBatch
        let record_batch = otlp_traces_to_arrow(&request);

        // Step 1: Write to WAL first for durability
        let batch_bytes = match record_batch_to_bytes(&record_batch) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Failed to serialize record batch: {e}");
                return;
            }
        };

        let wal_entry_id = match self
            .wal
            .append(WalOperation::WriteTraces, batch_bytes.clone())
            .await
        {
            Ok(id) => id,
            Err(e) => {
                log::error!("Failed to write traces to WAL: {e}");
                return;
            }
        };

        // Flush WAL to ensure durability
        if let Err(e) = self.wal.flush().await {
            log::error!("Failed to flush WAL: {e}");
            return;
        }

        log::debug!("Traces written to WAL with entry ID: {wal_entry_id}");

        // Step 2: Forward from WAL to writer via Flight
        // Get a Flight client for a writer service with storage capability (excludes acceptor)
        let mut client = match self
            .flight_transport
            .get_client_for_capability(ServiceCapability::Storage)
            .await
        {
            Ok(client) => client,
            Err(e) => {
                log::error!("Failed to get Flight client for storage service: {e}");
                // Data remains in WAL for retry by background processor
                return;
            }
        };

        let schema = record_batch.schema();
        let flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
            Ok(data) => data,
            Err(e) => {
                log::error!("Failed to convert batch to flight data: {e}");
                // Data remains in WAL for retry
                return;
            }
        };

        let flight_stream = stream::iter(flight_data);

        match client.do_put(flight_stream).await {
            Ok(response) => {
                let mut response_stream = response.into_inner();
                let mut success = true;
                while let Some(result) = response_stream.next().await {
                    match result {
                        Ok(put_result) => {
                            log::debug!("Flight put response: {put_result:?}");
                        }
                        Err(e) => {
                            log::error!("Flight put error: {e}");
                            success = false;
                            break;
                        }
                    }
                }

                if success {
                    log::debug!("Successfully forwarded traces via Flight protocol");
                    // Mark WAL entry as processed after successful forwarding
                    if let Err(e) = self.wal.mark_processed(wal_entry_id).await {
                        log::warn!("Failed to mark WAL entry {wal_entry_id} as processed: {e}");
                    }
                } else {
                    log::error!("Failed to forward traces - data remains in WAL for retry");
                }
            }
            Err(e) => {
                log::error!("Failed to forward traces via Flight protocol: {e}");
                // Data remains in WAL for retry by background processor
            }
        }
    }
}
