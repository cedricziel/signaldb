use std::sync::Arc;

use common::flight::conversion::otlp_traces_to_arrow;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
// Flight protocol imports
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt};

pub struct TraceHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
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
    /// Create a new handler with Flight transport
    pub fn new(flight_transport: Arc<InMemoryFlightTransport>) -> Self {
        Self { flight_transport }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        log::info!("Handling OTLP trace request");

        // Convert OTLP traces to Arrow RecordBatch
        let record_batch = otlp_traces_to_arrow(&request);

        // Get a Flight client for a writer service with trace ingestion capability
        let mut client = match self
            .flight_transport
            .get_client_for_capability(ServiceCapability::TraceIngestion)
            .await
        {
            Ok(client) => client,
            Err(e) => {
                log::error!("Failed to get Flight client for trace ingestion: {e}");
                return;
            }
        };

        let schema = record_batch.schema();
        let flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
            Ok(data) => data,
            Err(e) => {
                log::error!("Failed to convert batch to flight data: {e}");
                return;
            }
        };

        let flight_stream = stream::iter(flight_data);

        match client.do_put(flight_stream).await {
            Ok(response) => {
                let mut response_stream = response.into_inner();
                while let Some(result) = response_stream.next().await {
                    match result {
                        Ok(put_result) => {
                            log::debug!("Flight put response: {put_result:?}");
                        }
                        Err(e) => {
                            log::error!("Flight put error: {e}");
                            return;
                        }
                    }
                }
                log::debug!("Successfully forwarded traces via Flight protocol");
            }
            Err(e) => {
                log::error!("Failed to forward traces via Flight protocol: {e}");
            }
        }
    }
}
