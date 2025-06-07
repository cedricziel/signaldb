use std::sync::Arc;

use common::flight::conversion::otlp_traces_to_arrow;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tokio::sync::Mutex;
// Flight protocol imports
use arrow_flight::client::FlightClient;
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt};

#[derive(Debug)]
pub struct TraceHandler {
    /// Flight client for forwarding telemetry
    flight_client: Arc<Mutex<FlightClient>>,
}

#[cfg(test)]
pub struct MockTraceHandler {
    pub handle_grpc_otlp_traces_calls: tokio::sync::Mutex<Vec<ExportTraceServiceRequest>>,
}

#[cfg(test)]
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
    /// Create a new handler with Flight client
    pub fn new(flight_client: Arc<Mutex<FlightClient>>) -> Self {
        Self { flight_client }
    }

    pub async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        log::info!("Handling OTLP trace request");

        // Convert OTLP traces to Arrow RecordBatch
        let record_batch = otlp_traces_to_arrow(&request);

        // Forward via Flight protocol to writer
        if let Ok(mut client) = self.flight_client.try_lock() {
            let schema = record_batch.schema();
            let flight_data = match batches_to_flight_data(&schema, vec![record_batch]) {
                Ok(data) => data,
                Err(e) => {
                    log::error!("Failed to convert batch to flight data: {e}");
                    return;
                }
            };

            let flight_stream = stream::iter(flight_data.into_iter().map(Ok));

            match client.do_put(flight_stream).await {
                Ok(mut response_stream) => {
                    while let Some(response) = response_stream.next().await {
                        match response {
                            Ok(put_result) => {
                                log::debug!("Flight put response: {:?}", put_result);
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
        } else {
            log::error!("Failed to acquire Flight client lock");
        }
    }
}
