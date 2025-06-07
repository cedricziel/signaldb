use std::sync::Arc;
// Flight protocol imports
use arrow_flight::client::FlightClient;
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt};

use common::flight::conversion::otlp_metrics_to_arrow;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
};
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

pub struct MetricsAcceptorService {
    flight_client: Arc<Mutex<FlightClient>>,
}

impl MetricsAcceptorService {
    pub fn new(flight_client: Arc<Mutex<FlightClient>>) -> Self {
        Self { flight_client }
    }
}

#[async_trait]
impl MetricsService for MetricsAcceptorService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        log::info!("Got a metrics request: {:?}", request);

        let req = request.into_inner();

        // Convert OTLP to Arrow RecordBatch
        let record_batch = otlp_metrics_to_arrow(&req);

        // Forward via Flight protocol to writer
        if let Ok(mut client) = self.flight_client.try_lock() {
            let schema = record_batch.schema();
            let flight_data = batches_to_flight_data(&schema, vec![record_batch]).map_err(|e| {
                log::error!("Failed to convert batch to flight data: {e}");
                Status::internal("Failed to convert to flight data")
            })?;

            let flight_stream = stream::iter(flight_data.into_iter().map(Ok));

            match client.do_put(flight_stream).await {
                Ok(mut response_stream) => {
                    while let Some(response) = response_stream.next().await {
                        match response {
                            Ok(put_result) => {
                                log::debug!("Flight put response: {put_result:?}");
                            }
                            Err(e) => {
                                log::error!("Flight put error: {e}");
                                return Err(Status::internal("Flight forwarding failed"));
                            }
                        }
                    }
                    log::debug!("Successfully forwarded metrics via Flight protocol");
                }
                Err(e) => {
                    log::error!("Failed to forward metrics via Flight protocol: {e}");
                    return Err(Status::internal("Flight forwarding failed"));
                }
            }
        } else {
            log::error!("Failed to acquire Flight client lock");
            return Err(Status::internal("Flight client unavailable"));
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
