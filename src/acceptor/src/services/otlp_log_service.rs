use std::sync::Arc;
// Flight protocol imports
use arrow_flight::client::FlightClient;
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt};

use common::flight::conversion::otlp_logs_to_arrow;
use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

pub struct LogAcceptorService {
    flight_client: Arc<Mutex<FlightClient>>,
}

impl LogAcceptorService {
    pub fn new(flight_client: Arc<Mutex<FlightClient>>) -> Self {
        Self { flight_client }
    }
}

#[async_trait]
impl LogsService for LogAcceptorService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        log::info!("Got a logs request: {:?}", request);

        let req = request.into_inner();

        // Convert OTLP to Arrow RecordBatch
        let record_batch = otlp_logs_to_arrow(&req);

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
                                log::debug!("Flight put response: {:?}", put_result);
                            }
                            Err(e) => {
                                log::error!("Flight put error: {e}");
                                return Err(Status::internal("Flight forwarding failed"));
                            }
                        }
                    }
                    log::debug!("Successfully forwarded logs via Flight protocol");
                }
                Err(e) => {
                    log::error!("Failed to forward logs via Flight protocol: {e}");
                    return Err(Status::internal("Flight forwarding failed"));
                }
            }
        } else {
            log::error!("Failed to acquire Flight client lock");
            return Err(Status::internal("Flight client unavailable"));
        }

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
