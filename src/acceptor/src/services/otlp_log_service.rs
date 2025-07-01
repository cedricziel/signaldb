use std::sync::Arc;
// Flight protocol imports
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt};

use common::flight::conversion::otlp_logs_to_arrow;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tonic::{async_trait, Request, Response, Status};

pub struct LogAcceptorService {
    flight_transport: Arc<InMemoryFlightTransport>,
}

impl LogAcceptorService {
    pub fn new(flight_transport: Arc<InMemoryFlightTransport>) -> Self {
        Self { flight_transport }
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

        // Get a Flight client for a writer service with trace ingestion capability
        let mut client = match self
            .flight_transport
            .get_client_for_capability(ServiceCapability::TraceIngestion)
            .await
        {
            Ok(client) => client,
            Err(e) => {
                log::error!("Failed to get Flight client for log ingestion: {e}");
                return Err(Status::internal("No available writer service"));
            }
        };

        let schema = record_batch.schema();
        let flight_data = batches_to_flight_data(&schema, vec![record_batch]).map_err(|e| {
            log::error!("Failed to convert batch to flight data: {e}");
            Status::internal("Failed to convert to flight data")
        })?;

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

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
