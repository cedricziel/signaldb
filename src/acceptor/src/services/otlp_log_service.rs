use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tonic::{async_trait, Request, Response, Status};

pub struct LogAcceptorService;
#[async_trait]
impl LogsService for LogAcceptorService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        println!("Got a request: {:?}", request);
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
