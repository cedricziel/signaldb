use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
};
use tonic::{async_trait, Request, Response, Status};

pub struct MetricsAcceptorService;
#[async_trait]
impl MetricsService for MetricsAcceptorService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        println!("Got a request: {:?}", request);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
