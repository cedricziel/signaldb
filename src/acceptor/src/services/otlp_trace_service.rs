use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{async_trait, Request, Response, Status};

use crate::handler::otlp_grpc::handle_grpc_otlp_traces;

pub struct TraceAcceptorService;

#[async_trait]
impl TraceService for TraceAcceptorService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        println!("Got a request: {:?}", request);

        handle_grpc_otlp_traces(request);

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}
