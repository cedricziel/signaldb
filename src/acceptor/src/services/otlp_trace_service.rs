use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_grpc::TraceHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;

#[async_trait::async_trait]
pub trait TraceHandlerTrait {
    async fn handle_grpc_otlp_traces(
        &self,
        tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    );
}

#[async_trait::async_trait]
impl TraceHandlerTrait for TraceHandler {
    async fn handle_grpc_otlp_traces(
        &self,
        tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    ) {
        self.handle_grpc_otlp_traces(tenant_context, request).await;
    }
}

pub struct TraceAcceptorService<H: TraceHandlerTrait> {
    handler: H,
}

impl<H: TraceHandlerTrait> TraceAcceptorService<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl<H: TraceHandlerTrait + Send + Sync + 'static> TraceService for TraceAcceptorService<H> {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        // Extract tenant context from request extensions (added by auth middleware)
        let tenant_context = get_tenant_context(&request)?;

        let request_inner = request.into_inner();

        self.handler
            .handle_grpc_otlp_traces(&tenant_context, request_inner)
            .await;

        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl TraceHandlerTrait for crate::handler::otlp_grpc::MockTraceHandler {
    async fn handle_grpc_otlp_traces(
        &self,
        tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    ) {
        self.handle_grpc_otlp_traces(tenant_context, request).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::otlp_grpc::MockTraceHandler;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status as SpanStatus, span::SpanKind},
    };

    #[tokio::test]
    async fn test_trace_acceptor_service() {
        let mut mock_handler = MockTraceHandler::new();
        mock_handler.expect_handle_grpc_otlp_traces();

        let service = TraceAcceptorService::new(mock_handler);

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: b"1234567890123456".to_vec(),
                        span_id: b"12345678".to_vec(),
                        trace_state: "".to_string(),
                        parent_span_id: b"".to_vec(),
                        name: "test-span".to_string(),
                        kind: SpanKind::Server as i32,
                        start_time_unix_nano: 1234567890,
                        end_time_unix_nano: 1234567891,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(SpanStatus {
                            code: 1,
                            message: "OK".to_string(),
                        }),
                        flags: 0,
                    }],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };

        // Add TenantContext to request extensions (normally added by auth middleware)
        let mut tonic_request = Request::new(request);
        tonic_request.extensions_mut().insert(TenantContext {
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            api_key_name: Some("test-key".to_string()),
            source: common::auth::TenantSource::Config,
        });

        let response = service.export(tonic_request).await;

        assert!(response.is_ok());
    }
}
