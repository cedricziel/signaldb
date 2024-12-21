use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_grpc::TraceHandler;

#[async_trait::async_trait]
pub trait TraceHandlerTrait {
    async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest);
}

#[async_trait::async_trait]
impl TraceHandlerTrait for TraceHandler {
    async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
        self.handle_grpc_otlp_traces(request).await;
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
        let request = request.into_inner();

        self.handler
            .handle_grpc_otlp_traces(request)
            .await;

        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::otlp_grpc::MockTraceHandler;
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, KeyValue},
        resource::v1::Resource,
        trace::v1::{
            span::SpanKind, status::StatusCode, ResourceSpans, ScopeSpans, Span,
            Status as SpanStatus,
        },
    };

    #[async_trait::async_trait]
    impl TraceHandlerTrait for MockTraceHandler {
        async fn handle_grpc_otlp_traces(&self, request: ExportTraceServiceRequest) {
            self.handle_grpc_otlp_traces(request).await;
        }
    }

    #[tokio::test]
    async fn test_trace_service_export() {
        let mock_handler = MockTraceHandler::new();
        let service = TraceAcceptorService::new(mock_handler);

        let request = tonic::Request::new(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: String::new(),
                        parent_span_id: vec![0; 8],
                        name: "test-span".to_string(),
                        kind: SpanKind::Internal as i32,
                        start_time_unix_nano: 1703163191000000000,
                        end_time_unix_nano: 1703163192000000000,
                        attributes: vec![KeyValue {
                            key: "test.attribute".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("test-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(SpanStatus {
                            code: StatusCode::Ok as i32,
                            message: String::new(),
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        });

        let response = service.export(request).await.unwrap();
        assert_eq!(
            response.into_inner(),
            ExportTraceServiceResponse::default()
        );
        assert_eq!(service.handler.handle_grpc_otlp_traces_calls.lock().unwrap().len(), 1);
    }
}
