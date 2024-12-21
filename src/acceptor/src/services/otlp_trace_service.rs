use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{async_trait, Request, Response, Status};

use crate::handler::otlp_grpc::TraceHandling;

pub struct TraceAcceptorService<T: TraceHandling> {
    handler: T,
}

impl<T: TraceHandling> TraceAcceptorService<T> {
    pub fn new(handler: T) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<T: TraceHandling + Send + Sync + 'static> TraceService for TraceAcceptorService<T> {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        log::info!("Got a request: {:?}", request);

        self.handler
            .handle_grpc_otlp_traces(request.into_inner())
            .await;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::otlp_grpc::MockTraceHandling;
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, KeyValue},
        resource::v1::Resource,
        trace::v1::{
            span::SpanKind, status::StatusCode, ResourceSpans, ScopeSpans, Span,
            Status as SpanStatus,
        },
    };

    #[tokio::test]
    async fn test_trace_service_export() {
        let mut mock_handler = MockTraceHandling::new();
        mock_handler
            .expect_handle_grpc_otlp_traces()
            .times(1)
            .return_const(());

        let service = TraceAcceptorService::new(mock_handler);

        // Create a test request with a single span
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
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16], // 16-byte trace ID
                        span_id: vec![2; 8],   // 8-byte span ID
                        trace_state: String::new(),
                        parent_span_id: vec![],
                        name: "test-span".to_string(),
                        kind: SpanKind::Server as i32,
                        start_time_unix_nano: 1703163191000000000, // 2024-12-21 13:53:11 UTC
                        end_time_unix_nano: 1703163192000000000,   // One second later
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
        };

        // Call the export method
        let result = service
            .export(Request::new(request))
            .await
            .expect("Export should succeed");

        // Verify the response
        assert!(result.get_ref().partial_success.is_none());
    }
}
