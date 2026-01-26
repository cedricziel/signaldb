use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_log_handler::LogHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;

#[async_trait::async_trait]
pub trait LogHandlerTrait {
    async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    );
}

#[async_trait::async_trait]
impl LogHandlerTrait for LogHandler {
    async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) {
        self.handle_grpc_otlp_logs(tenant_context, request).await;
    }
}

pub struct LogAcceptorService<H: LogHandlerTrait> {
    handler: H,
}

impl<H: LogHandlerTrait> LogAcceptorService<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl<H: LogHandlerTrait + Send + Sync + 'static> LogsService for LogAcceptorService<H> {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        // Extract tenant context from request extensions (added by auth middleware)
        let tenant_context = get_tenant_context(&request)?;

        let request_inner = request.into_inner();

        self.handler
            .handle_grpc_otlp_logs(&tenant_context, request_inner)
            .await;

        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl LogHandlerTrait for crate::handler::otlp_log_handler::MockLogHandler {
    async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) {
        self.handle_grpc_otlp_logs(tenant_context, request).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::otlp_log_handler::MockLogHandler;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    #[tokio::test]
    async fn test_log_acceptor_service() {
        let mut mock_handler = MockLogHandler::new();
        mock_handler.expect_handle_grpc_otlp_logs();

        let service = LogAcceptorService::new(mock_handler);

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
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
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1234567890,
                        observed_time_unix_nano: 1234567890,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("test log message".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: "".to_string(),
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
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "test-dataset".to_string(),
            api_key_name: Some("test-key".to_string()),
            source: common::auth::TenantSource::Config,
        });

        let response = service.export(tonic_request).await;

        assert!(response.is_ok());
    }
}
