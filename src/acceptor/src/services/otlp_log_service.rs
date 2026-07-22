use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_log_handler::LogHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;
use common::ratelimit::TenantRateLimiter;
use prost::Message;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait LogHandlerTrait {
    async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl LogHandlerTrait for LogHandler {
    async fn handle_grpc_otlp_logs(
        &self,
        tenant_context: &TenantContext,
        request: ExportLogsServiceRequest,
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_logs(tenant_context, request).await
    }
}

pub struct LogAcceptorService<H: LogHandlerTrait> {
    handler: H,
    rate_limiter: Option<Arc<TenantRateLimiter>>,
}

impl<H: LogHandlerTrait> LogAcceptorService<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            rate_limiter: None,
        }
    }

    /// Enforce per-tenant ingest rate limits on this service.
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<TenantRateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
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

        // Per-tenant ingest rate limiting: RESOURCE_EXHAUSTED is the gRPC
        // analog of HTTP 429 and OTLP clients treat it as retryable.
        if let Some(limiter) = &self.rate_limiter {
            limiter
                .check_ingest(&tenant_context.tenant_id, request_inner.encoded_len())
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }

        let log_count: u64 = request_inner
            .resource_logs
            .iter()
            .flat_map(|rl| rl.scope_logs.iter())
            .map(|sl| sl.log_records.len() as u64)
            .sum();
        let rpc_start = std::time::Instant::now();

        // Anti-loop guard: processing the _system tenant's own telemetry must
        // not generate more self-monitoring telemetry.
        let handle = self
            .handler
            .handle_grpc_otlp_logs(&tenant_context, request_inner);
        let result =
            if common::self_monitoring::is_self_monitoring_tenant(&tenant_context.tenant_id) {
                common::self_monitoring::suppress_self_telemetry(handle).await
            } else {
                handle.await
            };

        // Reject the export if the data was not durably accepted, so the
        // client retries instead of dropping its copy (OTLP treats
        // UNAVAILABLE as retryable).
        if let Err(e) = result {
            tracing::error!(error = %e, "Failed to durably accept logs export");
            return Err(Status::unavailable(format!(
                "failed to durably accept logs export: {e:#}"
            )));
        }

        // Anti-loop guard: _system traffic is SignalDB's own telemetry and
        // must not be measured (would feed back into the export pipeline).
        if !common::self_monitoring::should_count_tenant(&tenant_context.tenant_id) {
            return Ok(Response::new(Default::default()));
        }
        let app_metrics = common::self_monitoring::app_metrics();
        app_metrics.rpc_server_duration.record(
            rpc_start.elapsed().as_secs_f64() * 1000.0,
            &[
                opentelemetry::KeyValue::new("rpc.system", "grpc"),
                opentelemetry::KeyValue::new(
                    "rpc.service",
                    "opentelemetry.proto.collector.logs.v1.LogsService",
                ),
                opentelemetry::KeyValue::new("rpc.method", "Export"),
            ],
        );
        app_metrics.ingest_logs_received.add(
            log_count,
            &[opentelemetry::KeyValue::new(
                "tenant_id",
                tenant_context.tenant_id.clone(),
            )],
        );

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
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_logs(tenant_context, request).await
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
                        key_strindex: 0,
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
