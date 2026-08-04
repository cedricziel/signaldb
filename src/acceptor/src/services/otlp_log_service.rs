use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_log_handler::LogHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;
use common::ratelimit::TenantRateLimiter;
use common::storage_usage::StorageUsageTracker;
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
    storage_quota: Option<Arc<StorageUsageTracker>>,
}

impl<H: LogHandlerTrait> LogAcceptorService<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            rate_limiter: None,
            storage_quota: None,
        }
    }

    /// Enforce per-tenant ingest rate limits on this service.
    pub fn with_rate_limiter(mut self, rate_limiter: Arc<TenantRateLimiter>) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Enforce per-tenant storage quotas on this service.
    pub fn with_storage_quota(mut self, storage_quota: Arc<StorageUsageTracker>) -> Self {
        self.storage_quota = Some(storage_quota);
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
        if !tenant_context.can_ingest("logs") {
            return Err(Status::permission_denied(
                "API key requires scope 'logs:write'",
            ));
        }

        let request_inner = request.into_inner();

        // Per-tenant ingest rate limiting: RESOURCE_EXHAUSTED is the gRPC
        // analog of HTTP 429 and OTLP clients treat it as retryable.
        if let Some(limiter) = &self.rate_limiter {
            limiter
                .check_ingest(&tenant_context.tenant_id, request_inner.encoded_len())
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }

        // Per-tenant storage quota: a tenant at or over max_storage_bytes
        // must free space (or get a raised quota) before ingesting more.
        if let Some(quota) = &self.storage_quota {
            quota
                .check_ingest(&tenant_context.tenant_id)
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

    /// Handler that always fails before WAL durability, e.g. when the
    /// OTLP -> Arrow conversion errors (issue #926) or the WAL write fails.
    struct FailingLogHandler;

    #[async_trait::async_trait]
    impl LogHandlerTrait for FailingLogHandler {
        async fn handle_grpc_otlp_logs(
            &self,
            _tenant_context: &TenantContext,
            _request: ExportLogsServiceRequest,
        ) -> anyhow::Result<()> {
            anyhow::bail!("WAL unavailable")
        }
    }

    /// Handler that always succeeds and counts invocations: rejection tests
    /// assert the guard fires BEFORE the handler, not merely that the
    /// endpoint returns the right status while the export is still accepted.
    struct RecordingLogHandler {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RecordingLogHandler {
        fn new() -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            (
                Self {
                    calls: calls.clone(),
                },
                calls,
            )
        }
    }

    #[async_trait::async_trait]
    impl LogHandlerTrait for RecordingLogHandler {
        async fn handle_grpc_otlp_logs(
            &self,
            _tenant_context: &TenantContext,
            _request: ExportLogsServiceRequest,
        ) -> anyhow::Result<()> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn test_tenant_context() -> TenantContext {
        TenantContext {
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "test-dataset".to_string(),
            api_key_name: Some("test-key".to_string()),
            api_key_scopes: None,
            api_key_dataset_id: None,
            user_id: None,
            role: None,
            is_instance_admin: false,
            session_id: None,
            source: common::auth::TenantSource::Config,
        }
    }

    fn sample_logs_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
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
        }
    }

    #[tokio::test]
    async fn export_forwards_logs_to_handler_and_succeeds() {
        let mock_handler = MockLogHandler::new();
        let service = LogAcceptorService::new(mock_handler);

        let request = sample_logs_request();
        let mut tonic_request = Request::new(request);
        tonic_request.extensions_mut().insert(test_tenant_context());

        let response = service
            .export(tonic_request)
            .await
            .expect("export of valid logs must succeed");
        assert_eq!(response.into_inner(), ExportLogsServiceResponse::default());

        let calls = service.handler.handle_grpc_otlp_logs_calls.lock().await;
        assert_eq!(calls.len(), 1, "handler must be invoked exactly once");
        assert_eq!(
            calls[0].resource_logs[0].scope_logs[0].log_records[0].body,
            Some(AnyValue {
                value: Some(Value::StringValue("test log message".to_string())),
            }),
            "handler must receive the exact request the client sent"
        );
    }

    #[tokio::test]
    async fn export_rejects_with_unavailable_when_write_path_fails() {
        let service = LogAcceptorService::new(FailingLogHandler);

        let mut tonic_request = Request::new(ExportLogsServiceRequest::default());
        tonic_request.extensions_mut().insert(test_tenant_context());

        let status = service
            .export(tonic_request)
            .await
            .expect_err("export must fail when data is not durably accepted");

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("durably accept"));
    }

    #[tokio::test]
    async fn export_rejects_with_resource_exhausted_when_rate_limited() {
        use common::config::{AuthConfig, TenantLimits};

        // One request per second: the first export passes, the second is
        // rejected with the gRPC analog of HTTP 429.
        let limiter = Arc::new(TenantRateLimiter::from_auth_config(&AuthConfig {
            default_limits: TenantLimits {
                max_ingest_requests_per_sec: Some(1),
                burst_seconds: 1.0,
                ..Default::default()
            },
            ..Default::default()
        }));
        let (handler, handler_calls) = RecordingLogHandler::new();
        let service = LogAcceptorService::new(handler).with_rate_limiter(limiter);

        let mut first = Request::new(ExportLogsServiceRequest::default());
        first.extensions_mut().insert(test_tenant_context());
        service
            .export(first)
            .await
            .expect("first request is within budget");

        let mut second = Request::new(ExportLogsServiceRequest::default());
        second.extensions_mut().insert(test_tenant_context());
        let status = service
            .export(second)
            .await
            .expect_err("second request must exceed the 1 rps budget");

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("request rate"));
        assert_eq!(
            handler_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "rate-limited export must be rejected before reaching the handler"
        );
    }

    #[tokio::test]
    async fn export_rejects_with_resource_exhausted_when_over_storage_quota() {
        use common::config::{AuthConfig, TenantLimits};
        use common::storage_usage::StorageUsageTracker;
        use std::collections::HashMap;

        let tenant_id = test_tenant_context().tenant_id;
        let tracker = Arc::new(StorageUsageTracker::from_auth_config(&AuthConfig {
            default_limits: TenantLimits {
                max_storage_bytes: Some(1_000),
                ..Default::default()
            },
            ..Default::default()
        }));
        let (handler, handler_calls) = RecordingLogHandler::new();
        let service = LogAcceptorService::new(handler).with_storage_quota(tracker.clone());

        // Under quota: export passes.
        tracker.replace_all(HashMap::from([(tenant_id.clone(), 999)]));
        let mut under = Request::new(ExportLogsServiceRequest::default());
        under.extensions_mut().insert(test_tenant_context());
        service
            .export(under)
            .await
            .expect("export under quota must pass");

        // Usage refresh reports the tenant at its cap: export is rejected.
        tracker.replace_all(HashMap::from([(tenant_id, 1_000)]));
        let mut over = Request::new(ExportLogsServiceRequest::default());
        over.extensions_mut().insert(test_tenant_context());
        let status = service
            .export(over)
            .await
            .expect_err("export at quota must be rejected");

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("quota_exceeded"));
        assert_eq!(
            handler_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "over-quota export must be rejected before reaching the handler"
        );
    }
}
