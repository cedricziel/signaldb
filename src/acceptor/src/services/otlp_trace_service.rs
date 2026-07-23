use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_grpc::TraceHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;
use common::ratelimit::TenantRateLimiter;
use common::storage_usage::StorageUsageTracker;
use prost::Message;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait TraceHandlerTrait {
    async fn handle_grpc_otlp_traces(
        &self,
        tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl TraceHandlerTrait for TraceHandler {
    async fn handle_grpc_otlp_traces(
        &self,
        tenant_context: &TenantContext,
        request: ExportTraceServiceRequest,
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_traces(tenant_context, request).await
    }
}

pub struct TraceAcceptorService<H: TraceHandlerTrait> {
    handler: H,
    rate_limiter: Option<Arc<TenantRateLimiter>>,
    storage_quota: Option<Arc<StorageUsageTracker>>,
}

impl<H: TraceHandlerTrait> TraceAcceptorService<H> {
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
impl<H: TraceHandlerTrait + Send + Sync + 'static> TraceService for TraceAcceptorService<H> {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
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

        // Per-tenant storage quota: a tenant at or over max_storage_bytes
        // must free space (or get a raised quota) before ingesting more.
        if let Some(quota) = &self.storage_quota {
            quota
                .check_ingest(&tenant_context.tenant_id)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?;
        }

        let span_count: u64 = request_inner
            .resource_spans
            .iter()
            .flat_map(|rs| rs.scope_spans.iter())
            .map(|ss| ss.spans.len() as u64)
            .sum();
        let rpc_start = std::time::Instant::now();

        // Anti-loop guard: processing the _system tenant's own telemetry must
        // not generate more self-monitoring telemetry.
        let handle = self
            .handler
            .handle_grpc_otlp_traces(&tenant_context, request_inner);
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
            tracing::error!(error = %e, "Failed to durably accept trace export");
            return Err(Status::unavailable(format!(
                "failed to durably accept trace export: {e:#}"
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
                    "opentelemetry.proto.collector.trace.v1.TraceService",
                ),
                opentelemetry::KeyValue::new("rpc.method", "Export"),
            ],
        );
        app_metrics.ingest_spans_received.add(
            span_count,
            &[opentelemetry::KeyValue::new(
                "tenant_id",
                tenant_context.tenant_id.clone(),
            )],
        );

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
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_traces(tenant_context, request).await
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

    /// Handler that always fails before WAL durability
    struct FailingTraceHandler;

    #[async_trait::async_trait]
    impl TraceHandlerTrait for FailingTraceHandler {
        async fn handle_grpc_otlp_traces(
            &self,
            _tenant_context: &TenantContext,
            _request: ExportTraceServiceRequest,
        ) -> anyhow::Result<()> {
            anyhow::bail!("WAL unavailable")
        }
    }

    fn test_tenant_context() -> TenantContext {
        TenantContext {
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "test-dataset".to_string(),
            api_key_name: Some("test-key".to_string()),
            source: common::auth::TenantSource::Config,
        }
    }

    #[tokio::test]
    async fn export_rejects_with_unavailable_when_write_path_fails() {
        let service = TraceAcceptorService::new(FailingTraceHandler);

        let mut tonic_request = Request::new(ExportTraceServiceRequest::default());
        tonic_request.extensions_mut().insert(test_tenant_context());

        let status = service
            .export(tonic_request)
            .await
            .expect_err("export must fail when data is not durably accepted");

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("durably accept"));
    }

    /// Handler that always succeeds (rate-limit tests must fail before it).
    struct NoopTraceHandler;

    #[async_trait::async_trait]
    impl TraceHandlerTrait for NoopTraceHandler {
        async fn handle_grpc_otlp_traces(
            &self,
            _tenant_context: &TenantContext,
            _request: ExportTraceServiceRequest,
        ) -> anyhow::Result<()> {
            Ok(())
        }
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
        let service = TraceAcceptorService::new(NoopTraceHandler).with_rate_limiter(limiter);

        let mut first = Request::new(ExportTraceServiceRequest::default());
        first.extensions_mut().insert(test_tenant_context());
        service
            .export(first)
            .await
            .expect("first request is within budget");

        let mut second = Request::new(ExportTraceServiceRequest::default());
        second.extensions_mut().insert(test_tenant_context());
        let status = service
            .export(second)
            .await
            .expect_err("second request must exceed the 1 rps budget");

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("request rate"));
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
        let service =
            TraceAcceptorService::new(NoopTraceHandler).with_storage_quota(tracker.clone());

        // Under quota: export passes.
        tracker.replace_all(HashMap::from([(tenant_id.clone(), 999)]));
        let mut under = Request::new(ExportTraceServiceRequest::default());
        under.extensions_mut().insert(test_tenant_context());
        service
            .export(under)
            .await
            .expect("export under quota must pass");

        // Usage refresh reports the tenant at its cap: export is rejected.
        tracker.replace_all(HashMap::from([(tenant_id, 1_000)]));
        let mut over = Request::new(ExportTraceServiceRequest::default());
        over.extensions_mut().insert(test_tenant_context());
        let status = service
            .export(over)
            .await
            .expect_err("export at quota must be rejected");

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert!(status.message().contains("quota_exceeded"));
    }

    #[tokio::test]
    async fn test_trace_acceptor_service() {
        let mut mock_handler = MockTraceHandler::new();
        mock_handler.expect_handle_grpc_otlp_traces();

        let service = TraceAcceptorService::new(mock_handler);

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
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
            tenant_slug: "test-tenant".to_string(),
            dataset_slug: "test-dataset".to_string(),
            api_key_name: Some("test-key".to_string()),
            source: common::auth::TenantSource::Config,
        });

        let response = service.export(tonic_request).await;

        assert!(response.is_ok());
    }
}
