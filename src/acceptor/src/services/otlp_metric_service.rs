use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_metrics_handler::MetricsHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;
use common::ratelimit::TenantRateLimiter;
use common::storage_usage::StorageUsageTracker;
use prost::Message;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait MetricsHandlerTrait {
    async fn handle_grpc_otlp_metrics(
        &self,
        tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl MetricsHandlerTrait for MetricsHandler {
    async fn handle_grpc_otlp_metrics(
        &self,
        tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_metrics(tenant_context, request).await
    }
}

pub struct MetricsAcceptorService<H: MetricsHandlerTrait> {
    handler: H,
    rate_limiter: Option<Arc<TenantRateLimiter>>,
    storage_quota: Option<Arc<StorageUsageTracker>>,
}

impl<H: MetricsHandlerTrait> MetricsAcceptorService<H> {
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
impl<H: MetricsHandlerTrait + Send + Sync + 'static> MetricsService for MetricsAcceptorService<H> {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
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

        let metric_count: u64 = request_inner
            .resource_metrics
            .iter()
            .flat_map(|rm| rm.scope_metrics.iter())
            .map(|sm| sm.metrics.len() as u64)
            .sum();
        let rpc_start = std::time::Instant::now();

        // Anti-loop guard: processing the _system tenant's own telemetry must
        // not generate more self-monitoring telemetry.
        let handle = self
            .handler
            .handle_grpc_otlp_metrics(&tenant_context, request_inner);
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
            tracing::error!(error = %e, "Failed to durably accept metrics export");
            return Err(Status::unavailable(format!(
                "failed to durably accept metrics export: {e:#}"
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
                    "opentelemetry.proto.collector.metrics.v1.MetricsService",
                ),
                opentelemetry::KeyValue::new("rpc.method", "Export"),
            ],
        );
        app_metrics.ingest_metrics_received.add(
            metric_count,
            &[opentelemetry::KeyValue::new(
                "tenant_id",
                tenant_context.tenant_id.clone(),
            )],
        );

        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl MetricsHandlerTrait for crate::handler::otlp_metrics_handler::MockMetricsHandler {
    async fn handle_grpc_otlp_metrics(
        &self,
        tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_metrics(tenant_context, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::otlp_metrics_handler::MockMetricsHandler;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data,
            number_data_point,
        },
        resource::v1::Resource,
    };

    #[tokio::test]
    async fn test_metrics_acceptor_service() {
        let mut mock_handler = MockMetricsHandler::new();
        mock_handler.expect_handle_grpc_otlp_metrics();

        let service = MetricsAcceptorService::new(mock_handler);

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
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
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "test_metric".to_string(),
                        description: "A test metric".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1234567890,
                                time_unix_nano: 1234567891,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                        metadata: vec![],
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
