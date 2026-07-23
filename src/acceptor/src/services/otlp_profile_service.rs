use opentelemetry_proto::tonic::collector::profiles::v1development::{
    ExportProfilesServiceRequest, ExportProfilesServiceResponse,
    profiles_service_server::ProfilesService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_profiles_handler::ProfileHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;
use common::ratelimit::TenantRateLimiter;
use prost::Message;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait ProfileHandlerTrait {
    async fn handle_grpc_otlp_profiles(
        &self,
        tenant_context: &TenantContext,
        request: ExportProfilesServiceRequest,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl ProfileHandlerTrait for ProfileHandler {
    async fn handle_grpc_otlp_profiles(
        &self,
        tenant_context: &TenantContext,
        request: ExportProfilesServiceRequest,
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_profiles(tenant_context, request)
            .await
    }
}

pub struct ProfileAcceptorService<H: ProfileHandlerTrait> {
    handler: H,
    rate_limiter: Option<Arc<TenantRateLimiter>>,
}

impl<H: ProfileHandlerTrait> ProfileAcceptorService<H> {
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
impl<H: ProfileHandlerTrait + Send + Sync + 'static> ProfilesService for ProfileAcceptorService<H> {
    async fn export(
        &self,
        request: Request<ExportProfilesServiceRequest>,
    ) -> Result<Response<ExportProfilesServiceResponse>, Status> {
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

        let profile_count: u64 = request_inner
            .resource_profiles
            .iter()
            .flat_map(|rp| rp.scope_profiles.iter())
            .map(|sp| sp.profiles.len() as u64)
            .sum();
        let rpc_start = std::time::Instant::now();

        // Anti-loop guard: processing the _system tenant's own telemetry must
        // not generate more self-monitoring telemetry.
        let handle = self
            .handler
            .handle_grpc_otlp_profiles(&tenant_context, request_inner);
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
            tracing::error!(error = %e, "Failed to durably accept profiles export");
            return Err(Status::unavailable(format!(
                "failed to durably accept profiles export: {e:#}"
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
                    "opentelemetry.proto.collector.profiles.v1development.ProfilesService",
                ),
                opentelemetry::KeyValue::new("rpc.method", "Export"),
            ],
        );
        app_metrics.ingest_profiles_received.add(
            profile_count,
            &[opentelemetry::KeyValue::new(
                "tenant_id",
                tenant_context.tenant_id.clone(),
            )],
        );

        Ok(Response::new(ExportProfilesServiceResponse::default()))
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl ProfileHandlerTrait for crate::handler::otlp_profiles_handler::MockProfileHandler {
    async fn handle_grpc_otlp_profiles(
        &self,
        tenant_context: &TenantContext,
        request: ExportProfilesServiceRequest,
    ) -> anyhow::Result<()> {
        self.handle_grpc_otlp_profiles(tenant_context, request)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::otlp_profiles_handler::MockProfileHandler;
    use opentelemetry_proto::tonic::profiles::v1development::{
        Profile, ResourceProfiles, ScopeProfiles,
    };

    #[tokio::test]
    async fn test_profile_acceptor_service() {
        let mut mock_handler = MockProfileHandler::new();
        mock_handler.expect_handle_grpc_otlp_profiles();

        let service = ProfileAcceptorService::new(mock_handler);

        let request = ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                resource: None,
                scope_profiles: vec![ScopeProfiles {
                    scope: None,
                    profiles: vec![Profile {
                        time_unix_nano: 1234567890,
                        profile_id: vec![1; 16],
                        ..Profile::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
            dictionary: None,
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
