use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use tonic::{Request, Response, Status};

use crate::handler::otlp_metrics_handler::MetricsHandler;
use crate::middleware::get_tenant_context;
use common::auth::TenantContext;

#[async_trait::async_trait]
pub trait MetricsHandlerTrait {
    async fn handle_grpc_otlp_metrics(
        &self,
        tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    );
}

#[async_trait::async_trait]
impl MetricsHandlerTrait for MetricsHandler {
    async fn handle_grpc_otlp_metrics(
        &self,
        tenant_context: &TenantContext,
        request: ExportMetricsServiceRequest,
    ) {
        self.handle_grpc_otlp_metrics(tenant_context, request).await;
    }
}

pub struct MetricsAcceptorService<H: MetricsHandlerTrait> {
    handler: H,
}

impl<H: MetricsHandlerTrait> MetricsAcceptorService<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
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

        self.handler
            .handle_grpc_otlp_metrics(&tenant_context, request_inner)
            .await;

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
    ) {
        self.handle_grpc_otlp_metrics(tenant_context, request).await;
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
