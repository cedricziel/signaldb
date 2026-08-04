//! # gRPC server-span layer
//!
//! Tower layer for the OTLP gRPC stack that roots every inbound call in a
//! semconv RPC SERVER span (via [`common::self_monitoring::spans`]) joined
//! to the caller-supplied W3C trace context. Applied once around the whole
//! tonic server, so every OTLP service — and any future one — gets the
//! boundary span without per-service wiring.
//!
//! Mirrors the HTTP middleware's anti-loop guard: `_system`-tenant requests
//! (SignalDB's own telemetry export) bypass the span entirely.
//!
//! The gRPC status is read from the response's `grpc-status` header, which
//! tonic sets on immediately-failed calls; a response without it (success,
//! or a streaming call whose status arrives in trailers) is recorded as
//! `OK`. The OTLP export surface is unary, so failed exports are captured.

use std::task::{Context, Poll};

use common::self_monitoring::spans::{RpcBoundary, record_rpc_result, rpc_server_span};
use tonic::codegen::http;
use tracing::Instrument;

/// Layer applying [`GrpcTrace`] to the tonic server stack.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcTraceLayer;

impl<S> tower::Layer<S> for GrpcTraceLayer {
    type Service = GrpcTrace<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcTrace { inner }
    }
}

/// Service wrapper producing one RPC SERVER span per inbound gRPC call.
#[derive(Clone, Debug)]
pub struct GrpcTrace<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for GrpcTrace<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let is_system_request = req
            .headers()
            .get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .is_some_and(common::self_monitoring::is_self_monitoring_tenant);
        if is_system_request {
            return Box::pin(self.inner.call(req));
        }

        // gRPC request path is "/{package.Service}/{Method}"; the
        // fully-qualified logical rpc.method drops only the leading slash.
        let rpc_method = req.uri().path().trim_start_matches('/').to_owned();
        let span = rpc_server_span(&rpc_method, None);
        // Parent must be adopted before the span is first entered.
        common::flight::trace_context::set_parent_from_http_headers(&span, req.headers());

        let fut = self.inner.call(req);
        let record_span = span.clone();
        Box::pin(
            async move {
                let response = fut.await?;
                let code = response
                    .headers()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<i32>().ok())
                    .map(tonic::Code::from_i32)
                    .unwrap_or(tonic::Code::Ok);
                record_rpc_result(&record_span, RpcBoundary::Server, code);
                Ok(response)
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tower::{ServiceBuilder, ServiceExt};
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    const EXPORT_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

    /// Mock gRPC service answering with the given response headers.
    fn mock_service(
        grpc_status: Option<&'static str>,
    ) -> impl tower::Service<
        http::Request<String>,
        Response = http::Response<String>,
        Error = std::convert::Infallible,
        Future: Send + 'static,
    > + Clone {
        tower::service_fn(move |_req: http::Request<String>| async move {
            let mut builder = http::Response::builder().status(200);
            if let Some(code) = grpc_status {
                builder = builder.header("grpc-status", code);
            }
            Ok::<_, std::convert::Infallible>(builder.body(String::new()).unwrap())
        })
    }

    async fn capture(
        request: http::Request<String>,
        grpc_status: Option<&'static str>,
    ) -> Vec<SpanData> {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        async {
            let svc = ServiceBuilder::new()
                .layer(GrpcTraceLayer)
                .service(mock_service(grpc_status));
            svc.oneshot(request).await.unwrap();
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        exporter.get_finished_spans().unwrap()
    }

    fn attr(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().to_string())
    }

    #[tokio::test]
    async fn emits_rpc_server_span_joined_to_caller() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .header(
                "traceparent",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            )
            .body(String::new())
            .unwrap();
        let spans = capture(request, None).await;

        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(
            span.name,
            "opentelemetry.proto.collector.trace.v1.TraceService/Export"
        );
        assert_eq!(span.span_kind, SpanKind::Server);
        assert_eq!(attr(span, "rpc.system.name").as_deref(), Some("grpc"));
        assert_eq!(
            span.span_context.trace_id(),
            opentelemetry::trace::TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
        );
        assert_eq!(
            span.parent_span_id,
            opentelemetry::trace::SpanId::from_hex("b7ad6b7169203331").unwrap()
        );
        // No grpc-status header = success; status stays unset.
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("OK")
        );
        assert_eq!(span.status, Status::Unset);
    }

    #[tokio::test]
    async fn server_fault_marks_span_error() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .body(String::new())
            .unwrap();
        // 13 = INTERNAL
        let spans = capture(request, Some("13")).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("INTERNAL")
        );
        assert!(matches!(span.status, Status::Error { .. }));
    }

    #[tokio::test]
    async fn client_fault_leaves_server_span_unset() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .body(String::new())
            .unwrap();
        // 16 = UNAUTHENTICATED: the caller's problem, not the server's.
        let spans = capture(request, Some("16")).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("UNAUTHENTICATED")
        );
        assert_eq!(span.status, Status::Unset);
    }

    #[tokio::test]
    async fn system_tenant_bypasses_span() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .header("x-tenant-id", "_system")
            .body(String::new())
            .unwrap();
        let spans = capture(request, None).await;
        assert!(spans.is_empty(), "self-monitoring export must not re-span");
    }
}
