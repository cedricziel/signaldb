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
//! The gRPC status is read from the response's `grpc-status` header (set by
//! tonic on immediately-failed calls) or, for streaming calls, from the
//! trailer-carried `grpc-status` inspected as the response body is drained
//! (see [`TrailerStatusBody`]). Either way the SERVER span stays open until
//! the body is exhausted, so the recorded status reflects the final outcome
//! rather than just the initial headers.
//!
//! An immediately-failed unary response's body reports
//! `is_end_stream() == true` (`tonic::body::Body::empty()`), and hyper's
//! HTTP/2 server takes that as license to skip polling it altogether —
//! `poll_frame` is never called for that shape. `TrailerStatusBody`'s
//! `Drop` impl is the backstop: it records the outcome (from the header,
//! or `Cancelled` if there was none) whenever the body goes away without
//! `poll_frame` ever reaching end-of-stream on its own.

use std::pin::Pin;
use std::task::{Context, Poll};

use common::self_monitoring::spans::{RpcBoundary, record_rpc_result, rpc_server_span};
use http_body::Frame;
use tonic::codegen::http;
use tracing::{Instrument, Span};

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
    ResBody: http_body::Body + Send + Unpin + 'static,
{
    type Response = http::Response<TrailerStatusBody<ResBody>>;
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
            let fut = self.inner.call(req);
            return Box::pin(async move {
                let (parts, body) = fut.await?.into_parts();
                Ok(http::Response::from_parts(
                    parts,
                    TrailerStatusBody::passthrough(body),
                ))
            });
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
                // tonic sets `grpc-status` as a response header on
                // immediately-failed calls; a streaming/successful call
                // instead carries it in a trailers frame at the end of the
                // body, inspected by `TrailerStatusBody` below.
                let header_status = parse_grpc_status(response.headers());
                let (parts, body) = response.into_parts();
                let body = TrailerStatusBody::new(body, record_span, header_status);
                Ok(http::Response::from_parts(parts, body))
            }
            .instrument(span),
        )
    }
}

/// Parses the `grpc-status` entry out of a header or trailer map, if present.
fn parse_grpc_status(headers: &http::HeaderMap) -> Option<tonic::Code> {
    headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i32>().ok())
        .map(tonic::Code::from_i32)
}

/// Wraps a gRPC response body to record the SERVER span's outcome once the
/// body is exhausted, keeping the span open across the whole stream rather
/// than closing it as soon as headers are sent.
///
/// A trailers frame carrying `grpc-status` (the streaming/success path)
/// takes precedence; otherwise the pre-parsed response-header status
/// (the immediately-failed path) is used, defaulting to `OK`.
pub struct TrailerStatusBody<B> {
    inner: B,
    span: Option<Span>,
    header_status: Option<tonic::Code>,
}

impl<B> TrailerStatusBody<B> {
    fn new(inner: B, span: Span, header_status: Option<tonic::Code>) -> Self {
        Self {
            inner,
            span: Some(span),
            header_status,
        }
    }

    /// Wraps a body with no span to record against (the `_system`-tenant
    /// bypass path), just to keep the response type uniform.
    fn passthrough(inner: B) -> Self {
        Self {
            inner,
            span: None,
            header_status: None,
        }
    }

    fn record_and_close(&mut self, code: tonic::Code) {
        if let Some(span) = self.span.take() {
            record_rpc_result(&span, RpcBoundary::Server, code);
        }
    }
}

impl<B> http_body::Body for TrailerStatusBody<B>
where
    B: http_body::Body + Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_frame(cx);
        if let Poll::Ready(frame) = &poll {
            match frame {
                None => {
                    let code = this.header_status.take().unwrap_or(tonic::Code::Ok);
                    this.record_and_close(code);
                }
                Some(Ok(frame)) => {
                    // A streaming/successful call carries its final status
                    // in a trailers frame rather than a response header;
                    // that's authoritative over any pre-parsed header
                    // status when present.
                    if let Some(code) = frame.trailers_ref().and_then(parse_grpc_status) {
                        this.record_and_close(code);
                    }
                }
                Some(Err(_)) => {}
            }
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl<B> Drop for TrailerStatusBody<B> {
    /// Backstop for every path that ends the body without `poll_frame`
    /// ever returning `None` (the normal end-of-stream branch already
    /// records and clears `span`, so this is a no-op there):
    ///
    /// - tonic builds an immediately-failed unary response from
    ///   `tonic::body::Body::empty()`, whose `is_end_stream()` is
    ///   hard-coded `true`. Hyper's HTTP/2 server treats that as a
    ///   promise it can rely on without polling: it sends a Trailers-Only
    ///   response and drops the body outright, so `poll_frame` is never
    ///   called at all. `header_status` (parsed from the response headers
    ///   before the body was even split off) covers this — it is the
    ///   known, real outcome, not a guess.
    /// - a body-error frame we chose not to treat as final on its own
    ///   (`poll_frame`'s `Some(Err(_))` arm), followed by the caller
    ///   giving up on the stream.
    /// - a genuine mid-stream disconnect: the caller drops the body
    ///   before it ever reports end-of-stream. There is no header and no
    ///   trailer to fall back on here, so defaulting to `Ok` would
    ///   misreport a cut-off call as successful; `Cancelled` reflects
    ///   that the true outcome is unknown rather than assuming success.
    fn drop(&mut self) {
        let code = self.header_status.take().unwrap_or(tonic::Code::Cancelled);
        self.record_and_close(code);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body::Body as _;
    use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tower::{ServiceBuilder, ServiceExt};
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;

    const EXPORT_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

    /// Minimal `http_body::Body` for tests: no data, then optionally one
    /// trailers frame — enough to exercise both the header-carried and the
    /// trailer-carried `grpc-status` paths. `end_stream` lets a test mirror
    /// tonic's real `tonic::body::Body::empty()`, whose `is_end_stream()`
    /// is hard-coded `true` and which hyper never polls at all.
    struct MockBody {
        trailer: Option<http::HeaderMap>,
        end_stream: bool,
    }

    impl http_body::Body for MockBody {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.get_mut().trailer.take() {
                Some(trailers) => Poll::Ready(Some(Ok(Frame::trailers(trailers)))),
                None => Poll::Ready(None),
            }
        }

        fn is_end_stream(&self) -> bool {
            self.end_stream
        }
    }

    /// A body whose single frame is an error, then ends — for exercising
    /// `poll_frame`'s `Some(Err(_))` arm.
    struct ErrorBody(bool);

    impl http_body::Body for ErrorBody {
        type Data = bytes::Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            if this.0 {
                Poll::Ready(None)
            } else {
                this.0 = true;
                Poll::Ready(Some(Err(std::io::Error::other("transport error"))))
            }
        }
    }

    /// Mock gRPC service answering with the given header- and/or
    /// trailer-carried `grpc-status`, and body `is_end_stream()` value.
    fn mock_service(
        header_status: Option<&'static str>,
        trailer_status: Option<&'static str>,
        end_stream: bool,
    ) -> impl tower::Service<
        http::Request<String>,
        Response = http::Response<MockBody>,
        Error = std::convert::Infallible,
        Future: Send + 'static,
    > + Clone {
        tower::service_fn(move |_req: http::Request<String>| async move {
            let mut builder = http::Response::builder().status(200);
            if let Some(code) = header_status {
                builder = builder.header("grpc-status", code);
            }
            let trailer = trailer_status.map(|code| {
                let mut map = http::HeaderMap::new();
                map.insert("grpc-status", http::HeaderValue::from_static(code));
                map
            });
            Ok::<_, std::convert::Infallible>(
                builder
                    .body(MockBody {
                        trailer,
                        end_stream,
                    })
                    .unwrap(),
            )
        })
    }

    /// Poll a body to completion, discarding frames. Mirrors what a real
    /// HTTP/2 server does while streaming a response — the thing that
    /// actually drives `TrailerStatusBody`'s status recording, since
    /// `oneshot` alone only awaits the response headers/future.
    async fn drain_body<B: http_body::Body + Unpin>(mut body: B) {
        let mut body = Pin::new(&mut body);
        while std::future::poll_fn(|cx| body.as_mut().poll_frame(cx))
            .await
            .is_some()
        {}
    }

    /// Mirrors hyper's *real* HTTP/2 dispatch decision (hyper-1.11.0
    /// `src/proto/h2/server.rs:518-531`): a body reporting
    /// `is_end_stream() == true` is never polled — hyper sends a
    /// Trailers-Only response and drops the body outright. `drain_body`
    /// polls unconditionally and can't reach that path, which is exactly
    /// what let the original regression slip past the test suite.
    async fn dispatch_like_hyper<B: http_body::Body + Unpin>(body: B) {
        if !body.is_end_stream() {
            drain_body(body).await;
        }
        // else: `body` is dropped right here, without a single
        // `poll_frame` call — the scenario `TrailerStatusBody`'s `Drop`
        // impl exists to cover.
    }

    async fn capture_with_dispatch<F, Fut>(
        request: http::Request<String>,
        header_status: Option<&'static str>,
        trailer_status: Option<&'static str>,
        end_stream: bool,
        dispatch: F,
    ) -> Vec<SpanData>
    where
        F: FnOnce(TrailerStatusBody<MockBody>) -> Fut,
        Fut: Future<Output = ()>,
    {
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
                .service(mock_service(header_status, trailer_status, end_stream));
            let response = svc.oneshot(request).await.unwrap();
            dispatch(response.into_body()).await;
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        exporter.get_finished_spans().unwrap()
    }

    async fn capture(
        request: http::Request<String>,
        header_status: Option<&'static str>,
        trailer_status: Option<&'static str>,
    ) -> Vec<SpanData> {
        capture_with_dispatch(request, header_status, trailer_status, false, drain_body).await
    }

    /// Exercises `TrailerStatusBody` directly, bypassing the tower/hyper
    /// dispatch harness entirely — for lifecycle edge cases (`Drop`
    /// without ever draining, a body-error frame) that don't correspond
    /// to a `mock_service` response shape.
    async fn capture_body<B, F, Fut>(
        header_status: Option<tonic::Code>,
        inner: B,
        run: F,
    ) -> Vec<SpanData>
    where
        B: http_body::Body + Unpin,
        F: FnOnce(TrailerStatusBody<B>) -> Fut,
        Fut: Future<Output = ()>,
    {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        async {
            let span = rpc_server_span(EXPORT_PATH, None);
            let body = TrailerStatusBody::new(inner, span, header_status);
            run(body).await;
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
        let spans = capture(request, None, None).await;

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
        let spans = capture(request, Some("13"), None).await;
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
        let spans = capture(request, Some("16"), None).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("UNAUTHENTICATED")
        );
        assert_eq!(span.status, Status::Unset);
    }

    #[tokio::test]
    async fn trailer_carried_fault_marks_span_error() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .body(String::new())
            .unwrap();
        // No `grpc-status` header — only a trailers frame at the end of the
        // body, as a streaming call reports it. Before the fix this was
        // read as `OK` because the header path was the only one checked.
        let spans = capture(request, None, Some("13")).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("INTERNAL")
        );
        assert!(matches!(span.status, Status::Error { .. }));
    }

    #[tokio::test]
    async fn trailer_carried_ok_status_does_not_invert_default() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .body(String::new())
            .unwrap();
        // grpc-status: 0 (OK) in trailers must still record success, not
        // flip to an error just because a trailers frame was present.
        let spans = capture(request, None, Some("0")).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("OK")
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
        let spans = capture(request, None, None).await;
        assert!(spans.is_empty(), "self-monitoring export must not re-span");
    }

    #[tokio::test]
    async fn trailer_status_overrides_header_status() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .body(String::new())
            .unwrap();
        // Header says INTERNAL, but the trailers frame (the authoritative
        // final status per the module doc) says OK — trailers must win.
        let spans = capture(request, Some("13"), Some("0")).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("OK")
        );
        assert_eq!(span.status, Status::Unset);
    }

    #[tokio::test]
    async fn trailers_only_response_is_never_polled_but_still_records_status() {
        let request = http::Request::builder()
            .uri(EXPORT_PATH)
            .body(String::new())
            .unwrap();
        // Mirrors tonic's real immediately-failed-unary response shape:
        // `grpc-status` in the header, `is_end_stream() == true` (as
        // `tonic::body::Body::empty()` reports), no trailers frame ever
        // produced. Dispatched the way hyper's HTTP/2 server actually
        // does: `is_end_stream() == true` short-circuits to a
        // Trailers-Only response and `poll_frame` is NEVER called. Before
        // the `Drop` fallback, this path recorded nothing at all.
        let spans =
            capture_with_dispatch(request, Some("13"), None, true, dispatch_like_hyper).await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("INTERNAL")
        );
        assert!(matches!(span.status, Status::Error { .. }));
    }

    #[tokio::test]
    async fn dropped_without_draining_records_cancelled_not_ok() {
        // No header, no trailers, and the body is dropped without a single
        // `poll_frame` call (e.g. a genuine mid-stream client disconnect).
        // There is nothing to fall back on, so the outcome must be
        // recorded as unknown (`CANCELLED`) rather than defaulting to
        // `OK`, which would misreport a cut-off call as successful.
        let spans = capture_body(
            None,
            MockBody {
                trailer: None,
                end_stream: false,
            },
            |body| async move {
                drop(body);
            },
        )
        .await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("CANCELLED")
        );
        // CANCELLED is not in the SERVER-boundary error set (it is the
        // caller's action, not a server fault), so the span status itself
        // stays unset — only the recorded code changes.
        assert_eq!(span.status, Status::Unset);
    }

    #[tokio::test]
    async fn body_error_frame_still_records_an_outcome() {
        let spans = capture_body(None, ErrorBody(false), |body| async move {
            let mut body = Box::pin(body);
            let frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
            assert!(matches!(frame, Some(Err(_))));
            // `body` drops here after just the one errored poll — mirrors
            // a transport aborting the stream right after an error frame,
            // the case that used to lose the outcome silently.
        })
        .await;
        let span = &spans[0];
        assert_eq!(
            attr(span, "rpc.response.status_code").as_deref(),
            Some("CANCELLED")
        );
    }
}
