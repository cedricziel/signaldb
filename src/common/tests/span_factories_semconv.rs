//! Conformance tests for the semconv span factories in
//! `common::self_monitoring::spans` — the single sanctioned construction
//! path for boundary spans. Each test pins the exported span's name, kind,
//! attributes, and status mapping against the OpenTelemetry semantic
//! conventions the factory implements.
//!
//! Uses a scoped subscriber per test (no process-global state), so all
//! factory tests can share this binary.

use common::self_monitoring::spans::{
    self, FLIGHT_DO_GET, FLIGHT_DO_PUT, RpcBoundary, db_client_span, http_client_span, job_span,
    mcp_tool_span, record_http_client_result, record_network_peer_from_addr, record_span_error,
    rpc_client_span, rpc_server_span,
};
use opentelemetry::trace::{SpanKind, Status, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_subscriber::prelude::*;

/// Run `f` under a scoped tracing→OTel bridge and return the finished spans.
fn capture_spans(f: impl FnOnce()) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::with_default(subscriber, f);
    provider.force_flush().unwrap();
    exporter.get_finished_spans().unwrap()
}

fn attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().to_string())
}

#[test]
fn rpc_server_span_follows_rpc_semconv() {
    let spans = capture_spans(|| {
        let span = rpc_server_span(FLIGHT_DO_GET, Some("query_ir"));
        let _guard = span.enter();
    });
    let span = &spans[0];

    // Span name is the fully-qualified logical method, disambiguated by the
    // low-cardinality ticket verb (permitted by the RPC naming rules).
    assert_eq!(
        span.name,
        "arrow.flight.protocol.FlightService/DoGet query_ir"
    );
    assert_eq!(span.span_kind, SpanKind::Server);
    assert_eq!(attr(span, "rpc.system.name").as_deref(), Some("grpc"));
    assert_eq!(
        attr(span, "rpc.method").as_deref(),
        Some("arrow.flight.protocol.FlightService/DoGet")
    );
    assert_eq!(
        attr(span, "signaldb.flight.ticket_verb").as_deref(),
        Some("query_ir")
    );
}

#[test]
fn rpc_server_span_client_fault_is_not_an_error() {
    // NOT_FOUND is the caller's problem: recorded, but status stays unset.
    let spans = capture_spans(|| {
        let span = rpc_server_span(FLIGHT_DO_GET, None);
        let _guard = span.enter();
        spans::record_rpc_result(&span, RpcBoundary::Server, tonic::Code::NotFound);
    });
    let span = &spans[0];
    assert_eq!(span.name, "arrow.flight.protocol.FlightService/DoGet");
    assert_eq!(
        attr(span, "rpc.response.status_code").as_deref(),
        Some("NOT_FOUND")
    );
    assert_eq!(span.status, Status::Unset);
    assert_eq!(attr(span, "error.type"), None);
}

#[test]
fn rpc_server_span_server_fault_is_an_error() {
    let spans = capture_spans(|| {
        let span = rpc_server_span(FLIGHT_DO_PUT, None);
        let _guard = span.enter();
        spans::record_rpc_result(&span, RpcBoundary::Server, tonic::Code::Internal);
    });
    let span = &spans[0];
    assert_eq!(
        attr(span, "rpc.response.status_code").as_deref(),
        Some("INTERNAL")
    );
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(attr(span, "error.type").as_deref(), Some("INTERNAL"));
}

#[test]
fn rpc_server_span_records_network_peer_from_socket_addr() {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    let spans = capture_spans(|| {
        let span = rpc_server_span(FLIGHT_DO_GET, None);
        let _guard = span.enter();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 54321));
        record_network_peer_from_addr(&span, Some(addr));
    });
    let span = &spans[0];
    assert_eq!(
        attr(span, "network.peer.address").as_deref(),
        Some("10.0.0.1")
    );
    assert_eq!(attr(span, "network.peer.port").as_deref(), Some("54321"));
}

#[test]
fn rpc_server_span_network_peer_is_noop_when_addr_is_none() {
    let spans = capture_spans(|| {
        let span = rpc_server_span(FLIGHT_DO_GET, None);
        let _guard = span.enter();
        record_network_peer_from_addr(&span, None);
    });
    let span = &spans[0];
    assert_eq!(attr(span, "network.peer.address"), None);
    assert_eq!(attr(span, "network.peer.port"), None);
}

#[test]
fn rpc_client_span_any_non_ok_is_an_error() {
    // The same NOT_FOUND that leaves a server span unset fails the client
    // span: per RPC semconv, clients treat every non-OK code as an error.
    let spans = capture_spans(|| {
        let span = rpc_client_span(
            FLIGHT_DO_GET,
            Some("find_trace"),
            Some("querier.example:50054"),
        );
        let _guard = span.enter();
        spans::record_rpc_result(&span, RpcBoundary::Client, tonic::Code::NotFound);
    });
    let span = &spans[0];
    // Client spans take the same low-cardinality detail as server spans.
    assert_eq!(
        span.name,
        "arrow.flight.protocol.FlightService/DoGet find_trace"
    );
    assert_eq!(span.span_kind, SpanKind::Client);
    assert_eq!(
        attr(span, "signaldb.flight.ticket_verb").as_deref(),
        Some("find_trace")
    );
    assert_eq!(attr(span, "rpc.system.name").as_deref(), Some("grpc"));
    assert_eq!(
        attr(span, "server.address").as_deref(),
        Some("querier.example")
    );
    assert_eq!(attr(span, "server.port").as_deref(), Some("50054"));
    assert_eq!(
        attr(span, "rpc.response.status_code").as_deref(),
        Some("NOT_FOUND")
    );
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(attr(span, "error.type").as_deref(), Some("NOT_FOUND"));
}

#[test]
fn rpc_client_span_ok_leaves_status_unset() {
    let spans = capture_spans(|| {
        let span = rpc_client_span(FLIGHT_DO_PUT, None, None);
        let _guard = span.enter();
        spans::record_rpc_result(&span, RpcBoundary::Client, tonic::Code::Ok);
    });
    let span = &spans[0];
    assert_eq!(
        attr(span, "rpc.response.status_code").as_deref(),
        Some("OK")
    );
    assert_eq!(span.status, Status::Unset);
    assert_eq!(attr(span, "error.type"), None);
}

#[test]
fn db_client_span_follows_db_semconv() {
    let spans = capture_spans(|| {
        let span = db_client_span("sqlite", "SELECT", "signaldb-catalog");
        let _guard = span.enter();
    });
    let span = &spans[0];

    // Name precedence: `{db.operation.name} {target}` with the namespace as
    // target — never raw SQL.
    assert_eq!(span.name, "SELECT signaldb-catalog");
    assert_eq!(span.span_kind, SpanKind::Client);
    assert_eq!(attr(span, "db.system.name").as_deref(), Some("sqlite"));
    assert_eq!(attr(span, "db.operation.name").as_deref(), Some("SELECT"));
    assert_eq!(
        attr(span, "db.namespace").as_deref(),
        Some("signaldb-catalog")
    );
}

#[test]
fn job_span_is_internal_with_tenancy_attributes() {
    let spans = capture_spans(|| {
        let span = job_span(
            "retention_enforcement",
            "acme",
            "production",
            Some("traces"),
        );
        let _guard = span.enter();
    });
    let span = &spans[0];

    assert_eq!(span.name, "retention_enforcement");
    assert_eq!(span.span_kind, SpanKind::Internal);
    assert_eq!(attr(span, "signaldb.tenant.id").as_deref(), Some("acme"));
    assert_eq!(
        attr(span, "signaldb.dataset.id").as_deref(),
        Some("production")
    );
    assert_eq!(attr(span, "signaldb.table").as_deref(), Some("traces"));
}

#[test]
fn mcp_tool_span_is_internal_with_mcp_and_tenancy_attributes() {
    let spans = capture_spans(|| {
        let span = mcp_tool_span("get_trace", "acme", Some("prod"), "sess-1");
        let _guard = span.enter();
    });
    let span = &spans[0];

    assert_eq!(span.name, "tools/call get_trace");
    assert_eq!(span.span_kind, SpanKind::Internal);
    assert_eq!(attr(span, "mcp.method.name").as_deref(), Some("tools/call"));
    assert_eq!(attr(span, "gen_ai.tool.name").as_deref(), Some("get_trace"));
    assert_eq!(attr(span, "mcp.session.id").as_deref(), Some("sess-1"));
    assert_eq!(attr(span, "signaldb.tenant.id").as_deref(), Some("acme"));
    assert_eq!(attr(span, "signaldb.dataset.id").as_deref(), Some("prod"));
    // Not failed: status unset, `error.type` never recorded.
    assert_eq!(span.status, Status::Unset);
    assert_eq!(attr(span, "error.type"), None);
}

#[test]
fn mcp_tool_span_without_dataset_leaves_the_attribute_unrecorded() {
    let spans = capture_spans(|| {
        let span = mcp_tool_span("server_info", "acme", None, "stdio");
        let _guard = span.enter();
    });
    assert_eq!(attr(&spans[0], "signaldb.dataset.id"), None);
}

#[test]
fn http_client_span_follows_http_semconv() {
    let spans = capture_spans(|| {
        let span = http_client_span(
            "GET",
            "https://idp.example.com/.well-known/openid-configuration",
        );
        let _guard = span.enter();
        record_http_client_result(&span, 200);
    });
    let span = &spans[0];

    assert_eq!(
        span.name,
        "GET https://idp.example.com/.well-known/openid-configuration"
    );
    assert_eq!(span.span_kind, SpanKind::Client);
    assert_eq!(attr(span, "http.request.method").as_deref(), Some("GET"));
    assert_eq!(
        attr(span, "url.full").as_deref(),
        Some("https://idp.example.com/.well-known/openid-configuration")
    );
    assert_eq!(
        attr(span, "http.response.status_code").as_deref(),
        Some("200")
    );
    assert_eq!(span.status, Status::Unset);
    assert_eq!(attr(span, "error.type"), None);
}

#[test]
fn http_client_span_error_status_fails_the_span() {
    let spans = capture_spans(|| {
        let span = http_client_span("POST", "https://idp.example.com/token");
        let _guard = span.enter();
        record_http_client_result(&span, 500);
    });
    let span = &spans[0];
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(attr(span, "error.type").as_deref(), Some("500"));
}

#[test]
fn http_client_span_transport_failure_uses_shared_error_recorder() {
    let spans = capture_spans(|| {
        let span = http_client_span("GET", "https://idp.example.com/jwks");
        let _guard = span.enter();
        record_span_error(&span, "connect_error");
    });
    let span = &spans[0];
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(attr(span, "error.type").as_deref(), Some("connect_error"));
    // No status code at all: the request never got a response.
    assert_eq!(attr(span, "http.response.status_code"), None);
}

#[test]
fn mcp_tool_span_records_error_only_when_told_to() {
    let spans = capture_spans(|| {
        let span = mcp_tool_span("search_logs", "acme", None, "sess-1");
        let _guard = span.enter();
        record_span_error(&span, "500");
    });
    let span = &spans[0];
    assert!(matches!(span.status, Status::Error { .. }));
    assert_eq!(attr(span, "error.type").as_deref(), Some("500"));
}
