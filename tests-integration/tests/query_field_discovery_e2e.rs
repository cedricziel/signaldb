//! End-to-end coverage for the query-field-discovery surface
//! (`openspec/changes/query-field-discovery`): ingest a little data through
//! the normal path, then exercise `describe: fields`, `describe: values`, and
//! `GET /api/v1/query/sources` through the native Query IR — never a
//! Tempo/Loki/Prometheus compat endpoint (CLAUDE.md, "Query IR is our own
//! query surface"). Reuses the OTLP ingest and IR helpers from
//! `query_ir_e2e.rs` rather than re-deriving a service topology.

use crate::query_ir_e2e::{
    TestServices, build_router, log_record, logs_request, post_ir, range, setup, span,
    test_tenant_context, traces_request,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use std::time::Duration;
use tokio::time::sleep;
use tower::ServiceExt;

/// GET `/api/v1/query/sources`, parsed as JSON.
async fn get_sources(app: &Router) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query/sources")
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
    )
}

/// Ingest one log line, so `logs` has a declared field to discover and an
/// available table to list.
async fn ingest_a_log(services: &TestServices) {
    let ctx = test_tenant_context();
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "checkout",
                vec![log_record(0, "INFO", "checkout completed")],
            ),
        )
        .await
        .expect("ingest checkout log");
}

/// Ingest one server span, so `traces` has a `span.kind` value ("Server")
/// among the ones `describe: values` declares.
async fn ingest_a_trace(services: &TestServices) {
    let ctx = test_tenant_context();
    let mut server_span = span("GET /checkout", 1, 10_000_000);
    server_span.kind = 2; // Server
    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("checkout", vec![server_span]))
        .await
        .expect("ingest checkout span");
}

/// `describe: fields` on `logs` advertises the declared logical field
/// `service.name`, which the ingested log actually carries.
#[tokio::test]
async fn describe_fields_reports_the_declared_field_the_ingested_log_carries() {
    let services = setup().await;
    ingest_a_log(&services).await;
    let app = build_router(&services).await;

    let (status, body) = post_ir(
        &app,
        serde_json::json!({
            "irVersion": 4,
            "from": "logs",
            "range": range(),
            "result": "metadata",
            "pipeline": [ { "describe": { "target": "fields" } } ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "describe fields: {body}");
    assert_eq!(body["result"], "metadata");
    assert_eq!(body["metadata"]["kind"], "fields");
    let names: Vec<&str> = body["metadata"]["fields"]
        .as_array()
        .expect("fields array")
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"service.name"),
        "declared logical field must be present: {names:?}"
    );
}

/// `describe: values` on `traces`/`span.kind` answers exactly, from the
/// declared enumeration — including the kind the ingested span actually
/// used — without needing a compactor statistics pass.
#[tokio::test]
async fn describe_values_reports_the_kind_the_ingested_span_used() {
    let services = setup().await;
    ingest_a_trace(&services).await;
    let app = build_router(&services).await;

    let (status, body) = post_ir(
        &app,
        serde_json::json!({
            "irVersion": 4,
            "from": "traces",
            "range": range(),
            "result": "metadata",
            "pipeline": [ { "describe": { "target": "values", "field": "span.kind" } } ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "describe values: {body}");
    assert_eq!(body["metadata"]["kind"], "values");
    let values: Vec<&str> = body["metadata"]["values"]
        .as_array()
        .expect("values array")
        .iter()
        .map(|v| v["value"].as_str().unwrap())
        .collect();
    assert!(
        values.contains(&"Server"),
        "the ingested span's kind must appear in the declared enumeration: {values:?}"
    );
}

/// `GET /api/v1/query/sources` reports `logs` as available once the tenant
/// has ingested through it, using the same `metadata` envelope the `describe`
/// documents return.
#[tokio::test]
async fn query_sources_reports_the_ingested_signal_as_available() {
    let services = setup().await;
    ingest_a_log(&services).await;
    let app = build_router(&services).await;

    // The writer persists the ingested table to Iceberg asynchronously (WAL
    // drain, ≥5s base interval), so `available` may briefly still read
    // `false` right after ingest; poll rather than assert on the first read.
    let mut status = StatusCode::OK;
    let mut body = serde_json::Value::Null;
    for _ in 0..40 {
        let (s, b) = get_sources(&app).await;
        status = s;
        let available = b["metadata"]["sources"]
            .as_array()
            .and_then(|sources| sources.iter().find(|s| s["name"] == "logs"))
            .and_then(|s| s["available"].as_bool())
            .unwrap_or(false);
        body = b;
        if status == StatusCode::OK && available {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }

    assert_eq!(status, StatusCode::OK, "query sources: {body}");
    assert_eq!(body["result"], "metadata");
    assert_eq!(body["metadata"]["kind"], "sources");
    let logs_source = body["metadata"]["sources"]
        .as_array()
        .expect("sources array")
        .iter()
        .find(|s| s["name"] == "logs")
        .expect("the registry always lists logs, ingested or not");
    assert_eq!(
        logs_source["available"], true,
        "logs must report available once the tenant has ingested through it: {body}"
    );
}
