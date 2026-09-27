//! An unchanged OTLP export is typed at write and read back through the
//! Query IR; an off-type value survives verbatim in the raw attribute bag.

use crate::query_ir_e2e::{build_router, post_ir_until_rows, range, setup, test_tenant_context};
use axum::http::StatusCode;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn status_span(name: &str, seq: u8, status_code_value: Value) -> Span {
    Span {
        trace_id: vec![seq; 16],
        span_id: vec![seq; 8],
        parent_span_id: vec![],
        name: name.to_string(),
        kind: 1,
        start_time_unix_nano: BASE_NS as u64,
        end_time_unix_nano: (BASE_NS + 1_000_000) as u64,
        attributes: vec![KeyValue {
            key: "http.status_code".to_string(),
            value: Some(AnyValue {
                value: Some(status_code_value),
            }),
            ..Default::default()
        }],
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        trace_state: String::new(),
        flags: 0,
    }
}

fn traces_request_with_spans(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("checkout".to_string())),
                    }),
                    ..Default::default()
                }],
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn unchanged_otlp_export_is_typed_at_write_and_off_type_survives_in_the_raw_bag() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // First span establishes `http.status_code` as Int64-canonical.
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request_with_spans(vec![status_span("first-span", 1, Value::IntValue(200))]),
        )
        .await
        .expect("ingest first span");
    common::testing::flush_storage_writers(&services.flight_transport, &ctx.tenant_id, None)
        .await
        .expect("flush writer");

    // Second span sends the same key as a string — off-type once the
    // canonical type is Int64.
    services
        .trace_handler
        .handle_grpc_otlp_traces(
            &ctx,
            traces_request_with_spans(vec![status_span(
                "second-span",
                2,
                Value::StringValue("404".to_string()),
            )]),
        )
        .await
        .expect("ingest second span (off-type, existing OTLP client, unchanged)");
    common::testing::flush_storage_writers(&services.flight_transport, &ctx.tenant_id, None)
        .await
        .expect("flush writer");

    let app = build_router(&services).await;

    // The typed-canonical span reads back as an integer, no cast.
    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "traces",
            "range": range(),
            "result": "rows",
            "fields": ["span.name", "http.status_code"],
            "pipeline": [
                { "where": { "field": "span.name", "op": "eq", "value": "first-span" } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "typed field IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "expected the typed span: {body}");
    assert_eq!(
        rows[0][1],
        serde_json::json!(200),
        "http.status_code must read back typed, no cast: {body}"
    );

    // The off-type span was still ingested (not dropped), and its original
    // string value survives in the retrieval-only raw attribute bag.
    let (status, body) = post_ir_until_rows(
        &app,
        serde_json::json!({
            "irVersion": 1,
            "from": "traces",
            "range": range(),
            "result": "rows",
            "fields": ["span.name", "span.attributes"],
            "pipeline": [
                { "where": { "field": "span.name", "op": "eq", "value": "second-span" } }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "raw bag IR query: {body}");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "the off-type span must be ingested, not dropped: {body}"
    );
    assert_eq!(
        rows[0][1]["http.status_code"],
        serde_json::json!("404"),
        "the off-type value must survive verbatim in the raw attribute bag: {body}"
    );
}
