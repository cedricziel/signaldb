//! Verifies the generated client exposes the Tempo trace query operations the
//! MCP read tools wrap, and that a client can be constructed with the
//! credential-forwarding default headers the MCP server sets per session.
//!
//! These are compile-and-construct assertions: the query surface is generated
//! from `api/signaldb-api.json`, so if the trace endpoints ever drop out of the
//! OpenAPI document this test stops compiling.

use signaldb_sdk::Client;

#[test]
fn client_exposes_trace_query_builders() {
    let client = Client::new("http://localhost:8080");

    // Each call must compile — this is the exact surface the MCP tools
    // (search_traces, get_trace, discover_attributes) forward to.
    let _search = client.search();
    let _trace = client.query_single_trace();
    let _tags = client.search_tags();
    let _tag_values = client.search_tag_values();
}

/// Task 7.1 — the generated `query` operation compiles and round-trips an IR
/// request type through the SDK's DTOs.
#[test]
fn client_exposes_ir_query_and_round_trips_the_request() {
    use signaldb_sdk::types::{QueryIrRequest, QueryRange};

    let client = Client::new("http://localhost:8080");
    let _query = client.query_ir(); // the native IR operation builder exists

    let request = QueryIrRequest {
        ir_version: 1,
        from: "logs".to_string(),
        range: QueryRange {
            from: "now-1h".to_string(),
            to: "now".to_string(),
        },
        result: "rows".to_string(),
        fields: None,
        pipeline: vec![
            serde_json::json!({
                "where": { "field": "service.name", "op": "eq", "value": "api" }
            })
            .as_object()
            .unwrap()
            .clone(),
        ],
    };
    // Serializes to the versioned IR document shape and back.
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["irVersion"], 1);
    assert_eq!(json["from"], "logs");
    let round: QueryIrRequest = serde_json::from_value(json).unwrap();
    assert_eq!(round.result, "rows");
}

#[test]
fn client_forwards_credentials_via_default_headers() {
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

    // The MCP server builds its per-session SDK client this way: the caller's
    // bearer and tenant header become reqwest default headers, so every
    // downstream request is made as the caller and the router enforces
    // isolation. This test pins that construction path.
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer sk-tenant-key"),
    );
    headers.insert("x-tenant-id", HeaderValue::from_static("acme"));

    let http = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .expect("reqwest client builds");

    let _client = Client::new_with_client("http://localhost:8080", http);
}
