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
