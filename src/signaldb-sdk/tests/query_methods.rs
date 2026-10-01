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
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );

    // Each call must compile — this is the exact surface the MCP tools
    // (search_traces, get_trace) forward to. The Tempo tag endpoints stay in
    // the SDK for external clients; first parties discover through `query_ir`.
    let _search = client.search();
    let _trace = client.query_single_trace();
    let _tags = client.search_tags();
    let _tag_values = client.search_tag_values();
}

#[test]
fn client_exposes_label_discovery_builders() {
    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );

    // Loki/Prometheus label-name and label-value discovery. These compat
    // metadata endpoints remain for external clients; the MCP server's
    // `discover_attributes` and `discover_metrics` use the IR `describe` stage.
    let _logql_labels = client.logql_labels();
    let _logql_label_values = client.logql_label_values();
    let _promql_labels = client.promql_labels();
    let _promql_label_values = client.promql_label_values();
}

/// Task 7.1 — the generated `query` operation compiles and round-trips an IR
/// request type through the SDK's DTOs.
#[test]
fn client_exposes_ir_query_and_round_trips_the_request() {
    use signaldb_sdk::types::{QueryIrRequest, QueryRange};

    let client = Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    );
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
        focus: None,
        depth: None,
        trace_id: None,
        step: None,
        constant: None,
        pipeline: vec![
            serde_json::from_value(serde_json::json!({
                "where": { "field": "service.name", "op": "eq", "value": "api" }
            }))
            .unwrap(),
        ],
    };
    // Serializes to the versioned IR document shape and back.
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(json["irVersion"], 1);
    assert_eq!(json["from"], "logs");
    let round: QueryIrRequest = serde_json::from_value(json).unwrap();
    assert_eq!(round.result, "rows");
}

/// A `match` stage's span-set order is significant (it orders each row's
/// `spansets` column), so a document passed through the SDK's typed stages
/// must reach the server with its span-sets in declaration order.
#[test]
fn match_stage_keeps_span_set_declaration_order() {
    use signaldb_sdk::types::IrStage;

    let names = ["zeta", "alpha", "mid", "beta", "omega", "gamma"];
    let spansets: serde_json::Map<String, serde_json::Value> = names
        .iter()
        .map(|n| {
            let leaf = serde_json::json!({ "field": "span.name", "op": "eq", "value": n });
            (n.to_string(), leaf)
        })
        .collect();
    let stage = serde_json::json!({
        "match": {
            "spansets": spansets,
            "relations": [{ "left": "zeta", "op": "child", "right": "alpha" }]
        }
    });

    let typed: IrStage = serde_json::from_value(stage).unwrap();
    let text = serde_json::to_string(&typed).unwrap();
    let positions: Vec<usize> = names
        .iter()
        .map(|n| text.find(&format!("\"{n}\":")).unwrap())
        .collect();
    assert!(positions.is_sorted(), "span-sets reordered: {text}");
}

/// Duplicate span-set names are not the SDK's to resolve: both entries go
/// out in order, and the server rejects the document when it validates it.
#[test]
fn match_stage_keeps_duplicate_span_set_names() {
    let text = r#"{"match":{"spansets":{"a":{"field":"span.name","op":"eq","value":"first"},"a":{"field":"span.name","op":"eq","value":"second"}}}}"#;

    let typed: signaldb_sdk::types::IrStage = serde_json::from_str(text).unwrap();
    let signaldb_sdk::types::IrStage::Match(stage) = &typed else {
        panic!("not a match stage: {typed:?}");
    };
    let names: Vec<&str> = stage.spansets.0.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["a", "a"]);
    assert_eq!(serde_json::to_string(&typed).unwrap(), text);
}

#[tokio::test]
async fn client_forwards_credentials_via_default_headers() {
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // The MCP server builds its per-session SDK client this way: the caller's
    // bearer and tenant header become reqwest default headers, so every
    // downstream request is made as the caller and the router enforces
    // isolation. This test drives a real request through the constructed
    // client against a local mock server and inspects the headers the
    // server actually received on the wire.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let addr = listener.local_addr().expect("mock server local addr");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut received = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = socket.read(&mut buf).await.expect("read request");
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
            if received.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let body = b"{\"tenants\":[]}";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response headers");
        socket.write_all(body).await.expect("write response body");
        socket.shutdown().await.ok();
        String::from_utf8_lossy(&received).to_string()
    });

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

    let client = Client::new_with_client(
        &format!("http://{addr}"),
        http,
        signaldb_sdk::RetryPolicy::default(),
    );
    client
        .list_tenants()
        .send()
        .await
        .expect("mock server responds to list_tenants");

    let received_request = server.await.expect("mock server task panicked");
    assert!(
        received_request
            .to_lowercase()
            .contains("authorization: bearer sk-tenant-key"),
        "expected forwarded authorization header on the wire, got request:\n{received_request}"
    );
    assert!(
        received_request
            .to_lowercase()
            .contains("x-tenant-id: acme"),
        "expected forwarded x-tenant-id header on the wire, got request:\n{received_request}"
    );
}
