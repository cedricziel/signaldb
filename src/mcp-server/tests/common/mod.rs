//! Shared MCP integration-test harness.
//!
//! Two layers, used by different tests:
//! - [`connect`]: an in-process duplex client (no HTTP layer) for
//!   registration/schema checks that never dispatch a tool call.
//! - [`spawn_router`], [`mcp_request_with_key`], [`read_jsonrpc_response`]:
//!   building blocks for a full Streamable HTTP round trip against a mock
//!   router, for tests that need the `Extension<Parts>` only that transport
//!   populates.
//!
//! Not every test binary that includes this module (each integration test
//! file compiles separately) uses every helper.
#![allow(dead_code)]

use axum::body::Body;
use axum::http::Request;
use axum::response::Response;
use futures::StreamExt;
use rmcp::{ClientHandler, RoleClient, ServiceExt, model::ClientInfo, service::RunningService};
use std::time::Duration;

#[derive(Clone)]
pub struct TestClient;

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }
}

/// An in-process duplex connection to a freshly constructed [`McpServer`],
/// with no HTTP layer.
///
/// [`McpServer`]: mcp_server::server::McpServer
pub async fn connect() -> RunningService<RoleClient, TestClient> {
    let (server_transport, client_transport) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let server = mcp_server::server::McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(5),
        );
        if let Ok(running) = server.serve(server_transport).await {
            let _ = running.waiting().await;
        }
    });
    TestClient
        .serve(client_transport)
        .await
        .expect("client connects to the in-memory server")
}

/// Binds `app` to an ephemeral localhost port, serves it in the background,
/// and returns its base URL.
pub async fn spawn_router(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock router");
    let addr = listener.local_addr().expect("mock router address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock router serves");
    });
    format!("http://{addr}")
}

/// Builds a Streamable HTTP `POST /mcp` request carrying `api_key` as the
/// bearer credential, tenant `acme`, and (once assigned) the session id.
pub fn mcp_request_with_key(
    api_key: &str,
    session_id: Option<&str>,
    body: serde_json::Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", "localhost")
        .header("authorization", format!("Bearer {api_key}"))
        .header("x-tenant-id", "acme")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(session_id) = session_id {
        builder = builder.header("mcp-session-id", session_id);
    }
    builder
        .body(Body::from(body.to_string()))
        .expect("build MCP request")
}

/// Reads a Streamable HTTP response (JSON or SSE) until the JSON-RPC message
/// with `id` arrives, then stops — the SSE stream may outlive the response.
///
/// Bytes are buffered raw and decoded with [`std::str::from_utf8`] rather
/// than [`String::from_utf8_lossy`] per chunk: a multi-byte UTF-8 sequence
/// split across a chunk boundary would otherwise be replaced with U+FFFD,
/// corrupting the JSON payload. An incomplete trailing sequence is left in
/// the buffer and completed by the next chunk.
pub async fn read_jsonrpc_response(response: Response, id: u64) -> serde_json::Value {
    let mut stream = response.into_body().into_data_stream();
    let mut raw: Vec<u8> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("response arrives before the deadline");
        let Some(chunk) = chunk else {
            let buffered = String::from_utf8_lossy(&raw);
            panic!("response stream ended without a reply for id {id}: {buffered}");
        };
        let chunk = chunk.expect("read response chunk");
        raw.extend_from_slice(&chunk);

        let text = match std::str::from_utf8(&raw) {
            Ok(text) => text,
            Err(err) if err.error_len().is_none() => {
                // Incomplete trailing sequence: decode the valid prefix and
                // wait for the next chunk to complete it.
                std::str::from_utf8(&raw[..err.valid_up_to()])
                    .expect("prefix up to a UTF-8 error's valid_up_to is valid UTF-8")
            }
            Err(err) => panic!("response chunk is not valid UTF-8: {err}"),
        };

        for line in text.lines() {
            let candidate = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(candidate)
                && value.get("id").and_then(|v| v.as_u64()) == Some(id)
            {
                return value;
            }
        }
    }
}
