//! Shared helpers for integration tests that drive the MCP server's
//! Streamable HTTP transport directly (as opposed to `tests-integration`'s
//! SDK-level test helpers).

use std::time::Duration;

use futures::StreamExt;

/// Read a Streamable HTTP response (JSON or SSE) until the JSON-RPC message
/// with `id` arrives.
pub async fn read_jsonrpc_response(
    response: axum::response::Response,
    id: u64,
) -> serde_json::Value {
    let mut stream = response.into_body().into_data_stream();
    let mut buffered = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("response arrives before the deadline");
        let Some(chunk) = chunk else {
            panic!("response stream ended without a reply for id {id}: {buffered}");
        };
        let chunk = chunk.expect("read response chunk");
        buffered.push_str(&String::from_utf8_lossy(&chunk));
        for line in buffered.lines() {
            let candidate = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(candidate)
                && value.get("id").and_then(|v| v.as_u64()) == Some(id)
            {
                return value;
            }
        }
    }
}
