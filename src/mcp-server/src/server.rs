//! # MCP server handler
//!
//! Exposes SignalDB's read/query surface over the Model Context Protocol. Tools
//! are thin wrappers over the generated [`signaldb_sdk`] query methods,
//! forwarding the caller's credential (the handler holds no key of its own).
//!
//! Tools (every authenticated tenant session, no role gating):
//! - `server_info` — connectivity + resolved tenant
//! - `search_traces` — TraceQL search
//! - `get_trace` — single trace by ID
//! - `discover_attributes` — queryable tag names / values
//! - `query_metrics` — PromQL query (native Prometheus result)
//! - `search_logs` — LogQL query (native Loki result)
//! - `query_ir` — native Query IR document (structured query surface)
//!
//! Raw SQL is served over Arrow Flight (gRPC) rather than the router HTTP API;
//! this server is an HTTP forwarder and holds no Flight client, so SQL stays a
//! CLI-only capability (see the `client-surface-parity` spec).

use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::JsonSchema;
use rmcp::{
    ErrorData, ServerHandler,
    handler::server::tool::Extension,
    model::{CallToolResult, ContentBlock},
    tool, tool_handler, tool_router,
};
use serde::Deserialize;

use crate::sdk_client_for;

/// The SignalDB MCP server handler. One instance is created per session by the
/// transport's service factory; it holds only the router base URL used to build
/// per-session forwarding clients — no credential of its own.
#[derive(Clone)]
pub struct McpServer {
    router_base_url: String,
}

/// Parameters for `search_traces`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct SearchTracesParams {
    /// TraceQL query, e.g. `{ .service.name = "api" && status = error }`. When
    /// omitted, returns recent traces within the time range.
    #[serde(default)]
    query: Option<String>,
    /// Legacy `key=value` tag filter (alternative to a TraceQL query).
    #[serde(default)]
    tags: Option<String>,
    /// Start of the search window, unix seconds.
    #[serde(default)]
    start: Option<i32>,
    /// End of the search window, unix seconds.
    #[serde(default)]
    end: Option<i32>,
    /// Maximum number of traces to return.
    #[serde(default)]
    limit: Option<i32>,
    /// Minimum trace duration, milliseconds.
    #[serde(default)]
    min_duration: Option<i32>,
    /// Maximum trace duration, milliseconds.
    #[serde(default)]
    max_duration: Option<i32>,
    /// Spans-per-spanset cap on returned spans.
    #[serde(default)]
    spss: Option<i32>,
    /// Dataset to query. Omit to use the session's default dataset. The router
    /// validates access; an inaccessible dataset returns an access-denied error.
    #[serde(default)]
    dataset: Option<String>,
}

/// Parameters for `get_trace`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct GetTraceParams {
    /// Trace ID to fetch.
    trace_id: String,
    /// Optional start-of-range hint, unix seconds, to prune the scan.
    #[serde(default)]
    start: Option<i64>,
    /// Optional end-of-range hint, unix seconds, to prune the scan.
    #[serde(default)]
    end: Option<i64>,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Parameters for `discover_attributes`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverAttributesParams {
    /// When set, returns the known values for this tag; when omitted, returns
    /// the list of queryable tag names.
    #[serde(default)]
    tag: Option<String>,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Parameters for `query_metrics`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct QueryMetricsParams {
    /// PromQL expression, e.g. `rate(http_requests_total[5m])`.
    query: String,
    /// Evaluation timestamp, unix seconds or RFC3339. Omit to evaluate at "now".
    #[serde(default)]
    time: Option<String>,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Parameters for `search_logs`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct SearchLogsParams {
    /// LogQL query, e.g. `{service_name="api"} |= "error"`.
    query: String,
    /// Maximum number of log entries to return.
    #[serde(default)]
    limit: Option<i64>,
    /// Log ordering: `forward` or `backward`.
    #[serde(default)]
    direction: Option<String>,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Parameters for `query_ir`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct QueryIrParams {
    /// The native Query IR document (the structured, versioned query surface).
    query: serde_json::Value,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

#[tool_router]
impl McpServer {
    /// Construct a handler that forwards to `router_base_url`.
    pub fn new(router_base_url: String) -> Self {
        Self { router_base_url }
    }

    #[tool(
        description = "Report the SignalDB MCP server identity and the authenticated tenant/dataset for this session. Use this to confirm connectivity and which tenant your credential resolves to."
    )]
    async fn server_info(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        // The caller's identity travels in the request headers, forwarded
        // verbatim to the router; the MCP server does not resolve or validate it.
        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let info = serde_json::json!({
            "server": "signaldb-mcp",
            "version": env!("CARGO_PKG_VERSION"),
            "credential_present": parts.headers.contains_key(AUTHORIZATION),
            "tenant": header("x-tenant-id"),
            "dataset": header("x-dataset-id"),
        });
        json_result(&info)
    }

    #[tool(
        description = "Search traces with TraceQL. Provide `query` as a TraceQL expression (e.g. `{ .service.name = \"api\" && status = error }`) and optionally `start`/`end` (unix seconds) and `limit`. Returns matching traces scoped to your tenant."
    )]
    async fn search_traces(
        &self,
        Parameters(p): Parameters<SearchTracesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = sdk_client_for(&parts, &self.router_base_url, p.dataset.as_deref());
        let mut req = client.search();
        if let Some(v) = p.query {
            req = req.q(v);
        }
        if let Some(v) = p.tags {
            req = req.tags(v);
        }
        if let Some(v) = p.start {
            req = req.start(v);
        }
        if let Some(v) = p.end {
            req = req.end(v);
        }
        if let Some(v) = p.limit {
            req = req.limit(v);
        }
        if let Some(v) = p.min_duration {
            req = req.min_duration(v);
        }
        if let Some(v) = p.max_duration {
            req = req.max_duration(v);
        }
        if let Some(v) = p.spss {
            req = req.spss(v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "search_traces"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Fetch a single trace by its ID, scoped to your tenant. Optional `start`/`end` (unix seconds) hints prune the scan. Returns a not-found error when the trace does not exist."
    )]
    async fn get_trace(
        &self,
        Parameters(p): Parameters<GetTraceParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = sdk_client_for(&parts, &self.router_base_url, p.dataset.as_deref());
        let mut req = client.query_single_trace().trace_id(p.trace_id);
        if let Some(v) = p.start {
            req = req.start(v);
        }
        if let Some(v) = p.end {
            req = req.end(v);
        }
        let resp = req.send().await.map_err(|e| map_sdk_err(e, "get_trace"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Discover queryable trace attributes for your tenant. Call with no arguments to list tag names; pass `tag` to list the known values for that tag. Use this to construct valid `search_traces` queries."
    )]
    async fn discover_attributes(
        &self,
        Parameters(p): Parameters<DiscoverAttributesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = sdk_client_for(&parts, &self.router_base_url, p.dataset.as_deref());
        match p.tag {
            Some(tag) => {
                let resp = client
                    .search_tag_values()
                    .tag_name(tag)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            None => {
                let resp = client
                    .search_tags()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
        }
    }

    #[tool(
        description = "Query metrics with PromQL. Provide `query` as a PromQL expression (e.g. `rate(http_requests_total[5m])`) and optionally `time` (unix seconds or RFC3339). Returns the native Prometheus result scoped to your tenant."
    )]
    async fn query_metrics(
        &self,
        Parameters(p): Parameters<QueryMetricsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = sdk_client_for(&parts, &self.router_base_url, p.dataset.as_deref());
        let mut req = client.promql_query().query(p.query);
        if let Some(v) = p.time {
            req = req.time(v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "query_metrics"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Search logs with LogQL. Provide `query` as a LogQL expression (e.g. `{service_name=\"api\"} |= \"error\"`) and optionally `limit` and `direction` (`forward`/`backward`). Returns the native Loki result scoped to your tenant."
    )]
    async fn search_logs(
        &self,
        Parameters(p): Parameters<SearchLogsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = sdk_client_for(&parts, &self.router_base_url, p.dataset.as_deref());
        let mut req = client.logql_query().query(p.query);
        if let Some(v) = p.limit {
            req = req.limit(v);
        }
        if let Some(v) = p.direction {
            req = req.direction(v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "search_logs"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Execute a native Query IR document (the structured, versioned query surface). Provide `query` as the IR JSON object. Returns the enveloped result scoped to your tenant."
    )]
    async fn query_ir(
        &self,
        Parameters(p): Parameters<QueryIrParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let request: signaldb_sdk::types::QueryIrRequest = serde_json::from_value(p.query)
            .map_err(|e| ErrorData::invalid_params(format!("invalid IR document: {e}"), None))?;
        let client = sdk_client_for(&parts, &self.router_base_url, p.dataset.as_deref());
        let resp = client
            .query_ir()
            .body(request)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "query_ir"))?;
        json_result(&resp.into_inner())
    }
}

impl McpServer {
    /// Whether a tool named `name` is registered. Exposed for cross-surface
    /// parity checks (see the `client-surface-parity` spec).
    pub fn has_tool(name: &str) -> bool {
        Self::tool_router().has_route(name)
    }
}

#[tool_handler]
impl ServerHandler for McpServer {}

/// Byte budget for a single tool result. A tool call must not blow an agent's
/// context window, so an oversized downstream result is not streamed verbatim.
const MAX_TOOL_PAYLOAD_BYTES: usize = 256 * 1024;

/// Serialize a value into a single-text-block tool result, bounded at
/// [`MAX_TOOL_PAYLOAD_BYTES`]. When the serialized result exceeds the budget,
/// the tool returns valid JSON marked `truncated` with a narrowing hint instead
/// of the oversized payload, so clients detect the cap from the flag.
fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ErrorData> {
    let text = serde_json::to_string(value)
        .map_err(|e| ErrorData::internal_error(format!("failed to serialize result: {e}"), None))?;
    if text.len() > MAX_TOOL_PAYLOAD_BYTES {
        let notice = serde_json::json!({
            "truncated": true,
            "bytes": text.len(),
            "limit_bytes": MAX_TOOL_PAYLOAD_BYTES,
            "hint": "Result exceeded the size cap; narrow the time range or lower `limit`, then retry.",
        });
        return Ok(CallToolResult::success(vec![ContentBlock::text(
            notice.to_string(),
        )]));
    }
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

/// Map a downstream router/SDK error onto an actionable MCP tool error, so
/// agents see "not found" / "invalid query" / "access denied" / "rate limited"
/// rather than an opaque transport failure.
fn map_sdk_err(err: signaldb_sdk::Error<()>, what: &str) -> ErrorData {
    match err.status().map(|s| s.as_u16()) {
        Some(400) | Some(422) | Some(501) => {
            ErrorData::invalid_params(format!("{what}: invalid request: {err}"), None)
        }
        Some(401) => ErrorData::invalid_request(
            format!("{what}: credential expired or was revoked; re-authenticate the session"),
            None,
        ),
        Some(403) => ErrorData::invalid_request(
            format!("{what}: access denied for the requested tenant/dataset"),
            None,
        ),
        Some(404) => ErrorData::resource_not_found(format!("{what}: not found"), None),
        Some(429) => {
            ErrorData::internal_error(format!("{what}: rate limited, retry shortly"), None)
        }
        _ => ErrorData::internal_error(format!("{what}: {err}"), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_tools_are_registered() {
        let router = McpServer::tool_router();
        for name in [
            "server_info",
            "search_traces",
            "get_trace",
            "discover_attributes",
            "query_metrics",
            "search_logs",
            "query_ir",
        ] {
            assert!(router.has_route(name), "tool `{name}` must be registered");
        }
    }
}
