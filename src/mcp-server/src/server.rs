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
//! - `get_profile` — single profile's flamegraph by ID (wraps the native
//!   Query IR `flamegraph` envelope)
//! - `discover_attributes` — queryable attribute/label names or values,
//!   signal-aware (`traces` via Tempo tags, `logs` via Loki labels,
//!   `metrics` via Prometheus labels)
//! - `discover_metrics` — distinct metric names for the tenant
//! - `query_metrics` — PromQL query (native Prometheus result)
//! - `search_logs` — LogQL query (native Loki result)
//! - `query_ir` — native Query IR document (structured query surface)
//! - `compact_run` / `compact_status` / `compact_dry_run` — operational
//!   compaction control (admin-authenticated)
//!
//! Raw SQL is served over Arrow Flight (gRPC) rather than the router HTTP API;
//! this server is an HTTP forwarder and holds no Flight client, so SQL stays a
//! CLI-only capability (see the `client-surface-parity` spec).
//!
//! `get_trace` additionally ships an interactive waterfall view, and
//! `get_profile` an interactive flamegraph view, via the MCP Apps extension;
//! see [`crate::apps`].
//!
//! Prompts (`prompts/list` / `prompts/get`, see [`crate::prompts`]) are
//! static, argument-only templates that seed an investigation using the
//! tools above — `investigate_trace`, `find_recent_errors`,
//! `build_promql_query`. `completion/complete` offers live autocompletion for
//! two of their arguments (`find_recent_errors`'s `service`, backed by Tempo
//! tag-value discovery, and `build_promql_query`'s `metric`, backed by
//! Prometheus label discovery); every other reference/argument pair returns
//! no suggestions rather than an error, since completions are advisory.

use axum::http::request::Parts;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::JsonSchema;
use rmcp::service::RequestContext;
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    handler::server::tool::Extension,
    model::{
        CacheScope, CallToolResult, CompleteRequestParams, CompleteResult, CompletionInfo,
        ContentBlock, GetPromptRequestParams, GetPromptResponse, ListPromptsResult,
        ListResourcesResult, ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams,
        ReadResourceResponse, ReadResourceResult, Reference, ServerCapabilities, ServerInfo,
    },
    tool, tool_handler, tool_router,
};
use serde::Deserialize;

use crate::apps;
use crate::prompts;
use crate::sdk_client_for;

/// The SignalDB MCP server handler. One instance is created per session by the
/// transport's service factory; it holds only the router base URL used to build
/// per-session forwarding clients — no credential of its own.
#[derive(Clone)]
pub struct McpServer {
    router_base_url: String,
    /// Overall timeout for each forwarded request, so a hung router fails the
    /// tool call instead of hanging it indefinitely (issue #885).
    router_timeout: std::time::Duration,
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

/// Parameters for `get_profile`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct GetProfileParams {
    /// Profile ID to fetch.
    profile_id: String,
    /// Optional start-of-range hint, unix seconds, to prune the scan.
    /// Defaults to 30 days before now.
    #[serde(default)]
    start: Option<i64>,
    /// Optional end-of-range hint, unix seconds, to prune the scan.
    /// Defaults to now.
    #[serde(default)]
    end: Option<i64>,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Which signal `discover_attributes` targets.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "lowercase")]
enum Signal {
    /// Tempo trace attributes (tags).
    #[default]
    Traces,
    /// Loki log labels.
    Logs,
    /// Prometheus metric labels.
    Metrics,
}

/// Parameters for `discover_attributes`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverAttributesParams {
    /// Which signal to discover attributes for: `traces` (default), `logs`,
    /// or `metrics`.
    #[serde(default)]
    signal: Signal,
    /// When set, returns the known values for this tag/label; when omitted,
    /// returns the list of queryable tag/label names.
    #[serde(default)]
    tag: Option<String>,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Parameters for `discover_metrics`.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverMetricsParams {
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
    #[schemars(schema_with = "query_ir_document_schema")]
    #[serde(deserialize_with = "deserialize_query_ir_document")]
    query: serde_json::Value,
    /// Dataset to query. Omit to use the session's default dataset.
    #[serde(default)]
    dataset: Option<String>,
}

/// Advertises `query` as a JSON object in the tool's schema. A bare
/// `serde_json::Value` renders with no `"type"` at all, and at least one MCP
/// client relies on the declared type to decide how to serialize a nested
/// argument — without it, it stringifies the document instead of sending it
/// as a nested object (issue #1113).
fn query_ir_document_schema(
    _generator: &mut rmcp::schemars::SchemaGenerator,
) -> rmcp::schemars::Schema {
    rmcp::schemars::json_schema!({ "type": "object" })
}

/// Accepts `query` as a native JSON object, and — as a fallback for clients
/// that stringify it despite the advertised object schema (issue #1113) — a
/// JSON-encoded string.
fn deserialize_query_ir_document<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(s) => serde_json::from_str(&s).map_err(serde::de::Error::custom),
        other => Ok(other),
    }
}

#[tool_router]
impl McpServer {
    /// Construct a handler that forwards to `router_base_url`, bounding each
    /// forwarded request by `router_timeout`.
    pub fn new(router_base_url: String, router_timeout: std::time::Duration) -> Self {
        Self {
            router_base_url,
            router_timeout,
        }
    }

    /// Build the per-request forwarding client, surfacing a construction
    /// failure as a clean MCP error instead of silently dropping the caller's
    /// credential headers.
    fn router_client(
        &self,
        parts: &Parts,
        dataset_override: Option<&str>,
    ) -> Result<signaldb_sdk::Client, ErrorData> {
        sdk_client_for(
            parts,
            &self.router_base_url,
            dataset_override,
            self.router_timeout,
        )
        .map_err(|e| ErrorData::internal_error(format!("failed to build router client: {e}"), None))
    }

    #[tool(
        description = "Report the SignalDB MCP server identity and the authenticated tenant/dataset for this session. Use this to confirm connectivity and which tenant your credential resolves to."
    )]
    async fn server_info(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let identity = self
            .router_client(&parts, None)?
            .whoami()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "server_info"))?
            .into_inner();
        let info = serde_json::json!({
            "server": "signaldb-mcp",
            "version": env!("CARGO_PKG_VERSION"),
            "tenant": identity.tenant.id,
            "dataset": identity.dataset,
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
        let client = self.router_client(&parts, p.dataset.as_deref())?;
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
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, p.dataset.as_deref())?;
        let mut req = client.query_single_trace().trace_id(p.trace_id);
        if let Some(v) = p.start {
            req = req.start(v);
        }
        if let Some(v) = p.end {
            req = req.end(v);
        }
        let resp = req.send().await.map_err(|e| map_sdk_err(e, "get_trace"))?;
        // The waterfall app renders from `structuredContent`, which the host
        // forwards to the iframe without adding it to the model's context. It
        // is attached only for UI-capable clients so a plain client is not sent
        // the same trace twice.
        json_result_for_app(&resp.into_inner(), client_supports_ui(&context))
    }

    #[tool(
        description = "Fetch a single profile's actual payload — its aggregated flamegraph (function names, per-level frame data, total/max-self sample values) — by `profile_id`, scoped to your tenant. Optional `start`/`end` (unix seconds) hints prune the scan; defaults to the last 30 days. Returns a not-found error when the profile does not exist."
    )]
    async fn get_profile(
        &self,
        Parameters(p): Parameters<GetProfileParams>,
        Extension(parts): Extension<Parts>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, p.dataset.as_deref())?;
        // The native Query IR's `flamegraph` envelope (profiles source only)
        // does the actual retrieval — this tool is a thin, single-ID wrapper
        // over the same `query_ir` path the generic tool exposes.
        let document = profile_flamegraph_document(&p.profile_id, p.start, p.end);
        let request: signaldb_sdk::types::QueryIrRequest = serde_json::from_value(document)
            .map_err(|e| ErrorData::internal_error(format!("failed to build query: {e}"), None))?;
        let resp = client
            .query_ir()
            .body(request)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "get_profile"))?;
        let flamegraph = flamegraph_or_not_found(resp.into_inner())?;
        // The flamegraph app renders from `structuredContent`, mirroring
        // `get_trace`'s waterfall.
        json_result_for_app(&flamegraph, client_supports_ui(&context))
    }

    #[tool(
        description = "Discover queryable attributes for your tenant. Call with no arguments to list trace tag names; pass `tag` to list the known values for that tag. Pass `signal: \"logs\"` or `signal: \"metrics\"` to discover Loki log labels or Prometheus metric labels instead. Use this to construct valid `search_traces`/`search_logs`/`query_metrics` queries."
    )]
    async fn discover_attributes(
        &self,
        Parameters(p): Parameters<DiscoverAttributesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, p.dataset.as_deref())?;
        match (p.signal, p.tag) {
            (Signal::Traces, Some(tag)) => {
                let resp = client
                    .search_tag_values()
                    .tag_name(tag)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Traces, None) => {
                let resp = client
                    .search_tags()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Logs, Some(name)) => {
                let resp = client
                    .logql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Logs, None) => {
                let resp = client
                    .logql_labels()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Metrics, Some(name)) => {
                let resp = client
                    .promql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Metrics, None) => {
                let resp = client
                    .promql_labels()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
        }
    }

    #[tool(
        description = "Discover metric names for your tenant. Returns the distinct metric names visible via PromQL (backed by Prometheus label discovery on `__name__`). Use this to construct valid `query_metrics` queries."
    )]
    async fn discover_metrics(
        &self,
        Parameters(p): Parameters<DiscoverMetricsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, p.dataset.as_deref())?;
        let resp = client
            .promql_label_values()
            .name("__name__")
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "discover_metrics"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Query metrics with PromQL. Provide `query` as a PromQL expression (e.g. `rate(http_requests_total[5m])`) and optionally `time` (unix seconds or RFC3339). Returns the native Prometheus result scoped to your tenant."
    )]
    async fn query_metrics(
        &self,
        Parameters(p): Parameters<QueryMetricsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, p.dataset.as_deref())?;
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
        let client = self.router_client(&parts, p.dataset.as_deref())?;
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
        let client = self.router_client(&parts, p.dataset.as_deref())?;
        let resp = client
            .query_ir()
            .body(request)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "query_ir"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Trigger a compaction pass now (operational control). Requires administrative credentials. Returns the run summary."
    )]
    async fn compact_run(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .ops_compact()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "compact_run"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Show active compaction leases and metrics (operational control). Requires administrative credentials."
    )]
    async fn compact_status(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .ops_compact_status()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "compact_status"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Plan compaction candidates without executing (read-only preview; operational control). Requires administrative credentials."
    )]
    async fn compact_dry_run(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .ops_compact_dry_run()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "compact_dry_run"))?;
        json_result(&resp.into_inner())
    }
}

impl McpServer {
    /// Whether a tool named `name` is registered. Exposed for cross-surface
    /// parity checks (see the `client-surface-parity` spec).
    pub fn has_tool(name: &str) -> bool {
        Self::tool_router().has_route(name)
    }

    /// Argument autocompletion, forwarding to the router when `parts` carries
    /// a credential. Every failure mode — no credential available (e.g. a
    /// transport that never attaches one), an unrecognized reference or
    /// argument, or a downstream error — degrades to an empty completion list
    /// rather than an error: completions are advisory, so a typeahead miss
    /// must never surface as a JSON-RPC error to the user still typing.
    async fn complete_impl(
        &self,
        request: CompleteRequestParams,
        parts: Option<Parts>,
    ) -> CompleteResult {
        let Reference::Prompt(prompt_ref) = &request.r#ref else {
            return CompleteResult::default();
        };
        let Some(source) =
            CompletionSource::for_prompt_argument(&prompt_ref.name, &request.argument.name)
        else {
            return CompleteResult::default();
        };
        let Some(parts) = parts else {
            return CompleteResult::default();
        };
        let Ok(client) = self.router_client(&parts, None) else {
            return CompleteResult::default();
        };

        let values = match source.fetch(&client).await {
            Ok(values) => values,
            Err(error) => {
                tracing::warn!(
                    %error,
                    argument = %request.argument.name,
                    "completion lookup failed; returning no suggestions"
                );
                return CompleteResult::default();
            }
        };

        let prefix = request.argument.value.as_str();
        let matches: Vec<String> = values
            .into_iter()
            .filter(|v| v.starts_with(prefix))
            .take(CompletionInfo::MAX_VALUES)
            .collect();
        match CompletionInfo::with_all_values(matches) {
            Ok(info) => CompleteResult::new(info),
            // Unreachable: bounded by `take(MAX_VALUES)` above.
            Err(_) => CompleteResult::default(),
        }
    }
}

/// A live data source [`McpServer::complete_impl`] can query for a prompt
/// argument's suggestions.
enum CompletionSource {
    /// `find_recent_errors`'s `service` argument — Tempo `service.name` tag values.
    ServiceName,
    /// `build_promql_query`'s `metric` argument — Prometheus `__name__` label values.
    MetricName,
}

impl CompletionSource {
    /// The source for a given prompt name + argument name, or `None` when
    /// this server has no live data for that pair.
    fn for_prompt_argument(prompt_name: &str, argument_name: &str) -> Option<Self> {
        match (prompt_name, argument_name) {
            ("find_recent_errors", "service") => Some(Self::ServiceName),
            ("build_promql_query", "metric") => Some(Self::MetricName),
            _ => None,
        }
    }

    async fn fetch(
        &self,
        client: &signaldb_sdk::Client,
    ) -> Result<Vec<String>, signaldb_sdk::Error<()>> {
        match self {
            Self::ServiceName => {
                let resp = client
                    .search_tag_values()
                    .tag_name("service.name")
                    .send()
                    .await?;
                Ok(resp.into_inner().tag_values)
            }
            Self::MetricName => {
                let resp = client.promql_label_values().name("__name__").send().await?;
                let values = resp
                    .into_inner()
                    .get("data")
                    .and_then(|d| d.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(values)
            }
        }
    }
}

/// SEP-2549 cache TTL for `tools/list`: short-lived because `_meta.ui` varies
/// with the connecting client's negotiated capabilities.
const TOOL_LIST_CACHE_TTL_MS: u64 = 30_000;

/// SEP-2549 cache TTL for the compiled-in, client-independent UI resources
/// (`resources/list` and `resources/read`).
const STATIC_RESOURCE_CACHE_TTL_MS: u64 = 3_600_000;

/// Whether the client on this request negotiated the MCP Apps extension.
///
/// `peer_info` is `None` before `initialize` completes; treat that as no UI,
/// which is the conservative answer (plain text works everywhere).
fn client_supports_ui(context: &RequestContext<RoleServer>) -> bool {
    context
        .peer
        .peer_info()
        .is_some_and(|info| apps::client_supports_ui(&info.capabilities))
}

#[tool_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        // `resources` is advertised because the MCP Apps UI documents are
        // served over `resources/read`; this server exposes no data resources.
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .enable_completions()
                .build(),
        )
        .with_instructions(
            "Query SignalDB traces, logs, and metrics for the authenticated tenant. \
             Call `server_info` first to confirm which tenant your credential resolves to. \
             Clients that negotiate the MCP Apps extension render `get_trace` results as an \
             interactive waterfall and `get_profile` results as an interactive flamegraph. \
             `prompts/list` offers ready-made investigation templates.",
        )
    }

    /// List tools, attaching `_meta.ui.resourceUri` to UI-backed tools when the
    /// client negotiated the MCP Apps extension. Clients that did not ask for
    /// apps get exactly the tool surface they got before.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = Self::tool_router().list_all();
        if client_supports_ui(&context) {
            for tool in &mut tools {
                if let Some(uri) = apps::tool_ui_uri(&tool.name) {
                    tool.meta = Some(apps::tool_ui_meta(uri));
                }
            }
        }
        // SEP-2549 (protocol 2026-07-28) requires `ttlMs`/`cacheScope` on this
        // result; rmcp leaves them optional for older clients, but a
        // conformant 2026-07-28 client rejects a response that omits them.
        // `_meta.ui.resourceUri` above depends on the connecting client's
        // negotiated capabilities, so this response is not safe for an
        // intermediary to serve to a different client — mark it private.
        Ok(ListToolsResult::with_all_items(tools)
            .with_ttl_ms(TOOL_LIST_CACHE_TTL_MS)
            .with_cache_scope(CacheScope::Private))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        // Static, compiled-in UI apps — identical for every client, so a long
        // TTL and public scope are safe. See the `list_tools` comment for why
        // these fields must be set at all (SEP-2549).
        Ok(ListResourcesResult::with_all_items(apps::ui_resources())
            .with_ttl_ms(STATIC_RESOURCE_CACHE_TTL_MS)
            .with_cache_scope(CacheScope::Public))
    }

    /// Serve a UI app document. The only resources this server holds are the
    /// compiled-in `ui://` apps — anything else is a not-found.
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        match apps::read_ui_resource(&request.uri) {
            Some(contents) => Ok(ReadResourceResult::new(vec![contents])
                .with_ttl_ms(STATIC_RESOURCE_CACHE_TTL_MS)
                .with_cache_scope(CacheScope::Public)
                .into()),
            None => Err(ErrorData::resource_not_found(
                format!("no resource at `{}`", request.uri),
                None,
            )),
        }
    }

    /// List the static prompt catalog; see [`crate::prompts`].
    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        // Static, compiled-in templates — identical for every client, so a
        // long TTL and public scope are safe, same as `list_resources`.
        Ok(ListPromptsResult::with_all_items(prompts::list())
            .with_ttl_ms(STATIC_RESOURCE_CACHE_TTL_MS)
            .with_cache_scope(CacheScope::Public))
    }

    /// Render a prompt template. Rendering is pure argument substitution (no
    /// router call), so this works even before this session's router
    /// credential has been validated.
    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        Ok(prompts::get(&request.name, request.arguments)?.into())
    }

    /// Argument autocompletion. See [`Self::complete_impl`] for the actual
    /// logic — this override only threads the caller's forwarding credential
    /// through from the request context.
    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let parts = context.extensions.get::<Parts>().cloned();
        Ok(self.complete_impl(request, parts).await)
    }
}

/// Byte budget for a single tool result. A tool call must not blow an agent's
/// context window, so an oversized downstream result is not streamed verbatim.
const MAX_TOOL_PAYLOAD_BYTES: usize = 256 * 1024;

/// Serialize a value into a single-text-block tool result, bounded at
/// [`MAX_TOOL_PAYLOAD_BYTES`]. When the serialized result exceeds the budget,
/// the tool returns valid JSON marked `truncated` with a narrowing hint instead
/// of the oversized payload, so clients detect the cap from the flag.
fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ErrorData> {
    json_result_for_app(value, false)
}

/// [`json_result`], additionally attaching the value as `structuredContent`
/// when `with_structured` is set.
///
/// A UI-capable host forwards `structuredContent` to the app's iframe without
/// adding it to the model's context, so the app gets typed data while the text
/// block stays the model's (and every other client's) view of the result. The
/// same size cap governs both: an oversized result carries neither.
fn json_result_for_app<T: serde::Serialize>(
    value: &T,
    with_structured: bool,
) -> Result<CallToolResult, ErrorData> {
    let json = serde_json::to_value(value)
        .map_err(|e| ErrorData::internal_error(format!("failed to serialize result: {e}"), None))?;
    let text = json.to_string();
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
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    if with_structured {
        result.structured_content = Some(json);
    }
    Ok(result)
}

/// Build the Query IR document `get_profile` submits: a `flamegraph`-enveloped
/// `profiles` query filtered to one `profile.id`, defaulting to the last 30
/// days when no `start`/`end` hint is given. Pure and synchronous, so it's
/// directly unit-testable without a router/session.
fn profile_flamegraph_document(
    profile_id: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> serde_json::Value {
    let range_from = start
        .map(|secs| secs.saturating_mul(1_000_000_000).to_string())
        .unwrap_or_else(|| "now-30d".to_string());
    let range_to = end
        .map(|secs| secs.saturating_mul(1_000_000_000).to_string())
        .unwrap_or_else(|| "now".to_string());
    serde_json::json!({
        "irVersion": 1,
        "from": "profiles",
        "range": { "from": range_from, "to": range_to },
        "result": "flamegraph",
        "pipeline": [
            { "where": { "field": "profile.id", "op": "eq", "value": profile_id } }
        ]
    })
}

/// Extract the flamegraph from a `get_profile` query response, or a clean
/// "not found" error when no profile matched. `flamegraph` is `Some` for
/// every `result: "flamegraph"` response, including a zero-match one (see
/// the router's `QueryIrResponse.flamegraph` doc comment) — `get_profile`
/// filters on exactly one `profile.id`, so an empty flamegraph (`names`
/// carries nothing) means that id matched nothing, not that the field was
/// omitted. The generic `flamegraph` envelope has no dedicated 404 of its
/// own; this tool's not-found signal is its own interpretation of "empty",
/// specific to a single-ID lookup.
fn flamegraph_or_not_found(
    response: signaldb_sdk::types::QueryIrResponse,
) -> Result<signaldb_sdk::types::FlamegraphResult, ErrorData> {
    match response.flamegraph {
        Some(flamegraph) if !flamegraph.names.is_empty() => Ok(flamegraph),
        _ => Err(ErrorData::resource_not_found(
            "get_profile: not found".to_string(),
            None,
        )),
    }
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
    use axum::http::header::AUTHORIZATION;
    use axum::http::request::Builder as RequestBuilder;

    // profile-payload-access task 4.1 — `get_profile`'s pure request/response
    // logic, tested directly since it needs no router/session: the tool
    // method itself (`Extension<Parts>` + `RequestContext`) can only be
    // exercised through a real transport, which the in-memory duplex
    // transport used elsewhere in this crate's tests does not populate with
    // HTTP `Parts` — the same limitation `get_trace` has (it has no
    // live-invocation test either). `profile_flamegraph_document` and
    // `flamegraph_or_not_found` are extracted specifically so the request
    // shape and the not-found branch are still directly verifiable.

    #[test]
    fn profile_flamegraph_document_defaults_to_the_last_30_days() {
        let doc = profile_flamegraph_document("abc123", None, None);
        assert_eq!(doc["range"]["from"], "now-30d");
        assert_eq!(doc["range"]["to"], "now");
        assert_eq!(doc["from"], "profiles");
        assert_eq!(doc["result"], "flamegraph");
    }

    #[test]
    fn profile_flamegraph_document_converts_start_end_hints_to_nanoseconds() {
        let doc = profile_flamegraph_document("abc123", Some(10), Some(20));
        assert_eq!(doc["range"]["from"], "10000000000");
        assert_eq!(doc["range"]["to"], "20000000000");
    }

    #[test]
    fn profile_flamegraph_document_filters_by_profile_id() {
        let doc = profile_flamegraph_document("abc123", None, None);
        let leaf = &doc["pipeline"][0]["where"];
        assert_eq!(leaf["field"], "profile.id");
        assert_eq!(leaf["op"], "eq");
        assert_eq!(leaf["value"], "abc123");
    }

    fn query_ir_response(body: serde_json::Value) -> signaldb_sdk::types::QueryIrResponse {
        serde_json::from_value(body).expect("response parses")
    }

    #[test]
    fn flamegraph_or_not_found_returns_the_flamegraph_when_present() {
        let response = query_ir_response(serde_json::json!({
            "result": "flamegraph",
            "window": { "start_ns": 0, "end_ns": 1 },
            "flamegraph": {
                "names": ["main"], "levels": [[0, 10, 10, 0]],
                "total": 10, "max_self": 10, "truncated": false
            }
        }));
        let flamegraph = flamegraph_or_not_found(response).expect("flamegraph is present");
        assert_eq!(flamegraph.total, 10);
        assert_eq!(flamegraph.names, vec!["main".to_string()]);
    }

    /// The realistic not-found case: the router always sets `flamegraph:
    /// Some(..)` for a `result: "flamegraph"` response, even when the
    /// filtered `profile.id` matched nothing — so `Some` with empty `names`
    /// is what `get_profile` actually sees for a missing profile, not `None`.
    #[test]
    fn flamegraph_or_not_found_errors_when_the_flamegraph_is_empty() {
        let response = query_ir_response(serde_json::json!({
            "result": "flamegraph",
            "window": { "start_ns": 0, "end_ns": 1 },
            "flamegraph": {
                "names": [], "levels": [], "total": 0, "max_self": 0, "truncated": false
            }
        }));
        let err = flamegraph_or_not_found(response).expect_err("empty flamegraph means not found");
        assert!(err.message.contains("not found"), "got {}", err.message);
    }

    /// Defensive case: the field is absent entirely (e.g. an older or
    /// malformed response). Still treated as not-found rather than a panic.
    #[test]
    fn flamegraph_or_not_found_errors_when_the_field_is_absent() {
        let response = query_ir_response(serde_json::json!({
            "result": "flamegraph",
            "window": { "start_ns": 0, "end_ns": 1 }
        }));
        let err = flamegraph_or_not_found(response).expect_err("no flamegraph means not found");
        assert!(err.message.contains("not found"), "got {}", err.message);
    }

    #[tokio::test]
    async fn server_info_rejects_a_credential_the_router_rejects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(request_len > 0, "mock router received an empty request");
            socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write rejection");
        });
        let parts = RequestBuilder::new()
            .header(AUTHORIZATION, "Bearer expired-token")
            .body(())
            .expect("build request")
            .into_parts()
            .0;
        let server = McpServer::new(format!("http://{addr}"), std::time::Duration::from_secs(1));

        let error = server
            .server_info(Extension(parts))
            .await
            .expect_err("server_info must reject a credential rejected by the router");

        assert!(
            error.message.contains("credential expired or was revoked"),
            "unexpected error: {}",
            error.message
        );
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn server_info_reports_identity_resolved_by_the_router() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..request_len])
                    .expect("request is UTF-8")
                    .starts_with("GET /api/v1/whoami "),
                "server_info must validate through the router whoami endpoint"
            );
            let body = b"{\"user_id\":\"user-a\",\"tenant\":{\"id\":\"acme\",\"slug\":\"acme\",\"name\":\"Acme\"},\"dataset\":\"production\"}";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response headers");
            socket.write_all(body).await.expect("write identity");
        });
        let parts = RequestBuilder::new()
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(())
            .expect("build request")
            .into_parts()
            .0;
        let server = McpServer::new(format!("http://{addr}"), std::time::Duration::from_secs(1));

        let result = server
            .server_info(Extension(parts))
            .await
            .expect("router accepted credential");

        let ContentBlock::Text(text) = &result.content[0] else {
            panic!("server_info returns a text result");
        };
        let identity: serde_json::Value =
            serde_json::from_str(&text.text).expect("server_info returns JSON");
        assert_eq!(identity["tenant"], "acme");
        assert_eq!(identity["dataset"], "production");
        router.await.expect("mock router task panicked");
    }

    /// Text is what the model (and every non-UI client) reads, so it is present
    /// either way; `structuredContent` is what the app renders from, so it
    /// appears only when the caller negotiated apps. Sending both to a client
    /// that cannot use the second would put the trace in its context twice.
    #[test]
    fn structured_content_is_attached_only_for_ui_clients() {
        let trace = serde_json::json!({ "traceID": "abc", "durationMs": 24 });

        let with_ui = json_result_for_app(&trace, true).expect("serializes");
        assert_eq!(
            with_ui.structured_content.as_ref().map(|v| &v["traceID"]),
            Some(&serde_json::json!("abc"))
        );
        assert!(
            !with_ui.content.is_empty(),
            "the text block always survives"
        );

        let without_ui = json_result_for_app(&trace, false).expect("serializes");
        assert!(without_ui.structured_content.is_none());
        assert!(!without_ui.content.is_empty());
    }

    /// The size cap governs both representations: an oversized result must not
    /// smuggle the full payload through `structuredContent`.
    #[test]
    fn oversized_result_carries_neither_representation() {
        let bulky = serde_json::json!({ "blob": "x".repeat(MAX_TOOL_PAYLOAD_BYTES + 1) });

        let result = json_result_for_app(&bulky, true).expect("serializes");
        assert!(
            result.structured_content.is_none(),
            "the cap must apply to structuredContent too"
        );
        let ContentBlock::Text(text) = &result.content[0] else {
            panic!("the truncation notice is a text block");
        };
        let notice: serde_json::Value =
            serde_json::from_str(&text.text).expect("the notice is valid JSON");
        assert_eq!(notice["truncated"], true);
    }

    #[test]
    fn read_tools_are_registered() {
        let router = McpServer::tool_router();
        for name in [
            "server_info",
            "search_traces",
            "get_trace",
            "discover_attributes",
            "discover_metrics",
            "query_metrics",
            "search_logs",
            "query_ir",
            "compact_run",
            "compact_status",
            "compact_dry_run",
        ] {
            assert!(router.has_route(name), "tool `{name}` must be registered");
        }
    }

    #[test]
    fn generic_query_ir_tool_parameters_accept_a_v2_heatmap_document() {
        let params: QueryIrParams = serde_json::from_value(serde_json::json!({
            "query": {
                "irVersion": 2, "from": "traces", "range": { "from": "now-1h", "to": "now" },
                "result": "heatmap", "pipeline": [{ "heatmap": {
                    "x": { "step": "1m", "align": "epoch" },
                    "y": { "of": "duration", "bounds": ["1ms"], "overflow": true },
                    "value": { "fn": "count", "as": "count" }
                }}]
            }
        }))
        .unwrap();
        let request: signaldb_sdk::types::QueryIrRequest =
            serde_json::from_value(params.query).unwrap();
        assert_eq!(request.ir_version, 2);
        assert_eq!(request.result, "heatmap");
        assert!(
            request.pipeline.len() == 1 && request.pipeline[0].contains_key("heatmap"),
            "the heatmap stage must survive the conversion: {:?}",
            request.pipeline
        );
    }

    #[test]
    fn generic_query_ir_tool_parameters_accept_a_profiles_document() {
        let params: QueryIrParams = serde_json::from_value(serde_json::json!({
            "query": {
                "irVersion": 1, "from": "profiles",
                "range": { "from": "now-1h", "to": "now" },
                "result": "rows", "pipeline": []
            }
        }))
        .unwrap();
        let request: signaldb_sdk::types::QueryIrRequest =
            serde_json::from_value(params.query).unwrap();
        assert_eq!(request.from, "profiles");
    }

    #[test]
    fn query_ir_tool_parameters_accept_a_json_encoded_string_query() {
        // Some MCP clients stringify nested-object arguments when the
        // advertised schema doesn't declare an explicit type (issue #1113).
        // The tool must still accept the document in that shape.
        let ir_document = serde_json::json!({
            "irVersion": 1, "from": "traces",
            "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": []
        });
        let params: QueryIrParams = serde_json::from_value(serde_json::json!({
            "query": ir_document.to_string()
        }))
        .unwrap();
        assert_eq!(params.query, ir_document);
    }

    #[test]
    fn query_ir_tool_schema_declares_query_as_an_object() {
        // A bare `serde_json::Value` field renders with no "type" at all,
        // which is what let a strict MCP client stringify the argument in
        // the first place (issue #1113).
        let schema = rmcp::schemars::schema_for!(QueryIrParams);
        let query_type = schema
            .pointer("/properties/query/type")
            .and_then(|t| t.as_str());
        assert_eq!(query_type, Some("object"));
    }

    // `complete_impl` is exercised directly (like `server_info` above) rather
    // than through a client<->server transport: the crate's in-memory duplex
    // test transport carries no HTTP layer, so nothing would ever populate
    // the `Extension<Parts>` a router-forwarding completion needs. See
    // `tests/prompts_and_completions.rs` for the completions that need no
    // credential, which *are* tested over a real transport.

    #[tokio::test]
    async fn completion_suggests_matching_service_names() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..request_len])
                    .expect("request is UTF-8")
                    .starts_with("GET /tempo/api/search/tag/service.name/values "),
                "must query Tempo tag values for service.name"
            );
            let body = br#"{"tagValues":["checkout","checkout-worker","payments"]}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response headers");
            socket.write_all(body).await.expect("write body");
        });
        let parts = RequestBuilder::new()
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(())
            .expect("build request")
            .into_parts()
            .0;
        let server = McpServer::new(format!("http://{addr}"), std::time::Duration::from_secs(1));

        let result = server
            .complete_impl(
                rmcp::model::CompleteRequestParams::new(
                    Reference::for_prompt("find_recent_errors"),
                    rmcp::model::ArgumentInfo::new("service", "checkout"),
                ),
                Some(parts),
            )
            .await;

        assert_eq!(
            result.completion.values,
            vec!["checkout", "checkout-worker"]
        );
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn completion_suggests_matching_metric_names() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..request_len])
                    .expect("request is UTF-8")
                    .starts_with("GET /prometheus/api/v1/label/__name__/values "),
                "must query Prometheus label values for __name__"
            );
            let body =
                br#"{"status":"success","data":["http_requests_total","http_request_duration_seconds"]}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response headers");
            socket.write_all(body).await.expect("write body");
        });
        let parts = RequestBuilder::new()
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(())
            .expect("build request")
            .into_parts()
            .0;
        let server = McpServer::new(format!("http://{addr}"), std::time::Duration::from_secs(1));

        let result = server
            .complete_impl(
                rmcp::model::CompleteRequestParams::new(
                    Reference::for_prompt("build_promql_query"),
                    rmcp::model::ArgumentInfo::new("metric", "http_request"),
                ),
                Some(parts),
            )
            .await;

        assert_eq!(
            result.completion.values,
            vec!["http_requests_total", "http_request_duration_seconds"]
        );
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn completion_is_empty_without_a_credential() {
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(1),
        );

        let result = server
            .complete_impl(
                rmcp::model::CompleteRequestParams::new(
                    Reference::for_prompt("build_promql_query"),
                    rmcp::model::ArgumentInfo::new("metric", "http"),
                ),
                None,
            )
            .await;

        assert!(result.completion.values.is_empty());
    }

    #[tokio::test]
    async fn completion_degrades_to_empty_when_the_router_is_unreachable() {
        let parts = RequestBuilder::new()
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(())
            .expect("build request")
            .into_parts()
            .0;
        // Port 1 is a reserved, never-listening port.
        let server = McpServer::new(
            "http://127.0.0.1:1".to_string(),
            std::time::Duration::from_secs(1),
        );

        let result = server
            .complete_impl(
                rmcp::model::CompleteRequestParams::new(
                    Reference::for_prompt("build_promql_query"),
                    rmcp::model::ArgumentInfo::new("metric", "http"),
                ),
                Some(parts),
            )
            .await;

        assert!(
            result.completion.values.is_empty(),
            "a downstream failure must degrade to no suggestions, not an error"
        );
    }
}
