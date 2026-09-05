//! # MCP server handler
//!
//! Exposes SignalDB's read/query surface over the Model Context Protocol. Tools
//! are thin wrappers over the generated [`signaldb_sdk`] query methods,
//! forwarding the caller's credential (the handler holds no key of its own).
//!
//! Tools (every authenticated tenant session, no role gating):
//! - `server_info` — connectivity + resolved tenant
//! - `connection_info` — this deployment's public ingest/query endpoints,
//!   headers, required API-key scopes, and ready-to-paste OTel env vars
//! - `discover_datasets` — the tenant and datasets your credential can
//!   access, as a nested Markdown list, marking the session's current
//!   default dataset
//! - `search_traces` — TraceQL search
//! - `get_trace` — single trace by ID
//! - `get_profile` — single profile's flamegraph by ID (wraps the native
//!   Query IR `flamegraph` envelope)
//! - `discover_attributes` — queryable attribute/label names or values,
//!   signal-aware (`traces` via Tempo tags, `logs` via Loki labels,
//!   `metrics` via Prometheus labels)
//! - `discover_metrics` — distinct metric names for the tenant
//! - `query_metrics` — PromQL query (native Prometheus result), instant or
//!   range (`start`/`end`/`step`)
//! - `search_logs` — LogQL query (native Loki result), instant or range
//! - `query_ir` — native Query IR document (structured query surface)
//! - `compact_run` / `compact_status` / `compact_dry_run` — operational
//!   compaction control (admin-authenticated)
//! - `list_schema_registries`, `get_schema_registry`, `resolve_attribute` /
//!   `resolve_entity` / `resolve_metric`, `search_schema` — schema-registry
//!   lookup: what an attribute key, entity type, or metric name *means*,
//!   precedence-ordered across the tenant's visible registries (custom →
//!   signaldb → otel), so a model can learn the vocabulary before building a
//!   query
//! - `create_schema_registry` / `replace_schema_registry` /
//!   `delete_schema_registry` / `validate_schema_registry` — custom-registry
//!   management (`schema:write`)
//!
//! Read/discovery tools also take an optional `tenant` argument (alongside
//! the existing optional `dataset` where applicable): a confirmation check
//! against the tenant the auth middleware already resolved for the request,
//! not a way to target a different tenant — one MCP session, and the
//! credential behind it, is permanently bound to exactly one tenant (see
//! `mcp_auth_middleware` in `lib.rs`). A mismatch fails the call before any
//! request reaches the router; call `discover_datasets` first if unsure.
//!
//! Management tools come in two families that differ only in which
//! credential the router expects (design D1); neither is hidden from
//! `tools/list` — a call the router does not authorize returns a clean
//! access-denied error:
//! - **Platform-admin** (unprefixed, admin API, requires the administrative
//!   credential): `list_tenants` / `get_tenant` / `create_tenant` /
//!   `update_tenant` / `delete_tenant`, `create_user`, `list_api_keys` /
//!   `create_api_key` / `update_api_key_scopes` / `revoke_api_key`,
//!   `list_datasets` / `create_dataset` / `delete_dataset`. Keys carry
//!   explicit scopes from the shared vocabulary (`common::auth::API_KEY_SCOPES`).
//! - **Tenant self-management** (`tenant_`-prefixed, acts as the caller's own
//!   identity within its tenant). Two sub-groups by transport:
//!   - Tenant tables/schemas (tenant self-service API, works with a plain
//!     tenant API key — the CLI's `tenant table` group reaches these too):
//!     `tenant_list_tables` / `tenant_create_tables` /
//!     `tenant_list_table_schemas`, and the
//!     tenant-scoped-but-credential-agnostic `list_available_table_schemas`.
//!   - The tenant self view (`tenant_info`, tenant self-service API, any
//!     valid key of the tenant — the CLI's `tenant show`).
//!   - Datasets/API-keys/memberships/schema (management API,
//!     `authorize_tenant`-gated): `tenant_list_datasets` /
//!     `tenant_create_dataset` / `tenant_delete_dataset`,
//!     `tenant_list_api_keys` / `tenant_create_api_key` /
//!     `tenant_update_api_key` / `tenant_revoke_api_key`,
//!     `tenant_list_memberships` / `tenant_upsert_membership` /
//!     `tenant_remove_membership`, `tenant_get_schema`. The router accepts
//!     a human principal (browser session or OAuth access token) holding
//!     the tenant-admin role or instance-admin flag, or an API key that
//!     explicitly carries the `tenant:manage` scope. Ingest-only keys and
//!     legacy unscoped keys get a clean access-denied error (management is
//!     opt-in; `router::endpoints::management::authorize_tenant`). The
//!     CLI's `tenant dataset|api-key|membership|schema` verbs reach the
//!     same endpoints — see `signaldb_cli::commands::tenant_self`.
//!
//! Tools that delete or revoke carry the MCP destructive annotation and
//! require a `confirm` argument equal to the identifier being destroyed;
//! read-only tools carry the read-only annotation. `create_api_key` /
//! `tenant_create_api_key` return key material exactly once, in that
//! response; the `list_*` tools never return key material.
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
use crate::audit::{
    self, AuditContext, DEFAULT_MAX_CONCURRENT_TOOL_CALLS, Outcome, PERMIT_WAIT,
    concurrency_limit_error, deadline_exceeded_error, with_http_status,
};
use crate::prompts;
use crate::sdk_client_for;

/// The SignalDB MCP server handler. One instance is created per session by the
/// transport's service factory; it holds only the router base URL used to build
/// per-session forwarding clients — no credential of its own — plus that
/// session's tool-call permits.
#[derive(Clone)]
pub struct McpServer {
    router_base_url: String,
    /// Overall timeout for each forwarded request, so a hung router fails the
    /// tool call instead of hanging it indefinitely (issue #885).
    router_timeout: std::time::Duration,
    /// Bound on tool calls in flight for this session. One handler instance
    /// serves one session (the transport builds a fresh one per session; stdio
    /// has exactly one), so a per-instance semaphore is a per-session bound
    /// that is dropped with the session — no registry to evict from.
    tool_permits: std::sync::Arc<tokio::sync::Semaphore>,
    max_concurrent_tool_calls: usize,
    /// Total deadline for one `tools/call`, defaulting to
    /// [`crate::tool_call_deadline`] of `router_timeout`; see
    /// [`Self::with_tool_call_deadline`].
    tool_call_deadline: std::time::Duration,
}

/// Parameters for `search_traces`.
#[derive(Debug, Deserialize, JsonSchema)]
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
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`. The router validates access; an inaccessible
    /// dataset returns an access-denied error.
    dataset: String,
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
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `list_api_keys`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ListApiKeysParams {
    /// Tenant whose keys to list.
    tenant_id: String,
}

/// Parameters for `create_api_key`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct CreateApiKeyParams {
    /// Tenant the key belongs to.
    tenant_id: String,
    /// Optional human-readable key name.
    #[serde(default)]
    name: Option<String>,
    /// Scopes the key carries (required, at least one). Vocabulary:
    /// `metrics:write`, `logs:write`, `traces:write`, `profiles:write`,
    /// `traces:read`, `logs:read`, `metrics:read`, `profiles:read`,
    /// `schema:read`, `schema:write`, `tenant:manage` (manage the key's own
    /// tenant — datasets, API keys, memberships, schema — through the
    /// management API; explicit only, never implied by an unscoped key).
    scopes: Vec<String>,
    /// Dataset set the key is restricted to (non-empty; a bare empty array
    /// is rejected). Omitted or `null` creates an unrestricted key.
    #[serde(default)]
    dataset_ids: Option<Vec<String>>,
}

/// Parameters for `update_api_key_scopes`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct UpdateApiKeyScopesParams {
    /// Tenant the key belongs to.
    tenant_id: String,
    /// Key to update (the `id` from `list_api_keys` / `create_api_key`).
    key_id: String,
    /// Replacement scope list (non-empty). Omit to keep the current scopes.
    #[serde(default)]
    scopes: Option<Vec<String>>,
    /// Replacement dataset set (non-empty; a bare empty array is rejected).
    /// Omit to keep the current restriction. Mutually exclusive with
    /// `clear_dataset_restriction: true`.
    #[serde(default)]
    dataset_ids: Option<Vec<String>>,
    /// Clear an existing dataset restriction back to unrestricted. Must not
    /// be combined with a non-empty `dataset_ids`.
    #[serde(default)]
    clear_dataset_restriction: bool,
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
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
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
    /// Pyroscope profile labels.
    Profiles,
}

/// Parameters for `discover_profile_types`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverProfileTypesParams {
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`.
    /// Omit for the server's default window.
    #[serde(default)]
    from: Option<String>,
    /// Range end, same forms as `from`.
    #[serde(default)]
    until: Option<String>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `search_profiles`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct SearchProfilesParams {
    /// Pyroscope selector, e.g.
    /// `process_cpu:cpu:nanoseconds{service_name="checkout"}`.
    query: String,
    /// Range start: unix seconds, unix milliseconds, or `now[-<N><s|m|h|d>]`.
    #[serde(default)]
    from: Option<String>,
    /// Range end, same forms as `from`.
    #[serde(default)]
    until: Option<String>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `compare_profiles`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct CompareProfilesParams {
    /// Pyroscope selector shared by both ranges.
    query: String,
    /// Baseline range start.
    #[serde(default)]
    left_from: Option<String>,
    /// Baseline range end.
    #[serde(default)]
    left_until: Option<String>,
    /// Comparison range start.
    #[serde(default)]
    right_from: Option<String>,
    /// Comparison range end.
    #[serde(default)]
    right_until: Option<String>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `profiles_for_trace`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ProfilesForTraceParams {
    /// Trace ID to fetch correlated profiles for.
    trace_id: String,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `discover_attributes`.
#[derive(Debug, Deserialize, JsonSchema)]
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
    /// Restrict trace tag discovery to one scope (`resource`, `span`, or
    /// `intrinsic`), routing through the Tempo v2 discovery endpoints
    /// instead of v1. Only valid with `signal: "traces"`.
    #[serde(default)]
    scope: Option<TraceTagScope>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Trace tag scope for `discover_attributes` v2 routing (`signal: "traces"`
/// only). `rename_all = "lowercase"` matches the Tempo v2 wire values (see
/// `tempo_api::TagScope`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "lowercase")]
enum TraceTagScope {
    Resource,
    Span,
    Intrinsic,
}

impl TraceTagScope {
    fn into_sdk(self) -> signaldb_sdk::types::TagScope {
        match self {
            TraceTagScope::Resource => signaldb_sdk::types::TagScope::Resource,
            TraceTagScope::Span => signaldb_sdk::types::TagScope::Span,
            TraceTagScope::Intrinsic => signaldb_sdk::types::TagScope::Intrinsic,
        }
    }

    /// The v2 scoped tag name (`resource.<tag>`, `span.<tag>`, or the bare
    /// `<tag>` for `intrinsic`) that `search_tag_values_v2` expects.
    fn scoped_tag_name(self, tag: &str) -> String {
        match self {
            TraceTagScope::Resource => format!("resource.{tag}"),
            TraceTagScope::Span => format!("span.{tag}"),
            TraceTagScope::Intrinsic => tag.to_string(),
        }
    }
}

/// Parameters for `discover_metrics`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverMetricsParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `query_metrics`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct QueryMetricsParams {
    /// PromQL expression, e.g. `rate(http_requests_total[5m])`.
    query: String,
    /// Evaluation timestamp, unix seconds or RFC3339. Omit to evaluate at
    /// "now". Ignored (and superseded by `start`/`end`) for a range query.
    #[serde(default)]
    time: Option<String>,
    /// Range query start (unix seconds or RFC3339). Providing `start` or
    /// `end` switches from an instant query to a range query.
    #[serde(default)]
    start: Option<String>,
    /// Range query end (unix seconds or RFC3339). See `start`.
    #[serde(default)]
    end: Option<String>,
    /// Range query resolution step (Go duration or seconds). Only used with
    /// `start`/`end`.
    #[serde(default)]
    step: Option<String>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
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
    /// Range query start (unix ns/s or RFC3339). Providing `start` or `end`
    /// switches from an instant query to a range query.
    #[serde(default)]
    start: Option<String>,
    /// Range query end (unix ns/s or RFC3339). See `start`.
    #[serde(default)]
    end: Option<String>,
    /// Range query evaluation interval for metric queries. Only used with
    /// `start`/`end`.
    #[serde(default)]
    step: Option<String>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `query_ir`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct QueryIrParams {
    /// The native Query IR document (the structured, versioned query surface).
    #[schemars(schema_with = "query_ir_document_schema")]
    #[serde(deserialize_with = "deserialize_query_ir_document")]
    query: serde_json::Value,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
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

/// Parameters for `discover_fields`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverFieldsParams {
    /// Signal source: `logs` (default), `traces`, `profiles`, `metrics`, or
    /// `metrics_histogram`.
    #[serde(default = "default_discovery_source")]
    source: String,
    /// Range start: RFC3339, a relative anchor like `now-1h` (default), or
    /// epoch nanoseconds.
    #[serde(default = "default_discovery_from")]
    from: String,
    /// Range end. Defaults to `now`.
    #[serde(default = "default_discovery_to")]
    to: String,
    /// Maximum fields to return.
    #[serde(default)]
    limit: Option<u64>,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `discover_field_values`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverFieldValuesParams {
    /// Signal source the field belongs to. Defaults to `logs`.
    #[serde(default = "default_discovery_source")]
    source: String,
    /// The logical field to suggest values for, as a dotted OTel name
    /// (`http.route`, `span.kind`).
    field: String,
    /// Range start: RFC3339, `now-1h` (default), or epoch nanoseconds.
    #[serde(default = "default_discovery_from")]
    from: String,
    /// Range end. Defaults to `now`.
    #[serde(default = "default_discovery_to")]
    to: String,
    /// Maximum values to return.
    #[serde(default)]
    limit: Option<u64>,
    /// Read data to answer when no declared value set or maintained statistics
    /// cover the field. Leave false (the default) to be told what would answer
    /// it instead of paying for a scan.
    #[serde(default)]
    sample: bool,
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

/// Parameters for `discover_sources`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DiscoverSourcesParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Dataset to query. Required: one MCP session may span several
    /// datasets, so there is no implicit session default; see
    /// `discover_datasets`.
    dataset: String,
}

fn default_discovery_source() -> String {
    "logs".to_string()
}

fn default_discovery_from() -> String {
    "now-1h".to_string()
}

fn default_discovery_to() -> String {
    "now".to_string()
}

/// The IR document for one `describe` request, built exactly as the Query IR
/// reference documents it.
fn describe_document(
    source: &str,
    from: &str,
    to: &str,
    stage: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "irVersion": 4,
        "from": source,
        "range": { "from": from, "to": to },
        "result": "metadata",
        "pipeline": [ { "describe": stage } ]
    })
}

/// Parameters for `resolve_attribute`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ResolveAttributeParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Attribute wire key, e.g. `k8s.pod.uid` or `service.name`.
    key: String,
}

/// Parameters for `resolve_entity`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ResolveEntityParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Entity type name, e.g. `k8s.pod` or `service`.
    name: String,
}

/// Parameters for `resolve_metric`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ResolveMetricParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Metric name, e.g. `k8s.pod.cpu.time`.
    name: String,
}

/// Which definition kind `search_schema` targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "lowercase")]
enum SchemaKind {
    /// Attribute keys (`k8s.pod.uid`, `http.request.method`, ...).
    Attribute,
    /// Entity types (`k8s.pod`, `service`, `host`, ...).
    Entity,
    /// Metric names (`k8s.pod.cpu.time`, `http.server.request.duration`, ...).
    Metric,
}

/// Parameters for `search_schema`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct SearchSchemaParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// What to search: `attribute`, `entity`, or `metric`.
    kind: SchemaKind,
    /// Name prefix, e.g. `k8s.pod.`. Omit or leave empty to list from the top.
    #[serde(default)]
    prefix: Option<String>,
    /// Maximum hits (server default 50, max 200).
    #[serde(default)]
    limit: Option<u64>,
}

/// Parameters for `create_schema_registry`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct CreateSchemaRegistryParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// The registry document in the OpenTelemetry Weaver semantic-convention
    /// model, as a JSON object: `name`, `version`, optional `schema_url` /
    /// `description` / `dependencies`, and `groups` (attribute_group, entity,
    /// metric). YAML must be converted to JSON first.
    #[schemars(schema_with = "json_object_schema")]
    #[serde(deserialize_with = "deserialize_json_object_or_string")]
    document: serde_json::Value,
}

/// Parameters for `replace_schema_registry`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ReplaceSchemaRegistryParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Registry namespace (the document's `name`).
    namespace: String,
    /// Registry version (the document's `version`).
    version: String,
    /// The replacement registry document (Weaver model) as a JSON object; its
    /// `name`/`version` must match `namespace`/`version`.
    #[schemars(schema_with = "json_object_schema")]
    #[serde(deserialize_with = "deserialize_json_object_or_string")]
    document: serde_json::Value,
}

/// Parameters for `delete_schema_registry`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DeleteSchemaRegistryParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Registry namespace.
    namespace: String,
    /// Registry version.
    version: String,
}

/// Advertises a nested-document parameter as a JSON object; identical to
/// [`query_ir_document_schema`], kept as its own name for readability at the
/// `#[schemars(schema_with = ...)]` call sites.
fn json_object_schema(generator: &mut rmcp::schemars::SchemaGenerator) -> rmcp::schemars::Schema {
    query_ir_document_schema(generator)
}

/// Accepts a nested document as a native JSON object, or — for clients that
/// stringify nested arguments (issue #1113) — a JSON-encoded string.
fn deserialize_json_object_or_string<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_query_ir_document(deserializer)
}

/// The registry document a schema mutation submits, as the JSON object the
/// API takes; anything but an object is a parameter error.
fn registry_document(
    document: serde_json::Value,
) -> Result<serde_json::Map<String, serde_json::Value>, ErrorData> {
    match document {
        serde_json::Value::Object(map) => Ok(map),
        _ => Err(ErrorData::invalid_params(
            "`document` must be a JSON object (the Weaver-model registry document: name, version, groups)",
            None,
        )),
    }
}

/// Enforces design D2's confirmation rule for destructive tools: `confirm`
/// must equal the identifier being destroyed, or the call is refused before
/// any downstream request.
fn require_confirm(confirm: &str, expected: &str, what: &str) -> Result<(), ErrorData> {
    if confirm != expected {
        return Err(ErrorData::invalid_params(
            format!("`confirm` must equal the {what} being deleted/revoked (\"{expected}\")"),
            None,
        ));
    }
    Ok(())
}

/// Confirms the required `tenant` tool argument matches the tenant the auth
/// middleware resolved for *this specific request* (`audit::CallerTenant`).
/// No tool can actually target a different tenant than the one its
/// credential authenticated as for this call (see `mcp_auth_middleware` in
/// `lib.rs`) — so this exists purely to fail an agent's wrong assumption
/// loudly (e.g. after `discover_datasets`) instead of silently running the
/// call against the real authenticated tenant.
fn check_tenant_scope(parts: &Parts, expected: &str) -> Result<(), ErrorData> {
    match parts.extensions.get::<audit::CallerTenant>() {
        Some(actual) if actual.0 == expected => Ok(()),
        Some(actual) => Err(ErrorData::invalid_params(
            format!(
                "`tenant` (\"{expected}\") does not match the authenticated tenant (\"{}\")",
                actual.0
            ),
            None,
        )),
        None => Ok(()),
    }
}

/// Whether `dataset` is visible to a credential carrying `restriction`
/// (`None` = unrestricted, every dataset visible) — design D10. Local to
/// this crate rather than reusing `common::auth::dataset_allowed`: this
/// server holds no auth dependency (see the `common` dependency comment in
/// `Cargo.toml`), it only forwards the caller's credential to the router.
/// This filters an already-authorized listing for display; it enforces
/// nothing the router itself does not already enforce on the data path.
fn dataset_visible(restriction: Option<&[String]>, dataset: &str) -> bool {
    match restriction {
        None => true,
        Some(allowed) => allowed.iter().any(|d| d == dataset),
    }
}

/// Reject an empty `scopes` list on API-key creation (platform-admin and
/// tenant-management variants share this validation).
fn require_nonempty_scopes(scopes: &[String]) -> Result<(), ErrorData> {
    if scopes.is_empty() {
        return Err(ErrorData::invalid_params(
            "at least one scope is required",
            None,
        ));
    }
    Ok(())
}

/// Reject an API-key update with none of `scopes`, `dataset_ids`, or
/// `clear_dataset_restriction` set (platform-admin and tenant-management
/// variants share this validation).
fn require_any_update(
    scopes: &Option<Vec<String>>,
    dataset_ids: &Option<Vec<String>>,
    clear_dataset_restriction: bool,
) -> Result<(), ErrorData> {
    if scopes.is_none() && dataset_ids.is_none() && !clear_dataset_restriction {
        return Err(ErrorData::invalid_params(
            "nothing to update: pass `scopes`, `dataset_ids`, and/or `clear_dataset_restriction`",
            None,
        ));
    }
    Ok(())
}

/// Reject `clear_dataset_restriction: true` combined with a non-empty
/// `dataset_ids` in the same update request (D1a) — checked before any
/// router request is made, since the server-side validation this mirrors
/// would otherwise be the only thing catching a contradictory request the
/// client should never have sent in the first place.
fn require_no_contradictory_dataset_update(
    dataset_ids: &Option<Vec<String>>,
    clear_dataset_restriction: bool,
) -> Result<(), ErrorData> {
    if clear_dataset_restriction && dataset_ids.as_ref().is_some_and(|ids| !ids.is_empty()) {
        return Err(ErrorData::invalid_params(
            "`clear_dataset_restriction: true` cannot be combined with a non-empty `dataset_ids`",
            None,
        ));
    }
    Ok(())
}

// ---- Platform-admin tool parameters (admin API; administrative credential) ----

/// Parameters for `get_tenant`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantIdParams {
    /// Tenant identifier.
    tenant_id: String,
}

/// Parameters for `create_tenant`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct CreateTenantParams {
    /// Unique tenant identifier.
    id: String,
    /// Human-readable tenant name.
    name: String,
    /// Default dataset name.
    #[serde(default)]
    default_dataset: Option<String>,
}

/// Parameters for `update_tenant`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct UpdateTenantParams {
    /// Tenant identifier.
    tenant_id: String,
    /// Replacement tenant name. Omit to keep the current name.
    #[serde(default)]
    name: Option<String>,
    /// Replacement default dataset. Omit to keep the current one.
    #[serde(default)]
    default_dataset: Option<String>,
}

/// Parameters for `delete_tenant`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DeleteTenantParams {
    /// Tenant identifier to delete.
    tenant_id: String,
    /// Must equal `tenant_id`, confirming the deletion.
    confirm: String,
}

/// Parameters for `create_user`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct CreateUserParams {
    /// Login email address.
    email: String,
    /// Optional display name.
    #[serde(default)]
    display_name: Option<String>,
    /// Tenant to grant the initial membership in.
    tenant: String,
    /// Initial tenant role: `admin`, `member`, or `viewer`. Defaults to `admin`.
    #[serde(default)]
    role: Option<String>,
    /// Grant instance-administrator status.
    #[serde(default)]
    instance_admin: Option<bool>,
    /// Password (at least 12 characters; hashed server-side).
    password: String,
}

/// Parameters for `list_datasets` / `tenant_list_datasets`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ListDatasetsParams {
    /// Tenant whose datasets to list.
    tenant_id: String,
}

/// Parameters for `create_dataset` / `tenant_create_dataset`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct CreateDatasetParams {
    /// Tenant the dataset belongs to.
    tenant_id: String,
    /// Dataset name.
    name: String,
}

/// Parameters for `delete_dataset` (admin API — identifies the dataset by
/// its opaque `dataset_id`; the management API's `tenant_delete_dataset`
/// identifies it by name instead — see [`TenantDeleteDatasetParams`]).
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct DeleteDatasetParams {
    /// Tenant the dataset belongs to.
    tenant_id: String,
    /// Dataset ID to delete.
    dataset_id: String,
    /// Must equal `dataset_id`, confirming the deletion.
    confirm: String,
}

/// Parameters for `revoke_api_key` (platform-admin; `tenant_revoke_api_key`
/// has its own params below).
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct RevokeApiKeyParams {
    /// Tenant the key belongs to.
    tenant_id: String,
    /// API key ID to revoke.
    key_id: String,
    /// Must equal `key_id`, confirming the revocation.
    confirm: String,
}

// ---- Tenant self-management tool parameters (management API; the caller's
// own tenant credential) ----

/// Parameters for `tenant_info`, `tenant_list_datasets`,
/// `tenant_list_api_keys`, `tenant_list_memberships`, `tenant_list_tables`,
/// `tenant_create_tables`, `tenant_list_table_schemas`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantOnlyParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
}

/// Parameters for `tenant_delete_dataset`. The management API identifies a
/// dataset by name (`dataset_name`), unlike the admin API's `dataset_id`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantDeleteDatasetParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
    /// Dataset name to delete.
    dataset_name: String,
    /// Must equal `dataset_name`, confirming the deletion.
    confirm: String,
}

/// Parameters for `tenant_create_api_key`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantCreateApiKeyParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
    /// Optional human-readable key name.
    #[serde(default)]
    name: Option<String>,
    /// Scopes the key carries (required, at least one). Vocabulary:
    /// `metrics:write`, `logs:write`, `traces:write`, `profiles:write`,
    /// `traces:read`, `logs:read`, `metrics:read`, `profiles:read`,
    /// `schema:read`, `schema:write`, `tenant:manage` (manage this tenant's
    /// datasets, API keys, memberships, and schema view; explicit only).
    scopes: Vec<String>,
    /// Dataset set the key is restricted to (non-empty; a bare empty array
    /// is rejected). Omitted or `null` creates an unrestricted key.
    #[serde(default)]
    dataset_ids: Option<Vec<String>>,
}

/// Parameters for `tenant_revoke_api_key`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantRevokeApiKeyParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
    /// API key ID to revoke.
    key_id: String,
    /// Must equal `key_id`, confirming the revocation.
    confirm: String,
}

/// Parameters for `tenant_update_api_key`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantUpdateApiKeyParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
    /// Key to update.
    key_id: String,
    /// Replacement scope list (non-empty). Omit to keep the current scopes.
    #[serde(default)]
    scopes: Option<Vec<String>>,
    /// Replacement dataset set (non-empty; a bare empty array is rejected).
    /// Omit to keep the current restriction. Mutually exclusive with
    /// `clear_dataset_restriction: true`.
    #[serde(default)]
    dataset_ids: Option<Vec<String>>,
    /// Clear an existing dataset restriction back to unrestricted. Must not
    /// be combined with a non-empty `dataset_ids`.
    #[serde(default)]
    clear_dataset_restriction: bool,
}

/// Tenant membership role, shared by `tenant_upsert_membership`.
#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "lowercase")]
enum TenantMembershipRole {
    Admin,
    Member,
    Viewer,
}

/// Parameters for `tenant_upsert_membership`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantUpsertMembershipParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
    /// The member's login email.
    email: String,
    /// Role to grant.
    role: TenantMembershipRole,
}

/// Parameters for `tenant_remove_membership`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct TenantRemoveMembershipParams {
    /// The caller's own tenant. Must match the authenticated tenant.
    tenant_id: String,
    /// User ID to remove.
    user_id: String,
    /// Must equal `user_id`, confirming the removal.
    confirm: String,
}

// ---- Schema-registry lookup parameters (tenant credential) ----

/// Parameters for `get_schema_registry`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct GetSchemaRegistryParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// Registry namespace (e.g. `otel`, `signaldb`, or a custom name).
    namespace: String,
    /// Registry version (e.g. `1.43.0`).
    version: String,
}

/// Parameters for `validate_schema_registry`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ValidateSchemaRegistryParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
    /// The registry document to validate (Weaver model) as a JSON object;
    /// nothing is stored.
    #[schemars(schema_with = "json_object_schema")]
    #[serde(deserialize_with = "deserialize_json_object_or_string")]
    document: serde_json::Value,
}

/// Parameters for `list_schema_registries`.
#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ListSchemaRegistriesParams {
    /// Tenant to query — must match the credential's authenticated tenant
    /// for this call (see `discover_datasets`). Required: one MCP session
    /// may hold credentials for several tenants across calls, so there is no
    /// single implicit default to fall back to; a mismatch fails the call
    /// before any router request is made.
    tenant: String,
}

#[tool_router]
impl McpServer {
    /// Construct a handler that forwards to `router_base_url`, bounding each
    /// forwarded request by `router_timeout`.
    pub fn new(router_base_url: String, router_timeout: std::time::Duration) -> Self {
        Self::with_max_concurrent_tool_calls(
            router_base_url,
            router_timeout,
            DEFAULT_MAX_CONCURRENT_TOOL_CALLS,
        )
    }

    /// [`Self::new`] with an explicit per-session bound on tool calls in
    /// flight (`[mcp].max_concurrent_tool_calls`). A bound of `0` is treated
    /// as `1` so the session can still make progress.
    pub fn with_max_concurrent_tool_calls(
        router_base_url: String,
        router_timeout: std::time::Duration,
        max_concurrent_tool_calls: usize,
    ) -> Self {
        let max_concurrent_tool_calls = max_concurrent_tool_calls.max(1);
        Self {
            router_base_url,
            router_timeout,
            tool_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(
                max_concurrent_tool_calls,
            )),
            max_concurrent_tool_calls,
            tool_call_deadline: crate::tool_call_deadline(router_timeout),
        }
    }

    /// Override the total per-`tools/call` deadline (default
    /// `crate::tool_call_deadline(router_timeout)`). Mainly for tests that
    /// need to observe a deadline-exceeded error without waiting out the
    /// full default (`router_timeout + RetryPolicy::default().total_cap`).
    pub fn with_tool_call_deadline(mut self, deadline: std::time::Duration) -> Self {
        self.tool_call_deadline = deadline;
        self
    }

    /// Run one tool call under the session's concurrency bound: wait up to
    /// [`PERMIT_WAIT`] for a permit, then dispatch through the tool router.
    /// The permit is released when the call finishes, whether it succeeded or
    /// failed.
    async fn dispatch_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, ErrorData> {
        let _permit = match tokio::time::timeout(PERMIT_WAIT, self.tool_permits.acquire()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_closed)) => {
                return Err(ErrorData::internal_error(
                    "tool-call permits closed for this session",
                    None,
                ));
            }
            Err(_timeout) => return Err(concurrency_limit_error(self.max_concurrent_tool_calls)),
        };
        let tcc = rmcp::handler::server::tool::ToolCallContext::new(self, request, context);
        Self::cached_tool_router().call(tcc).await
    }

    /// [`Self::tool_router`], built once and reused. The `#[tool_router]`
    /// macro generates that method as a plain constructor, so calling it
    /// directly reallocates the whole ~60-entry route map on every
    /// `tools/call`, `tools/list`, and lookup; the router itself is
    /// immutable once built, so it is safe to cache for the process
    /// lifetime.
    fn cached_tool_router() -> &'static rmcp::handler::server::router::tool::ToolRouter<Self> {
        static ROUTER: std::sync::OnceLock<
            rmcp::handler::server::router::tool::ToolRouter<McpServer>,
        > = std::sync::OnceLock::new();
        ROUTER.get_or_init(Self::tool_router)
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
        description = "Return everything needed to send data to and query this SignalDB deployment: public OTLP gRPC/HTTP endpoints, Prometheus remote-write, the query API base, required headers with your tenant and dataset filled in, the API-key scopes ingest needs, and ready-to-paste OTEL_EXPORTER_* env vars. Call this first when configuring or auto-instrumenting an application; then mint an ingest credential with `tenant_create_api_key` (scopes traces:write, logs:write, metrics:write, profiles:write) and substitute it for `<api-key>`.",
        annotations(read_only_hint = true)
    )]
    async fn connection_info(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let resp = self
            .router_client(&parts, None)?
            .connection_info()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "connection_info"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Search traces with TraceQL. Provide `query` as a TraceQL expression (e.g. `{ .service.name = \"api\" && status = error }`) and optionally `start`/`end` (unix seconds) and `limit`. Returns matching traces scoped to your tenant."
    )]
    async fn search_traces(
        &self,
        Parameters(p): Parameters<SearchTracesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
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
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
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
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
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
        description = "Discover the profile types with data for your tenant (e.g. CPU, heap). Optional `from`/`until` narrow the window (unix seconds/milliseconds, or `now[-<N><s|m|h|d>]`). Use this to construct a `search_profiles` selector.",
        annotations(read_only_hint = true)
    )]
    async fn discover_profile_types(
        &self,
        Parameters(p): Parameters<DiscoverProfileTypesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let mut req = client.pyroscope_profile_types();
        if let Some(v) = p.from {
            req = req.from(v);
        }
        if let Some(v) = p.until {
            req = req.until(v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "discover_profile_types"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Search profiles with a Pyroscope selector (e.g. `process_cpu:cpu:nanoseconds{service_name=\"checkout\"}`) and a time range. Returns the aggregated flame graph (flamebearer encoding) for your tenant.",
        annotations(read_only_hint = true)
    )]
    async fn search_profiles(
        &self,
        Parameters(p): Parameters<SearchProfilesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let mut req = client.pyroscope_render().query(p.query);
        if let Some(v) = p.from {
            req = req.from(v);
        }
        if let Some(v) = p.until {
            req = req.until(v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "search_profiles"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Compare profiles between two time ranges with a shared Pyroscope selector. Returns the differential flame graph (baseline vs comparison) for your tenant.",
        annotations(read_only_hint = true)
    )]
    async fn compare_profiles(
        &self,
        Parameters(p): Parameters<CompareProfilesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let mut req = client.pyroscope_render_diff().query(p.query);
        if let Some(v) = p.left_from {
            req = req.left_from(v);
        }
        if let Some(v) = p.left_until {
            req = req.left_until(v);
        }
        if let Some(v) = p.right_from {
            req = req.right_from(v);
        }
        if let Some(v) = p.right_until {
            req = req.right_until(v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "compare_profiles"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List the profiles correlated with a trace ID, scoped to your tenant.",
        annotations(read_only_hint = true)
    )]
    async fn profiles_for_trace(
        &self,
        Parameters(p): Parameters<ProfilesForTraceParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let resp = client
            .profiles_by_trace()
            .trace_id(p.trace_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "profiles_for_trace"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Discover queryable attributes for your tenant. Call with no arguments to list trace tag names; pass `tag` to list the known values for that tag. Pass `signal: \"logs\"`, `signal: \"metrics\"`, or `signal: \"profiles\"` to discover Loki log labels, Prometheus metric labels, or Pyroscope profile labels instead. With `signal: \"traces\"`, pass `scope: \"resource\"|\"span\"|\"intrinsic\"` to restrict discovery to one tag scope (routes through the Tempo v2 discovery endpoints). Use this to construct valid `search_traces`/`search_logs`/`query_metrics`/`search_profiles` queries."
    )]
    async fn discover_attributes(
        &self,
        Parameters(p): Parameters<DiscoverAttributesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        if p.scope.is_some() && !matches!(p.signal, Signal::Traces) {
            return Err(ErrorData::invalid_params(
                "discover_attributes: `scope` is only valid with signal: \"traces\"".to_string(),
                None,
            ));
        }
        let client = self.router_client(&parts, Some(&p.dataset))?;
        match (p.signal, p.tag, p.scope) {
            (Signal::Traces, Some(tag), Some(scope)) => {
                let resp = client
                    .search_tag_values_v2()
                    .tag_name(scope.scoped_tag_name(&tag))
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Traces, Some(tag), None) => {
                let resp = client
                    .search_tag_values()
                    .tag_name(tag)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Traces, None, Some(scope)) => {
                let resp = client
                    .search_tags_v2()
                    .scope(scope.into_sdk())
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Traces, None, None) => {
                let resp = client
                    .search_tags()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Logs, Some(name), _) => {
                let resp = client
                    .logql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Logs, None, _) => {
                let resp = client
                    .logql_labels()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Metrics, Some(name), _) => {
                let resp = client
                    .promql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Metrics, None, _) => {
                let resp = client
                    .promql_labels()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Profiles, Some(label), _) => {
                let resp = client
                    .pyroscope_label_values()
                    .label(label)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_attributes"))?;
                json_result(&resp.into_inner())
            }
            (Signal::Profiles, None, _) => {
                let resp = client
                    .pyroscope_label_names()
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
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let resp = client
            .promql_label_values()
            .name("__name__")
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "discover_metrics"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Query metrics with PromQL. Provide `query` as a PromQL expression (e.g. `rate(http_requests_total[5m])`) and optionally `time` (unix seconds or RFC3339) for an instant query. Provide `start`/`end` (and optionally `step`) instead of `time` for a range query. Returns the native Prometheus result scoped to your tenant.",
        annotations(read_only_hint = true)
    )]
    async fn query_metrics(
        &self,
        Parameters(p): Parameters<QueryMetricsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        if p.start.is_some() || p.end.is_some() {
            let mut req = client.promql_query_range().query(p.query);
            if let Some(v) = p.start {
                req = req.start(v);
            }
            if let Some(v) = p.end {
                req = req.end(v);
            }
            if let Some(v) = p.step {
                req = req.step(v);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| map_sdk_err(e, "query_metrics"))?;
            json_result(&resp.into_inner())
        } else {
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
    }

    #[tool(
        description = "Search logs with LogQL. Provide `query` as a LogQL expression (e.g. `{service_name=\"api\"} |= \"error\"`) and optionally `limit` and `direction` (`forward`/`backward`) for an instant query. Provide `start`/`end` (and optionally `step`) for a range query. Returns the native Loki result scoped to your tenant.",
        annotations(read_only_hint = true)
    )]
    async fn search_logs(
        &self,
        Parameters(p): Parameters<SearchLogsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        if p.start.is_some() || p.end.is_some() {
            let mut req = client.logql_query_range().query(p.query);
            if let Some(v) = p.limit {
                req = req.limit(v);
            }
            if let Some(v) = p.direction {
                req = req.direction(v);
            }
            if let Some(v) = p.start {
                req = req.start(v);
            }
            if let Some(v) = p.end {
                req = req.end(v);
            }
            if let Some(v) = p.step {
                req = req.step(v);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| map_sdk_err(e, "search_logs"))?;
            json_result(&resp.into_inner())
        } else {
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
    }

    #[tool(
        description = "List the queryable fields of a signal source, as logical dotted OTel names with their canonical type. Answered from the schema registry and maintained statistics — it reads no signal data — so call it freely before building a `query_ir` document. Each field carries `origin` (declared/registry/observed), and where statistics exist, `coverage` (the fraction of records carrying it) and an approximate `cardinality`. The response's `cost.as_of` says how recent those statistics are; `cost.window_scoped: false` means the range did not narrow the answer.",
        annotations(read_only_hint = true)
    )]
    async fn discover_fields(
        &self,
        Parameters(p): Parameters<DiscoverFieldsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let mut stage = serde_json::json!({ "target": "fields" });
        if let Some(limit) = p.limit {
            stage["limit"] = serde_json::json!(limit);
        }
        let document = describe_document(&p.source, &p.from, &p.to, stage);
        let request: signaldb_sdk::types::QueryIrRequest = serde_json::from_value(document)
            .map_err(|e| ErrorData::internal_error(format!("failed to build query: {e}"), None))?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let resp = client
            .query_ir()
            .body(request)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "discover_fields"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Suggest values for one logical field. A declared value set (a registry enumeration, or span kind / status code) is returned exactly and reads no data. When nothing covers the field the result has no values, `cost.mode: \"none\"`, and a `hint` naming the query that would compute the answer — pass `sample: true` only if you want that query run, which reads data bounded by the range and limit and reports `cost.mode: \"sampled_scan\"`.",
        annotations(read_only_hint = true)
    )]
    async fn discover_field_values(
        &self,
        Parameters(p): Parameters<DiscoverFieldValuesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        if p.field.trim().is_empty() {
            return Err(ErrorData::invalid_params(
                "discover_field_values: `field` must name a logical field".to_string(),
                None,
            ));
        }
        let mut stage = serde_json::json!({ "target": "values", "field": p.field });
        if let Some(limit) = p.limit {
            stage["limit"] = serde_json::json!(limit);
        }
        if p.sample {
            stage["sample"] = serde_json::json!(true);
        }
        let document = describe_document(&p.source, &p.from, &p.to, stage);
        let request: signaldb_sdk::types::QueryIrRequest = serde_json::from_value(document)
            .map_err(|e| ErrorData::internal_error(format!("failed to build query: {e}"), None))?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let resp = client
            .query_ir()
            .body(request)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "discover_field_values"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List the signal sources available to your tenant (`logs`, `traces`, `profiles`, `metrics`, `metrics_histogram`) with whether each is queryable. Use it to pick a valid `from` for a `query_ir` document or a `discover_fields` call.",
        annotations(read_only_hint = true)
    )]
    async fn discover_sources(
        &self,
        Parameters(p): Parameters<DiscoverSourcesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
        let resp = client
            .query_sources()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "discover_sources"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Discover the tenant and datasets your credential can access, as a nested Markdown list: the authenticated tenant, then its datasets (marking the session's current default) with each dataset's provisioned signal-table count. Call this before passing an explicit `dataset` argument to another tool, or a `tenant` argument to confirm your assumption.",
        annotations(read_only_hint = true)
    )]
    async fn discover_datasets(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        // The tenant id is already known from the auth middleware's own
        // `whoami()` call (stashed as `audit::CallerTenant`), so this
        // handler's `whoami()` — needed only for display fields
        // (`tenant.name`, the current default dataset) — can run alongside
        // `list_tenant_tables()` instead of gating it.
        let (identity, tables) = match parts.extensions.get::<audit::CallerTenant>() {
            Some(caller_tenant) => {
                let tenant_id = caller_tenant.0.clone();
                let (whoami, tables) = tokio::join!(
                    client.whoami().send(),
                    client.list_tenant_tables().tenant_id(&tenant_id).send()
                );
                (
                    whoami
                        .map_err(|e| map_sdk_err(e, "discover_datasets"))?
                        .into_inner(),
                    tables
                        .map_err(|e| map_sdk_err(e, "discover_datasets"))?
                        .into_inner(),
                )
            }
            None => {
                let identity = client
                    .whoami()
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_datasets"))?
                    .into_inner();
                let tables = client
                    .list_tenant_tables()
                    .tenant_id(&identity.tenant.id)
                    .send()
                    .await
                    .map_err(|e| map_sdk_err(e, "discover_datasets"))?
                    .into_inner();
                (identity, tables)
            }
        };

        // D10: a dataset-restricted credential must not see the name (or
        // table count) of a dataset outside its restriction, not even one
        // that is otherwise provisioned and empty.
        let restriction = identity.dataset_ids.as_deref();
        let visible_datasets: Vec<_> = tables
            .datasets
            .iter()
            .filter(|dataset| dataset_visible(restriction, &dataset.dataset))
            .collect();

        let mut markdown = format!(
            "- Tenant: **{}** (`{}`)\n",
            identity.tenant.name, identity.tenant.id
        );
        if visible_datasets.is_empty() {
            markdown.push_str("  - (no datasets provisioned yet)\n");
        } else {
            for dataset in visible_datasets {
                let current = if dataset.dataset == identity.dataset {
                    " (current)"
                } else {
                    ""
                };
                let count = dataset.tables.len();
                markdown.push_str(&format!(
                    "  - Dataset: `{}`{current} — {count} table{}\n",
                    dataset.dataset,
                    if count == 1 { "" } else { "s" },
                ));
            }
        }
        Ok(capped_text_result(markdown))
    }

    #[tool(
        description = "Execute a native Query IR document (the structured, versioned query surface). Provide `query` as the IR JSON object. Returns the enveloped result scoped to your tenant."
    )]
    async fn query_ir(
        &self,
        Parameters(p): Parameters<QueryIrParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let request: signaldb_sdk::types::QueryIrRequest = serde_json::from_value(p.query)
            .map_err(|e| ErrorData::invalid_params(format!("invalid IR document: {e}"), None))?;
        let client = self.router_client(&parts, Some(&p.dataset))?;
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

    #[tool(
        description = "List a tenant's API keys with their scopes and dataset restriction (admin API; requires administrative credentials). Raw secrets are never returned.",
        annotations(read_only_hint = true)
    )]
    async fn list_api_keys(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<ListApiKeysParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .list_api_keys()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "list_api_keys"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Create an API key for a tenant carrying exactly the given `scopes` (required, at least one; e.g. traces:write, schema:read) and optionally restricted to a set of datasets via `dataset_ids` (admin API; requires administrative credentials). The raw secret is returned once."
    )]
    async fn create_api_key(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<CreateApiKeyParams>,
    ) -> Result<CallToolResult, ErrorData> {
        require_nonempty_scopes(&p.scopes)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .create_api_key()
            .tenant_id(&p.tenant_id)
            .body(signaldb_sdk::types::CreateApiKeyRequest {
                name: p.name,
                scopes: p.scopes,
                dataset_ids: p.dataset_ids,
            })
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "create_api_key"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Update the scopes and/or dataset restriction of a live API key without rotating its secret (admin API; requires administrative credentials). `dataset_ids` replaces the restriction (non-empty, or omit to leave it unchanged); `clear_dataset_restriction: true` removes it back to unrestricted and must not be combined with a non-empty `dataset_ids`. Revoked keys cannot be updated; the change applies to the key's next request."
    )]
    async fn update_api_key_scopes(
        &self,
        Extension(parts): Extension<Parts>,
        Parameters(p): Parameters<UpdateApiKeyScopesParams>,
    ) -> Result<CallToolResult, ErrorData> {
        require_no_contradictory_dataset_update(&p.dataset_ids, p.clear_dataset_restriction)?;
        require_any_update(&p.scopes, &p.dataset_ids, p.clear_dataset_restriction)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .update_api_key()
            .tenant_id(&p.tenant_id)
            .key_id(&p.key_id)
            .body(signaldb_sdk::types::UpdateApiKeyRequest {
                scopes: p.scopes,
                dataset_ids: p.dataset_ids,
                clear_dataset_restriction: Some(p.clear_dataset_restriction),
            })
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "update_api_key_scopes"))?;
        json_result(&resp.into_inner())
    }

    // ---- Platform-admin tools (admin API; requires the administrative
    // credential). Read-only tools carry `read_only_hint`; `delete_tenant`,
    // `delete_dataset`, and `revoke_api_key` are destructive and require
    // `confirm` to equal the identifier being destroyed (design D2). ----

    #[tool(
        description = "List every configured tenant (admin API; requires the administrative credential).",
        annotations(read_only_hint = true)
    )]
    async fn list_tenants(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .list_tenants()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "list_tenants"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Get one tenant's details by ID (admin API; requires the administrative credential).",
        annotations(read_only_hint = true)
    )]
    async fn get_tenant(
        &self,
        Parameters(p): Parameters<TenantIdParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .get_tenant()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "get_tenant"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Create a new tenant (admin API; requires the administrative credential). `id` must be unique."
    )]
    async fn create_tenant(
        &self,
        Parameters(p): Parameters<CreateTenantParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .create_tenant()
            .body(signaldb_sdk::types::CreateTenantRequest {
                id: p.id,
                name: p.name,
                default_dataset: p.default_dataset,
            })
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "create_tenant"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Update a tenant's name and/or default dataset (admin API; requires the administrative credential). Absent fields are left unchanged."
    )]
    async fn update_tenant(
        &self,
        Parameters(p): Parameters<UpdateTenantParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .update_tenant()
            .tenant_id(&p.tenant_id)
            .body(signaldb_sdk::types::UpdateTenantRequest {
                name: p.name,
                default_dataset: p.default_dataset,
            })
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "update_tenant"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Delete a tenant and everything under it (admin API; requires the administrative credential). Requires `confirm` equal to `tenant_id`.",
        annotations(destructive_hint = true, read_only_hint = false)
    )]
    async fn delete_tenant(
        &self,
        Parameters(p): Parameters<DeleteTenantParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_confirm(&p.confirm, &p.tenant_id, "tenant_id")?;
        let client = self.router_client(&parts, None)?;
        client
            .delete_tenant()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "delete_tenant"))?;
        json_result(&serde_json::json!({ "deleted": true, "tenant_id": p.tenant_id }))
    }

    #[tool(
        description = "Create a human user and grant an initial tenant membership (admin API; requires the administrative credential). Password must be at least 12 characters."
    )]
    async fn create_user(
        &self,
        Parameters(p): Parameters<CreateUserParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        if p.password.len() < 12 {
            return Err(ErrorData::invalid_params(
                "password must be at least 12 characters",
                None,
            ));
        }
        let client = self.router_client(&parts, None)?;
        let resp = client
            .create_user()
            .body(signaldb_sdk::types::CreateUserRequest {
                email: p.email,
                display_name: p.display_name,
                tenant: p.tenant,
                role: p.role,
                instance_admin: p.instance_admin,
                password: p.password,
            })
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "create_user"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List a tenant's datasets (admin API; requires the administrative credential).",
        annotations(read_only_hint = true)
    )]
    async fn list_datasets(
        &self,
        Parameters(p): Parameters<ListDatasetsParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .list_datasets()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "list_datasets"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Create a dataset for a tenant (admin API; requires the administrative credential)."
    )]
    async fn create_dataset(
        &self,
        Parameters(p): Parameters<CreateDatasetParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .create_dataset()
            .tenant_id(&p.tenant_id)
            .body(signaldb_sdk::types::CreateDatasetRequest { name: p.name })
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "create_dataset"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Delete a tenant's dataset by ID (admin API; requires the administrative credential). Requires `confirm` equal to `dataset_id`.",
        annotations(destructive_hint = true, read_only_hint = false)
    )]
    async fn delete_dataset(
        &self,
        Parameters(p): Parameters<DeleteDatasetParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_confirm(&p.confirm, &p.dataset_id, "dataset_id")?;
        let client = self.router_client(&parts, None)?;
        client
            .delete_dataset()
            .tenant_id(&p.tenant_id)
            .dataset_id(&p.dataset_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "delete_dataset"))?;
        json_result(&serde_json::json!({ "deleted": true, "dataset_id": p.dataset_id }))
    }

    #[tool(
        description = "Revoke a tenant's API key by ID (admin API; requires the administrative credential). Requires `confirm` equal to `key_id`.",
        annotations(destructive_hint = true, read_only_hint = false)
    )]
    async fn revoke_api_key(
        &self,
        Parameters(p): Parameters<RevokeApiKeyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_confirm(&p.confirm, &p.key_id, "key_id")?;
        let client = self.router_client(&parts, None)?;
        client
            .revoke_api_key()
            .tenant_id(&p.tenant_id)
            .key_id(&p.key_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "revoke_api_key"))?;
        json_result(&serde_json::json!({ "revoked": true, "key_id": p.key_id }))
    }

    // ---- Tenant self-management tools (management API; act as the caller's
    // own identity within its tenant — `tenant_id` must match the
    // authenticated tenant).
    //
    // Every tool in this block wraps an endpoint gated by
    // `authorize_tenant` (or the same rule for `tenant_get_schema`): the
    // router accepts a human principal (browser session or OAuth token)
    // with the tenant-admin role or instance-admin flag, or an API key that
    // explicitly carries `tenant:manage`. An ingest-only or legacy unscoped
    // key gets a clean access-denied error, per the `mcp-tool-surface`
    // spec's "Unauthorized management call is denied cleanly" scenario. ----

    #[tool(
        description = "List the caller's own tenant's datasets (management API; tenant-admin session or an API key carrying `tenant:manage`).",
        annotations(read_only_hint = true)
    )]
    async fn tenant_list_datasets(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_list_datasets()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_list_datasets"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Create a dataset for the caller's own tenant (management API; tenant-admin session or an API key carrying `tenant:manage`)."
    )]
    async fn tenant_create_dataset(
        &self,
        Parameters(p): Parameters<CreateDatasetParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_create_dataset()
            .tenant_id(&p.tenant_id)
            .body(signaldb_sdk::types::ManageCreateDatasetRequest { name: p.name })
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_create_dataset"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Delete a dataset from the caller's own tenant, by name (management API; tenant-admin session or an API key carrying `tenant:manage`). Requires `confirm` equal to `dataset_name`.",
        annotations(destructive_hint = true, read_only_hint = false)
    )]
    async fn tenant_delete_dataset(
        &self,
        Parameters(p): Parameters<TenantDeleteDatasetParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_confirm(&p.confirm, &p.dataset_name, "dataset_name")?;
        let client = self.router_client(&parts, None)?;
        client
            .manage_delete_dataset()
            .tenant_id(&p.tenant_id)
            .dataset_name(&p.dataset_name)
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_delete_dataset"))?;
        json_result(&serde_json::json!({ "deleted": true, "dataset_name": p.dataset_name }))
    }

    #[tool(
        description = "List the caller's own tenant's API keys with their scopes (management API; tenant-admin session or an API key carrying `tenant:manage`). Raw secrets are never returned.",
        annotations(read_only_hint = true)
    )]
    async fn tenant_list_api_keys(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_list_api_keys()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_list_api_keys"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Create an API key for the caller's own tenant, carrying exactly the given `scopes` (required, at least one) and optionally restricted to a set of datasets via `dataset_ids` (management API; tenant-admin session or an API key carrying `tenant:manage`). The raw secret is returned once."
    )]
    async fn tenant_create_api_key(
        &self,
        Parameters(p): Parameters<TenantCreateApiKeyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_nonempty_scopes(&p.scopes)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_create_api_key()
            .tenant_id(&p.tenant_id)
            .body(signaldb_sdk::types::ManageCreateApiKeyRequest {
                name: p.name,
                scopes: p.scopes,
                dataset_ids: p.dataset_ids,
            })
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_create_api_key"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Revoke one of the caller's own tenant's API keys (management API; tenant-admin session or an API key carrying `tenant:manage`). Requires `confirm` equal to `key_id`.",
        annotations(destructive_hint = true, read_only_hint = false)
    )]
    async fn tenant_revoke_api_key(
        &self,
        Parameters(p): Parameters<TenantRevokeApiKeyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_confirm(&p.confirm, &p.key_id, "key_id")?;
        let client = self.router_client(&parts, None)?;
        client
            .manage_revoke_api_key()
            .tenant_id(&p.tenant_id)
            .key_id(&p.key_id)
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_revoke_api_key"))?;
        json_result(&serde_json::json!({ "revoked": true, "key_id": p.key_id }))
    }

    #[tool(
        description = "Update the scopes and/or dataset restriction of one of the caller's own tenant's API keys, without rotating its secret (management API; tenant-admin session or an API key carrying `tenant:manage`). `dataset_ids` replaces the restriction (non-empty, or omit to leave it unchanged); `clear_dataset_restriction: true` removes it back to unrestricted and must not be combined with a non-empty `dataset_ids`."
    )]
    async fn tenant_update_api_key(
        &self,
        Parameters(p): Parameters<TenantUpdateApiKeyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_no_contradictory_dataset_update(&p.dataset_ids, p.clear_dataset_restriction)?;
        require_any_update(&p.scopes, &p.dataset_ids, p.clear_dataset_restriction)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_update_api_key()
            .tenant_id(&p.tenant_id)
            .key_id(&p.key_id)
            .body(signaldb_sdk::types::ManageUpdateApiKeyRequest {
                scopes: p.scopes,
                dataset_ids: p.dataset_ids,
                clear_dataset_restriction: Some(p.clear_dataset_restriction),
            })
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_update_api_key"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List the caller's own tenant's memberships (management API; tenant-admin session or an API key carrying `tenant:manage`).",
        annotations(read_only_hint = true)
    )]
    async fn tenant_list_memberships(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_list_memberships()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_list_memberships"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Create or update a member's role in the caller's own tenant (management API; tenant-admin session or an API key carrying `tenant:manage`)."
    )]
    async fn tenant_upsert_membership(
        &self,
        Parameters(p): Parameters<TenantUpsertMembershipParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let role = match p.role {
            TenantMembershipRole::Admin => signaldb_sdk::types::MembershipRole::Admin,
            TenantMembershipRole::Member => signaldb_sdk::types::MembershipRole::Member,
            TenantMembershipRole::Viewer => signaldb_sdk::types::MembershipRole::Viewer,
        };
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_upsert_membership()
            .tenant_id(&p.tenant_id)
            .body(signaldb_sdk::types::UpsertMembershipRequest {
                email: p.email,
                role,
            })
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_upsert_membership"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Remove a member from the caller's own tenant (management API; tenant-admin session or an API key carrying `tenant:manage`). Requires `confirm` equal to `user_id`.",
        annotations(destructive_hint = true, read_only_hint = false)
    )]
    async fn tenant_remove_membership(
        &self,
        Parameters(p): Parameters<TenantRemoveMembershipParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        require_confirm(&p.confirm, &p.user_id, "user_id")?;
        let client = self.router_client(&parts, None)?;
        client
            .manage_remove_membership()
            .tenant_id(&p.tenant_id)
            .user_id(&p.user_id)
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_remove_membership"))?;
        json_result(&serde_json::json!({ "removed": true, "user_id": p.user_id }))
    }

    #[tool(
        description = "The registered logical (client-visible) and physical (storage) schema for every signal source (management API; tenant-admin session or an API key carrying `tenant:manage`).",
        annotations(read_only_hint = true)
    )]
    async fn tenant_get_schema(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .manage_get_schema()
            .send()
            .await
            .map_err(|e| map_manage_err(e, "tenant_get_schema"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "The caller's own tenant: id, enabled flag, and schema configuration (tenant self-service API; any valid key of the tenant).",
        annotations(read_only_hint = true)
    )]
    async fn tenant_info(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .get_tenant_self()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "tenant_info"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List the caller's own tenant's provisioned signal tables (tenant self-service API; the caller's tenant credential). Filtered to the caller's own dataset restriction, if any (D10): a dataset outside it never appears here.",
        annotations(read_only_hint = true)
    )]
    async fn tenant_list_tables(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let mut tables = client
            .list_tenant_tables()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "tenant_list_tables"))?
            .into_inner();
        // D10: hide any dataset outside the caller's own restriction — set
        // once per request by the auth middleware alongside
        // `audit::CallerTenant`. `dataset_visible` no-ops both `retain`
        // calls below when the caller is unrestricted.
        let restriction = parts
            .extensions
            .get::<audit::CallerDatasetIds>()
            .and_then(|r| r.0.as_deref());
        tables
            .datasets
            .retain(|dataset| dataset_visible(restriction, &dataset.dataset));
        tables.tables.retain(|table| {
            table
                .dataset
                .as_deref()
                .is_none_or(|dataset| dataset_visible(restriction, dataset))
        });
        json_result(&tables)
    }

    #[tool(
        description = "Provision (create) the caller's own tenant's enabled signal tables — the manual trigger from the table-provisioning docs (tenant self-service API; the caller's tenant credential)."
    )]
    async fn tenant_create_tables(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .create_tenant_tables()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "tenant_create_tables"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List the caller's own tenant's configured table schema types (tenant self-service API; the caller's tenant credential). Distinct from `tenant_list_tables`, which lists what is actually provisioned.",
        annotations(read_only_hint = true)
    )]
    async fn tenant_list_table_schemas(
        &self,
        Parameters(p): Parameters<TenantOnlyParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .list_tenant_schemas()
            .tenant_id(&p.tenant_id)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "tenant_list_table_schemas"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List every table schema type SignalDB knows how to provision, regardless of tenant configuration (tenant self-service API; any authenticated tenant credential).",
        annotations(read_only_hint = true)
    )]
    async fn list_available_table_schemas(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        let client = self.router_client(&parts, None)?;
        let resp = client
            .list_available_schemas()
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "list_available_table_schemas"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "List the schema registries visible to your tenant, in precedence order (custom tenant registries first, then the bundled `signaldb` and OpenTelemetry `otel` semantic conventions), with attribute/entity/metric counts. Use `resolve_attribute`, `resolve_entity`, `resolve_metric`, or `search_schema` to look up what a specific name means."
    )]
    async fn list_schema_registries(
        &self,
        Parameters(p): Parameters<ListSchemaRegistriesParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_list_registries()
            .send()
            .await
            .map_err(|e| map_schema_err(e, "list_schema_registries"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Look up what an attribute key means BEFORE filtering or grouping by it: returns every definition of `key` (e.g. `k8s.pod.uid`, `service.name`) across your tenant's visible schema registries, precedence-ordered with the tenant's own convention as `primary` and the OpenTelemetry definition as an alternative. Each hit carries the namespace, type, brief, examples, enum members, deprecation/replacement, and the entities the key identifies. Empty `hits` means the key is not in any registry (it may still exist in the data — see `discover_attributes`)."
    )]
    async fn resolve_attribute(
        &self,
        Parameters(p): Parameters<ResolveAttributeParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_resolve_attribute()
            .key(p.key)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "resolve_attribute"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Look up what an entity type means (e.g. `k8s.pod`, `service`, `host`) before building a query about it: returns every definition across your tenant's visible schema registries, precedence-ordered with `primary` first, including the identifying and descriptive attributes, the entity it extends, and the metrics associated with it. Use the identifying attributes as the keys to filter or group by."
    )]
    async fn resolve_entity(
        &self,
        Parameters(p): Parameters<ResolveEntityParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_resolve_entity()
            .name(p.name)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "resolve_entity"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Look up what a metric means (e.g. `k8s.pod.cpu.time`) before writing a PromQL query for it: returns every definition across your tenant's visible schema registries, precedence-ordered with `primary` first, including instrument, unit, brief, the attributes it is recorded with, and the entities it describes. Combine with `discover_metrics` to see which metric names actually have data."
    )]
    async fn resolve_metric(
        &self,
        Parameters(p): Parameters<ResolveMetricParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_resolve_metric()
            .name(p.name)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "resolve_metric"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Search the schema registries by name prefix to find the right vocabulary before building a query: `kind` is `attribute`, `entity`, or `metric`; `prefix` narrows by name (e.g. `k8s.pod.`), `limit` caps the hits (max 200). Each hit is namespace-tagged with its brief, so you can pick the correct attribute key, entity type, or metric name and then call the matching `resolve_*` tool for the full definition."
    )]
    async fn search_schema(
        &self,
        Parameters(p): Parameters<SearchSchemaParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        let prefix = p.prefix.unwrap_or_default();
        // Each kind has its own generated response type, so each arm sends and
        // serializes its own result; the shape is the HTTP response, unchanged.
        match p.kind {
            SchemaKind::Attribute => {
                let mut req = client.schema_search_attributes().prefix(prefix);
                if let Some(limit) = p.limit {
                    req = req.limit(limit);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| map_schema_err(e, "search_schema"))?;
                json_result(&resp.into_inner())
            }
            SchemaKind::Entity => {
                let mut req = client.schema_search_entities().prefix(prefix);
                if let Some(limit) = p.limit {
                    req = req.limit(limit);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| map_schema_err(e, "search_schema"))?;
                json_result(&resp.into_inner())
            }
            SchemaKind::Metric => {
                let mut req = client.schema_search_metrics().prefix(prefix);
                if let Some(limit) = p.limit {
                    req = req.limit(limit);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| map_schema_err(e, "search_schema"))?;
                json_result(&resp.into_inner())
            }
        }
    }

    #[tool(
        description = "Create a custom schema registry for your tenant from a Weaver-model document (JSON object with `name`, `version`, `groups`; convert YAML first). Requires a credential with the `schema:write` scope. The tenant's own definitions take precedence over the bundled OpenTelemetry conventions in every `resolve_*` result. Validation errors come back with document paths."
    )]
    async fn create_schema_registry(
        &self,
        Parameters(p): Parameters<CreateSchemaRegistryParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let document = registry_document(p.document)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_create_registry()
            .body(document)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "create_schema_registry"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Replace an existing custom schema registry (`namespace`/`version`) with a new Weaver-model document whose `name`/`version` match. Requires the `schema:write` scope. Bundled registries (`otel`, `signaldb`) are read-only and refuse replacement."
    )]
    async fn replace_schema_registry(
        &self,
        Parameters(p): Parameters<ReplaceSchemaRegistryParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let document = registry_document(p.document)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_replace_registry()
            .namespace(p.namespace)
            .version(p.version)
            .body(document)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "replace_schema_registry"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Delete a custom schema registry (`namespace`/`version`) from your tenant. Requires the `schema:write` scope. Bundled registries (`otel`, `signaldb`) are read-only and refuse deletion."
    )]
    async fn delete_schema_registry(
        &self,
        Parameters(p): Parameters<DeleteSchemaRegistryParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        client
            .schema_delete_registry()
            .namespace(&p.namespace)
            .version(&p.version)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "delete_schema_registry"))?;
        json_result(&serde_json::json!({
            "deleted": true,
            "namespace": p.namespace,
            "version": p.version,
        }))
    }

    #[tool(
        description = "Fetch one schema registry's summary and full document by `namespace`/`version`. Use `list_schema_registries` to discover which namespace/version pairs are visible to your tenant.",
        annotations(read_only_hint = true)
    )]
    async fn get_schema_registry(
        &self,
        Parameters(p): Parameters<GetSchemaRegistryParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_get_registry()
            .namespace(&p.namespace)
            .version(&p.version)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "get_schema_registry"))?;
        json_result(&resp.into_inner())
    }

    #[tool(
        description = "Validate a Weaver-model registry document without storing it. Errors come back with document paths, so this is the way to check a document before `create_schema_registry`/`replace_schema_registry`.",
        annotations(read_only_hint = true)
    )]
    async fn validate_schema_registry(
        &self,
        Parameters(p): Parameters<ValidateSchemaRegistryParams>,
        Extension(parts): Extension<Parts>,
    ) -> Result<CallToolResult, ErrorData> {
        check_tenant_scope(&parts, &p.tenant)?;
        let document = registry_document(p.document)?;
        let client = self.router_client(&parts, None)?;
        let resp = client
            .schema_validate_registry()
            .body(document)
            .send()
            .await
            .map_err(|e| map_schema_err(e, "validate_schema_registry"))?;
        json_result(&resp.into_inner())
    }
}

impl McpServer {
    /// Whether a tool named `name` is registered. Exposed for cross-surface
    /// parity checks (see the `client-surface-parity` spec).
    pub fn has_tool(name: &str) -> bool {
        Self::cached_tool_router().has_route(name)
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

    /// The error is boxed because `signaldb_sdk::Error` is large (136 bytes),
    /// and clippy's `result_large_err` rejects carrying that inline through a
    /// `Result` — every caller pays the size on the success path too. The one
    /// caller only formats it into a log line, so the indirection costs
    /// nothing that matters here.
    async fn fetch(
        &self,
        client: &signaldb_sdk::Client,
    ) -> Result<Vec<String>, Box<signaldb_sdk::Error<()>>> {
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
             `prompts/list` offers ready-made investigation templates. \
             Before filtering, grouping, or writing a query around an attribute key, entity, or \
             metric, call `resolve_attribute` / `resolve_entity` / `resolve_metric` (or \
             `search_schema` by prefix) to learn what the name means in this tenant's schema \
             registries; the tenant's own conventions take precedence over OpenTelemetry's.",
        )
    }

    /// The one dispatch wrapper every tool call goes through (issue #629):
    /// opens the `tools/call {tool}` span (parented to the caller's W3C
    /// context when the HTTP request carried one), holds the session's
    /// concurrency permit, times the call, classifies the outcome, emits
    /// exactly one audit event, and records the metrics. Tools themselves
    /// stay untouched. Replaces the `#[tool_handler]` default, which the macro
    /// skips when the method is present.
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, ErrorData> {
        use tracing::Instrument as _;

        let audit = AuditContext::capture(&request, &context);
        let span = common::self_monitoring::spans::mcp_tool_span(
            &audit.tool,
            &audit.tenant_id,
            audit.dataset.as_deref(),
            &audit.session_id,
        );
        if let Some(parts) = context.extensions.get::<Parts>() {
            // Parent must be adopted before the span is first entered.
            common::flight::trace_context::set_parent_from_http_headers(&span, &parts.headers);
        }
        let started = std::time::Instant::now();
        // Bound the whole call, not just one downstream HTTP attempt:
        // `sdk_client_for` times out each retry attempt individually, so
        // without this the retry policy's up-to-4-attempts-plus-sleeps could
        // otherwise keep a call alive far longer than any one attempt's
        // timeout suggests (see `tool_call_deadline`).
        let deadline = self.tool_call_deadline;
        let result = match tokio::time::timeout(deadline, self.dispatch_tool(request, context))
            .instrument(span.clone())
            .await
        {
            Ok(result) => result,
            Err(_elapsed) => Err(deadline_exceeded_error(deadline)),
        };
        let duration = started.elapsed();
        let outcome = Outcome::of(&result);
        if let Some(error_type) = outcome.error_type() {
            common::self_monitoring::spans::record_span_error(&span, error_type);
        }
        audit.finish(&outcome, duration);
        result
    }

    /// List tools, attaching `_meta.ui.resourceUri` to UI-backed tools when the
    /// client negotiated the MCP Apps extension. Clients that did not ask for
    /// apps get exactly the tool surface they got before.
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = Self::cached_tool_router().list_all();
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

/// Bound a text tool result at [`MAX_TOOL_PAYLOAD_BYTES`]. When `text`
/// exceeds the budget, the tool returns valid JSON marked `truncated` with a
/// narrowing hint instead of the oversized payload, so clients detect the cap
/// from the flag. Shared by every tool that returns a text block, whether
/// JSON ([`json_result_for_app`]) or plain Markdown (`discover_datasets`).
fn capped_text_result(text: String) -> CallToolResult {
    if text.len() > MAX_TOOL_PAYLOAD_BYTES {
        let notice = serde_json::json!({
            "truncated": true,
            "bytes": text.len(),
            "limit_bytes": MAX_TOOL_PAYLOAD_BYTES,
            "hint": "Result exceeded the size cap; narrow the time range or lower `limit`, then retry.",
        });
        return audit::mark_truncated(CallToolResult::success(vec![ContentBlock::text(
            notice.to_string(),
        )]));
    }
    CallToolResult::success(vec![ContentBlock::text(text)])
}

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
    let truncated = text.len() > MAX_TOOL_PAYLOAD_BYTES;
    let mut result = capped_text_result(text);
    if with_structured && !truncated {
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
fn map_sdk_err<E: std::fmt::Debug>(err: signaldb_sdk::Error<E>, what: &str) -> ErrorData {
    let status = err.status().map(|s| s.as_u16());
    let mapped = match status {
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
        // Retries are exhausted by the time a 429 reaches here (the SDK's
        // policy absorbed the brief ones): report a distinct throttled error
        // naming the server-stated wait, with `retryAfterMs` in `data`.
        Some(429) => throttled_error(what, signaldb_sdk::retry::retry_after(&err)),
        _ => ErrorData::internal_error(format!("{what}: {err}"), None),
    };
    // Carry the downstream status so the audit wrapper classifies denied /
    // throttled / failed calls from data, not message text.
    match status {
        Some(status) => with_http_status(mapped, status),
        None => mapped,
    }
}

/// The distinct throttled tool error: message prefix `throttled:` plus a
/// structured `retryAfterMs` so an agent can wait or narrow the query. Kept
/// JSON-RPC-safe by reusing the internal-error code; distinctness is by
/// prefix and data (the audit wrapper classifies it as `throttled`).
fn throttled_error(what: &str, retry_after: Option<std::time::Duration>) -> ErrorData {
    let message = match retry_after {
        Some(wait) => format!(
            "throttled: {what} was rate limited; the server asked to retry in {}s",
            wait.as_secs_f64().ceil() as u64
        ),
        None => format!("throttled: {what} was rate limited; retry shortly"),
    };
    ErrorData::internal_error(
        message,
        Some(serde_json::json!({
            "retryAfterMs": retry_after.map(|w| w.as_millis() as u64),
        })),
    )
}

/// Map a management-API error to an MCP error, keeping the router's typed
/// `error` text on a `403` so a model learns *why* it was denied — the
/// message names the required role or `tenant:manage` scope; other statuses
/// fall back to [`map_sdk_err`].
fn map_manage_err(
    err: signaldb_sdk::Error<signaldb_sdk::types::ManageError>,
    what: &str,
) -> ErrorData {
    match err {
        signaldb_sdk::Error::ErrorResponse(response) if response.status().as_u16() == 403 => {
            let body = response.into_inner();
            with_http_status(
                ErrorData::invalid_request(format!("{what}: access denied: {}", body.error), None),
                403,
            )
        }
        other => map_sdk_err(other, what),
    }
}

/// Map a schema-API error to an MCP error, keeping the router's typed body
/// (`error` plus per-path validation `errors`) in the message so a model can
/// fix an invalid registry document; other failures fall back to
/// [`map_sdk_err`].
fn map_schema_err(
    err: signaldb_sdk::Error<signaldb_sdk::types::SchemaError>,
    what: &str,
) -> ErrorData {
    let signaldb_sdk::Error::ErrorResponse(response) = err else {
        return map_sdk_err(err.into_untyped(), what);
    };
    let status = response.status().as_u16();
    let body = response.into_inner();
    let mut message = format!("{what}: {}", body.error);
    if !body.errors.is_empty() {
        let details: Vec<String> = body
            .errors
            .iter()
            .map(|e| format!("{}: {}", e.path, e.message))
            .collect();
        message.push_str(" [");
        message.push_str(&details.join("; "));
        message.push(']');
    }
    let mapped = match status {
        400 | 422 => ErrorData::invalid_params(message, None),
        401 => ErrorData::invalid_request(
            format!("{what}: credential expired or was revoked; re-authenticate the session"),
            None,
        ),
        403 | 409 => ErrorData::invalid_request(message, None),
        404 => ErrorData::resource_not_found(message, None),
        _ => ErrorData::internal_error(message, None),
    };
    with_http_status(mapped, status)
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

    // ---- Pyroscope profile tools (change: pyroscope-openapi-parity) ----

    /// A `Parts` carrying a bearer credential, as if forwarded from a real
    /// session (mirrors the `server_info`/`completion` tests below).
    fn valid_parts() -> axum::http::request::Parts {
        RequestBuilder::new()
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(())
            .expect("build request")
            .into_parts()
            .0
    }

    /// Spawn a one-shot mock router that asserts the request line starts
    /// with `expected_prefix`, replies with `body` as a 200 JSON response,
    /// and returns the base URL to point an `McpServer` at plus the task
    /// handle to await for a clean shutdown.
    async fn mock_json_router(
        expected_prefix: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..request_len])
                    .expect("request is UTF-8")
                    .starts_with(expected_prefix),
                "unexpected request, wanted prefix {expected_prefix:?}: {:?}",
                std::str::from_utf8(&request[..request_len])
            );
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
            socket.write_all(body.as_bytes()).await.expect("write body");
        });
        (format!("http://{addr}"), handle)
    }

    fn text_json(result: &CallToolResult) -> serde_json::Value {
        let ContentBlock::Text(text) = &result.content[0] else {
            panic!("expected a text content block");
        };
        serde_json::from_str(&text.text).expect("tool result is JSON")
    }

    /// Like [`mock_json_router`], but returns the full raw HTTP request text
    /// (headers + body) it received instead of only asserting a prefix, so a
    /// test can inspect the JSON body the client actually sent — e.g. proving
    /// a parameter was forwarded rather than dropped.
    async fn mock_capturing_router(
        expected_prefix: &'static str,
        status: u16,
        response_body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 8192];
            let request_len = socket.read(&mut request).await.expect("read request");
            let request = std::str::from_utf8(&request[..request_len])
                .expect("request is UTF-8")
                .to_string();
            assert!(
                request.starts_with(expected_prefix),
                "unexpected request, wanted prefix {expected_prefix:?}: {request}"
            );
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response_body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response headers");
            socket
                .write_all(response_body.as_bytes())
                .await
                .expect("write body");
            request
        });
        (format!("http://{addr}"), handle)
    }

    /// Extract and parse the JSON body from a request captured by
    /// [`mock_capturing_router`].
    fn captured_json_body(request: &str) -> serde_json::Value {
        let body = request
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .expect("request has a body");
        serde_json::from_str(body).expect("request body is JSON")
    }

    #[tokio::test]
    async fn discover_profile_types_lists_types_via_router() {
        let (base_url, router) = mock_json_router(
            "GET /pyroscope/profile-types",
            r#"[{"ID":"cpu:cpu:nanoseconds","name":"cpu","sampleType":"cpu","sampleUnit":"nanoseconds"}]"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .discover_profile_types(
                Parameters(DiscoverProfileTypesParams {
                    from: None,
                    until: None,
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("discover_profile_types succeeds");

        let types = text_json(&result);
        assert_eq!(types[0]["name"], "cpu");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn search_profiles_returns_the_flamegraph() {
        let (base_url, router) = mock_json_router(
            "GET /pyroscope/render?",
            r#"{"flamebearer":{"names":["total"],"levels":[[0,10,0,0]],"numTicks":10,"maxSelf":10},"metadata":{"format":"single","sampleRate":100,"units":"samples","name":"cpu"}}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .search_profiles(
                Parameters(SearchProfilesParams {
                    query: "cpu".to_string(),
                    from: Some("now-1h".to_string()),
                    until: None,
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("search_profiles succeeds");

        let flamegraph = text_json(&result);
        assert_eq!(flamegraph["flamebearer"]["numTicks"], 10);
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn compare_profiles_returns_the_diff() {
        let (base_url, router) = mock_json_router(
            "GET /pyroscope/render-diff?",
            r#"{"flamebearer":{"names":[],"levels":[],"numTicks":0,"maxSelf":0},"metadata":{"format":"double","sampleRate":0,"units":"","name":""},"leftTicks":5,"rightTicks":10}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .compare_profiles(
                Parameters(CompareProfilesParams {
                    query: "cpu".to_string(),
                    left_from: Some("now-2h".to_string()),
                    left_until: Some("now-1h".to_string()),
                    right_from: Some("now-1h".to_string()),
                    right_until: Some("now".to_string()),
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("compare_profiles succeeds");

        let diff = text_json(&result);
        assert_eq!(diff["leftTicks"], 5);
        assert_eq!(diff["rightTicks"], 10);
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn profiles_for_trace_lists_correlated_profiles() {
        let (base_url, router) = mock_json_router(
            "GET /api/profiles/trace/abc123",
            r#"[{"profileID":"p1","timeUnixNano":"1","durationNano":"1","sampleType":"cpu","sampleUnit":"nanoseconds","serviceName":"checkout"}]"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .profiles_for_trace(
                Parameters(ProfilesForTraceParams {
                    trace_id: "abc123".to_string(),
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("profiles_for_trace succeeds");

        let profiles = text_json(&result);
        assert_eq!(profiles[0]["profileID"], "p1");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn discover_attributes_profiles_signal_without_tag_lists_label_names() {
        let (base_url, router) = mock_json_router(
            "GET /pyroscope/label-names",
            r#"{"names":["service_name"]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .discover_attributes(
                Parameters(DiscoverAttributesParams {
                    signal: Signal::Profiles,
                    tag: None,
                    scope: None,
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("discover_attributes succeeds");

        let names = text_json(&result);
        assert_eq!(names["names"][0], "service_name");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn discover_attributes_profiles_signal_with_tag_lists_label_values() {
        let (base_url, router) =
            mock_json_router("GET /pyroscope/label-values?", r#"{"names":["checkout"]}"#).await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .discover_attributes(
                Parameters(DiscoverAttributesParams {
                    signal: Signal::Profiles,
                    tag: Some("service_name".to_string()),
                    scope: None,
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("discover_attributes succeeds");

        let values = text_json(&result);
        assert_eq!(values["names"][0], "checkout");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn discover_attributes_traces_scope_without_tag_routes_to_v2_tags() {
        let (base_url, router) = mock_json_router(
            "GET /tempo/api/v2/search/tags?",
            r#"{"scopes":[{"scope":"resource","tags":["service.name"]}]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .discover_attributes(
                Parameters(DiscoverAttributesParams {
                    signal: Signal::Traces,
                    tag: None,
                    scope: Some(TraceTagScope::Resource),
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("discover_attributes succeeds");

        let value = text_json(&result);
        assert_eq!(value["scopes"][0]["scope"], "resource");
        assert_eq!(value["scopes"][0]["tags"][0], "service.name");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn discover_attributes_traces_scope_with_tag_routes_to_v2_tag_values() {
        let (base_url, router) = mock_json_router(
            "GET /tempo/api/v2/search/tag/resource.service.name/values",
            r#"{"tagValues":[{"tag":"resource.service.name","value":"checkout"}]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .discover_attributes(
                Parameters(DiscoverAttributesParams {
                    signal: Signal::Traces,
                    tag: Some("service.name".to_string()),
                    scope: Some(TraceTagScope::Resource),
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("discover_attributes succeeds");

        let value = text_json(&result);
        assert_eq!(value["tagValues"][0]["value"], "checkout");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn discover_attributes_scope_on_a_non_traces_signal_is_rejected() {
        // No mock router needed: the tool must reject before any request is
        // sent, since `scope` (Tempo v2) has no meaning for logs/metrics.
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(1),
        );

        let err = server
            .discover_attributes(
                Parameters(DiscoverAttributesParams {
                    signal: Signal::Logs,
                    tag: None,
                    scope: Some(TraceTagScope::Resource),
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect_err("scope on signal: logs must be rejected");
        assert!(err.message.contains("traces"), "got {}", err.message);
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

    #[tokio::test]
    async fn discover_datasets_lists_the_tenant_and_its_datasets_as_markdown() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            // First request: whoami.
            let (mut socket, _) = listener.accept().await.expect("accept whoami request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..request_len])
                    .expect("request is UTF-8")
                    .starts_with("GET /api/v1/whoami "),
                "discover_datasets must call whoami first"
            );
            let body = br#"{"user_id":"user-a","tenant":{"id":"acme","slug":"acme","name":"Acme Corp"},"dataset":"production"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write whoami response headers");
            socket.write_all(body).await.expect("write whoami body");
            drop(socket);

            // Second request: list_tenant_tables, on its own connection.
            let (mut socket, _) = listener.accept().await.expect("accept tables request");
            let mut request = [0_u8; 4096];
            let request_len = socket.read(&mut request).await.expect("read request");
            assert!(
                std::str::from_utf8(&request[..request_len])
                    .expect("request is UTF-8")
                    .starts_with("GET /api/v1/tenants/acme/tables"),
                "discover_datasets must call list_tenant_tables for the resolved tenant"
            );
            let body = br#"{"tenant_id":"acme","tables":[],"datasets":[{"dataset":"production","tables":[{"name":"traces","schema_type":"traces","description":"d"}]},{"dataset":"staging","tables":[]}]}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write tables response headers");
            socket.write_all(body).await.expect("write tables body");
        });
        let server = McpServer::new(format!("http://{addr}"), std::time::Duration::from_secs(1));

        let result = server
            .discover_datasets(Extension(valid_parts()))
            .await
            .expect("discover_datasets succeeds");

        let ContentBlock::Text(text) = &result.content[0] else {
            panic!("discover_datasets returns a text result");
        };
        assert!(
            text.text.contains("Acme Corp") && text.text.contains("acme"),
            "missing tenant line: {}",
            text.text
        );
        assert!(
            text.text.contains("`production` (current)"),
            "missing current-dataset marker: {}",
            text.text
        );
        assert!(
            text.text.contains("`staging`") && !text.text.contains("`staging` (current)"),
            "staging must not be marked current: {}",
            text.text
        );
        assert!(
            text.text.contains("`production` (current) — 1 table"),
            "wrong production table count: {}",
            text.text
        );
        assert!(
            text.text.contains("`staging` — 0 tables"),
            "wrong staging table count: {}",
            text.text
        );
        router.await.expect("mock router task panicked");
    }

    /// D10: a dataset-restricted credential's `discover_datasets` listing
    /// never names a dataset outside its restriction, even one that is
    /// provisioned in the tenant.
    #[tokio::test]
    async fn discover_datasets_hides_datasets_outside_the_callers_restriction() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock router");
        let addr = listener.local_addr().expect("mock router address");
        let router = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept whoami request");
            let mut request = [0_u8; 4096];
            let _request_len = socket.read(&mut request).await.expect("read request");
            let body = br#"{"user_id":"","tenant":{"id":"acme","slug":"acme","name":"Acme Corp"},"dataset":"production","dataset_ids":["production"]}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write whoami response headers");
            socket.write_all(body).await.expect("write whoami body");
            drop(socket);

            let (mut socket, _) = listener.accept().await.expect("accept tables request");
            let mut request = [0_u8; 4096];
            let _request_len = socket.read(&mut request).await.expect("read request");
            let body = br#"{"tenant_id":"acme","tables":[],"datasets":[{"dataset":"production","tables":[{"name":"traces","schema_type":"traces","description":"d"}]},{"dataset":"staging","tables":[{"name":"logs","schema_type":"logs","description":"d"}]}]}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write tables response headers");
            socket.write_all(body).await.expect("write tables body");
        });
        let server = McpServer::new(format!("http://{addr}"), std::time::Duration::from_secs(1));

        let result = server
            .discover_datasets(Extension(valid_parts()))
            .await
            .expect("discover_datasets succeeds");

        let ContentBlock::Text(text) = &result.content[0] else {
            panic!("discover_datasets returns a text result");
        };
        assert!(
            text.text.contains("`production`"),
            "the restricted dataset must still be listed: {}",
            text.text
        );
        assert!(
            !text.text.contains("staging"),
            "a dataset outside the restriction must not appear, even by name: {}",
            text.text
        );
        router.await.expect("mock router task panicked");
    }

    /// D10: `tenant_list_tables` filters both the flat `tables` list and the
    /// per-dataset `datasets` grouping to the caller's restriction — an
    /// unlisted dataset must not appear in either shape.
    #[tokio::test]
    async fn tenant_list_tables_hides_datasets_outside_the_callers_restriction() {
        let (base_url, router) = mock_json_router(
            "GET /api/v1/tenants/acme/tables",
            r#"{"tenant_id":"acme","tables":[
                {"name":"traces","schema_type":"traces","description":"d","dataset":"production"},
                {"name":"logs","schema_type":"logs","description":"d","dataset":"staging"}
            ],"datasets":[
                {"dataset":"production","tables":[{"name":"traces","schema_type":"traces","description":"d","dataset":"production"}]},
                {"dataset":"staging","tables":[{"name":"logs","schema_type":"logs","description":"d","dataset":"staging"}]}
            ]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));
        let mut parts = valid_parts();
        parts.extensions.insert(audit::CallerDatasetIds(Some(vec![
            "production".to_string(),
        ])));

        let result = server
            .tenant_list_tables(
                Parameters(TenantOnlyParams {
                    tenant_id: "acme".to_string(),
                }),
                Extension(parts),
            )
            .await
            .expect("tenant_list_tables succeeds");

        let body = text_json(&result);
        let datasets: Vec<&str> = body["datasets"]
            .as_array()
            .expect("datasets array")
            .iter()
            .map(|d| d["dataset"].as_str().expect("dataset name"))
            .collect();
        assert_eq!(datasets, vec!["production"], "got {body}");
        let tables: Vec<&str> = body["tables"]
            .as_array()
            .expect("tables array")
            .iter()
            .map(|t| t["dataset"].as_str().expect("table dataset"))
            .collect();
        assert_eq!(tables, vec!["production"], "got {body}");
        router.await.expect("mock router task panicked");
    }

    /// An unrestricted credential's `tenant_list_tables` result is unchanged
    /// — every dataset the router reports is still listed.
    #[tokio::test]
    async fn tenant_list_tables_is_unfiltered_for_an_unrestricted_credential() {
        let (base_url, router) = mock_json_router(
            "GET /api/v1/tenants/acme/tables",
            r#"{"tenant_id":"acme","tables":[
                {"name":"traces","schema_type":"traces","description":"d","dataset":"production"},
                {"name":"logs","schema_type":"logs","description":"d","dataset":"staging"}
            ],"datasets":[
                {"dataset":"production","tables":[{"name":"traces","schema_type":"traces","description":"d","dataset":"production"}]},
                {"dataset":"staging","tables":[{"name":"logs","schema_type":"logs","description":"d","dataset":"staging"}]}
            ]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        let result = server
            .tenant_list_tables(
                Parameters(TenantOnlyParams {
                    tenant_id: "acme".to_string(),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("tenant_list_tables succeeds");

        let body = text_json(&result);
        assert_eq!(
            body["datasets"].as_array().expect("datasets array").len(),
            2
        );
        assert_eq!(body["tables"].as_array().expect("tables array").len(), 2);
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn check_tenant_scope_rejects_a_tenant_argument_that_does_not_match_the_credential() {
        // No mock router needed: the mismatch must be caught before any
        // request is sent.
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(1),
        );
        let mut parts = valid_parts();
        parts
            .extensions
            .insert(audit::CallerTenant("acme".to_string()));

        let err = server
            .discover_sources(
                Parameters(DiscoverSourcesParams {
                    tenant: "other".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(parts),
            )
            .await
            .expect_err("a `tenant` argument mismatching the credential must be rejected");

        assert!(err.message.contains("acme"), "got {}", err.message);
        assert!(err.message.contains("other"), "got {}", err.message);
    }

    #[tokio::test]
    async fn check_tenant_scope_allows_a_tenant_argument_that_matches_the_credential() {
        let (base_url, router) = mock_json_router(
            "GET /api/v1/query/sources",
            r#"{"result":"rows","window":{"start_ns":0,"end_ns":1},"rows":[["logs"]]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));
        let mut parts = valid_parts();
        parts
            .extensions
            .insert(audit::CallerTenant("acme".to_string()));

        let result = server
            .discover_sources(
                Parameters(DiscoverSourcesParams {
                    tenant: "acme".to_string(),
                    dataset: "production".to_string(),
                }),
                Extension(parts),
            )
            .await
            .expect("a `tenant` argument matching the credential passes through");

        let sources = text_json(&result);
        assert_eq!(sources["rows"][0][0], "logs");
        router.await.expect("mock router task panicked");
    }

    #[tokio::test]
    async fn get_schema_registry_rejects_a_tenant_argument_that_does_not_match_the_credential() {
        // The schema-registry admin/lookup tools (create/replace/delete/get/
        // validate) take the same `tenant` confirmation as every other tool
        // in this family; `get_schema_registry` must not be the one
        // exception that silently ignores it.
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(1),
        );
        let mut parts = valid_parts();
        parts
            .extensions
            .insert(audit::CallerTenant("acme".to_string()));

        let err = server
            .get_schema_registry(
                Parameters(GetSchemaRegistryParams {
                    tenant: "other".to_string(),
                    namespace: "otel".to_string(),
                    version: "1.43.0".to_string(),
                }),
                Extension(parts),
            )
            .await
            .expect_err("a `tenant` argument mismatching the credential must be rejected");

        assert!(err.message.contains("acme"), "got {}", err.message);
        assert!(err.message.contains("other"), "got {}", err.message);
    }

    #[test]
    fn tenant_and_dataset_are_required_by_the_json_schema_not_just_by_rust_defaults() {
        // Every other field on `SearchTracesParams` is optional, so an empty
        // object only fails on the two now-mandatory arguments — proving the
        // MCP client sees a hard requirement, not a Rust-level default.
        let err = serde_json::from_value::<SearchTracesParams>(serde_json::json!({}))
            .expect_err("omitting `tenant`/`dataset` must fail deserialization");
        let message = err.to_string();
        assert!(
            message.contains("tenant") || message.contains("dataset"),
            "expected the error to name the missing field, got: {message}"
        );
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
            "discover_fields",
            "discover_field_values",
            "discover_sources",
            "query_metrics",
            "search_logs",
            "query_ir",
            "compact_run",
            "compact_status",
            "compact_dry_run",
            "list_schema_registries",
            "resolve_attribute",
            "resolve_entity",
            "resolve_metric",
            "search_schema",
            "create_schema_registry",
            "replace_schema_registry",
            "delete_schema_registry",
        ] {
            assert!(router.has_route(name), "tool `{name}` must be registered");
        }
    }

    /// Discovery must not become a way to schedule scans by accident: the
    /// value tool has to say that reading data is opt-in, and the field tool
    /// has to say that it reads none.
    #[test]
    fn discovery_tool_descriptions_state_what_they_cost() {
        let tools = McpServer::tool_router().list_all();
        let describe = |name: &str| -> String {
            tools
                .iter()
                .find(|t| t.name == name)
                .unwrap_or_else(|| panic!("tool `{name}` is listed"))
                .description
                .as_deref()
                .unwrap_or_default()
                .to_string()
        };
        let fields = describe("discover_fields");
        assert!(
            fields.contains("reads no signal data"),
            "`discover_fields` must say it is free: {fields}"
        );
        let values = describe("discover_field_values");
        assert!(
            values.contains("sample: true") && values.contains("reads data"),
            "`discover_field_values` must say reading data is opt-in: {values}"
        );
    }

    /// The schema tools exist so a model looks up meaning before querying;
    /// their descriptions must say so.
    #[test]
    fn schema_tool_descriptions_steer_resolve_before_query() {
        let tools = McpServer::tool_router().list_all();
        for name in [
            "resolve_attribute",
            "resolve_entity",
            "resolve_metric",
            "search_schema",
        ] {
            let tool = tools
                .iter()
                .find(|t| t.name == name)
                .unwrap_or_else(|| panic!("tool `{name}` is listed"));
            let description = tool.description.as_deref().unwrap_or_default();
            assert!(
                description.contains("before") || description.contains("BEFORE"),
                "`{name}` must steer resolving before querying: {description}"
            );
            assert!(
                description.contains("precedence") || description.contains("namespace"),
                "`{name}` must explain namespace-tagged, precedence-ordered hits: {description}"
            );
        }
    }

    #[test]
    fn search_schema_kind_is_a_closed_lowercase_enum() {
        let attr: SearchSchemaParams = serde_json::from_value(serde_json::json!({
            "kind": "attribute", "prefix": "k8s.", "tenant": "acme", "dataset": "production"
        }))
        .unwrap();
        assert_eq!(attr.kind, SchemaKind::Attribute);
        assert_eq!(attr.prefix.as_deref(), Some("k8s."));
        assert!(attr.limit.is_none());
        assert!(
            serde_json::from_value::<SearchSchemaParams>(serde_json::json!({
                "kind": "span", "tenant": "acme", "dataset": "production"
            }))
            .is_err()
        );
        // The advertised schema names every kind (as `enum` or `oneOf`
        // consts), so a client can offer them without guessing.
        let schema = rmcp::schemars::schema_for!(SearchSchemaParams).to_value();
        let text = schema.to_string();
        for kind in ["\"attribute\"", "\"entity\"", "\"metric\""] {
            assert!(text.contains(kind), "schema names {kind}: {text}");
        }
    }

    #[test]
    fn schema_registry_document_accepts_object_or_json_string() {
        let doc = serde_json::json!({ "name": "acme", "version": "1.0.0", "groups": [] });
        let native: CreateSchemaRegistryParams =
            serde_json::from_value(serde_json::json!({ "tenant": "acme", "document": doc }))
                .unwrap();
        assert_eq!(native.document, doc);
        let stringified: CreateSchemaRegistryParams = serde_json::from_value(serde_json::json!({
            "tenant": "acme",
            "document": doc.to_string()
        }))
        .unwrap();
        assert_eq!(stringified.document, doc);
        assert!(registry_document(doc).is_ok());
        assert!(registry_document(serde_json::json!([1, 2])).is_err());

        let schema = rmcp::schemars::schema_for!(ReplaceSchemaRegistryParams);
        assert_eq!(
            schema
                .pointer("/properties/document/type")
                .and_then(|t| t.as_str()),
            Some("object")
        );
    }

    #[test]
    fn exhausted_429_maps_to_a_throttled_error_naming_the_wait() {
        let response = axum::http::Response::builder()
            .status(429)
            .header("retry-after", "30")
            .body("")
            .unwrap();
        let err: signaldb_sdk::Error<()> =
            signaldb_sdk::Error::UnexpectedResponse(reqwest::Response::from(response));
        let mapped = map_sdk_err(err, "search_logs");
        assert!(
            mapped.message.starts_with("throttled:"),
            "distinct prefix: {}",
            mapped.message
        );
        assert!(
            mapped.message.contains("retry in 30s"),
            "names the wait: {}",
            mapped.message
        );
        let data = mapped.data.expect("structured data");
        assert_eq!(data["retryAfterMs"], 30_000);
        assert_eq!(
            data["http_status"], 429,
            "audit classifier still sees the status"
        );
    }

    #[test]
    fn exhausted_429_without_retry_after_still_reads_as_throttled() {
        let response = axum::http::Response::builder()
            .status(429)
            .body("")
            .unwrap();
        let err: signaldb_sdk::Error<()> =
            signaldb_sdk::Error::UnexpectedResponse(reqwest::Response::from(response));
        let mapped = map_sdk_err(err, "search_logs");
        assert!(mapped.message.starts_with("throttled:"));
        let data = mapped.data.expect("structured data");
        assert!(data["retryAfterMs"].is_null());
        assert_eq!(data["http_status"], 429);
    }

    #[test]
    fn schema_errors_keep_validation_paths_in_the_message() {
        let body = signaldb_sdk::types::SchemaError {
            error: "registry document is invalid".to_string(),
            errors: vec![signaldb_sdk::types::ValidationError {
                path: "groups[0].attributes[1].type".to_string(),
                message: "unknown attribute type `strng`".to_string(),
            }],
        };
        let err = signaldb_sdk::Error::ErrorResponse(signaldb_sdk::ResponseValue::new(
            body,
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            reqwest::header::HeaderMap::new(),
        ));
        let mapped = map_schema_err(err, "create_schema_registry");
        assert_eq!(mapped.code, ErrorData::invalid_params("", None).code);
        assert!(
            mapped.message.contains("groups[0].attributes[1].type"),
            "{}",
            mapped.message
        );
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
            },
            "tenant": "acme", "dataset": "production"
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
            },
            "tenant": "acme", "dataset": "production"
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
            "query": ir_document.to_string(),
            "tenant": "acme", "dataset": "production"
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

    // ---- API-key tool dataset_ids / clear_dataset_restriction (phase 5.1
    // of multi-dataset-key-restriction) ----

    #[tokio::test]
    async fn create_api_key_forwards_dataset_ids() {
        let (base_url, router) = mock_capturing_router(
            "POST /api/v1/admin/tenants/acme/api-keys",
            201,
            r#"{"created_at":"2024-01-01T00:00:00Z","id":"key-1","key":"secret","scopes":["traces:read"],"dataset_ids":["production","staging"]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        server
            .create_api_key(
                Extension(valid_parts()),
                Parameters(CreateApiKeyParams {
                    tenant_id: "acme".to_string(),
                    name: None,
                    scopes: vec!["traces:read".to_string()],
                    dataset_ids: Some(vec!["production".to_string(), "staging".to_string()]),
                }),
            )
            .await
            .expect("create_api_key succeeds");

        let request = router.await.expect("mock router task panicked");
        let body = captured_json_body(&request);
        assert_eq!(
            body["dataset_ids"],
            serde_json::json!(["production", "staging"])
        );
    }

    #[tokio::test]
    async fn tenant_create_api_key_forwards_dataset_ids() {
        let (base_url, router) = mock_capturing_router(
            "POST /api/v1/manage/tenants/acme/api-keys",
            201,
            r#"{"id":"key-1","key":"secret","scopes":["traces:read"],"dataset_ids":["production"]}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        server
            .tenant_create_api_key(
                Parameters(TenantCreateApiKeyParams {
                    tenant_id: "acme".to_string(),
                    name: None,
                    scopes: vec!["traces:read".to_string()],
                    dataset_ids: Some(vec!["production".to_string()]),
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("tenant_create_api_key succeeds");

        let request = router.await.expect("mock router task panicked");
        let body = captured_json_body(&request);
        assert_eq!(body["dataset_ids"], serde_json::json!(["production"]));
    }

    #[tokio::test]
    async fn update_api_key_scopes_forwards_dataset_ids_and_clear_flag() {
        let (base_url, router) = mock_capturing_router(
            "PATCH /api/v1/admin/tenants/acme/api-keys/key-1",
            200,
            r#"{"created_at":"2024-01-01T00:00:00Z","id":"key-1"}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        server
            .update_api_key_scopes(
                Extension(valid_parts()),
                Parameters(UpdateApiKeyScopesParams {
                    tenant_id: "acme".to_string(),
                    key_id: "key-1".to_string(),
                    scopes: None,
                    dataset_ids: Some(vec!["production".to_string()]),
                    clear_dataset_restriction: false,
                }),
            )
            .await
            .expect("update_api_key_scopes succeeds");

        let request = router.await.expect("mock router task panicked");
        let body = captured_json_body(&request);
        assert_eq!(body["dataset_ids"], serde_json::json!(["production"]));
    }

    #[tokio::test]
    async fn update_api_key_scopes_forwards_clear_dataset_restriction() {
        let (base_url, router) = mock_capturing_router(
            "PATCH /api/v1/admin/tenants/acme/api-keys/key-1",
            200,
            r#"{"created_at":"2024-01-01T00:00:00Z","id":"key-1"}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        server
            .update_api_key_scopes(
                Extension(valid_parts()),
                Parameters(UpdateApiKeyScopesParams {
                    tenant_id: "acme".to_string(),
                    key_id: "key-1".to_string(),
                    scopes: None,
                    dataset_ids: None,
                    clear_dataset_restriction: true,
                }),
            )
            .await
            .expect("update_api_key_scopes succeeds");

        let request = router.await.expect("mock router task panicked");
        let body = captured_json_body(&request);
        assert_eq!(body["clear_dataset_restriction"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn tenant_update_api_key_forwards_dataset_ids_and_clear_flag() {
        let (base_url, router) = mock_capturing_router(
            "PATCH /api/v1/manage/tenants/acme/api-keys/key-1",
            200,
            r#"{"created_at":"2024-01-01T00:00:00Z","id":"key-1","revoked":false}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        server
            .tenant_update_api_key(
                Parameters(TenantUpdateApiKeyParams {
                    tenant_id: "acme".to_string(),
                    key_id: "key-1".to_string(),
                    scopes: None,
                    dataset_ids: Some(vec!["staging".to_string()]),
                    clear_dataset_restriction: false,
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("tenant_update_api_key succeeds");

        let request = router.await.expect("mock router task panicked");
        let body = captured_json_body(&request);
        assert_eq!(body["dataset_ids"], serde_json::json!(["staging"]));
    }

    #[tokio::test]
    async fn tenant_update_api_key_forwards_clear_dataset_restriction() {
        let (base_url, router) = mock_capturing_router(
            "PATCH /api/v1/manage/tenants/acme/api-keys/key-1",
            200,
            r#"{"created_at":"2024-01-01T00:00:00Z","id":"key-1","revoked":false}"#,
        )
        .await;
        let server = McpServer::new(base_url, std::time::Duration::from_secs(1));

        server
            .tenant_update_api_key(
                Parameters(TenantUpdateApiKeyParams {
                    tenant_id: "acme".to_string(),
                    key_id: "key-1".to_string(),
                    scopes: None,
                    dataset_ids: None,
                    clear_dataset_restriction: true,
                }),
                Extension(valid_parts()),
            )
            .await
            .expect("tenant_update_api_key succeeds");

        let request = router.await.expect("mock router task panicked");
        let body = captured_json_body(&request);
        assert_eq!(body["clear_dataset_restriction"], serde_json::json!(true));
    }

    /// D1a: `clear_dataset_restriction: true` together with a non-empty
    /// `dataset_ids` is contradictory and must be rejected before any router
    /// request is made — the router base URL is deliberately invalid so the
    /// test fails loudly if the handler tries to reach it anyway.
    #[tokio::test]
    async fn update_api_key_scopes_rejects_contradictory_dataset_update_without_calling_router() {
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(1),
        );

        let err = server
            .update_api_key_scopes(
                Extension(valid_parts()),
                Parameters(UpdateApiKeyScopesParams {
                    tenant_id: "acme".to_string(),
                    key_id: "key-1".to_string(),
                    scopes: None,
                    dataset_ids: Some(vec!["production".to_string()]),
                    clear_dataset_restriction: true,
                }),
            )
            .await
            .expect_err("a contradictory dataset_ids + clear_dataset_restriction must be rejected");

        assert!(
            err.message.contains("dataset_ids")
                && err.message.contains("clear_dataset_restriction"),
            "got {}",
            err.message
        );
    }

    #[tokio::test]
    async fn tenant_update_api_key_rejects_contradictory_dataset_update_without_calling_router() {
        let server = McpServer::new(
            "http://router.invalid".to_string(),
            std::time::Duration::from_secs(1),
        );

        let err = server
            .tenant_update_api_key(
                Parameters(TenantUpdateApiKeyParams {
                    tenant_id: "acme".to_string(),
                    key_id: "key-1".to_string(),
                    scopes: None,
                    dataset_ids: Some(vec!["production".to_string(), "staging".to_string()]),
                    clear_dataset_restriction: true,
                }),
                Extension(valid_parts()),
            )
            .await
            .expect_err("a contradictory dataset_ids + clear_dataset_restriction must be rejected");

        assert!(
            err.message.contains("dataset_ids")
                && err.message.contains("clear_dataset_restriction"),
            "got {}",
            err.message
        );
    }
}
