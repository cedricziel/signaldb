## Context

See proposal.md — Why. Current state: `src/mcp-server/src/server.rs` dispatches tools through rmcp's `#[tool_router]` / `#[tool_handler]` macros; there is no wrapper around dispatch. `lib.rs::mcp_http_router` nests the `StreamableHttpService` under `/mcp` behind `mcp_auth_middleware` only — the shared `common::self_monitoring::{http_metrics_middleware, http_trace_context_middleware}` used by the router and acceptor are absent. Session identity already exists (`session_bindings: DashMap<String, SessionBinding>` keyed by `Mcp-Session-Id`). Outbound SDK requests carry no `traceparent`. The pinned semconv 1.43 snapshot has _moved_ the MCP conventions to the GenAI repo (`vendor/otel-semconv/1.43.0/model/mcp/deprecated/*`), so `mcp.*`/`gen_ai.tool.name` are not in the pinned registry; SignalDB has its own registry at `otel/registry/signaldb.yaml` validated by the weaver conformance check.

Constraints: boundary spans come from `common::self_monitoring::spans` factories; no `otel.kind` outside that module; no `#[instrument]` without `skip_all`; logs are structured, no payloads at info.

## Goals / Non-Goals

**Goals:** one audit event + one span + two metrics per tool call, cheap and uniform; `/mcp` gets the same HTTP server spans as other surfaces; per-session concurrency bound; end-to-end trace continuity MCP → router.

**Non-Goals:** rate limiting inside the MCP server (the router's tenant budget + `sdk-retry-on-throttle` cover it); auditing `prompts/get`, `resources/read`, `completion/complete` (cheap, no tenant data — a debug log suffices; can be added later); persisting audit events anywhere but the log/metrics pipeline.

## Decisions

**D1 — One dispatch wrapper, not per-tool code.** Override `ServerHandler::call_tool` on `McpServer` (rmcp lets the `#[tool_handler]` expansion be replaced by a manual impl that delegates to `Self::tool_router().call(...)`) — or, if the macro fights that, wrap the `ToolRouter` in a `AuditedToolRouter` implementing the same trait. The wrapper: acquires the session permit, opens `mcp_tool_span(tool, tenant, dataset, session_id)`, times the call, classifies the outcome from the `CallToolResult`/`ErrorData` (`ok`, `truncated` when the result JSON has `truncated: true`, `denied` for the 401/403 mapping, `throttled` for the `throttled:` prefix from `sdk-retry-on-throttle`, else `error`), emits the audit event, records the metrics. Every existing and future tool is covered without touching it. _Alternative:_ a macro or manual call inside each tool fn — rejected, 23 tools and growing, guaranteed drift.

**D2 — Span factory + registry.** `common::self_monitoring::spans::mcp_tool_span(tool: &str, tenant_id: &str, dataset_id: Option<&str>, session_id: &str) -> Span`: INTERNAL, name `tools/call {tool}`, fields `mcp.method.name="tools/call"`, `gen_ai.tool.name`, `mcp.session.id`, `signaldb.tenant.id`, `signaldb.dataset.id`, `error.type` (empty, recorded on failure). Declare `mcp.method.name`, `mcp.session.id`, `gen_ai.tool.name` in `otel/registry/signaldb.yaml` mirroring the GenAI-repo definitions (brief + stability `development`) with a note that they migrate to the upstream pin when it includes them; the weaver check then passes. Status Error only for `outcome=error`.

**D3 — Session id.** Read `Mcp-Session-Id` from the request `Parts` already available in tools via `Extension<Parts>` (the wrapper has the same `RequestContext`); stdio has no session → `"stdio"`. Never log the bearer or its hash.

**D4 — Trace continuity: SDK injects W3C context on outbound requests.** In `signaldb_sdk::retry::execute` (the `ClientHooks::exec` override from `sdk-retry-on-throttle`), before sending, inject `traceparent`/`tracestate` from `tracing::Span::current()` using the OTel global propagator via `tracing_opentelemetry::OpenTelemetrySpanExt::context()`. `opentelemetry` + `tracing-opentelemetry` are already workspace deps; feature-gate as `signaldb-sdk/tracing` (default on) so a minimal consumer can opt out. When no OTel layer is installed the context is empty and nothing is injected. This makes the router's server span a descendant of the tool span. _Depends on `sdk-retry-on-throttle`_ for the exec hook; if that lands later, the injection goes into a `pre` hook instead (progenitor supports `with_pre_hook_async`).

**D5 — HTTP server spans on `/mcp`.** `mcp_http_router` layers `http_metrics_middleware` and `http_trace_context_middleware` exactly as the router does, outermost. `http.route` will be `/mcp` (nested service; the middleware falls back to `{method}` when no route template — verify it sees `/mcp` via `MatchedPath`; if not, set the route explicitly for the nested service).

**D6 — Concurrency: per-session `tokio::sync::Semaphore` in a `DashMap<session_id, Arc<Semaphore>>` on `McpAppState`.** Default permits 8 (`[mcp].max_concurrent_tool_calls`), acquire with `timeout(2s)`; on timeout return `ErrorData::invalid_request("too many concurrent tool calls (limit 8); wait for in-flight calls to finish", data {limit})` and audit `outcome=error, error.type=concurrency_limit`. Entries are dropped when the session is evicted (same hook that clears `session_bindings`). Stdio: single shared semaphore. _Alternative:_ a global cap — rejected, one session must not starve another.

**D7 — Metrics.** `signaldb_mcp_tool_calls_total{tool,outcome}` counter, `signaldb_mcp_tool_call_duration_seconds{tool}` histogram registered next to the existing app metrics in `common::self_monitoring::app_metrics`. Cardinality: tools (~25) × outcomes (5).

## Risks / Trade-offs

- [rmcp macro may not allow overriding `call_tool` cleanly] → the `AuditedToolRouter` wrapper path is the fallback; both keep tools untouched.
- [Nested-service route template may not resolve] → assert in a test that the exported span is `POST /mcp`, not `POST`.
- [`traceparent` injection changes outbound headers for CLI too] → harmless (routers ignore unknown context; ours adopts it — CLI calls become traceable, a bonus); feature flag exists.
- [Audit at `info` adds one log line per tool call] → that is the requirement; volume is bounded by the concurrency cap and tenant budgets.

## Migration Plan

Additive; deploy the sidecar image. New config key defaults sensibly. Rollback = revert image.

## Open Questions

- Should `prompts/get` / `resources/read` be audited too? Deferrable; not tenant data today.
