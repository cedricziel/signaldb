## Why

The MCP server is the surface agents use to read tenant data, yet an operator cannot answer "which tenant called which tool, how often, and did it succeed?" — `server.rs` emits a single `warn!` in 1,700 lines, no per-call event, no span, and the `/mcp` router installs none of the HTTP server-span middleware every other HTTP surface has (the self-monitoring spec already names "MCP server HTTP" as required). There is also no bound on how many tool calls one session may run at once, so a runaway agent can monopolise the sidecar and, through it, the router's per-tenant budget. Issue #629 asked for both; with `sdk-retry-on-throttle` making the SDK absorb 429s, the remaining hardening is audit + concurrency.

## What Changes

- Every MCP tool call emits exactly one structured audit event (`info` on success/denied, `warn` on denied-for-authorization, `error` on failure) with bounded-cardinality fields: `tool`, `tenant_id`, `dataset` (when supplied), `outcome` (`ok | denied | throttled | error | truncated`), `duration_ms`, `session_id`, and — for `error` — `error.type`. Argument payloads and query text are never logged above `debug`.
- Every tool call runs inside a span from a self-monitoring factory (`mcp_tool_span`) carrying the same fields, and the `/mcp` router gets the shared HTTP server-span and metrics middleware so MCP requests appear as `POST /mcp` server spans like every other surface.
- A `signaldb_mcp_tool_calls_total{tool,outcome}` counter and a `signaldb_mcp_tool_call_duration_seconds{tool}` histogram are registered.
- Per-session concurrency is bounded: at most `N` tool calls in flight per MCP session (default 8, config `[mcp].max_concurrent_tool_calls`); an excess call is not queued indefinitely — it waits up to a short bound and then returns a distinct "too many concurrent tool calls" error, so a stuck agent fails fast rather than piling up.
- Denied calls (router 401/403) are distinguishable in logs from failed calls (`outcome=denied`, `warn`).

No **BREAKING** changes.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `mcp-server`: ADDED requirements — "Every tool call is audited" and "Concurrent tool calls are bounded per session".
- `self-monitoring-traces`: ADDED requirement — "MCP tool-call spans" (INTERNAL span per tool call from a factory, with bounded attributes; MCP HTTP requests already fall under the existing HTTP-server-span requirement, which the MCP router now satisfies).

## Impact

- **mcp-server**: `server.rs` (a `call_tool` wrapper around the `tool_router` dispatch that opens the span, times, classifies outcome, emits the audit event, records metrics, and holds the per-session semaphore permit), `lib.rs` (`mcp_http_router` installs `http_metrics_middleware` + `http_trace_context_middleware`; session-keyed semaphores), config (`[mcp].max_concurrent_tool_calls`).
- **common**: `self_monitoring::spans::mcp_tool_span(tool, tenant, session)`; metric registrations.
- **CLI/UI/API parity**: not applicable — this is server-side observability of the MCP surface; the surfaces are unaffected.
- **docs**: `docs/users/mcp.md` (audit event shape, concurrency error), operations docs (metrics), `configuration` skill.
- Closes #629.
