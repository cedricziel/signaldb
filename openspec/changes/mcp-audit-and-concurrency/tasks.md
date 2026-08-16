## 1. Span factory + registry (common)

- [ ] 1.1 Failing test in `common::self_monitoring::spans`: `mcp_tool_span("get_trace", "acme", Some("prod"), "sess-1")` is INTERNAL, named `tools/call get_trace`, carries `mcp.method.name`, `gen_ai.tool.name`, `mcp.session.id`, `signaldb.tenant.id`, `signaldb.dataset.id`, empty `error.type` (`cargo test -p common`)
- [ ] 1.2 Implement `mcp_tool_span`; declare `mcp.method.name`, `mcp.session.id`, `gen_ai.tool.name` in `otel/registry/signaldb.yaml` (mirroring the GenAI-repo definitions, stability development, note on migration); run the weaver registry check
- [ ] 1.3 Register `signaldb_mcp_tool_calls_total{tool,outcome}` and `signaldb_mcp_tool_call_duration_seconds{tool}` in `app_metrics`; unit test they record

## 2. Audit wrapper (mcp-server)

- [ ] 2.1 Failing tests (`cargo test -p mcp-server`, tracing test subscriber + mock router): one audit event per call with `tool`, `tenant_id`, `dataset`, `session_id`, `outcome`, `duration_ms`; `ok` at info, 403 → `denied` at warn, 500 → `error` at error with `error.type`, truncated result → `truncated`, `throttled:` error → `throttled`; the LogQL expression passed to `search_logs` appears in no info-level event; metrics increment
- [ ] 2.2 Implement the dispatch wrapper (override `call_tool` or `AuditedToolRouter`) that opens `mcp_tool_span`, times, classifies, emits the audit event, records metrics; session id from `Mcp-Session-Id` (`"stdio"` on stdio)
- [ ] 2.3 Failing test then implement: tool span status is Error only for `outcome=error`; denied/throttled leave status unset

## 3. HTTP server spans on /mcp

- [ ] 3.1 Failing test: a `POST /mcp` through `mcp_http_router` with a `traceparent` yields a SERVER span named `POST /mcp` parented to the caller (in-memory exporter, same harness as the router's tests)
- [ ] 3.2 Layer `http_metrics_middleware` + `http_trace_context_middleware` on the MCP router; ensure the nested service reports `http.route=/mcp`

## 4. Trace continuity MCP → router

- [ ] 4.1 (Depends on `sdk-retry-on-throttle`, which makes the SDK inject W3C trace context on outbound requests.) tests-integration: an MCP `get_trace` call produces a router server span whose trace id equals the tool span's; if the SDK change has not merged yet, land this task in a follow-up PR once it has

## 5. Per-session concurrency bound

- [ ] 5.1 Failing tests: 8 concurrent calls in one session all run; the 9th (with a tool that blocks) returns "too many concurrent tool calls (limit 8)" within the wait bound, audited `outcome=error, error.type=concurrency_limit`; a second session is unaffected; permits are released after completion and on error
- [ ] 5.2 Implement `[mcp].max_concurrent_tool_calls` (default 8), per-session semaphores on `McpAppState` with a 2 s acquire timeout, cleanup on session eviction; stdio uses one shared semaphore

## 6. Docs, skills, hygiene

- [ ] 6.1 Docs (route via the docs skill): `docs/users/mcp.md` — audit event fields and levels, concurrency limit and its error; operations self-monitoring docs — the two MCP metrics and the `tools/call {tool}` span; configuration reference for `max_concurrent_tool_calls`
- [ ] 6.2 Update the `configuration` skill (new key) and the `mcp`-related section of any skill describing MCP observability
- [ ] 6.3 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`; weaver live-check on a local run; `openspec validate mcp-audit-and-concurrency --type change --strict`; close #629
