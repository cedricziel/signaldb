## ADDED Requirements

### Requirement: Every tool call is audited

The MCP server SHALL emit exactly one structured audit event per tool call, after the call completes, carrying: `tool` (the tool name), `tenant_id`, `dataset` (when the call named one), `session_id`, `outcome` (`ok`, `truncated`, `denied`, `throttled`, or `error`), `duration_ms`, and for `error` the classified `error.type`. Successful and truncated calls SHALL log at `info`; denied calls (the router rejected the caller's credential or tenant/dataset access) SHALL log at `warn` so probing is visible; failed calls SHALL log at `error`. Argument payloads, query expressions, and result contents SHALL NOT appear in the audit event or any log at `info` or above. The same fields SHALL be exported as a `signaldb_mcp_tool_calls_total{tool,outcome}` counter and a `signaldb_mcp_tool_call_duration_seconds{tool}` histogram (OTel instruments `signaldb.mcp.tool_calls` and `signaldb.mcp.tool_call.duration`; the tool label is the semconv `gen_ai.tool.name` attribute and the outcome label `signaldb.mcp.outcome`).

#### Scenario: A successful call is audited once

- **WHEN** a session calls `search_traces` and it returns results
- **THEN** exactly one audit event is emitted with `tool="search_traces"`, the session's `tenant_id`, `outcome="ok"`, and a `duration_ms`, and `signaldb_mcp_tool_calls_total{tool="search_traces",outcome="ok"}` increases by one

#### Scenario: A denied call is distinguishable from a failed one

- **WHEN** a session's tool call is rejected by the router with `403` for a dataset it may not access
- **THEN** the audit event has `outcome="denied"` at `warn`, whereas a downstream `500` produces `outcome="error"` at `error`

#### Scenario: Query text stays out of the audit log

- **WHEN** a session calls `search_logs` with a LogQL expression
- **THEN** the audit event names the tool and tenant but does not contain the expression, and no `info`-level log carries it

#### Scenario: A throttled-then-failed call is audited as throttled

- **WHEN** a tool call fails because the router throttled it past the retry budget
- **THEN** the audit event has `outcome="throttled"`

### Requirement: Concurrent tool calls are bounded per session

The MCP server SHALL limit the number of tool calls one session may have in flight at once (`[mcp].max_concurrent_tool_calls`, default 8). A call arriving while the session is at its limit SHALL wait for a permit for a short bounded time and, if none frees up, SHALL return a distinct "too many concurrent tool calls" error rather than queueing indefinitely or failing the whole session. The bound is per session, so one runaway agent cannot starve another session's tenant.

#### Scenario: Calls within the bound proceed

- **WHEN** a session issues 8 tool calls concurrently with the default bound
- **THEN** all 8 execute

#### Scenario: Excess concurrent calls fail fast and distinctly

- **WHEN** a session already has 8 tool calls in flight and issues a 9th that cannot obtain a permit within the wait bound
- **THEN** the 9th returns a "too many concurrent tool calls" error naming the bound, the other 8 are unaffected, and the audit event for the 9th has `outcome="error"` with `error.type="concurrency_limit"`

#### Scenario: Sessions are isolated

- **WHEN** session A is at its concurrency bound
- **THEN** session B's tool calls are admitted normally
