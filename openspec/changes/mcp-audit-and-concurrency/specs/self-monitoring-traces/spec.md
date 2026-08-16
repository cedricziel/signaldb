## ADDED Requirements

### Requirement: MCP tool-call spans

Every MCP tool call SHALL run inside one INTERNAL span opened through a
self-monitoring span factory, named `tools/call {tool}` per the MCP
conventions (span name `{mcp.method.name} {gen_ai.tool.name}`) and carrying
`mcp.method.name = "tools/call"`, `gen_ai.tool.name` (the tool), the
session identity under `mcp.session.id`, and `signaldb.tenant.id` /
`signaldb.dataset.id`; because the pinned semconv snapshot has moved the
MCP attributes to the GenAI conventions repository, SignalDB SHALL keep
the emitted names pinned to the semconv constants and covered by its own
convention registry (referencing the upstream definitions where the
snapshot allows it) so the conformance checks still validate them; downstream SDK calls to the router are children of it,
so an agent's tool call is traceable end-to-end into the query it caused.
The span SHALL set status Error only for a failed call — never for a
denied (`4xx`) or throttled outcome — and SHALL then carry `error.type`.
Tool arguments and results SHALL NOT be recorded on the span. MCP HTTP
requests are covered by the existing HTTP-server-span requirement; the MCP
server SHALL satisfy it (`POST /mcp` server spans with W3C parent adoption).

#### Scenario: A tool call is one span with its router calls beneath it

- **WHEN** a session calls `get_trace` and the server fetches it from the router
- **THEN** one `tools/call get_trace` INTERNAL span is exported with
  `gen_ai.tool.name="get_trace"` and the tenant, the outbound request to the
  router carries W3C trace context from that span, and the router's
  `GET /tempo/api/traces/{trace_id}` server span is its descendant

#### Scenario: Denied is not an error span

- **WHEN** a tool call is denied by the router with `403`
- **THEN** the tool span's status is not Error, and `outcome`-style
  classification lives in the audit event, not the span status

#### Scenario: MCP HTTP requests are server spans

- **WHEN** an MCP client posts to `/mcp` with a `traceparent`
- **THEN** a `POST /mcp` SERVER span is exported as a child of the caller's
  span, per the HTTP-server-span requirement
