## Purpose

Defines the Model Context Protocol surface SignalDB exposes to AI agents: how they authenticate, which read/exploration tools they can use, and how those calls stay scoped to the caller's tenant.

## ADDED Requirements

### Requirement: MCP transport and session initialization

The MCP server SHALL run as a standalone service whose only channel to SignalDB is the router's HTTP API via the generated SDK; it SHALL NOT be mounted as an in-process route on the router, and `/mcp` SHALL always be served on the MCP service's own port (a sidecar), never on the router's port. It SHALL expose the Model Context Protocol over Streamable HTTP at the `/mcp` path as its production transport, and SHALL additionally support a stdio transport for single-user local development. It SHALL respond to the MCP `initialize` handshake advertising `tools` and `resources` capabilities.

Streamable HTTP SHALL carry credentials in the `Authorization` and `X-Tenant-ID` headers on every request, and each request is forwarded as that caller. The stdio transport has no per-request headers and holds no credential of its own: `initialize`, `tools/list`, `prompts/list`, and `resources/list` work, but a tool that forwards the caller's credential to the router SHALL fail with a clear MCP error rather than reaching the router unauthenticated. Stdio is documented as development-only, never for production.

The `resources` capability SHALL be advertised only because the compiled-in MCP Apps UI documents (the `get_trace` waterfall and `get_profile` flamegraph apps) are served over `resources/list` and `resources/read`; the server SHALL expose no tenant data as MCP resources.

#### Scenario: Streamable HTTP initialize succeeds

- **WHEN** an MCP client sends an `initialize` request over Streamable HTTP to `/mcp` with a valid tenant bearer token and `X-Tenant-ID` header
- **THEN** the server completes the handshake and advertises `tools` and `resources` capabilities

#### Scenario: Stdio has no credential

- **WHEN** the server is started in stdio mode
- **THEN** an MCP client can `initialize` and list tools, but invoking a tool that reads tenant data returns an MCP error, and no unauthenticated request reaches the router

#### Scenario: Resources hold only UI documents

- **WHEN** an MCP client issues `resources/list`
- **THEN** the result contains only the compiled-in MCP Apps UI documents and no tenant schema or data resource

### Requirement: Bearer authentication and credential forwarding

The MCP server SHALL hold no credential of its own and SHALL NOT validate credentials — the router is the sole authority on whether a credential is valid and what it may access. On each Streamable HTTP request it SHALL require the presence of a bearer token and `X-Tenant-ID` header, reject a request that carries neither, and forward the caller's bearer and tenant headers verbatim on every downstream call, so the router enforces the same tenant isolation and quotas as for any HTTP caller. An invalid, expired, or revoked credential is not rejected locally; it is rejected by the router and surfaces as a clean MCP tool error. A session SHALL be bound to the tenant and credential seen on its first request; a later request on the same session declaring a different tenant, or presenting a different credential, SHALL be rejected.

#### Scenario: Missing credential is rejected at the MCP layer

- **WHEN** a client sends a request to `/mcp` without a bearer token or without `X-Tenant-ID`
- **THEN** the server returns 401 and the request never reaches the MCP transport

#### Scenario: Invalid credential is rejected by the router

- **WHEN** a session presents a bearer token that the router rejects as invalid or revoked
- **THEN** the tool call surfaces the router's rejection as a clean MCP error (the MCP server does not pre-validate)

#### Scenario: Session cannot switch identity mid-stream

- **WHEN** a session established for tenant A sends a later request declaring tenant B, or a different credential
- **THEN** the request is rejected rather than served as either identity

#### Scenario: Downstream calls are made as the caller

- **WHEN** an authenticated session invokes a tool that reads tenant data
- **THEN** the resulting request to the query API carries the caller's bearer token and `X-Tenant-ID`, and the server adds no privilege of its own

#### Scenario: Cross-tenant access is denied

- **WHEN** a session authenticated for tenant A invokes a tool referencing data that belongs to tenant B
- **THEN** no tenant B data is returned, because the forwarded credential scopes the query to tenant A

### Requirement: Query and exploration tools

The MCP server SHALL expose read-only tools that wrap the SignalDB query API: trace search (`search_traces`), single-trace retrieval (`get_trace`), log search (`search_logs`), metric query (`query_metrics`), attribute discovery (`discover_attributes`), and metric-name discovery (`discover_metrics`). Each tool SHALL return structured JSON derived from the API response. In v1 these tools SHALL be visible to every authenticated tenant session without role-based filtering.

**Signal-aware attribute discovery.** `discover_attributes` SHALL accept an optional `signal` argument (`traces` | `logs` | `metrics`, default `traces`) selecting the backend it queries: `traces` uses the Tempo tag-name/tag-value endpoints, `logs` uses the Loki label-name/label-value endpoints, and `metrics` uses the Prometheus label-name/label-value endpoints. Called without a `tag` argument it SHALL return the known names for that signal; called with a `tag` it SHALL return the known values for that name. Results SHALL be scoped to the caller's tenant regardless of signal.

**Metric-name discovery.** `discover_metrics` SHALL return the distinct metric names visible to the caller's tenant, sourced from the Prometheus label-value endpoint for the `__name__` label. It SHALL accept the same optional `dataset` argument, dataset-scoping, and payload-cap rules as the other query tools.

**Dataset selection.** Each tool SHALL accept an optional `dataset` argument. When omitted, the session's default dataset (from the resolved tenant context) is used. When provided, it SHALL be forwarded as `X-Dataset-ID` and validated server-side against the caller's tenant context; a dataset the caller may not access SHALL be rejected with an access-denied error rather than silently substituting the default.

**Bounded payloads.** Each tool SHALL cap its serialized result at a fixed byte budget. When the downstream response exceeds the cap, the tool SHALL return valid structured JSON truncated at a record boundary, carrying a `truncated: true` flag and a hint to narrow the query; it SHALL NOT return an unbounded or malformed payload. Clients detect truncation via the flag.

#### Scenario: Trace search returns matching traces

- **WHEN** an authenticated session calls `search_traces` with a TraceQL query and time range
- **THEN** the tool returns the matching traces scoped to the caller's tenant as structured JSON

#### Scenario: Omitted dataset uses the session default

- **WHEN** a session calls a query tool without a `dataset` argument
- **THEN** the query is forwarded for the session's default dataset

#### Scenario: Explicit accessible dataset is forwarded

- **WHEN** a session calls a query tool with a `dataset` the caller's tenant may access
- **THEN** the query is forwarded with that dataset as `X-Dataset-ID`

#### Scenario: Inaccessible dataset is rejected

- **WHEN** a session calls a query tool with a `dataset` the caller's tenant may not access
- **THEN** the tool returns an access-denied error and forwards no query

#### Scenario: Oversized result is truncated with a flag

- **WHEN** a query tool's downstream result exceeds the payload cap
- **THEN** the tool returns valid JSON marked `truncated: true` with a narrowing hint, not an unbounded blob

#### Scenario: Get trace by id when absent

- **WHEN** a session calls `get_trace` with a trace id that does not exist for the caller's tenant
- **THEN** the tool returns a clean MCP "not found" error rather than an empty success or a transport failure

#### Scenario: Invalid query surfaces an actionable error

- **WHEN** a session calls a query tool with a malformed query expression
- **THEN** the tool returns an MCP tool error describing the problem, not a generic internal error

#### Scenario: Rate-limited call is reported as retryable

- **WHEN** a query tool call is rejected by the router's per-tenant rate limit
- **THEN** the tool returns an MCP error indicating the request was throttled and can be retried

#### Scenario: Attribute discovery defaults to traces

- **WHEN** a session calls `discover_attributes` without a `signal` argument
- **THEN** the tool returns Tempo trace-attribute names for the caller's tenant

#### Scenario: Attribute discovery for logs

- **WHEN** a session calls `discover_attributes` with `signal: "logs"` and no `tag`
- **THEN** the tool returns the Loki label names known for the caller's tenant

#### Scenario: Attribute discovery for metrics

- **WHEN** a session calls `discover_attributes` with `signal: "metrics"` and a `tag`
- **THEN** the tool returns the Prometheus label values for that label name, scoped to the caller's tenant

#### Scenario: Discover metric names

- **WHEN** a session calls `discover_metrics`
- **THEN** the tool returns the distinct metric names visible to the caller's tenant

#### Scenario: Tools are listed for any authenticated tenant session

- **WHEN** any authenticated tenant session issues `tools/list`
- **THEN** every advertised query/exploration tool is present in the returned list
