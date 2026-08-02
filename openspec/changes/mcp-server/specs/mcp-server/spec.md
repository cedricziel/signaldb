## Purpose

Defines the Model Context Protocol surface SignalDB exposes to AI agents: how they authenticate, which read/exploration tools and schema resources they can use, and how those calls stay scoped to the caller's tenant.

## ADDED Requirements

### Requirement: MCP transport and session initialization

The MCP server SHALL run as a standalone service whose only channel to SignalDB is the router's HTTP API via the generated SDK; it SHALL NOT be mounted as an in-process route on the router, and `/mcp` SHALL always be served on the MCP service's own port (a sidecar), never on the router's port. It SHALL expose the Model Context Protocol over Streamable HTTP at the `/mcp` path as its production transport, and SHALL additionally support a stdio transport for single-user local development. It SHALL respond to the MCP `initialize` handshake advertising `tools` and `resources` capabilities.

Streamable HTTP SHALL carry credentials in the `Authorization` and `X-Tenant-ID` headers on every request, and each request is forwarded as that caller. The stdio transport has no per-request headers; the standalone binary MAY be given a single fixed credential (token + tenant, optional dataset) via CLI flags, environment, or config, which query tools use to reach the router. Started without a configured credential, stdio runs unauthenticated: `initialize`/`tools/list` still work, but a query tool returns a clear "stdio requires a configured credential" error rather than an opaque router auth failure. Stdio is documented as development-only, never for production.

#### Scenario: Streamable HTTP initialize succeeds

- **WHEN** an MCP client sends an `initialize` request over Streamable HTTP to `/mcp` with a valid tenant bearer token and `X-Tenant-ID` header
- **THEN** the server completes the handshake and advertises `tools` and `resources` capabilities

#### Scenario: Stdio without a configured credential

- **WHEN** the server is started in stdio mode with no configured credential
- **THEN** an MCP client can `initialize` and list tools, but invoking a query tool returns a "stdio requires a configured credential" error

#### Scenario: Stdio with a configured credential

- **WHEN** the standalone binary is started in stdio mode with a token + tenant configured, and a query tool is invoked
- **THEN** the tool reaches the router as that configured credential and returns results scoped to that tenant

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

The MCP server SHALL expose read-only tools that wrap the SignalDB query API: trace search (`search_traces`), single-trace retrieval (`get_trace`), log search (`search_logs`), metric query (`query_metrics`), and attribute discovery (`discover_attributes`). Each tool SHALL return structured JSON derived from the API response. In v1 these tools SHALL be visible to every authenticated tenant session without role-based filtering.

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

#### Scenario: Tools are listed for any authenticated tenant session

- **WHEN** any authenticated tenant session issues `tools/list`
- **THEN** every advertised tool — the query/exploration tools and the discovery tools (`list_datasets`, `list_schemas`, `list_tables`) — is present in the returned list

### Requirement: Discovery tools

The MCP server SHALL expose discovery tools scoped to the caller's tenant: `list_datasets` (the datasets the caller may access), `list_schemas` (available signal schemas), and `list_tables` (tables in a dataset). Each SHALL return structured JSON and reflect only the caller's tenant. They take no privileged action and are visible to every authenticated tenant session.

#### Scenario: List datasets returns only the caller's tenant

- **WHEN** an authenticated session for tenant A calls `list_datasets`
- **THEN** the tool returns tenant A's datasets and no other tenant's

#### Scenario: List tables for a dataset

- **WHEN** an authenticated session calls `list_tables` for one of its datasets
- **THEN** the tool returns the tables in that dataset as structured JSON

### Requirement: Schema resources

The MCP server SHALL expose the caller's table schemas (traces, logs, metrics column definitions) as MCP resources readable via `resources/list` and `resources/read`, so agents can ground queries without issuing tool calls.

**URI grammar.** Resource URIs SHALL follow a stable grammar `signaldb://schema/{dataset}/{table}` that identifies the dataset and table but SHALL NOT encode tenant identity — the tenant is taken from the authenticated session, never from the URI. `resources/read` SHALL resolve the schema using the authenticated tenant plus the dataset/table from the URI, and SHALL reject a URI that names an unknown or foreign dataset/table with a not-found error that reveals no schema data. A URI minted for one tenant therefore returns nothing when read by another.

#### Scenario: List and read table schemas

- **WHEN** an authenticated session issues `resources/list` and then `resources/read` for a `signaldb://schema/{dataset}/{table}` URI in its tenant
- **THEN** the server returns the current column definitions for that table in the caller's tenant

#### Scenario: Foreign-tenant URI reveals nothing

- **WHEN** a session for tenant B issues `resources/read` for a URI whose dataset/table belongs only to tenant A
- **THEN** the server returns a not-found error and no schema data, because the tenant comes from the session, not the URI
