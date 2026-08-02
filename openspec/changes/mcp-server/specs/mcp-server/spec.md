## Purpose

Defines the Model Context Protocol surface SignalDB exposes to AI agents: how they authenticate, which read/exploration tools and schema resources they can use, and how those calls stay scoped to the caller's tenant.

## ADDED Requirements

### Requirement: MCP transport and session initialization

The MCP server SHALL expose the Model Context Protocol over Streamable HTTP at the `/mcp` path, and SHALL additionally support a stdio transport for local development. It SHALL respond to the MCP `initialize` handshake advertising `tools` and `resources` capabilities.

#### Scenario: Streamable HTTP initialize succeeds

- **WHEN** an MCP client sends an `initialize` request over Streamable HTTP to `/mcp` with a valid tenant bearer token and `X-Tenant-ID` header
- **THEN** the server completes the handshake and advertises `tools` and `resources` capabilities

#### Scenario: Stdio transport available for development

- **WHEN** the server is started in stdio mode
- **THEN** an MCP client connected over stdio can complete `initialize` and list tools

### Requirement: Bearer authentication and credential forwarding

The MCP server SHALL authenticate every session against the caller's bearer token using the platform Authenticator before any tool or resource call is served, and SHALL hold no credential of its own. All downstream requests it makes SHALL carry the caller's bearer token and tenant headers, so the router enforces the same tenant isolation and quotas as for any HTTP caller.

#### Scenario: Missing or invalid token is rejected

- **WHEN** a client attempts to initialize a session without a bearer token, or with a token that fails authentication
- **THEN** the server returns an MCP authentication error and establishes no session, exposing no tools

#### Scenario: Downstream calls are made as the caller

- **WHEN** an authenticated session invokes a tool that reads tenant data
- **THEN** the resulting request to the query API carries the caller's bearer token and `X-Tenant-ID`, and the server adds no privilege of its own

#### Scenario: Cross-tenant access is denied

- **WHEN** a session authenticated for tenant A invokes a tool referencing data that belongs to tenant B
- **THEN** no tenant B data is returned, because the forwarded credential scopes the query to tenant A

### Requirement: Query and exploration tools

The MCP server SHALL expose read-only tools that wrap the SignalDB query API: trace search (`search_traces`), single-trace retrieval (`get_trace`), log search (`search_logs`), metric query (`query_metrics`), and attribute discovery (`discover_attributes`). Each tool SHALL accept an optional dataset argument, validated server-side, and SHALL return structured JSON derived from the API response with bounded payload size. In v1 these tools SHALL be visible to every authenticated tenant session without role-based filtering.

#### Scenario: Trace search returns matching traces

- **WHEN** an authenticated session calls `search_traces` with a TraceQL query and time range
- **THEN** the tool returns the matching traces scoped to the caller's tenant as structured JSON

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
- **THEN** the query and exploration tools are present in the returned list

### Requirement: Schema resources

The MCP server SHALL expose the caller's table schemas (traces, logs, metrics column definitions) as MCP resources with stable URIs, readable via `resources/list` and `resources/read`, so agents can ground queries without issuing tool calls. Resources SHALL reflect only the caller's tenant.

#### Scenario: List and read table schemas

- **WHEN** an authenticated session issues `resources/list` and then `resources/read` for a table-schema resource
- **THEN** the server returns the current column definitions for that table in the caller's tenant

#### Scenario: Resources are tenant-scoped

- **WHEN** a session authenticated for tenant A reads schema resources
- **THEN** only tenant A's schemas are returned
