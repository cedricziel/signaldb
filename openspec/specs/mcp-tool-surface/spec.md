# mcp-tool-surface Specification

## Purpose
Defines the tool set the `signaldb-mcp` server exposes to MCP clients and its
obligation to remain feature-equal with the CLI while consuming only the SDK.
## Requirements
### Requirement: MCP tools cover the full client capability set

The MCP server SHALL expose a tool for every SDK-backed SignalDB capability the
CLI exposes — the HTTP query languages (PromQL, LogQL, TraceQL, and Query IR;
SQL is served over Arrow Flight and stays CLI-only), admin (tenant/API-key/
dataset management and custom schema-registry management), operational
compaction control, and schema-registry lookup (registry list, attribute /
entity / metric resolution, prefix search). Local-only utilities (`tui`,
`completions`, user bootstrap) are out of scope.

#### Scenario: Query is available as a tool

- **WHEN** an MCP client lists available tools
- **THEN** the list includes query tools for each HTTP query language (PromQL,
  LogQL, TraceQL, and Query IR); SQL stays CLI-only (Flight transport)

#### Scenario: Operational control is available as a tool

- **WHEN** an MCP client lists available tools
- **THEN** the list includes tools for operational compaction control (run,
  status, and dry-run)

#### Scenario: Schema lookup is available as a tool

- **WHEN** an MCP client lists available tools
- **THEN** the list includes tools to list registries and to resolve an
  attribute key, an entity type, or a metric name (including prefix search),
  returning namespace-tagged, precedence-ordered definitions with briefs so an
  LLM can learn what a key means before building a query

### Requirement: MCP query results match the SDK's native shape

A query invoked through an MCP tool SHALL return the same result the SDK
produces for that language — tabular rows for SQL, native Tempo/Loki/Prometheus
JSON for TraceQL/LogQL/PromQL — so that a query issued via MCP and the
equivalent query issued via the CLI yield equivalent data.

#### Scenario: Equivalent results across MCP and CLI

- **WHEN** the same query in the same language is issued once through an MCP tool
  and once through the CLI
- **THEN** both return equivalent data in that language's native shape

### Requirement: MCP server propagates caller identity through the SDK

The MCP server SHALL carry the caller's authentication and tenant context into
the SDK call for each tool invocation, and SHALL surface SDK errors to the MCP
client as tool errors rather than crashing or leaking internal transport
details.

#### Scenario: Unauthorized tool call

- **WHEN** an MCP tool is invoked without valid credentials for the target
  operation
- **THEN** the tool returns an error result derived from the SDK error
- **AND** the server continues serving subsequent requests
