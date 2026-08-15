## MODIFIED Requirements

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
