# mcp-tool-surface Specification

## Purpose
Defines the tool set the `signaldb-mcp` server exposes to MCP clients and its
obligation to remain feature-equal with the CLI while consuming only the SDK.
## Requirements
### Requirement: MCP tools cover the full client capability set

The MCP server SHALL expose a tool for every SDK-backed SignalDB capability the
CLI exposes — the HTTP query languages (PromQL, LogQL, TraceQL, and Query IR;
SQL is served over Arrow Flight and stays CLI-only), platform administration
(tenant, user, API-key, and dataset management through the admin API), tenant
self-management (the caller's own datasets, API keys, memberships, schema, and
signal tables through the management API), operational compaction control, and
schema-registry lookup and custom-registry management. Local-only utilities
(`tui`, `completions`, user bootstrap) are out of scope.

Management tools come in two families that differ only in which credential the
router expects: **platform-admin tools** wrap the admin API and succeed when the
session's forwarded credential is the administrative key; **tenant tools**
(prefixed `tenant_`) wrap the management API and act as the caller's own
identity within its tenant, for a human-authenticated (OAuth) session or an
API-key session whose key carries `tenant:manage` (the table/schema-listing
tenant tools need only a valid key of that tenant). Neither
family is hidden from `tools/list`; a call the router does not authorize
returns a clean access-denied error. Tools that
delete or revoke SHALL carry the MCP destructive annotation and SHALL require a
`confirm` argument equal to the identifier being destroyed; read-only tools
SHALL carry the read-only annotation. Tools that create an API key SHALL return
the key material exactly once in that response; tools that list keys SHALL never
return key material.

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

#### Scenario: Platform administration is available as tools

- **WHEN** an MCP client lists available tools
- **THEN** the list includes tenant list/get/create/update/delete, user create,
  API-key create/list/update/revoke, and dataset list/create/delete tools that
  wrap the admin API

#### Scenario: Tenant self-management is available as tools

- **WHEN** an MCP client lists available tools
- **THEN** the list includes `tenant_`-prefixed tools for the caller's datasets
  (list/create/delete), API keys (list/create/update/revoke), memberships
  (list/upsert/remove), schema, signal tables (list/provision/schemas), and the
  tenant self view (`tenant_info`)

#### Scenario: A tenant:manage key drives the tenant tools

- **WHEN** an MCP session authenticated with an API key carrying
  `tenant:manage` calls `tenant_create_dataset`
- **THEN** the dataset is created in the key's tenant and the tool returns the
  SDK-shaped result

#### Scenario: Destructive tool requires confirmation

- **WHEN** a session calls `delete_dataset` (or `tenant_delete_dataset`,
  `delete_tenant`, `revoke_api_key`, `tenant_revoke_api_key`) without `confirm`
  equal to the target identifier
- **THEN** the tool refuses with an error naming the required confirmation and
  performs no change

#### Scenario: Unauthorized management call is denied cleanly

- **WHEN** a session whose credential the router does not authorize for a
  management operation calls that tool
- **THEN** the tool returns an access-denied error and the router performed no
  change

#### Scenario: Key material appears once

- **WHEN** a session creates an API key through a tool and later lists keys
- **THEN** the create response contains the key exactly once and the list
  response contains no key material

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

### Requirement: Profile discovery and query are available as tools

The MCP server SHALL expose the Pyroscope-compatible profile surface as tools,
tenant-scoped like every other tool: `discover_profile_types` (the profile
types with data), `discover_attributes` with `signal: "profiles"` (label names
and, with `tag`, label values), `search_profiles` (a Pyroscope selector plus a
time range → the aggregated flame graph, subject to the same payload cap and
truncation flag as other query tools), `compare_profiles` (two ranges → the
diff flame graph), and `profiles_for_trace` (the profiles correlated with a
trace id). The existing `get_profile` (single profile by id) is unchanged.
Results SHALL be the SDK's native shapes.

#### Scenario: Profile types are discoverable

- **WHEN** a tenant has ingested CPU profiles and a session calls
  `discover_profile_types`
- **THEN** the tool returns the CPU profile type among the types with data,
  scoped to the caller's tenant

#### Scenario: Profile labels through discover_attributes

- **WHEN** a session calls `discover_attributes` with `signal: "profiles"` and
  no `tag`
- **THEN** the tool returns the profile label names for the caller's tenant;
  with a `tag` it returns that label's values

#### Scenario: A selector renders a flame graph

- **WHEN** a session calls `search_profiles` with a selector such as
  `process_cpu:cpu:nanoseconds{service_name="checkout"}` and a range
- **THEN** the tool returns the aggregated flame graph as structured JSON,
  truncated with `truncated: true` if it exceeds the payload cap

#### Scenario: Profiles for a trace

- **WHEN** a session calls `profiles_for_trace` with a trace id that has
  correlated profiles
- **THEN** the tool returns those profiles' identities scoped to the caller's
  tenant
