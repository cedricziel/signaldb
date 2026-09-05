## MODIFIED Requirements

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
return key material. API-key creation tools (`create_api_key`,
`tenant_create_api_key`) take `dataset_ids: Option<Vec<String>>` — an
optional set of datasets the key is restricted to, in place of a single
optional dataset; omitting it creates an unrestricted key, and an explicit
empty array is rejected. API-key update tools (`update_api_key_scopes`,
`tenant_update_api_key`) additionally take `clear_dataset_restriction:
bool` — omitting `dataset_ids` (with `clear_dataset_restriction` absent or
`false`) leaves the key's restriction unchanged, a non-empty `dataset_ids`
replaces it, and `clear_dataset_restriction: true` (with `dataset_ids`
omitted) clears it back to unrestricted; an explicit empty `dataset_ids`
array, or both `dataset_ids` and `clear_dataset_restriction: true` together,
are rejected before any router request is made — following the same
semantics as the underlying management API.

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

#### Scenario: An API-key tool restricts a key to a dataset set

- **WHEN** a session calls `create_api_key` (or `tenant_create_api_key`) with
  `dataset_ids: ["production", "staging"]`
- **THEN** the created key is restricted to exactly those datasets, and the
  tool result names the set

#### Scenario: Dataset discovery is available as a tool

- **WHEN** a session calls `discover_datasets`
- **THEN** the tool returns a Markdown list nesting the authenticated
  tenant's datasets under it, marking the session's current default dataset
  and each dataset's provisioned signal-table count

#### Scenario: A mismatched tenant confirmation argument is rejected

- **WHEN** a session passes a `tenant` argument to a query, discovery, or
  schema-lookup tool that does not match the tenant this call's own
  credential resolved to
- **THEN** the tool rejects the call with an error naming both tenants,
  before any request reaches the router — `tenant` (and `dataset`, for
  every tool that takes one) are required arguments on every such tool,
  since one MCP session may hold credentials for several tenants and
  datasets across calls and there is no longer an implicit session-wide
  default to omit them in favor of; the argument only confirms the
  caller's assumption against what this specific call authenticated as, it
  never selects which credential/tenant a call authenticates as.
  `discover_datasets` is the one exception to the `dataset`-required rule:
  it takes no `dataset` argument at all, on either side of this change,
  since discovering which datasets exist is how a caller learns what to
  pass as `dataset` to every other tool — requiring it here would be
  circular

#### Scenario: Dataset discovery and table listing respect a restricted credential

- **WHEN** a session authenticated with an API key or OAuth token restricted
  to `dataset_ids: ["production"]` calls `discover_datasets` or
  `tenant_list_tables` for a tenant with datasets `production` and
  `staging`
- **THEN** the result lists only `production`; `staging` does not appear,
  even by name, in either tool's result

#### Scenario: An unrestricted credential sees every dataset, unchanged

- **WHEN** a session authenticated with an unrestricted API key or OAuth
  token calls `discover_datasets` or `tenant_list_tables`
- **THEN** the result lists every dataset in the tenant, exactly as before
  this change
