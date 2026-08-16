## ADDED Requirements

### Requirement: A tenant:manage scope grants tenant self-management to API keys

The API-key scope vocabulary SHALL include `tenant:manage`. A key carrying it
MAY call the tenant management API for the tenant the key belongs to — list,
create, and delete datasets; list, create, update, and revoke API keys; list,
upsert, and remove memberships; and read the tenant schema — exactly as a
tenant administrator's session may. The scope SHALL be explicit only: a legacy
key without scopes SHALL NOT gain management, and `tenant:manage` SHALL NOT be
grantable through OAuth consent. A key with `tenant:manage` SHALL still be
bound to its own tenant — it cannot manage another tenant, cannot create
tenants, and cannot mint a key with scopes for a different tenant.

#### Scenario: A tenant:manage key lists and creates datasets

- **WHEN** a client authenticates with an API key carrying `tenant:manage` for
  tenant `acme` and lists then creates a dataset through the management API
- **THEN** both calls succeed and the dataset exists for `acme`

#### Scenario: An ingest-only key is still refused

- **WHEN** a client authenticates with a key carrying only `traces:write` and
  calls a management endpoint
- **THEN** the request is refused with `403`

#### Scenario: A legacy unscoped key is refused

- **WHEN** a client authenticates with a key that predates scopes (no explicit
  scope set) and calls a management endpoint
- **THEN** the request is refused with `403`, because management is opt-in

#### Scenario: The scope does not cross tenants

- **WHEN** a `tenant:manage` key for tenant `acme` targets tenant `globex`
- **THEN** the request is refused with `403`

#### Scenario: OAuth cannot grant it

- **WHEN** an OAuth consent screen is rendered
- **THEN** `tenant:manage` is not among the grantable scopes

## MODIFIED Requirements

### Requirement: Scopes are selectable on every key-management surface

Every surface that creates an API key — the management UI, the admin/management
HTTP API, the SDK, the CLI (`signaldb admin api-key create` and
`signaldb tenant api-key create`), and the MCP admin and tenant toolsets —
SHALL let the caller choose the key's scopes from the same vocabulary
(`metrics:write`, `logs:write`, `traces:write`, `profiles:write`,
`traces:read`, `logs:read`, `metrics:read`, `profiles:read`, `schema:read`,
`schema:write`, `tenant:manage`) and an optional dataset restriction. A
creation request naming an unknown scope SHALL be rejected. Listing keys on any
surface SHALL show each key's scopes. A creation request without scopes SHALL
be rejected on every surface — a key's permissions are always explicit; the
unrestricted legacy behaviour applies only to keys that predate scopes.

#### Scenario: CLI creates a key with scopes

- **WHEN** a user runs `signaldb admin api-key create acme --name ci
--scope traces:write --scope schema:read`
- **THEN** the key is created carrying exactly those two scopes and the CLI
  prints them with the secret

#### Scenario: MCP admin tool creates a key with scopes

- **WHEN** an MCP client invokes the API-key creation tool with
  `scopes: ["schema:read"]`
- **THEN** the key is created with that scope and the tool result lists it

#### Scenario: A management key is created with tenant:manage

- **WHEN** a key is created via the UI, HTTP, CLI, or MCP with scope
  `tenant:manage`
- **THEN** the key is created carrying it and every listing shows it

#### Scenario: Unknown scope is rejected everywhere

- **WHEN** a key is created via HTTP, CLI, or MCP with scope `schema:admin`
- **THEN** the request is rejected with a validation error naming the scope

#### Scenario: Scopes are required

- **WHEN** a key is created via HTTP, CLI, or MCP with no scopes
- **THEN** the request is rejected with a validation error stating that at
  least one scope is required
