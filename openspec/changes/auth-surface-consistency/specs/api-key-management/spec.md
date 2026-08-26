## MODIFIED Requirements

### Requirement: Scopes are selectable on every key-management surface

Every surface that creates an API key — the management UI (both the tenant
management panel and the API-keys page), the admin/management HTTP API, the
SDK, the CLI (`signaldb admin api-key create` and
`signaldb tenant api-key create`), and the MCP admin and tenant toolsets —
SHALL let the caller choose the key's scopes from the same vocabulary
(`metrics:write`, `logs:write`, `traces:write`, `profiles:write`,
`traces:read`, `logs:read`, `metrics:read`, `profiles:read`, `schema:read`,
`schema:write`, `tenant:manage`) and an optional dataset restriction. That
vocabulary SHALL be published once, as an enumeration in the API contract, and
every first-party surface — including the OAuth consent view's list of
grantable read scopes — SHALL derive its scope list from the generated client
rather than a hand-maintained copy, so that no surface offers a subset of the
vocabulary and adding a scope to the server is sufficient to surface it
everywhere. A creation request naming an unknown scope SHALL be rejected.
Listing keys on any surface SHALL show each key's scopes. A creation request
without scopes SHALL be rejected on every surface — a key's permissions are
always explicit; the unrestricted legacy behaviour applies only to keys that
predate scopes.

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

#### Scenario: Every UI key-creation surface offers the full vocabulary

- **WHEN** a tenant admin opens key creation on the management panel and on
  the API-keys page
- **THEN** both offer all eleven scopes, including `schema:read`,
  `schema:write`, and `tenant:manage`, grouped identically

#### Scenario: Consent view shows what the server will grant

- **WHEN** an OAuth client requests authorization without naming scopes
- **THEN** the consent view lists exactly the read scopes the server grants by
  default, sourced from the contract, with no scope omitted

#### Scenario: Scope enum is the single source

- **WHEN** a scope is added to the server's vocabulary and the clients are
  regenerated
- **THEN** the CLI's `--scope` validation, the MCP tool schemas, and every UI
  scope picker accept and display it without a hand edit
