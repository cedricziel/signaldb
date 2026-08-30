## ADDED Requirements

### Requirement: Dataset restriction is a set, not a single dataset

A database-backed API key MAY be restricted to a named set of datasets
within its tenant (`dataset_ids`), replacing the single optional dataset
restriction. Omitting `dataset_ids` on creation SHALL leave the key
unrestricted — reachable against every dataset in its tenant, exactly as an
unrestricted key behaves today; an explicit empty set on creation SHALL be
rejected as invalid input (there is no prior state for it to mean "leave
unchanged", so it has no legitimate meaning there — see the dataset-update
requirement for its distinct, well-defined meaning on update). A non-empty
set SHALL restrict the key to exactly the named datasets: a request naming a
dataset outside the set SHALL be refused, and a request naming one inside it
SHALL succeed. Config-based (TOML) tenant API keys have no dataset
restriction concept and are unaffected by this requirement.

#### Scenario: A key restricted to two datasets

- **WHEN** an API key is created with `dataset_ids: ["production",
  "staging"]`
- **THEN** requests authenticated with that key against `production` or
  `staging` succeed, and a request against `archive` is refused

#### Scenario: Omitting dataset_ids leaves the key unrestricted

- **WHEN** an API key is created without `dataset_ids`
- **THEN** the key reaches every dataset in its tenant, exactly as before
  this change

#### Scenario: An empty explicit set is rejected on creation

- **WHEN** an API key is created with `dataset_ids: []`
- **THEN** the request is rejected with a validation error and no key is
  created

#### Scenario: A legacy single-dataset key keeps its restriction

- **WHEN** a key created before this change with a single bound dataset
  authenticates a request
- **THEN** it behaves exactly as a key created after this change with
  `dataset_ids` containing that one dataset

## MODIFIED Requirements

### Requirement: Scopes are selectable on every key-management surface

Every surface that creates an API key — the management UI, the admin/management
HTTP API, the SDK, the CLI (`signaldb admin api-key create` and
`signaldb tenant api-key create`), and the MCP admin and tenant toolsets —
SHALL let the caller choose the key's scopes from the same vocabulary
(`metrics:write`, `logs:write`, `traces:write`, `profiles:write`,
`traces:read`, `logs:read`, `metrics:read`, `profiles:read`, `schema:read`,
`schema:write`, `tenant:manage`) and an optional set of dataset restrictions
(`dataset_ids`). A creation request naming an unknown scope SHALL be
rejected. Listing keys on any surface SHALL show each key's scopes and its
dataset set (or that it is unrestricted). A creation request without scopes
SHALL be rejected on every surface — a key's permissions are always
explicit; the unrestricted legacy behaviour applies only to keys that
predate scopes.

#### Scenario: CLI creates a key with scopes

- **WHEN** a user runs `signaldb admin api-key create acme --name ci
--scope traces:write --scope schema:read`
- **THEN** the key is created carrying exactly those two scopes and the CLI
  prints them with the secret

#### Scenario: CLI creates a key restricted to two datasets

- **WHEN** a user runs `signaldb admin api-key create acme --name ci
--scope traces:write --dataset production --dataset staging`
- **THEN** the key is created restricted to exactly `production` and
  `staging`, and the CLI prints the set with the secret

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

### Requirement: An existing key's scopes can be updated

Tenant admins SHALL be able to change the scopes and the dataset set of an
existing, non-revoked API key on every key-management surface without
rotating the secret. The change SHALL take effect for subsequent requests
made with that key. On update, `dataset_ids` has three distinct, explicit
meanings: omitted leaves the key's current restriction unchanged; a
non-empty set replaces the restriction entirely (not merged with the
existing one); an explicit empty set (`[]`) clears the restriction back to
unrestricted. Revoked keys SHALL NOT be updatable.

#### Scenario: Scope added to a live key

- **WHEN** an admin adds `schema:write` to a key that previously carried only
  `schema:read`
- **THEN** the next request with that key may create a custom registry, and the
  key list shows both scopes

#### Scenario: Scope removed from a live key

- **WHEN** an admin removes `traces:write` from a key
- **THEN** the next trace ingest with that key is rejected with an authorization
  error

#### Scenario: A key's dataset set is narrowed

- **WHEN** an admin updates a key that was unrestricted to
  `dataset_ids: ["production"]`
- **THEN** subsequent requests with that key against any dataset other than
  `production` are refused

#### Scenario: A key's dataset set is cleared back to unrestricted

- **WHEN** an admin updates a key that was restricted to `["production"]`
  with an explicit empty `dataset_ids: []`
- **THEN** the key's restriction is cleared and it reaches every dataset in
  its tenant afterward

#### Scenario: Omitting dataset_ids on update leaves the restriction alone

- **WHEN** an admin updates only a key's scopes, without sending
  `dataset_ids` at all
- **THEN** the key's existing dataset restriction (or lack of one) is
  unchanged

#### Scenario: Revoked key cannot be updated

- **WHEN** an admin attempts to change the scopes of a revoked key
- **THEN** the request is rejected
