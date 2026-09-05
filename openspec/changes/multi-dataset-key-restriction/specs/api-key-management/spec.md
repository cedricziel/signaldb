## ADDED Requirements

### Requirement: Dataset restriction is a set, not a single dataset

A database-backed API key MAY be restricted to a named set of datasets
within its tenant (`dataset_ids`), replacing the single optional dataset
restriction. Every named dataset MUST belong to the key's own tenant; a
creation or replacement request naming a dataset outside the tenant SHALL
be rejected in full — no key is created and no existing restriction is
changed. Omitting `dataset_ids` (or sending it as JSON `null`) on creation
SHALL leave the key unrestricted — reachable against every dataset in its
tenant, exactly as an unrestricted key behaves today. An explicit empty
array (`dataset_ids: []`) SHALL be rejected as invalid input on every
surface and in every context — creation, update, and OAuth consent alike —
it never means "unrestricted," "clear the restriction," or "deny every
dataset"; a caller that means "no restriction" omits the field, and a
caller that means "remove an existing restriction" uses the dedicated
clear signal (see the dataset-update requirement). A non-empty set SHALL
restrict the key to exactly the named datasets: a request naming a dataset
outside the set SHALL be refused, and a request naming one inside it SHALL
succeed. Config-based (TOML) tenant API keys have no dataset restriction
concept and are unaffected by this requirement.

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

#### Scenario: A dataset outside the tenant is rejected on creation

- **WHEN** an API key for tenant `acme` is created with `dataset_ids`
  naming a dataset that belongs to a different tenant or does not exist
- **THEN** the request is rejected with a validation error and no key is
  created

#### Scenario: A legacy single-dataset key keeps its restriction

- **WHEN** a key created before this change with a single bound dataset
  authenticates a request
- **THEN** it behaves exactly as a key created after this change with
  `dataset_ids` containing that one dataset

#### Scenario: A multi-dataset-restricted key requires an explicit dataset

- **WHEN** a key restricted to `["production", "staging"]` authenticates a
  request that names no dataset (no `X-Dataset-ID` header, no MCP `dataset`
  argument)
- **THEN** the request is rejected with an error asking the caller to name
  a dataset explicitly, rather than silently resolving to the tenant's
  default dataset

### Requirement: A multi-element restriction is gated behind a rollout-complete flag

Creating or replacing a key's restriction with two or more datasets SHALL
be rejected unless the `[auth].dataset_restriction_rollout_complete`
config key is `true`. This key defaults to `false`, so a fresh deployment
and every existing deployment upgrading into this capability start in the
safe state without operator action. A single-dataset restriction,
clearing a restriction, and an unrestricted key are unaffected by this
flag in either state, since those are the cases a legacy `dataset_id`
column can represent and are therefore safe throughout a mixed-version
rollout (see `design.md` D2's operational constraint). An operator sets
the flag to `true` only after confirming every node that authenticates
API keys is running code that reads `dataset_ids`.

#### Scenario: A multi-element restriction is refused before rollout is confirmed complete

- **WHEN** `[auth].dataset_restriction_rollout_complete` is `false` (the
  default) and a request creates or updates a key with `dataset_ids`
  naming two or more datasets
- **THEN** the request is rejected with a validation error naming the
  config key, and no key is created or changed

#### Scenario: The safe cases are unaffected by the flag

- **WHEN** `[auth].dataset_restriction_rollout_complete` is `false` and a
  request creates or updates a key with a single-element `dataset_ids`,
  clears an existing restriction, or creates an unrestricted key
- **THEN** the request succeeds exactly as it would with the flag `true`

#### Scenario: A multi-element restriction succeeds once rollout is confirmed complete

- **WHEN** an operator sets `[auth].dataset_restriction_rollout_complete`
  to `true` and a request creates or updates a key with `dataset_ids`
  naming two or more datasets
- **THEN** the request succeeds

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
made with that key. On update, the dataset restriction has three distinct,
explicitly signaled operations, none of which is spelled `dataset_ids: []`:
omitting `dataset_ids` (or sending `null`) leaves the key's current
restriction unchanged; a non-empty set replaces the restriction entirely
(not merged with the existing one); and a separate boolean,
`clear_dataset_restriction: true`, clears the restriction back to
unrestricted — sent with `dataset_ids` omitted, never together with a
non-empty set (that combination is a contradictory request and SHALL be
rejected). Revoked keys SHALL NOT be updatable.

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
  with `clear_dataset_restriction: true` and no `dataset_ids`
- **THEN** the key's restriction is cleared and it reaches every dataset in
  its tenant afterward

#### Scenario: Omitting dataset_ids on update leaves the restriction alone

- **WHEN** an admin updates only a key's scopes, without sending
  `dataset_ids` or `clear_dataset_restriction` at all
- **THEN** the key's existing dataset restriction (or lack of one) is
  unchanged

#### Scenario: An empty set is rejected on update, not treated as clear

- **WHEN** an admin updates a key with `dataset_ids: []`
- **THEN** the request is rejected with a validation error naming
  `clear_dataset_restriction` as the way to remove a restriction

#### Scenario: Clearing and setting in the same request is rejected

- **WHEN** an admin updates a key with both `clear_dataset_restriction:
  true` and a non-empty `dataset_ids`
- **THEN** the request is rejected with a validation error and the key's
  restriction is unchanged

#### Scenario: Revoked key cannot be updated

- **WHEN** an admin attempts to change the scopes of a revoked key
- **THEN** the request is rejected

### Requirement: A tenant:manage scope grants tenant self-management to API keys

The API-key scope vocabulary SHALL include `tenant:manage`. A key carrying it
MAY call the tenant management API for the tenant the key belongs to — list,
create, and delete datasets; list, create, update, and revoke API keys; list,
upsert, and remove memberships; and read the tenant schema — exactly as a
tenant administrator's session may, **unless the key also carries a
non-empty `dataset_ids` restriction, in which case it SHALL be refused for
every management-API operation regardless of `tenant:manage`.** A dataset
restriction and full tenant administration are mutually exclusive on the
same key: `tenant:manage` grants the whole tenant, and a `dataset_ids`
restriction is meaningless to reconcile against operations (delete a
dataset, create another key, remove a membership) that are not themselves
scoped to a subset of datasets. The scope SHALL be explicit only: a legacy
key without scopes SHALL NOT gain management, and `tenant:manage` SHALL NOT
be grantable through OAuth consent. A key with `tenant:manage` SHALL still
be bound to its own tenant — it cannot manage another tenant, cannot create
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

#### Scenario: A dataset-restricted tenant:manage key is refused entirely

- **WHEN** a client authenticates with a key carrying both `tenant:manage`
  and `dataset_ids: ["production"]` and calls any management-API endpoint,
  including one that only touches `production`
- **THEN** the request is refused with `403` — the restriction and
  `tenant:manage` do not combine into "manage only my datasets"

#### Scenario: OAuth cannot grant it

- **WHEN** an OAuth consent screen is rendered
- **THEN** `tenant:manage` is not among the grantable scopes
