## MODIFIED Requirements

### Requirement: Command taxonomy

The CLI SHALL organize commands into top-level capability groups: a single
`query` command that takes the query language as a mutually-exclusive flag
(`--sql`, `--promql`, `--logql`, `--traceql`), `admin <noun> <verb>` for
platform administration through the admin API, `tenant <noun> <verb>` for the
caller's own tenant through the management API — datasets, API keys,
memberships, schema, signal tables, and `show` — authenticated by an API key
carrying `tenant:manage` (tables and table schemas need only a valid key of
that tenant), `ops <verb>` for operational control, and `schema <noun> <verb>`
for schema-registry lookup — plus the existing `tui`, `completions`, `whoami`,
and user-bootstrap utilities. Tenant, API-key, dataset, user, and custom
schema-registry administration across tenants SHALL live under `admin`;
self-management of the authenticated tenant SHALL live under `tenant`. Exactly
one language flag SHALL be required on `query`.

#### Scenario: Query language is a flag on one `query` command

- **WHEN** a user runs `signaldb query --promql '<expr>'` (or `--sql`, `--logql`,
  `--traceql`)
- **THEN** the CLI dispatches that query through the SDK using the transport the
  language maps to

#### Scenario: Missing or ambiguous language flag is rejected

- **WHEN** a user runs `signaldb query '<expr>'` with no language flag, or with
  more than one language flag
- **THEN** the CLI rejects the invocation with a usage error and exits non-zero

#### Scenario: Tenant management lives under admin

- **WHEN** a user runs `signaldb admin tenant list`
- **THEN** the CLI performs the tenant-list operation through the SDK

#### Scenario: Self-management lives under tenant

- **WHEN** a user runs `signaldb tenant dataset list`, `signaldb tenant api-key
create --name ci --scope traces:write`, `signaldb tenant membership list`,
  `signaldb tenant schema get`, `signaldb tenant show`, or `signaldb tenant
table provision --dataset production` with an API key carrying
  `tenant:manage`
- **THEN** the CLI performs the corresponding management-API operation through
  the SDK as the caller's own tenant identity

#### Scenario: A key without tenant:manage is refused, not hidden

- **WHEN** a user runs `signaldb tenant dataset create staging` with a key that
  lacks `tenant:manage`
- **THEN** the CLI reports the server's access-denied error naming the required
  scope and exits non-zero

#### Scenario: Schema lookup lives under schema

- **WHEN** a user runs `signaldb schema registry list`,
  `signaldb schema attribute get k8s.pod.uid`, `signaldb schema entity get
k8s.pod`, or `signaldb schema metric get k8s.pod.cpu.time`
- **THEN** the CLI performs the corresponding registry list / resolved lookup
  through the SDK and prints the namespace-tagged, precedence-ordered result

#### Scenario: Custom registry management lives under admin

- **WHEN** a user runs `signaldb admin schema create --file conventions.yaml`
  (or `replace`, `delete`)
- **THEN** the CLI performs the custom-registry mutation through the SDK
