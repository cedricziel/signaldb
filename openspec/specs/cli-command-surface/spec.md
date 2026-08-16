# cli-command-surface Specification

## Purpose
Defines the observable behavior of the `signaldb` CLI as a scriptable client:
its command taxonomy, how it authenticates and selects endpoints, and how it
renders query results for downstream tooling.
## Requirements
### Requirement: Command taxonomy

The CLI SHALL organize commands into top-level capability groups: a single
`query` command that takes the query language as a mutually-exclusive flag
(`--sql`, `--promql`, `--logql`, `--traceql`), `admin <noun> <verb>` for
platform administration through the admin API, `tenant <noun> <verb>` for the
caller's own tenant with its API key (today: signal tables and table schemas —
the management API's dataset/API-key/membership operations require a human
session, which the CLI does not hold, so they are MCP/UI-only), `ops <verb>`
for operational control,
and `schema <noun> <verb>` for schema-registry lookup — plus the existing
`tui`, `completions`, and user-bootstrap utilities. Tenant, API-key, dataset,
user, and custom schema-registry administration SHALL live under `admin`;
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
create --name ci`, `signaldb tenant membership list`, or `signaldb tenant
table provision --dataset production`
- **THEN** the CLI performs the corresponding management-API operation through
  the SDK as the caller's own identity

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

### Requirement: Native per-language query output

The CLI SHALL emit each query language's native result shape: `--sql` returns
tabular rows selectable as `table`, `csv`, or `ndjson`; `--promql`, `--logql`,
and `--traceql` return their native Prometheus/Loki/Tempo JSON responses
unchanged. The CLI SHALL NOT normalize the native-language responses into a
common row model.

#### Scenario: SQL returns selectable tabular rows

- **WHEN** a user runs `signaldb query --sql 'SELECT ...' --format ndjson`
- **THEN** the CLI emits newline-delimited JSON rows

#### Scenario: PromQL returns the native Prometheus shape

- **WHEN** a user runs `signaldb query --promql '<expr>'`
- **THEN** the CLI emits the Prometheus HTTP API JSON response shape as returned
  by the server

### Requirement: Deterministic exit codes for scripting

The CLI SHALL exit `0` only when the requested operation succeeds. It SHALL exit
non-zero on authentication failure, invalid query, unreachable service, or any
server-reported error, and SHALL write human-readable diagnostics to stderr so
that stdout carries only result data.

#### Scenario: Failed query is scriptable

- **WHEN** a query fails because the service is unreachable
- **THEN** the CLI exits non-zero
- **AND** stdout contains no result rows
- **AND** stderr contains the failure reason

#### Scenario: Empty result is a success

- **WHEN** a valid query returns zero rows
- **THEN** the CLI exits `0`

### Requirement: Endpoint and credential resolution

The CLI SHALL resolve its endpoint, tenant/dataset context, and credentials from
explicit flags, environment variables, and an optional configuration file. Each
field resolves independently, in the order: explicit flag > `SIGNALDB_*`
environment variable > configuration-file value > built-in default. The caller
need not know which transport a given capability uses.

#### Scenario: Environment configures a scripted invocation

- **WHEN** endpoint and API key are provided via environment variables and no
  conflicting flags are passed
- **THEN** the CLI uses those values to authenticate and route the request

#### Scenario: Flag overrides environment

- **WHEN** an endpoint is set in the environment and a different endpoint is
  passed as a flag
- **THEN** the CLI uses the flag value

### Requirement: Throttled commands retry, then exit distinctly

The CLI SHALL retry a throttled request through the SDK's shared retry policy before failing. When retries are exhausted it SHALL write a diagnostic to stderr stating the command was rate limited and, when the server stated one, how long it asked to wait, and SHALL exit with a dedicated exit code (`4`) distinct from generic failure so scripts can back off and re-run. `--no-retry` (or `SIGNALDB_NO_RETRY=1`) SHALL disable retry for scripting that prefers fail-fast. When stderr is a terminal the CLI SHALL print one short note per retry so an interactive user knows the command is waiting, not hung.

#### Scenario: Throttled command exits with the throttled code

- **WHEN** a command's request is throttled past the retry budget with `Retry-After: 5` on the last response
- **THEN** stderr reads that the command was rate limited and the server asked to retry in 5 seconds, stdout carries no partial result, and the exit code is `4`

#### Scenario: Fail-fast opt-out

- **WHEN** a command is run with `--no-retry` and its first request is throttled
- **THEN** the CLI exits with code `4` immediately without waiting

#### Scenario: Interactive retry is visible

- **WHEN** an interactive command is retried after throttling
- **THEN** stderr shows one line per retry naming the wait, and stdout is untouched
