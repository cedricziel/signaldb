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

### Requirement: The profile compat surface lives under profiles

The CLI SHALL expose the Pyroscope-compatible profile surface as a
`profiles <verb>` group — `types`, `labels`, `label-values <label>`,
`render <selector> --from --until`, `diff <selector> --left-from --left-until
--right-from --right-until`, and `by-trace <trace_id>` — dispatched through the
SDK and printing the native Pyroscope JSON responses unchanged, consistent with
how the other compat surfaces are surfaced. It lives outside `query` because
Pyroscope has no single query-language flag; the selector and ranges are
per-verb parameters.

#### Scenario: Profile types are listed

- **WHEN** a user runs `signaldb profiles types`
- **THEN** the CLI prints the tenant's profile types with data as the native
  JSON response

#### Scenario: A flame graph is rendered

- **WHEN** a user runs `signaldb profiles render
'process_cpu:cpu:nanoseconds{service_name="checkout"}' --from now-1h`
- **THEN** the CLI prints the native flame-graph JSON returned by the render
  endpoint through the SDK

#### Scenario: Profiles for a trace

- **WHEN** a user runs `signaldb profiles by-trace <trace_id>`
- **THEN** the CLI prints the correlated profiles for that trace
