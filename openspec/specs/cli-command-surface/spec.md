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
management, and `ops <verb>` for operational control — plus the existing `tui`,
`completions`, and user-bootstrap utilities. Tenant, API-key, and dataset
management SHALL live under `admin`. Exactly one language flag SHALL be required
on `query`.

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

