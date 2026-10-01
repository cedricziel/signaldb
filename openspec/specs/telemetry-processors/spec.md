# telemetry-processors Specification

## Purpose
Lets a tenant admin declare ordered OTTL statements per tenant, dataset, and
signal that the acceptor applies to incoming telemetry before anything is made
durable, so data such as PII or secrets can be redacted or reshaped at ingest.

## Requirements

### Requirement: Processors are tenant-scoped, named, and bound to one signal

A processor SHALL belong to exactly one tenant and SHALL be identified within
that tenant by a `name` slug (`[a-z0-9][a-z0-9-]{0,62}`). It SHALL declare a
`signal` of `traces`, `logs`, or `metrics`, an optional `dataset` — the dataset
*name*, the same value carried in `X-Dataset-ID` (unset means every dataset of
the tenant), `enabled` (default true), an integer
`priority` (default 100), an `error_mode` of `ignore` (default), `silent`, or
`propagate`, an optional `description`, and an ordered list of OTTL
`statements`. A `dataset` SHALL reference an existing dataset of the same
tenant. Names are unique per tenant; a tenant SHALL never see or affect another
tenant's processors.

#### Scenario: Create a dataset-scoped processor

- **WHEN** a tenant admin creates processor `redact-emails` with `signal:
  logs`, `dataset: prod`, and one statement
- **THEN** the processor is stored, returned with `created_at`/`updated_at`,
  and listed only for that tenant

#### Scenario: Unknown dataset is rejected

- **WHEN** a processor is created with a `dataset` that does not belong to
  the tenant
- **THEN** the request is rejected with a validation error naming the dataset

#### Scenario: Duplicate name is rejected

- **WHEN** a processor is created with a name already used by the tenant
- **THEN** the request fails with a conflict error and the existing processor
  is unchanged

### Requirement: The OTTL subset is validated at write time

Every statement SHALL be parsed and compiled when a processor is created,
replaced, or validated. The accepted grammar is `editor(args) [where
condition]` with: context-qualified keyed paths (`resource.attributes["k"]`,
`instrumentation_scope.name|version|attributes["k"]`,
`span.name|kind|status.code|status.message|attributes["k"]`,
`log.body|severity_text|severity_number|attributes["k"]`,
`metric.name|description|unit`, `datapoint.attributes["k"]`), bare map paths
(`attributes`, `resource.attributes`, `instrumentation_scope.attributes`,
`datapoint.attributes`) as the target of the map editors, unqualified
`attributes[...]` / `name` / `body` resolving to the signal's leaf item (for
metrics: `attributes` is the data point's attributes and `name` is
`metric.name`); replacement strings in `regex`-crate syntax with `$$`
normalised to `$`; string escapes `\\`, `\"`, `\n`, `\t`;
string, integer, float, boolean literals and `nil`; comparison operators
(`==`, `!=`, `<`, `<=`, `>`, `>=`), `and`, `or`, `not`, parentheses; editors
`set`, `delete_key`, `delete_matching_keys`, `keep_keys`, `truncate_all`,
`limit`, `replace_pattern`, `replace_all_patterns`, `replace_match`,
`replace_all_matches`; converters `IsMatch`, `IsString`, `Concat`, `String`,
`Int`, `Double`, `Len`, `SHA256`, `Substring`, `ToLowerCase`, `ToUpperCase`,
`Truncate`. A path that is not defined for the processor's signal, an unknown
editor or converter, an argument-count mismatch, an invalid regex, or a regex
exceeding the configured size limit SHALL be a validation error that reports
the statement index, the column, and a message naming the offending token.
Programs exceeding the statement limit SHALL be rejected.

#### Scenario: Collector-style redaction statements compile

- **WHEN** a traces processor is validated with
  `replace_pattern(attributes["url.full"], "\\?.*$", "")` and
  `set(attributes["user.email"], SHA256(attributes["user.email"])) where
  attributes["user.email"] != nil`
- **THEN** validation succeeds with zero errors

#### Scenario: Unsupported editor is a positional error

- **WHEN** a processor is validated with `merge_maps(attributes,
  resource.attributes, "upsert")`
- **THEN** validation fails with an error at statement 0 naming `merge_maps`
  as unsupported

#### Scenario: Path from another signal is rejected

- **WHEN** a logs processor contains `set(span.name, "x")`
- **THEN** validation fails naming `span.name` as not available for `logs`

### Requirement: Processors are applied at ingest before durability

For every OTLP traces, logs, or metrics export the acceptor SHALL select the
tenant's enabled processors whose `signal` matches and whose `dataset` is
unset or equals the request's dataset, order them tenant-wide first, then
dataset-scoped, each group by ascending `priority` then `name`, and apply
their statements in order to every leaf item (span, log record, or metric data
point of every point type, including summary and exponential histogram) of the
decoded request — with `resource`, `instrumentation_scope`, and for metrics
the enclosing `metric` reachable from each item, so `resource.*` and `metric.*`
edits run once per leaf — before the request is converted to Arrow, before
metric-type partitioning, and before any WAL append or forward. The persisted,
forwarded, and retried data SHALL be the transformed data only. A tenant with
no matching processors SHALL observe unchanged ingest behaviour and output.
Processors SHALL NOT apply to Prometheus remote-write or to batches written
directly to the writer's Flight endpoint; the documentation SHALL say so.

#### Scenario: Redacted attribute never reaches storage

- **WHEN** a tenant has an enabled logs processor `set(attributes["user.email"],
  "[redacted]")` and exports a log record with `user.email = "a@b.c"`
- **THEN** the WAL entry, the writer, and any query result contain
  `user.email = "[redacted]"` and the original value appears nowhere

#### Scenario: Ordering across scopes

- **WHEN** a tenant-wide traces processor with priority 100 sets
  `attributes["env"]` to `"tenant"` and a dataset-scoped processor for the
  request's dataset with priority 10 sets it to `"dataset"`
- **THEN** the stored span carries `env = "dataset"`

#### Scenario: Disabled or other-dataset processors are skipped

- **WHEN** a processor is disabled, or bound to a dataset other than the
  request's
- **THEN** none of its statements run for that request

#### Scenario: Metric rename partitions by the new name

- **WHEN** a metrics processor sets `metric.name` to a new value
- **THEN** metric-type partitioning and storage use the new name

### Requirement: Runtime errors follow the processor's error mode

A statement that fails at runtime (type conversion failure, editor on a
missing required path, replacement template error) SHALL, under `ignore`, be
logged at most once per minute per tenant and processor and skipped for that
item; under `silent`, be skipped without logging; under `propagate`, abort the
export with an invalid-argument error (HTTP 400 / gRPC `InvalidArgument`)
before anything is written. A `where` condition referencing an absent key
SHALL evaluate to false and is never an error.

#### Scenario: Propagate rejects the export

- **WHEN** a processor with `error_mode: propagate` contains
  `set(attributes["n"], Int(attributes["s"]))` and a span has
  `s = "not-a-number"`
- **THEN** the export is rejected with an invalid-argument error and no WAL
  entry is created

#### Scenario: Ignore continues

- **WHEN** the same processor has `error_mode: ignore`
- **THEN** the span is ingested with `n` unchanged and the failure is counted

### Requirement: Processors are persisted in the catalog and refreshed without restart

Processors SHALL be stored in the catalog on both SQLite and PostgreSQL. Each
service that applies processors SHALL cache the compiled program per tenant,
refresh it after `[processors].reload_interval` (default 30s) elapses, and
invalidate it immediately on writes made through the same process. Deleting a
dataset SHALL delete the processors bound to it. A stored processor that no
longer compiles SHALL be skipped at ingest, logged, and reported with
`status: invalid` on list/get; it SHALL never block ingest.

#### Scenario: Change propagates to a separate acceptor

- **WHEN** a processor is created through the router while a separate acceptor
  process is running
- **THEN** exports arriving after `reload_interval` are transformed by it

#### Scenario: Empty tenant is cheap

- **WHEN** a tenant has no processors
- **THEN** ingest performs no catalog query per request after the first cache
  fill

### Requirement: Processors can be validated and dry-run without side effects

`POST /api/v1/processors:validate` SHALL compile a processor specification and
return the list of positional errors (empty on success) without storing it.
`POST /api/v1/processors:test` SHALL accept a `signal`, an optional
`dataset`, an optional inline list of processor specifications (when
omitted, the tenant's stored processors for that signal and dataset are used),
and an OTLP JSON export payload up to `[processors].test_payload_max_bytes`
(default 1 MiB), and SHALL return the transformed payload and, per statement,
the number of items where it ran and the number of errors. The dry-run SHALL
never write to the WAL, forward data, or count against ingest quotas.

#### Scenario: Dry-run shows the redaction

- **WHEN** a client posts a logs payload containing `user.email = "a@b.c"` with
  an inline processor `set(attributes["user.email"], "[redacted]")`
- **THEN** the response payload carries `[redacted]`, statement 0 reports one
  match, and no data is ingested

### Requirement: HTTP API and access control

The router SHALL expose `GET /api/v1/processors`, `POST /api/v1/processors`,
`GET|PUT|DELETE /api/v1/processors/{name}`, `POST /api/v1/processors:validate`,
and `POST /api/v1/processors:test` under the tenant credential auth layer.
List, get, validate, and test SHALL require `processors:read` (or a tenant
session); create, replace, and delete SHALL require tenant-admin rights and
`processors:write`. Write responses SHALL include `applies_within_seconds`
equal to the configured reload interval. The endpoints SHALL be described in
the OpenAPI document and available in the generated Rust SDK and TypeScript
client.

#### Scenario: Tenant session can read

- **WHEN** a logged-in tenant member session with no explicit scopes calls
  `GET /api/v1/processors`
- **THEN** the tenant's processors are returned

#### Scenario: Ingest-only key cannot read processors

- **WHEN** a key holding only `traces:write` calls `GET /api/v1/processors`
- **THEN** the response is 403

#### Scenario: Member session cannot write

- **WHEN** a non-admin tenant member session calls `DELETE
  /api/v1/processors/{name}`
- **THEN** the response is 403 and the processor remains

### Requirement: Processor activity is observable

The acceptor SHALL emit counters for statements applied, statements errored,
and exports rejected by `propagate`, labelled by tenant and processor name, and
SHALL wrap each application in a span carrying tenant, dataset, signal, and
processor count.

#### Scenario: Counters increment

- **WHEN** an export runs one processor whose two statements each apply to
  three spans
- **THEN** the applied counter for that tenant and processor increases by six
