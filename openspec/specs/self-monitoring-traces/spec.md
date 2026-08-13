# self-monitoring-traces Specification

## Purpose

Defines the contract for the trace telemetry SignalDB emits about its own
operation when self-monitoring is enabled: span names, kinds, attributes,
status mapping, resource identity, and cross-service trace continuity, all
conformant with a pinned OpenTelemetry semantic-conventions version so that
SignalDB's own traces are a reference example of the conventions it ingests.

## Requirements

### Requirement: Pinned semantic-conventions version

SignalDB's self-emitted telemetry SHALL target a single pinned OpenTelemetry
semantic-conventions version (initially v1.43.0), recorded in one place in
the repository, and SHALL emit the corresponding `schema_url` on both the
resource and the instrumentation scope of exported spans.

#### Scenario: Schema URL on exported spans

- **WHEN** self-monitoring is enabled and any service exports a span
- **THEN** the span's resource and instrumentation scope carry
  `https://opentelemetry.io/schemas/<pinned-version>` as their schema URL

### Requirement: Resource identity

Every service's telemetry resource SHALL carry `service.name` (per-service,
e.g. `signaldb-querier`), `service.namespace` = `signaldb`,
`service.version`, a per-process `service.instance.id` (UUID), and
`deployment.environment.name` sourced from configuration. The deprecated
`deployment.environment` attribute SHALL NOT be emitted.

#### Scenario: Distinct instances are distinguishable

- **WHEN** two instances of the same service export telemetry
- **THEN** their resources share `service.namespace` and `service.name` but
  differ in `service.instance.id`

#### Scenario: Environment reflects configuration

- **WHEN** an operator configures a deployment environment name
- **THEN** exported resources carry it as `deployment.environment.name`,
  and no resource attribute named `deployment.environment` is present

### Requirement: HTTP server spans on every HTTP surface

Every HTTP surface SignalDB serves (router query/API, acceptor OTLP/HTTP
and remote-write, compactor ops API, MCP server HTTP) SHALL produce one
SERVER span per request following the stable HTTP semantic conventions:
name `{method} {http.route}` (falling back to `{method}` when no route
template exists, never the raw path), the required `http.request.method`,
`url.path`, `url.scheme` attributes, `http.route` and
`http.response.status_code` when available, and W3C trace-context parent
adoption from inbound headers. Server spans SHALL set status Error only for
5xx or transport failure — never for 4xx alone — and SHALL then carry
`error.type`.

#### Scenario: Ingest request over OTLP/HTTP joins the caller's trace

- **WHEN** a client sends `POST /v1/traces` to the acceptor with a
  `traceparent` header
- **THEN** the acceptor's exported SERVER span is a child of the caller's
  span and is named `POST /v1/traces`

#### Scenario: Client error does not mark the server span failed

- **WHEN** a request is rejected with a 4xx status
- **THEN** the SERVER span records `http.response.status_code` but its
  status is not Error

### Requirement: RPC spans on gRPC and Flight boundaries

Every gRPC service SignalDB exposes (acceptor OTLP gRPC, Flight services in
querier/writer/compactor/router) SHALL produce a SERVER span per call, and
every internal Flight/gRPC call SHALL be wrapped in a CLIENT span on the
caller, following the RPC semantic conventions with the post-1.39 attribute
names: `rpc.system.name` = `grpc`, `rpc.method` as the fully-qualified
logical method (e.g. `arrow.flight.protocol.FlightService/DoGet`), and
`rpc.response.status_code` as the string gRPC status code. Trace context
SHALL propagate caller→callee so the CLIENT span is the parent of the
SERVER span. Server spans SHALL map only server-fault gRPC codes
(`UNKNOWN`, `DEADLINE_EXCEEDED`, `UNIMPLEMENTED`, `INTERNAL`,
`UNAVAILABLE`, `DATA_LOSS`) to status Error; client spans SHALL treat any
non-`OK` code as Error. Flight-specific detail (ticket verb, batch and row
counts) SHALL be carried in `signaldb.*` attributes, and the ticket verb
MAY be appended to the span name for disambiguation.

#### Scenario: Router-to-querier hop has both span kinds

- **WHEN** the router executes a query by calling the querier over Flight
- **THEN** the exported trace contains a CLIENT span on the router and a
  child SERVER span on the querier, both carrying `rpc.system.name` and
  `rpc.method`, with no kind-less span standing in for either side

#### Scenario: Not-found is not a querier failure

- **WHEN** a Flight `do_get` completes with gRPC status `NOT_FOUND`
- **THEN** the querier's SERVER span records
  `rpc.response.status_code = "NOT_FOUND"` without status Error, while the
  router's CLIENT span for the same call is marked Error

### Requirement: Ingest-to-persistence trace continuity across the WAL

The ingest path SHALL remain traceable across the asynchronous WAL
boundary: the acceptor's ingest trace context SHALL be persisted with each
WAL entry, and the writer's batch-processing span SHALL reference each
distinct source ingest trace via span links (one link per source trace,
deduplicated) — never by electing a single parent. Writer processing spans
SHALL use `signaldb.wal.*` attributes rather than the unstable messaging
conventions.

#### Scenario: Batch flush links to every contributing ingest trace

- **WHEN** the writer flushes a batch containing WAL entries from three
  distinct ingest requests
- **THEN** the writer's processing span carries exactly three span links,
  one to each ingest trace, and has no parent in any of them

### Requirement: Database client spans for catalog access

Operations against the SQL catalog (SQLite or PostgreSQL) SHALL produce
CLIENT spans following the stable database semantic conventions:
`db.system.name` (`sqlite` or `postgresql`), `db.operation.name`,
`db.namespace`, and sanitized `db.query.text` (literals replaced with
placeholders before recording; bound/parameterized values never inlined),
named per the DB span-naming precedence (never raw SQL as the span name).

#### Scenario: Catalog query is visible as a DB client span

- **WHEN** a service performs a catalog read while serving a traced request
- **THEN** the trace contains a CLIENT span with `db.system.name` and
  `db.operation.name` beneath the serving span

#### Scenario: Catalog statement text is captured and sanitized

- **WHEN** a service issues a catalog register, heartbeat, list, or
  deregister ingester operation
- **THEN** the resulting CLIENT span carries `db.query.text` with any
  literal values replaced by placeholders

### Requirement: Query execution stage spans

Query execution in the querier SHALL emit INTERNAL child spans for its
major stages (planning, table/Iceberg scan, execution, result encoding)
under the Flight SERVER span, carrying result-size attributes (row/byte
counts) in the `signaldb.*` namespace. Any recorded query text SHALL follow
the DB-semconv sanitization rules: literals replaced with placeholders
before recording, and parameterized values never inlined.

#### Scenario: Slow query is attributable to a stage

- **WHEN** a query executes with self-monitoring enabled
- **THEN** its trace decomposes the Flight SERVER span into per-stage child
  spans such that the dominant time contributor is identifiable

#### Scenario: Query literals do not leak into telemetry

- **WHEN** a query containing string or numeric literals is recorded on a
  span
- **THEN** the recorded query text carries placeholders in place of the
  literal values

### Requirement: Compactor lifecycle job spans

Each compactor background job execution — compaction, retention
enforcement, snapshot expiration, orphan-file cleanup — SHALL produce a
root INTERNAL span per job run carrying tenant, dataset, and table
identifiers in `signaldb.*` attributes, and recording counts of affected
objects (files deleted, partitions dropped, snapshots expired), so that
data-lifecycle actions are reconstructable after the fact.

#### Scenario: Retention deletion is traceable

- **WHEN** retention enforcement drops a partition
- **THEN** a span exists for that enforcement run identifying the tenant,
  dataset, table, and the number of partitions dropped

### Requirement: Error recording

Failed operations SHALL set span status Error with a low-cardinality
`error.type` attribute (well-known value where one exists, status code as
string for HTTP/gRPC failures), and status SHALL be left unset on success.
Errors that are retried and ultimately succeed SHALL NOT mark the
encompassing span as failed. Failure detail beyond `error.type` goes in the
span status description, not in unbounded attributes.

#### Scenario: Retried transient failure ends successful

- **WHEN** an internal operation fails transiently, is retried, and
  succeeds
- **THEN** the operation's span status is not Error

### Requirement: Attribute namespace and cardinality discipline

SignalDB-specific span attributes SHALL live under the `signaldb.`
namespace (e.g. `signaldb.tenant.id`, `signaldb.dataset.id`,
`signaldb.wal.*`) and SHALL be defined in a machine-readable convention
registry versioned in the repository alongside the pinned upstream
conventions. Spans SHALL NOT record request/handler arguments wholesale:
every recorded field is an explicit, bounded-cardinality choice, and
credentials or API keys SHALL never appear in any span name, attribute, or
status description.

#### Scenario: Registry validates emitted telemetry

- **WHEN** the conformance check runs against telemetry produced by an
  exercised SignalDB deployment
- **THEN** every emitted span attribute is either a pinned-version OTel
  convention attribute or declared in the SignalDB registry, and no
  violation-level finding is reported

#### Scenario: No credential leakage

- **WHEN** a request carrying an `authorization` header or API key is
  processed and traced
- **THEN** no exported span contains the credential in any field
