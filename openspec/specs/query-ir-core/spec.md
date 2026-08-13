# query-ir-core Specification

## Purpose

Defines SignalDB's native, structured query capability at its foundational
scope: a versioned Query IR with a defined type system, submitted over a native
HTTP surface and executed by lowering to a DataFusion query plan, for
single-signal (logs, traces) queries. It lets the first-party UI and CLI build
queries structurally — without formulating a LogQL/TraceQL string — and
establishes the stable contract that later changes (cross-signal correlation,
structural trace matching, metrics) and future front-ends lower into. The
compatibility dialects are unchanged; this sits alongside them.

## Requirements

### Requirement: Versioned, structured query IR

SignalDB SHALL accept a structured query as a versioned JSON document carrying a
time range, a source selection, a declared result envelope, and an ordered
pipeline of typed stages. The document SHALL carry an explicit IR version, and
the server SHALL accept a bounded range of versions and report the range it
supports. The IR schema SHALL evolve additively — new stages, operators, and
optional fields MAY be added; accepted operators SHALL be deprecated before any
removal — so that a previously valid stored query remains valid. A stage or
envelope introduced in a later IR version SHALL be rejected when submitted under
an earlier version.

#### Scenario: A versioned query executes

- **WHEN** a client submits an IR document with a supported version, a logs or
  traces source, a filter, and an aggregation
- **THEN** the query executes and returns a result in the declared envelope

#### Scenario: Unsupported version is reported, not misinterpreted

- **WHEN** a client submits an IR document whose version is outside the range the
  server supports
- **THEN** the request is rejected with an error identifying the supported
  version range, rather than being parsed under a different version's rules

#### Scenario: Additive evolution preserves stored queries

- **WHEN** the IR schema gains a new optional field or stage in a later server
  version and an older stored query is replayed
- **THEN** the older query still validates and executes with unchanged meaning

#### Scenario: A v2-only heatmap is rejected under v1

- **WHEN** a client submits a `heatmap` stage or `heatmap` result envelope with
  `irVersion: 1`
- **THEN** the server rejects the document as unsupported for that version

### Requirement: Defined type system and evaluation semantics

The IR SHALL have a type system and evaluation semantics defined independently of
the query plan it is compiled to: each logical field has one canonical value
type owned by the attribute registry; each stage consumes and produces a typed
relation; and null/absent handling and literal coercion are defined by the IR,
not inherited from the execution engine. The result of a query SHALL be
determined by these semantics, not by engine-version-specific behaviour.

#### Scenario: Absent-field comparison is defined by the IR

- **WHEN** a query filters on a field that is absent from some records (for
  example a negated equality `not(field = x)`)
- **THEN** records where the field is absent are handled per the IR's specified
  absent-value semantics, consistently regardless of the underlying execution
  engine version

#### Scenario: Literal coercion follows the registry type

- **WHEN** a query compares a field against a literal (e.g. a duration `"500ms"`
  or a numeric `"17"`)
- **THEN** the literal is coerced to the field's registry-declared canonical
  type, and a literal that cannot be coerced is rejected at validation rather
  than silently cast at runtime

### Requirement: Structured stage operands

Stage operands (aggregation targets, ordering keys, rank expressions) SHALL be
structured values, not embedded expression strings that require parsing. A
client SHALL NOT express an operand as a mini-expression string.

#### Scenario: Rank references a structured aggregate

- **WHEN** a query ranks results by an aggregated value
- **THEN** the rank stage references the aggregate by its declared output name as
  a structured operand, and a request that supplies an operand as an unparsed
  expression string is rejected

#### Scenario: Aggregate outputs are uniquely named and referenced by name

- **WHEN** a query declares one or more aggregates, each with an output name, and
  a later stage (rank or order) references one of those names
- **THEN** the reference resolves to exactly that aggregate; a query with two
  outputs sharing a name, or a reference to a name no stage produced, is rejected

#### Scenario: Rank size must be a positive integer

- **WHEN** a `topk`/`bottomk` stage declares a non-positive or non-integer size
- **THEN** the query is rejected at validation

### Requirement: Supported stage set

This capability SHALL support the single-signal stage set `from`, `where`,
`extract`, `aggregate`, `topk`/`bottomk`, `order`, and `limit` in IR v1. IR v2
SHALL additionally support the terminal `heatmap` stage for a bounded
time-by-numeric-distribution count aggregate. An unknown stage, or a stage
illegal for the source or IR version, SHALL be rejected as unsupported rather
than silently ignored. The `extract` stage SHALL support the `json` and
`logfmt` parsers; the `regex` parser is not part of this capability and,
together with the predicate `regex` operator, SHALL run only behind a bounded,
timeout-guarded matcher.

#### Scenario: A supported stage executes

- **WHEN** a query uses only stages supported by its IR version on a logs or
  traces source
- **THEN** the query validates and executes

#### Scenario: An out-of-set stage is rejected, not ignored

- **WHEN** a query includes a stage its IR version does not support
- **THEN** the query is rejected identifying the unsupported stage, rather than
  the stage being dropped

#### Scenario: Extract offers json and logfmt

- **WHEN** a logs query uses an `extract` stage with the `json` or `logfmt`
  parser
- **THEN** the derived fields are available to subsequent stages; a request for a
  parser this capability does not provide is rejected

#### Scenario: Extracted fields are typed, resolvable, and non-shadowing

- **WHEN** an `extract` stage declares derived fields with names and value types,
  and a later `where`/`aggregate`/`order` references one
- **THEN** the reference resolves to the derived field with its declared type
  and a derived name that collides with a registry-owned logical field or an
  earlier extracted field is rejected rather than silently shadowing it

### Requirement: Shared predicate grammar over logical field names

Filtering SHALL use one predicate grammar — comparison leaves (`{field, op,
value}`) composed with `and`/`or`/`not` — where `field` is a logical, dotted
OTel-native attribute name. A client SHALL NOT reference a physical column name,
the attribute-JSON blob, or any storage detail; such a query SHALL be rejected.
Supported operators SHALL include equality, ordered comparison, membership,
range, substring match, existence, and regular-expression match (`regex`), each
a member of the versioned operator registry. The `regex` operator takes a string
pattern operand and SHALL be evaluated only behind a bounded, timeout-guarded
matcher so a pathological pattern cannot exhaust resources.

#### Scenario: Predicate filters on an OTel attribute

- **WHEN** a query filters on `service.name = "checkout"` combined with
  `http.status_code >= 500` under an `and`
- **THEN** only matching records are returned, and the query contains no physical
  column or storage-layout reference

#### Scenario: Physical addressing is rejected

- **WHEN** a query attempts to filter directly on the attribute-JSON blob or a
  raw storage column name
- **THEN** the query is rejected as invalid rather than executed

### Requirement: Registry-mediated field resolution independent of promotion

Every logical field SHALL be resolved to its physical location — a promoted
column or an attribute-JSON extraction — through the attribute registry at plan
time. The result of a query SHALL NOT depend on whether a field is currently
promoted; promotion state SHALL affect only performance.

#### Scenario: Same result before and after promotion

- **WHEN** the same IR query is executed against a field served as an
  attribute-JSON extraction, and later against the same field after it has been
  promoted to a physical column
- **THEN** both executions return the same result set

### Requirement: Extensible signal-source model

The IR source SHALL reference a registered signal source rather than a fixed,
hardcoded set. Logs, traces, and profiles SHALL be available as sources in this
capability; the model SHALL allow additional sources (e.g. metrics) to be added
by later changes without altering the IR document shape. A profile source query
SHALL operate on profile-summary rows and SHALL expose only registered scalar
profile metadata and registered resource or scope attributes; it SHALL NOT
expose sample, stacktrace, or attribute JSON payloads as logical fields.

#### Scenario: Logs and traces are queryable sources

- **WHEN** a client selects `logs` or `traces` as the query source
- **THEN** the query executes against that signal

#### Scenario: Profiles is a queryable source

- **WHEN** a client selects `profiles` as the query source
- **THEN** the query executes against that signal

#### Scenario: Profile summary query returns registered metadata

- **WHEN** a client requests profile fields such as `profile.id`, `timestamp`,
  `duration`, `sample.type`, or `service.name`
- **THEN** the result contains the requested typed metadata values without raw
  sample, stacktrace, or attribute JSON payloads

#### Scenario: Profile payload addressing is rejected

- **WHEN** a profile query references a raw sample, stacktrace, or attribute JSON
  storage payload
- **THEN** validation rejects the query as an unregistered logical field

#### Scenario: Adding a source does not reshape the IR

- **WHEN** a later change registers an additional signal source
- **THEN** existing IR documents remain valid and the document shape is unchanged

### Requirement: Source-specific read authorization

The native Query IR endpoint SHALL authorize a request for a registered signal
source using that signal's read scope before it dispatches the query. A request
for `profiles` SHALL require `profiles:read`; authorization SHALL remain bound
to the authenticated tenant and dataset rather than any client-supplied tenant
or dataset value.

#### Scenario: Profile scope permits profile IR query

- **WHEN** an authenticated request with `profiles:read` submits a profile IR
  document for its tenant and dataset
- **THEN** the endpoint dispatches the query using that authenticated context

#### Scenario: Missing profile scope is rejected

- **WHEN** an authenticated request without `profiles:read` submits a profile IR
  document
- **THEN** the endpoint rejects the request before dispatching it to a querier

#### Scenario: Other source scopes remain isolated

- **WHEN** an authenticated request with only `profiles:read` submits a logs or
  traces IR document
- **THEN** the endpoint rejects the request for lacking that source's read scope

### Requirement: Legal-stage enforcement by relation type

Each stage SHALL declare its input-relation constraint and output relation, and
the system SHALL infer the relation type through the pipeline and reject a stage
whose input constraint is unmet — before execution, with an error naming the
offending stage.

#### Scenario: Illegal stage is rejected pre-execution

- **WHEN** a query applies a log-only field-extraction stage to a traces source
- **THEN** the query is rejected during validation with an error naming the
  stage and source, and no plan is executed

### Requirement: Declared and validated result envelope

A query SHALL declare its result envelope (`rows`, `series`, or `table` in
IR v1; `heatmap` additionally in IR v2; and, for the `profiles` source
only, `flamegraph`), and the system SHALL validate the declared envelope
against the inferred terminal relation type and against the selected
source, rejecting a mismatch before execution. Each envelope SHALL have a
single canonical response payload shape and value encoding, described by
the OpenAPI schema so the generated clients decode one contract. The
columns of a `rows`/`table` result SHALL be a curated projection: taken
from an explicit document-level `fields` list of logical names when
present, otherwise a bounded server default — never all physical columns
implicitly. A `fields` entry absent from the terminal relation, or a
`fields` list on a `series`, `heatmap`, or `flamegraph` result, SHALL be
rejected.

#### Scenario: Envelope mismatch is rejected

- **WHEN** a query declares the `series` envelope but its terminal stage produces
  a non-time-series relation
- **THEN** the query is rejected at validation time with an envelope-mismatch
  error

#### Scenario: Row results are a curated projection

- **WHEN** a query returns the `rows` envelope, with or without an explicit
  `fields` list
- **THEN** the response contains an explicit, bounded set of named/typed fields
  (the `fields` list, or the bounded server default) rather than every physical
  column of the underlying table

#### Scenario: Invalid projection is rejected

- **WHEN** a query's `fields` list names something the terminal relation
  does not carry, or a `series`, `heatmap`, or `flamegraph` query declares
  `fields`
- **THEN** the query is rejected at validation time

#### Scenario: Flamegraph envelope requires the profiles source

- **WHEN** a query declares the `flamegraph` envelope with `from: "logs"` or
  `from: "traces"`
- **THEN** the query is rejected at validation as an envelope/source
  mismatch, naming the source

### Requirement: Bounded two-dimensional heatmap aggregate

An IR v2 `heatmap` stage SHALL be terminal and SHALL aggregate matching records
into epoch-aligned time buckets on the x-axis and typed numeric or duration
buckets on the y-axis. The stage SHALL resolve its y-axis field through the
logical field registry, SHALL use a count value, and SHALL return sparse cells.
The server SHALL enforce bounded time-bucket and duration-bound cardinalities.

#### Scenario: Trace duration heatmap returns sparse count cells

- **WHEN** a traces query filters a selected window and applies a heatmap over
  `duration` with valid duration bounds and a time step
- **THEN** the response contains time-axis metadata, duration bounds, and only
  non-zero count cells
- **AND** every matching span contributes to exactly one time and duration cell

#### Scenario: Duration boundaries have deterministic inclusion semantics

- **WHEN** a span duration equals a configured duration boundary
- **THEN** it is included in the bucket beginning at that boundary
- **AND** a duration below the first boundary is included in the first bucket
- **AND** a duration at or above the final boundary is included in the overflow
  bucket

#### Scenario: Heatmap preserves native query isolation

- **WHEN** authenticated requests for different tenant or dataset contexts submit
  otherwise identical heatmap documents
- **THEN** each response contains only records from its authenticated tenant and
  dataset

#### Scenario: Unsafe heatmap dimensions are rejected

- **WHEN** a client requests non-increasing bounds, a non-positive time step, or
  an axis cardinality above the server limit
- **THEN** the server rejects the document before executing the query

### Requirement: Profile flamegraph retrieval

For the `profiles` source, a query declaring the `flamegraph` result
envelope SHALL aggregate every profile row matched by its `from`/`where`
stages into one bounded flamegraph structure (names, per-level frame data,
total sample value, max self value), computed by the same aggregation the
Pyroscope-compatible render endpoint uses, rather than returning raw
`samples_json`/`stacktraces_json` payload text. Filtering to a single
`profile.id` SHALL yield that profile's own flamegraph; filtering more
broadly (by service, sample type, or time range) SHALL aggregate across
every matching profile, consistent with the Pyroscope render endpoint's
selector/range semantics over the same data.

A `flamegraph`-terminated pipeline SHALL support only the `from` and
`where` stages before the envelope; a pipeline that places `extract`,
`aggregate`, `topk`/`bottomk`, or `order` before a `flamegraph` envelope
SHALL be rejected at validation, naming the offending stage, because the
flamegraph aggregation is itself the terminal computation and does not
compose with DataFusion-lowered stages. `profiles:read` authorization
SHALL apply identically to `flamegraph` queries as to `rows`/`table`/
`series` queries on the same source; no new authorization scope is
introduced.

The aggregated flamegraph response SHALL be bounded: if more than a fixed
number of profile rows match (a row-count cap, not a response-byte-size
cap), the response SHALL still aggregate the first of them up to that cap
and SHALL carry a `truncated: true` flag rather than returning an unbounded
payload or failing the request.

#### Scenario: Single profile id yields its own flamegraph

- **WHEN** a `profiles` source query filters on a specific `profile.id` and
  declares the `flamegraph` envelope
- **THEN** the result is that one profile's flamegraph

#### Scenario: Broader filter aggregates across matching profiles

- **WHEN** a `profiles` source query filters by `service.name` and a time
  range (no `profile.id` filter) and declares the `flamegraph` envelope
- **THEN** the result aggregates every matching profile into one flamegraph,
  the same aggregation the Pyroscope render endpoint would produce for an
  equivalent selector and range

#### Scenario: Raw payload fields remain unaddressable

- **WHEN** a `profiles` source query references `samples_json` or
  `stacktraces_json` by name, in `fields` or in a `where` predicate, on any
  envelope including `flamegraph`
- **THEN** the query is rejected as an unregistered logical field; the
  `flamegraph` envelope is the only way to retrieve payload-derived data,
  and only in its bounded, aggregated form

#### Scenario: Non-compatible stage before flamegraph is rejected

- **WHEN** a `profiles` source query places an `aggregate` or `order` stage
  before a `flamegraph` envelope
- **THEN** the query is rejected at validation, naming the offending stage,
  rather than executing with the stage ignored

#### Scenario: Flamegraph query is authorized like other profile queries

- **WHEN** an authenticated request without `profiles:read` submits a
  `profiles` source query with the `flamegraph` envelope
- **THEN** the endpoint rejects the request before dispatching it to a
  querier, identically to a `rows`/`table`/`series` request on the same
  source

#### Scenario: Oversized flamegraph is truncated with a flag

- **WHEN** a `flamegraph` query matches more profile rows than the
  server's fixed row-count cap
- **THEN** the response is returned aggregated over the first rows up to
  the cap and marked `truncated: true`, not as an unbounded payload or a
  failed request

### Requirement: Native query surface

SignalDB SHALL expose the IR over a native HTTP endpoint (`POST /api/v1/query`)
that is authenticated and tenant-scoped consistently with the other query APIs.
The endpoint, the versioned IR request schema, and the result-envelope schemas
SHALL be described by the code-first OpenAPI specification, and the first-party
UI and CLI SHALL consume it exclusively through their generated clients.

#### Scenario: Authenticated, tenant-scoped submission

- **WHEN** an authenticated client submits an IR query with a tenant context
- **THEN** the query executes only against that tenant's datasets and returns the
  enveloped result, honouring the same auth/tenant rules as the Tempo/LogQL/
  Prometheus surfaces

#### Scenario: Surface is generated-client-only for first parties

- **WHEN** the UI or CLI issues an IR query
- **THEN** it does so through the generated TypeScript client / Rust SDK derived
  from the OpenAPI spec, not through hand-written HTTP calls

### Requirement: Additive, non-regressing surface

The IR query capability SHALL be additive: OTLP ingestion, the Tempo/LogQL/
Prometheus query surfaces, existing Flight wire schemas, and the on-disk Iceberg/
WAL layout SHALL be unchanged, and no existing query SHALL regress.

#### Scenario: Existing dialects are unaffected

- **WHEN** the IR capability is deployed
- **THEN** existing TraceQL/LogQL/PromQL queries continue to execute exactly as
  before, and no ingest, Flight schema, or on-disk layout changes

### Requirement: Aggregates may be scoped by a predicate

An aggregate SHALL accept an optional predicate scoping which records it
consumes, so that a single grouped query can report both an overall measure and a
measure over a subset of the same groups. The predicate SHALL use the same
grammar and logical field names as the `where` stage, and SHALL be evaluated
against the records reaching the aggregate stage — the stage's grouping and any
earlier stage's filtering apply first.

Scoping SHALL be per-aggregate: aggregates within one stage may carry different
predicates, or none. An unscoped aggregate SHALL consume every record in its
group, unchanged from today.

#### Scenario: A scoped and an unscoped aggregate share one grouping

- **WHEN** a query groups records and declares both an unscoped count and a count
  scoped to a predicate
- **THEN** each group reports the total for the group and the count of only the
  records satisfying the predicate
- **AND** the grouping is performed once, not once per aggregate

#### Scenario: A group where no record satisfies the predicate

- **WHEN** a group contains no record satisfying a scoped aggregate's predicate
- **THEN** the group is still returned, with the scoped count reported as zero
- **AND** the group is not dropped from the result

#### Scenario: Scoping does not change the group set

- **WHEN** the same query is issued with and without a scoping predicate on one
  of its aggregates
- **THEN** both return the same groups in the same order under the same `order`
  stage, differing only in the scoped aggregate's values

#### Scenario: Scoped non-count aggregates measure only matching records

- **WHEN** a quantile or sum aggregate carries a scoping predicate
- **THEN** its value is computed over only the records in the group satisfying
  that predicate

#### Scenario: An invalid scoping predicate is rejected at validation

- **WHEN** a scoping predicate references a field that cannot be resolved, or
  uses an operator the predicate grammar does not define
- **THEN** the query is rejected at validation identifying the offending
  predicate, rather than executing with the scope ignored

#### Scenario: The scoping predicate is a structured operand

- **WHEN** a client supplies an aggregate's scope
- **THEN** it is expressed with the structured predicate grammar, and a scope
  supplied as an unparsed expression string is rejected
