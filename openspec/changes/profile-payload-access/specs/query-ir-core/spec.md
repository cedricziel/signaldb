## MODIFIED Requirements

### Requirement: Declared and validated result envelope

A query SHALL declare its result envelope (`rows`, `series`, `table`, or —
for the `profiles` source only — `flamegraph`), and the system SHALL
validate the declared envelope against the inferred terminal relation type
and against the selected source, rejecting a mismatch before execution.
Each envelope SHALL have a single canonical response payload shape and
value encoding, described by the OpenAPI schema so the generated clients
decode one contract. The columns of a `rows`/`table` result SHALL be a
curated projection: taken from an explicit document-level `fields` list of
logical names when present, otherwise a bounded server default — never all
physical columns implicitly. A `fields` entry absent from the terminal
relation, or a `fields` list on a `series` or `flamegraph` result, SHALL be
rejected.

#### Scenario: Envelope mismatch is rejected

- **WHEN** a query declares the `series` envelope but its terminal stage
  produces a non-time-series relation
- **THEN** the query is rejected at validation time with an
  envelope-mismatch error

#### Scenario: Row results are a curated projection

- **WHEN** a query returns the `rows` envelope, with or without an explicit
  `fields` list
- **THEN** the response contains an explicit, bounded set of named/typed
  fields (the `fields` list, or the bounded server default) rather than
  every physical column of the underlying table

#### Scenario: Invalid projection is rejected

- **WHEN** a query's `fields` list names something the terminal relation
  does not carry, or a `series` or `flamegraph` query declares `fields`
- **THEN** the query is rejected at validation time

#### Scenario: Flamegraph envelope requires the profiles source

- **WHEN** a query declares the `flamegraph` envelope with `from: "logs"` or
  `from: "traces"`
- **THEN** the query is rejected at validation as an envelope/source
  mismatch, naming the source

## ADDED Requirements

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
