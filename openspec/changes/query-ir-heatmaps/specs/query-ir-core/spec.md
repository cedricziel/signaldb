## MODIFIED Requirements

### Requirement: Versioned, structured query IR

SignalDB SHALL accept a structured query as a versioned JSON document carrying a
time range, a source selection, a declared result envelope, and an ordered
pipeline of typed stages. The document SHALL carry an explicit IR version, and
the server SHALL accept a bounded range of versions and report the range it
supports. The IR schema SHALL evolve additively: new stages, operators, and
optional fields MAY be added; accepted operators SHALL be deprecated before any
removal, so that a previously valid stored query remains valid. A stage or
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

### Requirement: Declared and validated result envelope

A query SHALL declare its result envelope (`rows`, `series`, or `table` in IR
v1; `heatmap` additionally in IR v2), and the system SHALL validate the declared
envelope against the inferred terminal relation type, rejecting a mismatch before
execution. Each envelope SHALL have one canonical response payload shape and
value encoding described by the OpenAPI schema. The columns of a `rows`/`table`
result SHALL be a curated projection: taken from an explicit document-level
`fields` list of logical names when present, otherwise a bounded server default.
A `fields` entry absent from the terminal relation, or a `fields` list on a
`series` or `heatmap` result, SHALL be rejected.

#### Scenario: Envelope mismatch is rejected

- **WHEN** a query declares the `series` envelope but its terminal stage produces
  a non-time-series relation
- **THEN** the query is rejected at validation time with an envelope-mismatch
  error

#### Scenario: Row results are a curated projection

- **WHEN** a query returns the `rows` envelope, with or without an explicit
  `fields` list
- **THEN** the response contains an explicit, bounded set of named/typed fields
  rather than every physical column of the underlying table

#### Scenario: Invalid projection is rejected

- **WHEN** a query's `fields` list names something the terminal relation does not
  carry, or a `series` or `heatmap` query declares `fields`
- **THEN** the query is rejected at validation time

## ADDED Requirements

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
