## MODIFIED Requirements

### Requirement: Supported stage set

This capability SHALL support the single-signal stage set `from`, `where`,
`extract`, `aggregate`, `topk`/`bottomk`, `order`, and `limit` in IR v1. IR v2
SHALL additionally support the terminal `heatmap` stage for a bounded
time-by-numeric-distribution count aggregate. IR v4 SHALL additionally support
the terminal `describe` stage, which introspects the source rather than reading
its records and is legal only with the `metadata` result envelope. An unknown
stage, or a stage illegal for the source or IR version, SHALL be rejected as
unsupported rather than silently ignored. The `extract` stage SHALL support the
`json` and `logfmt` parsers; the `regex` parser is not part of this capability
and, together with the predicate `regex` operator, SHALL run only behind a
bounded, timeout-guarded matcher.

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

#### Scenario: A v4-only describe stage is rejected under an earlier version

- **WHEN** a client submits a `describe` stage or a `metadata` result envelope
  with an `irVersion` below 4
- **THEN** the server rejects the document as unsupported for that version,
  naming the version the stage requires

#### Scenario: Describe is terminal and admits no record stages before it

- **WHEN** a document places a `where`, `extract`, `aggregate`, `topk`/`bottomk`,
  `order`, `limit`, or `heatmap` stage before a `describe` stage, or places any
  stage after it
- **THEN** the document is rejected at validation naming the offending stage,
  rather than executing with the stage ignored

### Requirement: Declared and validated result envelope

A query SHALL declare its result envelope (`rows`, `series`, or `table` in
IR v1; `heatmap` additionally in IR v2; `metadata` additionally in IR v4;
and, for the `profiles` source only, `flamegraph`), and the system SHALL
validate the declared envelope against the inferred terminal relation type
and against the selected source, rejecting a mismatch before execution. Each
envelope SHALL have a single canonical response payload shape and value
encoding, described by the OpenAPI schema so the generated clients decode one
contract. The `metadata` envelope SHALL be legal only for a pipeline whose
terminal stage is `describe`, and a `describe`-terminated pipeline SHALL be
legal only with the `metadata` envelope. The columns of a `rows`/`table` result
SHALL be a curated projection: taken from an explicit document-level `fields`
list of logical names when present, otherwise a bounded server default — never
all physical columns implicitly. A `fields` entry absent from the terminal
relation, or a `fields` list on a `series`, `heatmap`, `flamegraph`, or
`metadata` result, SHALL be rejected.

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
  does not carry, or a `series`, `heatmap`, `flamegraph`, or `metadata` query
  declares `fields`
- **THEN** the query is rejected at validation time

#### Scenario: Flamegraph envelope requires the profiles source

- **WHEN** a query declares the `flamegraph` envelope with `from: "logs"` or
  `from: "traces"`
- **THEN** the query is rejected at validation as an envelope/source
  mismatch, naming the source

#### Scenario: Metadata envelope requires a describe terminal

- **WHEN** a document declares the `metadata` envelope without a terminal
  `describe` stage, or terminates in `describe` while declaring another envelope
- **THEN** the document is rejected at validation as an envelope mismatch
