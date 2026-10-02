## MODIFIED Requirements

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

The `rows` and `trace` envelopes SHALL additionally carry an optional `page`
member when the document requested pagination (IR v14, see
`query-result-pagination`), and an optional `tail` member when the document
requested a live tail (IR v15, see `query-live-tail`). Each member SHALL be
present only when requested, so a response to a document without `page` or
`tail` is unchanged. A document-level `page` or `tail` on any other envelope
SHALL be rejected at validation.

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

#### Scenario: Oversized flamegraph is truncated with a flag

- **WHEN** a `flamegraph` query matches more profile rows than the
  server's fixed row-count cap
- **THEN** the response is returned aggregated over the first rows up to
  the cap and marked `truncated: true`, not as an unbounded payload or a
  failed request

#### Scenario: Paging members appear only when requested

- **WHEN** the same `rows` document is submitted once without `page` and once
  with `page`
- **THEN** the first response has no `page` member and is otherwise identical
  to the response before this capability existed, and the second carries a
  `page` member

#### Scenario: Paging on another envelope is rejected

- **WHEN** a `series` or `metadata` document carries `page` or `tail`
- **THEN** it is rejected at validation, naming the envelope
