## Why

`unified-table-schema` (proposed separately) collapses schema _declaration_
onto `schemas.toml` but deliberately leaves schema _materialization_ —
turning a declared field into an actual physical column, for an actual
batch of rows — as hand-written Rust, once in
`conversion_traces.rs`/`conversion_logs.rs`/`conversion_metrics.rs` (OTLP →
wire) and again in `writer::schema_transform` (wire → physical v1→v2).
That second layer has a concrete, measurable inefficiency today:
`transform_trace_v1_to_v2` resolves every field by string name
(`get_column_by_name(&batch, &field.name)`) on every batch, for every
field, rather than resolving column positions once per schema version. The
underlying pattern — "given a declared schema and a source of typed
values, produce a physical Arrow batch" — is itself generic and only needs
to be resolved once per schema _version_, not once per row or batch. We
also already know we need essentially the same mechanism again: SignalDB's
Iceberg fork (`iceberg-rust`) was chosen in part for its materialized-view
support, and a materialized view is exactly "a declared derived schema,
generically materialized from some input" — the same shape of problem as
turning an OTLP span into a physical row. Building the compiled-plan
mechanism now, for ingest, is the path that makes materialized views a
second consumer later instead of a second bespoke implementation.

## What Changes

- Introduce a **compiled column plan**: resolved once per schema version
  (at startup or on first use, cached — never per-row or per-batch) from a
  schema definition, pairing each physical column with a small, named
  extractor. Materializing a batch becomes "run the plan," not "match on
  every field name."
- Named extractors are hand-written Rust closures, one per distinct
  extraction rule (e.g. "read `Span.kind` verbatim as int32", "derive the
  display string from the kind number"), registered by name and selected
  by each field's declared source in `schemas.toml`. This keeps the
  irreducible OTel semantic logic in Rust — only the _wiring_ (which
  extractor feeds which column, in which order, into which builder) is
  generic and schema-driven.
- Replace `writer::schema_transform`'s per-field, per-batch
  `get_column_by_name` string lookups with a plan resolved once per (v1,
  v2) version pair and reused for every batch — a straightforward,
  measurable win on the concrete inefficiency identified above.
- Apply the same plan structure to the OTLP → wire step in
  `conversion_traces.rs` et al., replacing today's per-signal hand-written
  match arms with plan execution, while keeping today's columnar
  construction style (one Arrow array built across all rows per column,
  not row-by-row).
- Gate this change on the acceptor OTLP-decode and writer v1→v2-transform
  benchmarks from `performance-benchmarking-suite` (proposed separately):
  the compiled plan must show no regression, and is expected to show a
  measurable improvement on the v1→v2 step specifically, given it removes
  per-field string lookups from the hot path.

## Capabilities

### New Capabilities

- `schema-materialization`: guarantees that producing a physical batch
  from a declared schema and a source of values is resolved once per
  schema version, not per row or batch, and that adding a schema field
  whose value is read verbatim or via an already-registered extraction
  rule requires no new per-column materialization code.

### Modified Capabilities

- `performance-benchmarking-suite`: none — this change consumes that
  capability's existing acceptor-OTLP-decode and writer-append benchmark
  coverage as its regression gate; it does not change what that capability
  requires. (Listed here for visibility only; no delta spec follows,
  since the requirement itself is unchanged.)

## Impact

- **common**: `flight/conversion/conversion_traces.rs` (and the logs/metrics
  equivalents) — hand-written match arms replaced by compiled-plan
  execution.
- **writer**: `schema_transform.rs` — `get_column_by_name`-per-field
  replaced by a plan resolved once per version pair.
- **Depends on**: `unified-table-schema` (the compiled plan is built from
  that change's generated schema resolution, plus the field-level
  provenance/extraction-rule metadata this change adds to it) and
  `performance-benchmarking-suite` (regression gate). Sequenced after
  both; not implementable in isolation.
- **Not implemented in this change**: materialized views themselves. This
  change makes the materializer reusable for that future work; it does
  not add view definitions, refresh, or query rewriting.
- Not **BREAKING**: identical OTLP ingest behavior and identical physical
  output for every existing field, verified by the same golden-test
  discipline as `unified-table-schema`.
