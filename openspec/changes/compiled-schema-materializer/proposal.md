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

**Scope correction found while confirming prerequisites**: `unified-table-schema`
landed without the field-level provenance/extraction-rule metadata this
proposal originally assumed it would introduce — that piece was dropped
during its implementation for exactly the reasons this proposal's own
OTLP→wire migration would have hit: traces' wire format needs pre-rename
names and a different attribute type than physical; logs' wire fields
don't correspond 1:1 to physical ones at all; metrics' wire format is one
polymorphic table against five normalized physical tables. A per-field
`schemas.toml` rule name can't select "how to build this wire column" any
more cleanly than it could select "what this wire column even is." This
change now drops the OTLP→wire migration and keeps only the part that
doesn't depend on that metadata: the writer's v1→v2 transform, whose
column-to-extractor mapping is fully determined by the (v1, physical
version) pair alone and can be resolved once in hand-written Rust — the
same selection logic `transform_trace_v1_to_v2`'s match arms already
encode, just built once per version pair instead of re-executed per batch.

- Introduce a **compiled column plan**: resolved once per (v1, physical
  version) pair (cached — never per-row or per-batch), pairing each
  physical column with a small, named extractor selected in Rust from the
  target schema's resolved field list. Materializing a batch becomes "run
  the plan," not "match on every field name per batch."
- Named extractors are hand-written Rust closures, one per distinct
  extraction rule (e.g. "read a same-named UInt64 column and cast to
  Int64", "derive the display string from the kind number"). This keeps
  the irreducible OTel semantic logic in Rust — only the _wiring_ (which
  extractor feeds which column, in which order) is generic and resolved
  once.
- Replace `writer::schema_transform::transform_trace_v1_to_v2`'s per-field,
  per-batch `get_column_by_name` string lookups with a plan resolved once
  per version pair and reused for every batch — a straightforward,
  measurable win on the concrete inefficiency identified above.
- Gate this change on a new writer v1→v2-transform benchmark (added as
  part of this change, since `performance-benchmarking-suite` doesn't yet
  have one) plus the existing acceptor OTLP-decode benchmark: the compiled
  plan must show no regression on either, and is expected to show a
  measurable improvement on the v1→v2 step specifically, given it removes
  per-field string lookups from the hot path.
- **Not implemented in this change**: migrating the OTLP→wire construction
  step (`conversion_traces.rs` et al.) to compiled plans — see the scope
  correction above. Remains hand-written until a real transform-primitive
  design exists for the wire/physical divergence, same disposition as
  `unified-table-schema`'s dropped wire-schema-generation goal.

## Capabilities

### New Capabilities

- `schema-materialization`: guarantees that producing a physical batch
  from a declared schema and a source of values is resolved once per
  schema version, not per row or batch, and that adding a schema field
  whose value is read verbatim or via an already-registered extraction
  rule requires no new per-column materialization code.

### Modified Capabilities

- `performance-benchmarking-suite`: adds the writer v1→v2-transform
  benchmark that capability didn't yet have, since this change's
  regression gate needs it and none existed.

## Impact

- **common**: none — the field-level provenance/extraction-rule metadata
  this change originally planned to add to `schemas.toml` is dropped (see
  the scope correction above); `conversion_traces.rs` et al. stay
  hand-written.
- **writer**: `schema_transform.rs` — `transform_trace_v1_to_v2`'s
  `get_column_by_name`-per-field match replaced by a plan resolved once
  per version pair; new benchmark under `writer/benches/`.
- **Depends on**: `unified-table-schema` (landed; the compiled plan
  resolves against its `SCHEMA_DEFINITIONS`/`ResolvedSchema`, no
  additional metadata needed) and `performance-benchmarking-suite`
  (regression gate; this change adds the one benchmark it was missing).
- **Not implemented in this change**: migrating OTLP→wire construction
  (see scope correction), and materialized views themselves — this change
  makes the materializer mechanism reusable for that future work, but does
  not add view definitions, refresh, or query rewriting.
- Not **BREAKING**: identical OTLP ingest behavior and identical physical
  output for every existing field, verified by the same golden-test
  discipline as `unified-table-schema`.
