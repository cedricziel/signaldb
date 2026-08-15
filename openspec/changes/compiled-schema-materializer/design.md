## Context

See `proposal.md` for motivation. This change assumes `unified-table-schema`
has landed: `schemas.toml` is the generated source for physical, wire, and
logical-registry schema, and (per that change's design) fields carry the
metadata needed to know how each is sourced.

**The concrete inefficiency this replaces.** `writer::schema_transform::transform_trace_v1_to_v2`
loops the v2 schema's fields and, per field, calls `get_column_by_name(&batch, &field.name)`
— a string-keyed lookup into the incoming `RecordBatch`'s schema, for every
field, for every batch, forever. The mapping from a v1 column name to its
position and cast rule is fully determined by the (v1, v2) version pair
alone; it never needs to be recomputed once those two versions are fixed.

**Where extraction logic already lives.** `conversion_traces.rs`'s
`otlp_traces_to_arrow` already builds columns in a columnar style — a
`Vec<T>` accumulated across every span, then converted to one Arrow array
per column (e.g. `StringArray::from(span_kinds)`) — not row-by-row
dispatch. Any compiled-plan design must preserve exactly this shape: the
genericization target is _which_ extractor runs for _which_ column, not
the batching strategy that makes today's code fast.

## Goals / Non-Goals

**Goals:**

- Resolve column-to-extractor mappings once per schema version (cached),
  eliminating the per-batch, per-field name lookup in
  `schema_transform.rs`.
- Let an ordinary field addition (verbatim value, or a value shaped like
  one already handled) require a schema declaration plus a small
  independently-testable extractor — not a new match arm threaded through
  batch-construction control flow.
- Preserve today's columnar (whole-column-at-once) construction style
  exactly; no per-row dynamic dispatch.
- No ingest throughput regression, verified by
  `performance-benchmarking-suite`'s acceptor-decode and writer-append
  benchmarks; the v1→v2 step specifically is expected to improve by
  removing string lookups from its hot loop.

**Non-Goals:**

- Generating OTel semantic logic itself. An extractor for "derive
  `span_kind` string from `span_kind_number`" is exactly as hand-written
  as it is today — what's generic is that the plan knows _to run it for
  this column_, not what it computes.
- Implementing materialized views. This change makes the same compiled-plan
  mechanism a plausible foundation for that later work (a view is a
  declared derived schema over a different kind of input — query results
  rather than OTLP spans — materialized the same way), but does not define
  view syntax, refresh, or storage here.
- A fully dynamic/reflective extraction mechanism (e.g. resolving field
  access by string path at materialization time). Extractors are named,
  statically registered Rust closures resolved to a fixed order once per
  version; nothing about materializing a batch does string-keyed work.

## Decisions

### The compiled plan: an ordered list of (physical column, extractor), resolved once

For a given schema version, a `MaterializationPlan` is
`Vec<(ColumnSpec, Extractor<Source>)>` where `ColumnSpec` carries the
target physical position/type and `Extractor<Source>` is a function
pointer `fn(&[Source]) -> ArrayRef` (or `fn(&Source, &mut dyn ArrayBuilder)`
per-row, batched at the call site — exact signature is an implementation
choice, not a spec-level concern) selected by the field's extraction-rule
name at plan-build time. Building the plan does the name resolution;
running it does not. Alternative considered: resolve lazily, memoizing per
field name inside the hot loop (e.g. a `HashMap` cache keyed by name,
populated on first miss). Rejected — it still pays a hash lookup per field
per batch, and a version-keyed plan cache (below) removes even that.

### Plan cache keyed by schema version pair, not recomputed per call

`schema_transform` (and the OTLP→wire step) holds a
`OnceLock`/`Lazy`-style cache from `(source_version, target_version)` (or
just `target_version` for the OTLP→wire case, whose source is always "the
current OTel proto shape") to its resolved `MaterializationPlan`. First use
of a version pair builds and caches the plan; every subsequent batch reuses
it. This is the direct fix for the `get_column_by_name`-per-field problem:
the lookup happens once per version pair for the lifetime of the process,
not once per batch.

### Extractors are named and registered per extraction rule, not per column

An extraction rule is identified by a short name (`verbatim:span.kind`,
`derived:span_kind_str_from_number`, `verbatim:dropped_attributes_count`,
...) mapping to a hand-written Rust function. `schemas.toml` fields declare
which rule they use (this is the field-level metadata `unified-table-schema`
introduces — the "provenance" concept discussed alongside these proposals:
`verbatim` fields name the source path directly; `derived` fields name the
rule). Multiple fields with the same shape of extraction (e.g. every
plain int64 counter read verbatim off a same-shaped proto field) can share
one generic verbatim-extractor parameterized by field name — genuinely new
extraction shapes still require a genuinely new hand-written rule. This is
the same non-goal as `unified-table-schema`'s: the _set_ of rules is small
and hand-written; what's generic is selecting and sequencing them.

### Golden tests carry across from `unified-table-schema`

The same discipline applies: for each conversion function migrated to
plan-based construction, a test asserts identical output (same values,
types, nullability) against the hand-written code it replaces, on
representative fixtures, before the hand-written code is deleted.

## Risks / Trade-offs

- **[Risk]** A subtly wrong extraction rule selected by name (e.g. a typo
  in `schemas.toml`'s rule reference resolving to the wrong registered
  closure) fails silently at plan-build time if rule names aren't
  exhaustively validated → **Mitigation**: plan construction SHALL fail
  fast (panic or startup error, not silent default) on an unregistered
  rule name; a test enumerates every field in every current schema version
  and asserts its rule resolves.
- **[Risk]** Benchmark-driven claims ("this will be faster") not landing
  as claimed if the plan abstraction itself introduces overhead (e.g.
  boxed closures with dynamic dispatch instead of monomorphized function
  pointers) → **Mitigation**: the explicit benchmark gate in `proposal.md`
  — this ships only once `performance-benchmarking-suite`'s acceptor/writer
  benchmarks confirm no regression, treated as a hard gate, not a
  nice-to-have.
- **[Trade-off]** Indirection through a plan/registry is harder to read at
  a call site than an inline match arm — a maintainer can no longer see
  "what happens to `span_kind`" by reading one function top to bottom, only
  by following the rule name to its registration → accepted: the
  alternative (today's state) is the exact duplication-across-layers
  problem this proposal and `unified-table-schema` both exist to remove.

## Migration Plan

1. Requires `unified-table-schema` landed (schema-level provenance/rule
   metadata to select from).
2. Requires `performance-benchmarking-suite`'s acceptor-decode and
   writer-append benchmarks in place as the regression gate.
3. Build `MaterializationPlan`/extractor registry in `common`; golden tests
   against current hand-written output.
4. Migrate `schema_transform.rs`'s v1→v2 step first (the identified
   inefficiency, isolated, lowest-risk starting point); benchmark before
   deleting the old code.
5. Migrate `conversion_traces.rs`/logs/metrics OTLP→wire construction the
   same way.
6. No deployment-visible change; no rollback machinery beyond redeploying
   the previous binary, since output is byte-for-byte identical by golden
   test.

## Open Questions

None.
