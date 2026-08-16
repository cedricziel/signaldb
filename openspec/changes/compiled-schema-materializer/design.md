## Context

See `proposal.md` for motivation and its scope-correction note.
`unified-table-schema` has landed, but without the field-level
provenance/extraction-rule metadata this design originally planned to
build on — that piece was dropped during its implementation, for the same
structural reasons (wire/physical divergence across renames, attribute
representation, and metrics' table fan-out) this change's own OTLP→wire
migration would have hit. This design now targets only the writer's v1→v2
transform, whose extraction-rule selection is fully determined by the
(v1, physical version) pair and needs no schema-declared metadata beyond
`schemas.toml`'s existing resolved field list (name, type, rename/addition
history) that `unified-table-schema` already provides.

**The concrete inefficiency this replaces.** `writer::schema_transform::transform_trace_v1_to_v2`
loops the v2 schema's fields and, per field, calls `get_column_by_name(&batch, &field.name)`
— a string-keyed lookup into the incoming `RecordBatch`'s schema, for every
field, for every batch, forever. The mapping from a v1 column name to its
position and cast rule is fully determined by the (v1, v2) version pair
alone; it never needs to be recomputed once those two versions are fixed.

**Preserving the existing batching shape.** `transform_trace_v1_to_v2`
already builds output columns in a columnar style — one Arrow array
constructed per field across the whole incoming batch, not row-by-row
dispatch. The compiled plan changes _which code decides_ which extractor
runs for which column (resolved once, ahead of time) not _how_ each
extractor builds its array (unchanged: still one array per column per
batch).

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
- No ingest throughput regression, verified by the acceptor-decode
  benchmark that already exists and a new writer v1→v2-transform benchmark
  this change adds; the v1→v2 step specifically is expected to improve by
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

**[CodeRabbit review, PR #1230]** requires the extractor interface be
columnar by contract, not merely by convention — a per-row signature must
not exist as an option a future rule could reach for. For a given schema
version, a `TraceV1ToV2Plan` is `Vec<ColumnPlan>` where `ColumnPlan` pairs
the target field with an `Extractor = Box<dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync>`
— always whole-batch-in, whole-array-out, never per-row. A boxed closure
(not a bare `fn` pointer) is required specifically because
field-parameterized extractors (e.g. "cast this UInt64 column, whichever
one, to Int64") need to capture the source column name; a bare `fn`
pointer can't close over that. Extractor selection happens once, in plan
construction, via the same match `transform_trace_v1_to_v2`'s inline code
uses today. Building the plan does the name resolution; running it does
not. Alternative considered: resolve lazily, memoizing per field name
inside the hot loop (e.g. a `HashMap` cache keyed by name, populated on
first miss). Rejected — it still pays a hash lookup per field per batch,
and a version-keyed plan cache (below) removes even that.

### Plan cache built eagerly at startup, never lazily under traffic

**[CodeRabbit review, PR #1230]** flagged first-use plan construction
combined with a panic on an unregistered rule as a production hazard:
`panic = "abort"` in the release profile means the first ingested batch
after a bad deploy kills the process instead of failing at startup. Plan
construction returns `Result<TraceV1ToV2Plan, PlanError>`, never panics,
and the writer resolves the plan once during startup (alongside its other
init calls) rather than on first use — a bad rule reference fails the
process before it accepts traffic, with a clear error, not mid-ingest.
`schema_transform` holds a `OnceLock` populated by that startup call;
`transform_trace_v1_to_v2` reads the already-built plan, never triggers
construction itself. This is the direct fix for the
`get_column_by_name`-per-field problem: the lookup happens once, at
startup, for the lifetime of the process, not once per batch.

### Extractors are named and registered per extraction rule, selected in Rust, not declared in `schemas.toml`

An extraction rule is identified by a short name (`verbatim:cast_uint64_to_int64`,
`derived:span_kind_str_from_number`, `verbatim:same_name`, ...) mapping to
a hand-written Rust function. Plan construction selects a field's rule the
same way `transform_trace_v1_to_v2`'s match arms do today — a Rust match
on the field name/shape, not a `schemas.toml`-declared rule reference (the
"provenance" metadata that would have driven this was dropped along with
the OTLP→wire migration; see Context). This keeps the plan's selection
logic exactly as inspectable as today's match arms, just evaluated once
per version pair instead of once per batch. Multiple fields with the same
shape of extraction (e.g. every plain UInt64→Int64 cast) share one generic
extractor parameterized by field name — genuinely new extraction shapes
still require a genuinely new hand-written rule. The _set_ of rules is small
and hand-written; what's generic is selecting and sequencing them.

### Golden tests compare the complete output batch, not a weaker subset

**[CodeRabbit review, PR #1230]** flagged that "same values, types,
nullability" doesn't rule out a plan that reorders fields, drops schema
metadata, or reorders rows while still passing a field-by-field value
check. The golden test asserts full `RecordBatch` equality against the
hand-written output: same `Schema` (field names, order, types, nullability,
metadata) and the same `Array` content column-for-column, row-for-row —
not a hand-picked subset of properties. This fixture is kept permanently
(not deleted once the migration lands) as a standing regression guard,
since the plan's field selection and ordering logic is exactly the kind of
thing a future rule addition could silently disturb.

## Risks / Trade-offs

- **[Risk]** A subtly wrong extraction rule selected for a field (e.g. a
  copy-paste mistake in the Rust selection match) goes unnoticed if rules
  aren't exhaustively validated → **Mitigation**: plan construction returns
  `Err` (never panics — see the startup-validation decision above) on a
  field with no matching rule, resolved eagerly at startup so a bad build
  fails before the process serves traffic; a test enumerates every field in
  the current traces schema version and asserts its rule resolves.
- **[Risk]** Benchmark-driven claims ("this will be faster") not landing
  as claimed if the plan abstraction itself introduces overhead (e.g.
  boxed closures with dynamic dispatch instead of monomorphized function
  pointers) → **Mitigation**: the explicit benchmark gate in `proposal.md`
  — this ships only once the new writer v1→v2-transform benchmark and the
  existing acceptor-decode benchmark confirm no regression, treated as a
  hard gate, not a nice-to-have. A boxed-closure `Fn(&RecordBatch) -> Result<ArrayRef>`
  still avoids the actual measured cost (string-keyed lookup per field per
  batch); dynamic dispatch overhead on a handful of calls per batch is not
  in the same order of magnitude as a `HashMap`/linear-scan lookup per
  field per batch.
- **[Trade-off]** Indirection through a plan/registry is harder to read at
  a call site than an inline match arm — a maintainer can no longer see
  "what happens to `span_kind`" by reading one function top to bottom, only
  by following the rule name to its registration → accepted: the plan's
  selection logic is still a single Rust match, read in one place
  (`build_trace_v1_to_v2_plan` or equivalent) rather than re-executed
  inline in the hot loop — the indirection is "moved once," not "hidden
  behind a second file format."

## Migration Plan

1. `unified-table-schema` is a prerequisite in name only now — this design
   needs nothing from it beyond the `SCHEMA_DEFINITIONS`/`ResolvedSchema`
   API that predates it.
2. Add the writer v1→v2-transform benchmark `performance-benchmarking-suite`
   didn't have; record a baseline on `main` before changing
   `schema_transform.rs`.
3. Build `MaterializationPlan`/extractor registry in `writer`; golden test
   against current hand-written output.
4. Migrate `schema_transform.rs`'s v1→v2 step; benchmark before deleting
   the old code — require no regression, expect measurable improvement.
5. No deployment-visible change; no rollback machinery beyond redeploying
   the previous binary, since output is byte-for-byte identical by golden
   test.

## Open Questions

None.
