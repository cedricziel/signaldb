## Context

See `proposal.md` for motivation. Grounding facts gathered while scoping
this change:

**The four representations today.**

1. `schemas.toml` → `SchemaDefinitions`/`ResolvedSchema`
   (`common::schema::schema_parser`): per-version `TableSchemaDefinition`
   with `inherits`/`field_renames`/`field_additions`, each field a
   `FieldDefinition { name, field_type, required, computed, physical_only }`.
   Drives **`traces` and `logs`' real physical tables** today. It also has
   `metrics_gauge`/`metrics_sum`/`metrics_histogram` sections — but,
   confirmed while scoping this change, those are wired only to the admin
   schema-introspection endpoint (`router::endpoints::management`), not to
   the actual `iceberg::schemas::create_metrics_*_schema_with` functions
   that build those tables' real physical schema. `metrics_exponential_histogram`/
   `metrics_summary` have no `schemas.toml` section at all. So all five
   metrics representations, and profiles (no section either), are
   currently 100% hand-written — a bigger gap than originally scoped here.
2. `flight/schema.rs`: hand-written `Field` lists
   (`create_trace_schema`, `create_log_schema`, `create_metric_schema`,
   `create_profile_schema`, `create_span_batch_schema`) for the Arrow wire
   format used in OTLP→Flight conversion. `schemas.toml`'s own
   `physical-v1` description already says "matching OTLP Flight
   conversion" — v1 is conceptually the wire schema already, it just isn't
   wired up as the source for `flight/schema.rs`.
3. `LogicalSchema::core()` (`common::schema::logical`): hand-written
   `LogicalField::record_metadata(source, name, LogicalType)` /
   `LogicalField::attribute(source, level, name, LogicalType)` calls per
   source. `LogicalType` is a coarse 8-variant enum (`String`, `Bool`,
   `Int64`, `Float64`, `TimestampNs`, `DurationNs`, `Bytes`, `AnyValue`);
   `Filterability` is `Filterable`/`RetrievalOnly` (logs' `body` uses
   `.retrieval_only()`); `LogicalFieldKind` is `Attribute`/`RecordMetadata`/
   `JoinKey`/`SignalDbDefined`. Consumed by `querier::ir_planner` (query
   IR resolution) and exposed via `router::endpoints::management` into the
   generated SDK/UI clients — this is the layer every query surface shares.
4. Conversion code (`common::flight::conversion::conversion_traces`/
   `conversion_logs`/`conversion_metrics`): the actual OTel proto ↔ Arrow
   field-by-field logic. This layer's _logic_ (e.g. `span_kind_to_str`)
   cannot be generated — it encodes real OTel semantics — but _which
   columns it's expected to touch_ should be checked against (1)-(3).

**Existing hooks that make this tractable.** `FieldDefinition.physical_only`
already distinguishes "exists in Iceberg but not on the wire" (set for
computed fields like `traces.timestamp`/`date_day`/`hour`, and for
partition columns) — no v1 field is `physical_only`, meaning v1's field
list, minus nothing, is already exactly what the wire schema should
contain. `map<string,string>` fields (`attributes_json`/`resource_json`/
etc., renamed to `span_attributes`/`resource_attributes` at v2) back the
_dynamic_, per-key attribute resolution (`AttributeLevel::Resource/Scope/
Record` in `LogicalSchema::resolve`) rather than one named `LogicalField`
each — these stay a structural "this source has resource/scope/record
attribute levels" fact, not a per-key enumeration, since attribute keys are
unbounded.

## Goals / Non-Goals

**Goals:**

- `schemas.toml` as the one physical schema source of truth for every
  built-in table (traces, logs, all five metrics representations,
  profiles) — done in §1, already merged. (Originally also targeted
  generating the Flight wire schema and `LogicalSchema::core()` from it —
  both dropped, see Decisions below.)
- A consistency check that catches at test/CI time when a declared
  physical field has no real conversion-code path, closing the exact
  failure mode that let `dropped_*_count` go silently wrong.

**Non-Goals:**

- Generating the OTel semantic derivation logic itself (`span_kind_to_str`,
  proto field extraction, JSON serialization of events/links). This stays
  hand-written Rust; TOML declares that a column exists and its type, not
  the OTel-specific logic that populates it.
- Implementing operator-defined custom tables. This change removes the
  structural blocker (`dataset-table-provisioning`'s "no generic pipeline"
  problem) but does not add tenant-facing authoring, storage, or admin API
  for custom schemas — that is a follow-up change once this foundation
  exists.
- Solving Iceberg field-ID stability for live schema evolution — orthogonal
  and already owned by `iceberg-schema-evolution`; this change doesn't
  touch how an existing table's live schema is mutated, only how the
  _target_ definition is authored and where its non-Iceberg
  representations come from.
- A generic expression language for `computed` fields. `computed` stays a
  named-recipe tag (`"date_from_timestamp"` etc.) resolved by a fixed match
  in Rust, not an executable formula in TOML — the set of computed fields
  is small and changes rarely enough that a full expression language would
  be speculative complexity.

## Decisions

### Wire schema generation: dropped, `flight/schema.rs` stays hand-written

**[Found while implementing]** The plan to generate each signal's Flight
wire schema from `SCHEMA_DEFINITIONS.resolve_*_schema("physical-v1")` does
not hold up once actually compared field-by-field against today's
hand-written `flight/schema.rs`:

- **Traces**: the real wire format (`otlp_traces_to_arrow`) already writes
  `span_kind_number`/`status_code_number`/the three `dropped_*_count`
  columns, but those only exist in `schemas.toml` from `physical-v3`
  onward — `physical-v1` doesn't have them (they were added post-hoc by
  `iceberg-schema-evolution` as a physical-only evolution step, not a wire
  addition). Wire field names are also pre-rename (`name`,
  `duration_nano`, `attributes_json`) while `physical-v1`→`physical-v2`'s
  renames are exactly what turns those into `physical-v1`'s post-rename
  names — so even "just use physical-v1" doesn't reproduce the actual wire
  names once v1's own field list is inherited-and-renamed by v2. And the
  wire format uses a flat JSON-string type for attributes
  (`attributes_json: Utf8`) while `schemas.toml` already types that same
  v1 field `map<string,string>` (the physical/Iceberg representation) —
  one `field_type` string per field cannot mean two different Arrow types
  for the same field depending on which layer reads it.
- **Logs**: worse — wire field names and types don't correspond 1:1 to
  physical ones at all. `resource_schema_url`/`scope_name`/`scope_version`
  exist as physical columns but aren't separate wire columns; they're
  unpacked from the wire's `resource_json`/`scope_json` blobs by
  hand-written writer logic. That's real ETL, not a rename or type cast a
  declarative model can express.
- **Metrics**: structurally incompatible, not just diverged. The wire
  format is one polymorphic table (`metric_type` discriminator + generic
  `data_json` blob); the physical layer is five separate normalized tables
  (gauge/sum/histogram/exponential-histogram/summary). There is no 1:1
  schema for a generator to target — a single wire row can only become one
  of five different physical row shapes, decided by a value inside the
  blob.

Closing this gap for real would need genuine transform primitives in
`schemas.toml` (a way to declare "unpack this JSON field into these named
columns," "select physical table by this field's value," a name/type
override for the pre-materialization representation) — a materially larger
change than a rename list, and one this change's motivating bug
(`dropped_*_count` never reaching a physical column) doesn't actually need
solved to fix. `flight/schema.rs` stays hand-written; the wire-format
consistency problem is deferred to a future change if it becomes a real
recurring source of bugs the way the physical/logical split was.

### `LogicalSchema::core()` generation: dropped, stays hand-written

**[Found while implementing, after the wire-schema finding above]**
Generating `core()`'s physical-backed entries (`LogicalField::record_metadata(source, name, ...)`
keyed by each resolved field's own physical name) runs into the same class
of problem as the wire schema, just less severely: most of `core()`'s
current entries are query-ergonomics aliases that don't equal any real
physical column name at all — e.g. traces registers `"name"` and
`"span.name"` (physical column: `span_name`), `"duration"` and
`"duration_nano"` (physical: `duration_nanos`), `"status.code"` (physical:
`status_code`) — some using the pre-rename wire name, some a dotted
TraceQL-style alias, inconsistently per field. A generator keyed by
physical name would never match or replace any of these; it could only add
new, different-named entries alongside them. Checked against this change's
actual value proposition (closing the "declared field, no real path"
failure mode) that addition buys little the consistency check below
doesn't already cover directly, for real implementation cost (a
`filterable` field-model addition, a type-mapping function, and a
golden-equivalence test suite that can't actually assert equivalence with
the current hand-written set, since the two are keyed differently by
design). `LogicalSchema::core()` stays entirely hand-written; if it drifts
from the physical schema again, the consistency check below is what
catches it.

### Consistency check: a test-time reflection, not a runtime guard

"Every declared field has a conversion-code path" cannot be verified by the
running binary without also generating the conversion layer (a Non-Goal) —
there's no way to statically prove a `match` arm in `conversion_traces.rs`
handles a given column name without either parsing Rust or maintaining a
second manifest that itself could drift. Instead: a `#[test]` per signal,
run in CI like every other test, that resolves the signal's current schema
version and asserts (via a small manually-maintained "fields this converter
touches" set, one per conversion module, updated whenever a field is added
or removed) that the two sets are equal. This is deliberately a narrower,
weaker guarantee than a load-time check — it catches the mistake at PR/CI
time for anyone working in this codebase, which is where `dropped_*_count`
should have been caught, without pretending to give a compile-time or
production-runtime guarantee that Rust's type system can't actually back.

## Risks / Trade-offs

- **[Found during §1 implementation]** Generating metrics/profiles schemas
  through `ResolvedSchema::to_iceberg_schema_with_labels` orders label
  columns' field IDs _after_ map key/value IDs, whereas the old hand-written
  `append_materialized_label_fields`/`mapify_attr_fields` pair ordered them
  _before_ → accepted as harmless: Iceberg only requires field IDs be
  unique and stable once assigned, nothing reads meaning into their
  relative ordering, and this only affects the shape assigned to a table
  created after this change — no existing table's IDs are touched.
- **[Risk]** The manually-maintained "fields this converter touches" set
  (for the consistency test) is itself hand-maintained and could drift the
  same way the four representations did → **Mitigation**: it's one flat set
  per conversion module, updated in the same PR as any field addition/
  removal by construction (task-level discipline, same as any other test
  update) — smaller and more local than keeping four _different_
  representations in sync across files.
- **[Risk]** `iceberg-schema-evolution` (already merged/implemented) also
  touches `schema_parser.rs`/`schemas.toml` → **Mitigation**: that change's
  evolution engine is scoped to traces/logs only and does not depend on or
  duplicate the metrics/profiles consolidation this change owns; no
  coordination needed beyond a normal rebase.

## Migration Plan

1. Fold all five metrics representations and profiles into `schemas.toml`,
   matching the fields their current hand-written `iceberg::schemas`
   functions already produce; wire those functions to resolve from
   `SCHEMA_DEFINITIONS` like traces/logs already do. (Done — merged as
   #1237.)
2. Add a per-signal consistency test (traces/logs/metrics): the signal's
   current resolved schema's non-computed field names must equal a
   hand-maintained "fields this converter touches" set in that signal's
   conversion module.
3. No deployment-visible change and no rollback machinery needed — this is
   a test-only addition, not a wire or storage format change. (Wire-schema
   and `LogicalSchema::core()` generation, both originally planned as
   steps here, are dropped — see Decisions.)

## Open Questions

None.
