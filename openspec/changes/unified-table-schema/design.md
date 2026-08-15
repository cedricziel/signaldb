## Context

See `proposal.md` for motivation. Grounding facts gathered while scoping
this change:

**The four representations today.**

1. `schemas.toml` → `SchemaDefinitions`/`ResolvedSchema`
   (`common::schema::schema_parser`): per-version `TableSchemaDefinition`
   with `inherits`/`field_renames`/`field_additions`, each field a
   `FieldDefinition { name, field_type, required, computed, physical_only }`.
   Drives `traces`, `logs`, `metrics_gauge`, `metrics_sum`,
   `metrics_histogram` today; `metrics_exponential_histogram`/
   `metrics_summary` are absent (shared gap with `iceberg-schema-evolution`).
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

- One load path (`schemas.toml`) producing the physical Iceberg schema,
  the Flight wire schema, and the `LogicalSchema::core()` registration for
  every field that has a real physical column, for every signal.
- A consistency check that fails before/at deployment when a declared
  field has no real conversion-code path, closing the exact failure mode
  that let `dropped_*_count` go silently wrong.
- Zero behavior change for any existing signal — enforced by golden tests,
  not asserted by inspection.

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

### Wire schema: generate from `physical-v1`, not a new TOML section

Rather than inventing a separate "wire schema" concept in `schemas.toml`,
each signal's Flight wire schema is `SCHEMA_DEFINITIONS.resolve_*_schema("physical-v1")`
converted to an `Arrow::Schema` (excluding any `physical_only` field, which
today is none for any v1 definition — the filter exists for correctness as
future versions could in principle mark a v1 field `physical_only`, not
because it does anything today). Alternative considered: a distinct
`wire-v1` schema block mirroring `physical-v1`. Rejected — `physical-v1`'s
own description already claims to be the wire-matching schema; a second
parallel block would just reintroduce the two-representations problem this
change exists to remove, with an even more confusing name.

### Logical schema: generate for physical fields, hand-register the rest

Add a `filterable: bool` (default `true`) field to `FieldDefinition`/
`ResolvedField`, and a pure function
`physical_type_to_logical_type(field_type: &str) -> LogicalType`
(`string`→`String`, `int32`/`int64`/`uint64`→`Int64`, `double`→`Float64`,
`boolean`→`Bool`, `timestamp_ns`→`TimestampNs`, `date`→handled as
`physical_only`/not client-visible today so excluded, same as now).
For each resolved field that isn't `physical_only`, emit
`LogicalField::record_metadata(source, name, mapped_type)`, marked
`.retrieval_only()` when `filterable = false`. Fields with
`kind = JoinKey` (rare — none exist yet outside test fixtures) or
`SignalDbDefined` synthetic fields with no physical column (`resource.identity`)
are not generated; they stay explicit `LogicalField::join_key(...)`/
`LogicalField::signaldb_resource_identity(...)` calls layered on top of the
generated set in `LogicalSchema::core()`. Attribute-level (`resource.`/
`scope.`/`record.`) resolution stays the existing structural mechanism in
`LogicalSchema::resolve` — it is not per-field generated, since attribute
keys are unbounded; `schemas.toml` only needs to mark which map columns
exist per source, which it already implies through the `map<string,string>`
field type.

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

### Golden tests, not manual review, prove zero behavior change

For each function converted from hand-written to generated
(`create_trace_schema`, etc., and `LogicalSchema::core()`'s generated
portion), a test asserts field-for-field equality (name, Arrow `DataType`,
nullability) between the old hand-written definition (kept temporarily as
a `#[cfg(test)]`-only reference constant) and the new generated output.
Once the tests pass and land, the hand-written reference constants are
deleted — they only exist transiently to prove the migration was
behavior-preserving.

## Risks / Trade-offs

- **[Risk]** The manually-maintained "fields this converter touches" set
  (for the consistency test) is itself hand-maintained and could drift the
  same way the four representations did → **Mitigation**: it's one flat set
  per conversion module, updated in the same PR as any field addition/
  removal by construction (task-level discipline, same as any other test
  update) — smaller and more local than keeping four _different_
  representations in sync across files.
- **[Risk]** Sequencing conflict with `iceberg-schema-evolution` — both
  change `schema_parser.rs`/`schemas.toml` and both need the
  `ExponentialHistogram`/`Summary` consolidation → **Mitigation**: proposal.md
  flags this explicitly; whichever change is implemented first should do
  the metrics consolidation once, and the other rebases on top rather than
  duplicating it.
- **[Trade-off]** `filterable`/type-mapping metadata added to
  `FieldDefinition` makes `schemas.toml` denser and more coupled to the
  query layer's concerns, not purely "physical schema" anymore →
  accepted: that coupling is the point — it's what makes drift structurally
  impossible instead of a matter of remembering to update a second file.

## Migration Plan

1. Land (or coordinate with) `iceberg-schema-evolution`'s
   `ExponentialHistogram`/`Summary` → `schemas.toml` consolidation once,
   shared by both changes.
2. `schema_parser.rs`: add `filterable`, the type-mapping function, golden
   reference constants for current hand-written schemas.
3. Generate wire schemas; golden tests pass; delete hand-written
   `flight/schema.rs` field lists.
4. Generate `LogicalSchema::core()`'s physical-backed entries; golden tests
   pass; delete the corresponding hand-written calls, keep the synthetic/
   join-key ones.
5. Add the per-signal consistency tests.
6. No deployment-visible change and no rollback machinery needed — this is
   a test-verified internal refactor, not a wire or storage format change.

## Open Questions

None.
