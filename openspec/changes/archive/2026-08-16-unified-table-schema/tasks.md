## 1. Prerequisite: full metrics + profiles coverage in `schemas.toml`

**Scope correction found while implementing `iceberg-schema-evolution`**:
this is bigger than originally scoped — not just `metrics_exponential_histogram`/
`metrics_summary`. None of the five metrics representations (gauge, sum,
histogram, exponential histogram, summary) or profiles are actually
sourced from `schemas.toml` for physical table creation; `schemas.toml`'s
`metrics_gauge`/`metrics_sum`/`metrics_histogram` sections are wired only
to admin introspection. `iceberg-schema-evolution` found this and deferred
the entire consolidation here rather than doing it partially — this
change owns all of it, not just a shared two-schema prerequisite.

- [x] 1.1 Failing test: resolving each of `metrics_gauge`/`metrics_sum`/
      `metrics_histogram`/`metrics_exponential_histogram`/`metrics_summary`/
      `profiles` from `SCHEMA_DEFINITIONS` produces the same field set
      (name, type, nullability) as the corresponding
      `iceberg::schemas::create_*_schema_with` function produces today.
      **Confirmed the drift this task suspected**: `metrics_gauge`/
      `metrics_sum`/`metrics_histogram`'s existing sections typed their
      three attribute fields as plain `string`, but the real hand-written
      functions convert them to `Map<String,String>` via `mapify_attr_fields`
      afterward — using the TOML declarations verbatim would have been a
      silent behavior change, not a safe swap.
- [x] 1.2 Added `schemas.toml` sections for `metrics_exponential_histogram`,
      `metrics_summary`, and `profiles` (new); corrected the three existing
      metrics sections' attribute fields to `map<string,string>`.
- [x] 1.3 Changed all six `create_*_schema_with` functions in
      `iceberg::schemas` to resolve from `SCHEMA_DEFINITIONS` via
      `ResolvedSchema::to_iceberg_schema_with_labels`, which already
      handles both the map-typing and materialized-label-column injection
      `mapify_attr_fields`/`append_materialized_label_fields` used to do by
      hand — those two functions (and the `required_field`/`optional_field`
      helpers only they used) are now dead code, deleted. Test from 1.1
      passes for all six (name/type/nullability parity); a new test
      (`metrics_and_profiles_schemas_inject_labels_now_that_theyre_schemas_toml_sourced`)
      confirms label injection still works, noting one harmless divergence:
      label columns for metrics/profiles now get field IDs _after_ map
      key/value IDs (matching how `ResolvedSchema` already orders traces/
      logs) instead of _before_ (the old hand-written order) — ID
      uniqueness/stability holds either way, nothing depends on the exact
      numbering, and this only affects tables created from this point
      forward, never an existing one.

## 2. Consistency check (`table-schema-consistency` capability)

**Second scope correction found while implementing**: generating
`LogicalSchema::core()`'s physical-backed entries (formerly groups 2-4)
turned out to hit the same class of problem as the wire schema did, just
less severely — see `design.md`'s "`LogicalSchema::core()` generation:
dropped" decision. Most of `core()`'s entries are query-ergonomics aliases
(`name`/`span.name`/`duration`/`duration_nano`/`status.code`) that don't
match any real physical column name at all, so a generator keyed by
physical name would only ever ADD parallel entries, never actually replace
or de-risk the hand-written ones, for comparatively little safety gain
over what the consistency check below already provides on its own. This
change now does only the consistency check, which directly targets the
proposal's motivating bug (a field declared in `LogicalSchema::core()`
with no real physical/conversion-code path) without requiring a schema
generator at all.

**Correction on where the check lives**: the "fields this converter
touches" logic named `conversion_traces.rs`/`conversion_logs.rs`/
`conversion_metrics.rs` (`common::flight::conversion`), but those modules
only handle the Flight wire format. The actual physical-column population
happens one layer further in, in `writer::schema_transform`'s
`transform_*_v1_to_v2`/`transform_*_v1_to_iceberg` functions — that's
where the check needs to live to catch a real gap. Investigating them
found `transform_trace_v1_to_v2`/`transform_logs_v1_to_iceberg`/
`transform_profiles_v1_to_iceberg` already self-check at runtime (each
iterates its own resolved-schema field list with an exhaustive match that
errors on an unhandled field), but the five `transform_metrics_*_v1_to_iceberg`
functions build columns positionally against their own hand-written
`create_metrics_*_arrow_schema()`, entirely independent of
`SCHEMA_DEFINITIONS` — no runtime check at all. Implemented tests for all
eight tables (traces, logs, profiles, and all five metrics
representations) rather than just traces/logs/metrics, since the
mechanism is identical and profiles had the exact same unguarded gap as
metrics.

- [x] 2.1 Added `writer::schema_transform::schema_consistency`, a test
      module with one test per table (traces, logs, profiles,
      metrics_gauge/sum/histogram/exponential_histogram/summary) asserting
      the table's current resolved schema's non-computed field names
      exactly equal a hand-maintained "fields this transform touches" set.
      All eight pass today (no existing drift).
- [x] 2.2 Populated each hand-maintained set from the actual current state
      of the corresponding `transform_*` function (read directly off its
      match arms for traces/logs/profiles, and off its
      `create_metrics_*_arrow_schema()` for the five metrics tables, which
      each transform's column-building code follows positionally).
      `dropped_*_count` was already fixed by `iceberg-schema-evolution`
      before this task ran, so no mismatch to reconcile.
- [x] 2.3 Documented above `schema_consistency` (doc comment on the module)
      that adding or removing a handled field requires updating its
      table's set in the same PR, and why traces/logs/profiles already
      self-check at runtime while metrics doesn't.

## 3. Docs and specs hygiene

- [x] 3.1 Updated `flight-schemas` skill: removed two stale references to
      deleted functions (`mapify_attr_fields`, `append_materialized_label_fields`,
      both dead since #1237/#1235), and added a note on the new
      `schema_consistency` test module and why it matters for the five
      metrics tables specifically. Updated `storage-layout` skill's
      evolution section, which still said metrics/profiles were
      hand-written in `iceberg_schemas.rs` (stale since #1237 — they
      resolve from `schemas.toml` now, just aren't wired into the
      evolution mechanism).
- [x] 3.2 `openspec validate --strict unified-table-schema` passes.
