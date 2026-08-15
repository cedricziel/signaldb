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

## 2. Golden references (prove zero behavior change before touching anything)

- [ ] 2.1 Add `#[cfg(test)]` reference constants capturing today's
      hand-written `create_trace_schema`/`create_log_schema`/
      `create_metric_schema`/`create_profile_schema`/
      `create_span_batch_schema` field lists (name, `DataType`, nullable).
- [ ] 2.2 Add a `#[cfg(test)]` reference capturing today's hand-registered
      `LogicalSchema::core()` fields per source (name, `LogicalType`,
      `Filterability`, `LogicalFieldKind`).

## 3. Extend `schemas.toml`'s field model

- [ ] 3.1 Failing test: a `FieldDefinition` with no `filterable` key
      resolves to `filterable = true`; one with `filterable = false`
      resolves accordingly.
- [ ] 3.2 Add `filterable: bool` (`#[serde(default = "true")]`) to
      `FieldDefinition`/`ResolvedField`. Test from 3.1 passes.
- [ ] 3.3 Mark `logs.body` (and any other current `.retrieval_only()`
      field) `filterable = false` in `schemas.toml`.
- [ ] 3.4 Failing test: `physical_type_to_logical_type` maps every
      `field_type` string currently used in `schemas.toml`
      (`string`/`int32`/`int64`/`uint64`/`double`/`boolean`/
      `timestamp_ns`/`date`) to the correct `LogicalType`, and panics or
      errors clearly on an unmapped type rather than guessing.
- [ ] 3.5 Implement `physical_type_to_logical_type`. Test from 3.4 passes.

## 4. Generate the Flight wire schema

- [ ] 4.1 Failing test: `create_trace_schema()`'s generated output equals
      the golden reference from 2.1, field-for-field.
- [ ] 4.2 Implement `create_trace_schema()` as
      `SCHEMA_DEFINITIONS.resolve_trace_schema("physical-v1")` → `Arrow::Schema`
      (excluding any `physical_only` field). Test from 4.1 passes.
- [ ] 4.3 Repeat 4.1/4.2 for `create_log_schema`, `create_metric_schema`,
      `create_profile_schema`, `create_span_batch_schema`.
- [ ] 4.4 Delete the now-dead hand-written field-list code and the golden
      reference constants from §2.1 (their job is done).

## 5. Generate `LogicalSchema::core()`'s physical-backed entries

- [ ] 5.1 Failing test: for each source (`logs`, `traces`, `metrics`,
      `metrics_histogram`), the generated `RecordMetadata`/`Attribute`
      entries equal the golden reference from 2.2.
- [ ] 5.2 Implement generation: for each non-`physical_only` field in a
      signal's current schema version, emit
      `LogicalField::record_metadata(source, name, physical_type_to_logical_type(...))`,
      applying `.retrieval_only()` when `filterable = false`. Test from 5.1
      passes.
- [ ] 5.3 Confirm synthetic/non-generated entries
      (`LogicalField::signaldb_resource_identity`, any `JoinKey` fields)
      remain hand-registered in `LogicalSchema::core()`, layered on top of
      the generated set.
- [ ] 5.4 Delete the now-dead hand-written physical-backed
      `LogicalField::record_metadata`/`attribute` calls and the golden
      reference constants from §2.2 (their job is done).

## 6. Consistency check (`table-schema-consistency` capability)

- [ ] 6.1 Failing test per signal (traces/logs/metrics): the signal's
      current resolved schema's non-computed field names equal a
      hand-maintained "fields this converter touches" set for that
      signal's conversion module.
- [ ] 6.2 Populate the "fields this converter touches" sets from the
      actual current state of `conversion_traces.rs`/`conversion_logs.rs`/
      `conversion_metrics.rs` (this should immediately surface today's
      `dropped_*_count` mismatch if it hasn't already been fixed by
      `iceberg-schema-evolution`'s tasks §5 — reconcile rather than
      duplicate).
- [ ] 6.3 Document in each conversion module (doc comment) that adding or
      removing a handled field requires updating its matching set in the
      same PR.

## 7. Docs and specs hygiene

- [ ] 7.1 Update the `flight-schemas` skill to describe wire schemas as
      generated from `schemas.toml` rather than hand-written.
- [ ] 7.2 Update the `crate-map`/`architecture` skills if they describe
      `flight/schema.rs` or `logical.rs` in a way this change affects.
- [ ] 7.3 Run `openspec validate --strict unified-table-schema` and fix any
      findings before archiving.
