## 1. `schemas.toml` foundation

- [ ] 1.1 Add `FieldRemoval { name: String }` to `schema_parser.rs`, a
      `field_removals: Vec<FieldRemoval>` field on `TableSchemaDefinition`,
      and apply it in `resolve_table_schema` (remove by name from
      `resolved_fields`, after renames/additions).
- [ ] 1.2 Unit test: a version with `field_removals` resolves without the
      removed field, and a version that doesn't declare it is unaffected.
- [ ] 1.3 Add a doc comment on `build_iceberg_schema` stating its field IDs
      are positional and MUST NOT be diffed against a live table's schema —
      only its name/type/required set may be used for that.

## 2. Metrics schema consolidation (prerequisite for evolution coverage)

- [ ] 2.1 Failing test: resolving `metrics_exponential_histogram`/
      `metrics_summary` from `SCHEMA_DEFINITIONS` produces the same field
      set as `iceberg::schemas::create_metrics_exponential_histogram_schema_with`/
      `create_metrics_summary_schema_with` produce today.
- [ ] 2.2 Add `metrics.exponential_histogram-v1` and `metrics.summary-v1`
      blocks to `schemas.toml` matching the existing hand-written fields.
- [ ] 2.3 Change `create_metrics_exponential_histogram_schema_with`/
      `create_metrics_summary_schema_with` to resolve from
      `SCHEMA_DEFINITIONS` like the gauge/sum/histogram schemas already do;
      test from 2.1 passes.

## 3. Schema evolution engine (`common::iceberg`)

- [ ] 3.1 Failing test: given a live `Schema` and a target field set with
      one new field, the diff produces exactly one addition with field id
      `highest_field_id() + 1`, all other ids unchanged.
- [ ] 3.2 Failing test: given a live `Schema` and a target field set missing
      one existing field, the diff produces exactly one removal by id, all
      other ids and the removed field's id itself unchanged in the
      remaining fields.
- [ ] 3.3 Implement the diff function (`common::iceberg::table_manager` or a
      new `schema_evolution` submodule): additions get fresh trailing ids,
      removals drop by id, everything else passes through untouched. Tests
      from 3.1/3.2 pass.
- [ ] 3.4 Failing test: applying one version hop to a table builds a
      `CommitTable` with `requirements: [AssertCurrentSchemaId(current)]`
      and `updates: [AddSchema, SetCurrentSchema(-1), SetProperties(
    signaldb.schema.version -> target)]`, and after commit the table's
      current schema and `signaldb.schema.version` property both reflect
      the target version.
- [ ] 3.5 Implement `apply_schema_migration(catalog, table, target_version)`
      using `Catalog::update_table` directly (not `Transaction`, per
      design.md's `Transaction::add_schema` gap). Test from 3.4 passes.
- [ ] 3.6 Failing test: two concurrent `apply_schema_migration` calls
      against the same table and target version result in the table at
      that version exactly once (one commit succeeds, the other's
      `AssertCurrentSchemaId` fails and it reloads-and-no-ops since the
      table is already at target).
- [ ] 3.7 Implement the reload-and-recheck retry path for the losing
      commit. Test from 3.6 passes.
- [ ] 3.8 Failing test: a table two versions behind is brought forward one
      hop at a time (assert an intermediate commit lands at version N+1
      before N+2 is attempted).
- [ ] 3.9 Implement the multi-hop walk (`ensure_schema_current`: read
      `signaldb.schema.version` or default to the oldest known version,
      loop calling `apply_schema_migration` for each hop up to the
      signal's current version, stopping and returning the error on a
      failed hop without attempting the next one). Test from 3.8 passes.

## 4. Wire evolution into table lifecycle

- [ ] 4.1 Failing test: `TableManager::ensure_table` on an existing table
      that is behind the current schema version evolves it (reuses the
      3.x test harness against `ensure_table`'s existing-table branch).
- [ ] 4.2 Call `ensure_schema_current` from `ensure_table`'s existing-table
      branch, next to `backfill_metadata_pruning_properties`. Test from 4.1
      passes.
- [ ] 4.3 Failing test: a table with no `signaldb.schema.version` property
      (predates this change) is treated as the oldest known version for
      its signal and evolves forward correctly, not treated as an error.
- [ ] 4.4 Implement the missing-property default. Test from 4.3 passes.
- [ ] 4.5 Integration test (`tests-integration`): a traces table created
      under an older in-test schema version, then reconciled after the
      current version advances, ends up with the new columns and unchanged
      existing row data.

## 5. Trace OTel-parity fixes (first real use of the mechanism)

- [ ] 5.1 Bump `traces.physical-v3` in `schemas.toml`: add
      `span_kind_number` (int32, nullable), `status_code_number` (int32,
      nullable), `dropped_attributes_count`/`dropped_events_count`/
      `dropped_links_count` (int64, nullable) — the latter three already
      exist as physical-v1 fields per `LogicalSchema::core()`'s
      expectations; confirm whether they need only physical addition or
      also appear in `field_additions` at v3 depending on what v1/v2
      already declare, and reconcile schemas.toml accordingly (see design.md
      §"span_kind_number / status_code_number / dropped_*_count").
- [ ] 5.2 Failing test: `otlp_traces_to_arrow` on a span with a given
      OTel `kind` produces a `span_kind_number` column equal to the raw
      proto int, and a `span_kind` string still equal to
      `span_kind_to_str(kind)`.
- [ ] 5.3 Failing test: same for `status.code` → `status_code_number` /
      `status_code`.
- [ ] 5.4 Failing test: `otlp_traces_to_arrow` on a span with nonzero
      `dropped_attributes_count`/`dropped_events_count`/`dropped_links_count`
      produces columns with those exact values, not zero.
- [ ] 5.5 Implement the three read-side fixes in `conversion_traces.rs`
      (add the two number columns and derive the strings from them; read
      the three dropped-count fields instead of hardcoding). Tests 5.2-5.4
      pass.
- [ ] 5.6 Failing test: `arrow_to_otlp_traces` prefers `span_kind_number`/
      `status_code_number` when present, falling back to
      `span_kind_from_str`/the string status match only when the number
      column is null (pre-migration historical rows).
- [ ] 5.7 Implement the write-side (OTLP-export) fallback logic. Test 5.6
      passes.
- [ ] 5.8 Add `span_kind_number`/`status_code_number` columns to
      `create_trace_schema()`/`create_span_batch_schema()`
      (`flight/schema.rs`).
- [ ] 5.9 Add the four new columns to the writer's v1→v2 direct-mapping
      match arm (`schema_transform.rs`).
- [ ] 5.10 Register `span_kind_number`/`status_code_number` in
      `LogicalSchema::core()` for the `traces` source (the three
      `dropped_*_count` fields are already registered).

## 6. Cross-surface verification

No new HTTP endpoints or SDK-visible types are introduced by this change —
the new columns become resolvable through the existing `LogicalSchema`
registry, so verify the existing surfaces pick them up rather than adding
new ones.

- [ ] 6.1 Query-IR test: a query filtering/selecting `span_kind_number` (or
      `status_code_number`) resolves and executes against a traces table.
- [ ] 6.2 TraceQL test: an existing `kind=Server`-style query still resolves
      correctly post-change (string convenience path unaffected).
- [ ] 6.3 Integration test confirming `dropped_events_count > 0` (etc.) now
      returns real spans instead of always empty, closing the
      logical/physical mismatch from the proposal's Why.

## 7. Docs and specs hygiene

- [ ] 7.1 Update `docs/operations/table-provisioning.md` (per CLAUDE.md's
      existing reference to it) to describe that provisioning now also
      evolves an existing table's schema, not just creates missing tables.
- [ ] 7.2 Update the `storage-layout` skill if it describes table lifecycle
      in a way this change affects.
- [ ] 7.3 Run `openspec validate --strict iceberg-schema-evolution` and fix
      any findings before archiving.
