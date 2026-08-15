## 1. `schemas.toml` foundation

- [x] 1.1 Add `FieldRemoval { name: String }` to `schema_parser.rs`, a
      `field_removals: Vec<FieldRemoval>` field on `TableSchemaDefinition`,
      and apply it in `resolve_table_schema` (remove by name from
      `resolved_fields`, after renames/additions).
- [x] 1.2 Unit test: a version with `field_removals` resolves without the
      removed field, and a version that doesn't declare it is unaffected.
- [x] 1.3 Add a doc comment on `build_iceberg_schema` stating its field IDs
      are positional and MUST NOT be diffed against a live table's schema —
      only its name/type/required set may be used for that.

## 2. Scope correction found during implementation (superseded)

Implementation revealed the premise behind this section was wrong: **none**
of the five metrics tables (gauge/sum/histogram/exponential_histogram/
summary) or profiles are actually sourced from `schemas.toml` for physical
table creation — all are hand-written in `iceberg/schemas.rs`
(`create_metrics_*_schema_with`, `mapify_attr_fields`,
`append_materialized_label_fields`), which says so explicitly in its own
doc comment. `schemas.toml`'s `metrics_gauge`/`metrics_sum`/
`metrics_histogram` sections exist but are wired to nothing but the admin
schema-introspection endpoint (`router::endpoints::management`) — not to
real table creation. So "add the two missing metrics schemas to
`schemas.toml`" would not have made `schemas.toml` the source of truth for
metrics; it would have added a third disconnected representation.

Migrating all five metrics schemas (and profiles) onto `schemas.toml` is
real, larger work correctly scoped to `unified-table-schema` (which exists
specifically to unify schema representations) — moved there rather than
done partially here. **This change's evolution engine (§3-4) targets
traces and logs only**, since both already resolve their physical schema
from `schemas.toml` today; metrics/profiles gain evolution support once
`unified-table-schema` gives them a real `schemas.toml` definition to
evolve against. `table-schema-evolution`'s spec has been narrowed to match
(see the updated delta spec).

- [x] 2.1 Correct `iceberg-schema-evolution`'s proposal/design/spec to
      reflect the above; flag the wider metrics/profiles consolidation as
      `unified-table-schema`'s scope, not a shared prerequisite done here.

## 3. Schema evolution engine (`common::iceberg`)

**Scope correction found during implementation**: a production schema-evolution
mechanism already existed at `common::iceberg::evolution` (built for
attribute auto-promotion, epic #737/#734) — `add_label_columns`/
`remove_label_columns` using exactly the `AddSchema`+`SetCurrentSchema`
via `Catalog::update_table` pattern this section planned to build from
scratch, plus a `max_field_id` helper that correctly walks nested
struct/list/map field ids (which a naive top-level-only `.max()` would
have gotten wrong for any table with a `map<string,string>` attributes
column — every traces/logs table). It also uses commit-then-reload-verify
for concurrency safety rather than an explicit `TableRequirement`
assertion — the Iceberg catalog's own commit protocol already CAS's on
the base metadata version regardless of caller-supplied requirements, so
`AssertCurrentSchemaId` wasn't actually load-bearing for the "exactly one
concurrent writer wins" guarantee. Extended that module in place (new
`diff_schema`/`apply_schema_migration`/`ensure_schema_current` functions
alongside the existing label-column ones, sharing `max_field_id`/
`load_table`) instead of duplicating it.

- [x] 3.1 Failing test: given a live `Schema` and a target field set with
      one new field, the diff produces exactly one addition with field id
      equal to the given `next_id`, all other ids unchanged.
- [x] 3.2 Failing test: given a live `Schema` and a target field set missing
      one existing field, the diff produces exactly one removal by id, all
      other ids and the removed field's id itself unchanged in the
      remaining fields.
- [x] 3.3 Implement `diff_schema` in `common::iceberg::evolution`: additions
      get fresh trailing ids from a caller-supplied `next_id` (computed the
      same way `add_label_columns` does: past both `max_field_id` and the
      metadata's `last_column_id`), removals drop by id, everything else
      passes through untouched. Tests from 3.1/3.2 pass.
- [x] 3.4 Failing test: applying one version hop to a table commits
      `AddSchema` + `SetCurrentSchema` + `SetProperties(signaldb.schema.version
-> target)` in one commit, and after it the table's current schema
      and `signaldb.schema.version` property both reflect the target
      version.
- [x] 3.5 Implement `apply_schema_migration(catalog, identifier, target_fields,
target_version)` using `Catalog::update_table` directly (not
      `Transaction`, per design.md's `Transaction::add_schema` gap),
      mirroring `add_label_columns`'s commit-then-reload-verify pattern.
      Test from 3.4 passes.
- [x] 3.6 Failing test: two concurrent `apply_schema_migration` calls
      against the same table and target version result in the table at
      that version exactly once, with no duplicated column and no
      duplicate field ids (mirrors the existing
      `concurrent_double_call_leaves_consistent_schema` label-column test).
- [x] 3.7 (No separate retry path needed — the existing reload-and-verify
      pattern already surfaces a lost race as an error; the caller is a
      periodic reconciler that retries on its next pass, same as
      `backfill_metadata_pruning_properties`'s existing failure handling.)
      Test from 3.6 passes as-is.
- [x] 3.8 Failing test: a table two versions behind is brought forward one
      hop at a time (asserts both intermediate columns land and the schema
      count reflects two distinct hop commits, not one combined jump).
- [x] 3.9 Implement `ensure_schema_current`: read `signaldb.schema.version`.
      When absent, or when recorded but not actually found while walking
      `version_chain` back from the current version (an unrecognized or
      stale property), migrate directly to the current version in one
      step, additions only, never removing -- the live schema's
      relationship to an inferred baseline isn't trusted. When present and
      found on the chain, loop calling `apply_schema_migration` for each
      hop in order, renames and removals enabled, stopping and returning
      the error on a failed hop without attempting the next one. Test from
      3.8 passes.

## 4. Wire evolution into table lifecycle

- [x] 4.1 Failing test: `TableManager::ensure_table` on an existing table
      that is behind the current schema version evolves it (reuses the
      3.x test harness against `ensure_table`'s existing-table branch).
- [x] 4.2 Call `ensure_schema_evolved` (wraps `evolution::ensure_schema_current`,
      scoped to traces/logs) from `ensure_table`'s existing-table branch,
      next to `backfill_metadata_pruning_properties`; reload the table
      afterward to pick up the committed change. Test from 4.1 passes.
- [x] 4.3 Failing test: a table with no `signaldb.schema.version` property
      (predates this change) evolves forward correctly rather than
      erroring — **and, per the §3 scope correction, does so via a single
      additions-only migration straight to current, never removal**, since
      an inferred baseline can't safely support hop-by-hop removal (see
      `ensure_schema_current_with_no_recorded_version_never_removes_extra_fields`).
- [x] 4.4 Implement the missing-property default, and stamp
      `signaldb.schema.version` at table _creation_ too (a freshly created
      table already IS the current version — recording it immediately
      means `ensure_schema_evolved` never treats a brand-new table as
      pre-dating the mechanism). Test from 4.3 passes.
- [x] 4.5 Resolved without a new `tests-integration` test:
      `table_manager.rs`'s `ensure_table_evolves_an_existing_table_behind_the_current_version`
      and `reconcile_existing_table_evolves_a_table_handed_back_after_a_lost_create_race`
      already exercise exactly this (a table created under an older schema
      shape via a real in-memory Iceberg catalog, reconciled via the real
      `ensure_table` path, new columns present afterward) — the only gap a
      `tests-integration`-crate duplicate would close is running through
      the full acceptor/writer/Flight stack instead of `IcebergTableManager`
      directly, which the evolution logic itself doesn't touch. Not worth
      the added Docker-free-harness weight for the same assertion.

## 5. Trace OTel-parity fixes (first real use of the mechanism)

- [x] 5.1 Bump `traces.physical-v3` in `schemas.toml`: add
      `span_kind_number` (int32, nullable), `status_code_number` (int32,
      nullable), `dropped_attributes_count`/`dropped_events_count`/
      `dropped_links_count` (int64, nullable). **Resolved**: the three
      dropped-count fields did _not_ already exist physically at v1 —
      only `LogicalSchema::core()` registered them (the exact bug this
      change closes). All five are new `physical-v3` `field_additions`
      inheriting from `physical-v2`.
- [x] 5.2 Failing test: `otlp_traces_to_arrow` on a span with a given
      OTel `kind` produces a `span_kind_number` column equal to the raw
      proto int, and a `span_kind` string still equal to
      `span_kind_to_str(kind)`.
- [x] 5.3 Failing test: same for `status.code` → `status_code_number` /
      `status_code` (`extract_status_preserves_the_original_numeric_code`;
      `extract_status` now returns `(i32, String, String)`, string always
      derived from the number via the new `status_code_to_str`).
- [x] 5.4 Failing test: `otlp_traces_to_arrow` on a span with nonzero
      `dropped_attributes_count`/`dropped_events_count`/`dropped_links_count`
      produces columns with those exact values, not zero
      (`otlp_traces_to_arrow_preserves_nonzero_dropped_counts`).
- [x] 5.5 Implement the three read-side fixes in `conversion_traces.rs`
      (add the two number columns and derive the strings from them; read
      the three dropped-count fields instead of hardcoding). Tests 5.2-5.4
      pass.
- [x] 5.6 Failing test: `arrow_to_otlp_traces` prefers `span_kind_number`/
      `status_code_number` when present, falling back to
      `span_kind_from_str`/the string status match only when the number
      column is absent entirely (`test_arrow_to_otlp_traces` now builds a
      batch with the numeric columns omitted, simulating pre-#1208 data).
- [x] 5.7 Implement the write-side (OTLP-export) fallback logic. Test 5.6
      passes.
- [x] 5.8 Add `span_kind_number`/`status_code_number` (+ the three
      dropped-count columns) to `create_trace_schema()` (`flight/schema.rs`).
      **Not done**: `create_span_batch_schema()` (the single-trace/waterfall
      Flight response schema in `querier::flight`/`router::tempo`) was left
      untouched — extending it needs the querier's trace-lookup query to
      also select the new columns, a separate surface from the ingest path
      this task covers; tracked as a gap, not silently claimed done.
- [x] 5.9 Add the five new columns to the writer's v1→v2 direct-mapping
      match arm (`schema_transform.rs`), and bump its resolve target from
      `"physical-v2"` to `"physical-v3"` (a hardcoded literal, matching
      existing style — it wasn't dynamic before this change either).
- [x] 5.10 Register `span_kind_number`/`status_code_number` in
      `LogicalSchema::core()` for the `traces` source (the three
      `dropped_*_count` fields were already registered — that mismatch
      against physical reality was this change's motivating bug).

## 6. Cross-surface verification

No new HTTP endpoints or SDK-visible types are introduced by this change —
the new columns become resolvable through the existing `LogicalSchema`
registry, so verify the existing surfaces pick them up rather than adding
new ones.

- [x] 6.1 Added `traces_ir_query_resolves_numeric_span_kind_status_and_dropped_counts`
      (`tests-integration/tests/query_ir_e2e.rs`): ingests a span with
      `kind=Server`/`status.code=Error`/nonzero dropped counts through the
      real gRPC handler, queries `span_kind_number`/`status_code_number`/
      all three `dropped_*_count` fields via `POST /api/v1/query`, and
      asserts the raw OTel ints/counts come back, not zero/null.
- [x] 6.2 Added `traces_ir_query_string_span_kind_and_status_still_resolve_post_1208`:
      the same span, filtered by the pre-existing string fields
      (`span_kind = "Server"`, `status.code = "Error"`) — confirms the
      convenience strings still resolve correctly now that they're derived
      from the numeric columns rather than being independently
      read/written.
- [x] 6.3 Covered by 6.1 above (same test asserts nonzero
      `dropped_attributes_count`/`dropped_events_count`/`dropped_links_count`
      come back as real ingested values, not the always-empty/zero result
      the proposal's Why describes) — a separate test would have
      duplicated the same ingest-and-query path for no added coverage.

## 7. Docs and specs hygiene

- [x] 7.1 `docs/operations/table-provisioning.md` already describes
      `ensure_table` evolving an existing traces/logs table's schema
      (added while implementing this change's PR).
- [x] 7.2 `storage-layout` skill already has a dedicated schema-evolution
      section describing the mechanism (added while implementing this
      change's PR, refined further while implementing `unified-table-schema`).
- [x] 7.3 `openspec validate --strict iceberg-schema-evolution` passes.
