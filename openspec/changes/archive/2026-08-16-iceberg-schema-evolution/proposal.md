## Why

Traces store `span_kind` and `status_code` as strings derived once from OTel's
numeric enums (`Span.kind`, `Status.code`) and then discard the number —
exactly the gap that let a `span_kind` off-by-one ship silently and become
unrecoverable once written (#1208). The same ingest path also drops
`dropped_attributes_count`/`dropped_events_count`/`dropped_links_count`
entirely while `LogicalSchema::core()` still advertises them as queryable
fields, so a filter against them silently always returns false instead of
erroring. Fixing any of this only reaches tables created _after_ the fix
ships: `TableManager::ensure_table` has no path today to evolve a table that
already exists, so every pre-existing tenant/dataset table would keep
whatever schema it was born with forever. We need the evolution mechanism and
the OTel-parity fixes together, or the fixes are dead on arrival for live
deployments.

## What Changes

- Add `span_kind_number` (int32) and `status_code_number` (int32) to the
  traces schema as the numeric OTel source of truth, written directly from
  `Span.kind`/`Status.code` with no string round trip. `span_kind`/
  `status_code` (string) remain as derived query-ergonomics columns, computed
  at write time from the number, never the reverse.
- Fix `otlp_traces_to_arrow`/`arrow_to_otlp_traces` to actually read and
  write `dropped_attributes_count`/`dropped_events_count`/
  `dropped_links_count` instead of hardcoding `0` on read, closing the
  logical/physical mismatch against `LogicalSchema::core()`.
- New `schemas.toml` construct, `field_removals`, alongside the existing
  `field_renames`/`field_additions`, so a schema version can drop a column,
  not just rename or add one.
- New schema-evolution mechanism: on every `ensure_table` load of an
  existing table, compare its `signaldb.schema.version` table property
  against the signal's current `schemas.toml` version and, if behind, walk
  the version chain forward one hop at a time, committing each hop's
  additive/removal diff to Iceberg as a single atomic, field-ID-preserving
  schema update. No backfill of historical data — old rows keep whatever
  values (or absence of values) they already have; #1209 (the companion
  backfill issue) is closed and out of scope.
- **Scope correction found during implementation**: all five metrics
  tables and profiles are hand-written in `iceberg/schemas.rs` with no
  `schemas.toml` backing at all for physical creation (`schemas.toml`'s
  `metrics_gauge`/`metrics_sum`/`metrics_histogram` sections are wired only
  to admin introspection, not real tables) — not just
  `ExponentialHistogram`/`Summary` as originally scoped. Migrating all five
  onto `schemas.toml` is real, separate work, correctly owned by
  `unified-table-schema` rather than done partially here. **This change's
  evolution mechanism covers traces and logs only** — both already resolve
  their physical schema from `schemas.toml` today; metrics/profiles gain
  evolution support once that migration lands.

## Capabilities

### New Capabilities

- `table-schema-evolution`: versions each signal table against its
  `schemas.toml` definition via a `signaldb.schema.version` table property,
  and additively evolves existing Iceberg tables (add/remove nullable
  columns, one version hop at a time) so schema changes reach tables that
  already exist, not just newly-created ones.

### Modified Capabilities

- `otlp-traces-ingestion`: strengthens the "Span data preservation"
  requirement — OTel's numeric `kind` and `status.code` SHALL be preserved
  as their original integer value (not only a derived string), and
  `dropped_attributes_count`/`dropped_events_count`/`dropped_links_count`
  SHALL be preserved rather than discarded.
- `dataset-table-provisioning`: relaxes "Provisioning SHALL NOT alter the
  schema of a table that already exists" to permit additive-only schema
  evolution (add or remove a nullable column per an explicit migration) when
  a table's tracked schema version is behind the tenant's configured
  definition; provisioning still never rewrites or reinterprets existing
  data.

## Impact

- **common**: `schema/schema_parser.rs` (new `FieldRemoval`), `schema/mod.rs`
  (`schemas.toml` additions for traces), `schema/logical.rs` (register the
  two `_number` fields), `flight/schema.rs` (wire schema columns),
  `flight/conversion/conversion_traces.rs` (stop deriving-only, read/write
  the numbers and the dropped-counts), `iceberg/table_manager.rs` (new
  evolution path in `ensure_table`, scoped to traces/logs — see the scope
  correction above; `iceberg/schemas.rs`'s hand-written metrics/profiles
  schemas are untouched by this change).
- **writer**: `schema_transform.rs` (v1→v2 mapping for the new columns),
  `reconcile.rs` (schema evolution runs on the existing reconcile pass, no
  new job).
- **querier**: none functionally — the new columns become resolvable through
  the existing `LogicalSchema` registry, so TraceQL/query-IR/MCP/HTTP pick
  them up without per-surface changes.
- Not **BREAKING**: every schema change here is additive (new nullable
  columns) or a same-shape internal derivation fix; no existing column,
  wire format, or query surface is renamed or removed.
