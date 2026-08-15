## Why

"The schema" for a signal exists in four independently hand-maintained
places today: `schemas.toml` (Iceberg physical layout), `flight/schema.rs`
(Arrow wire format for OTLP→Flight conversion), `LogicalSchema::core()`
(the query/registry layer every surface — TraceQL, query IR, MCP, HTTP —
resolves fields through), and the OTLP conversion code's own field-by-field
handling. Nothing keeps these in sync by construction, only by review
discipline, and that discipline has already failed twice: traces'
`dropped_attributes_count`/`dropped_events_count`/`dropped_links_count` are
registered as queryable in `LogicalSchema::core()` but were never given a
physical column or a conversion-code read path, so filtering on them
silently always returns false; `ExponentialHistogram`/`Summary` metrics
have real hand-written Iceberg schemas that never made it into
`schemas.toml` at all. Collapsing physical, wire, and registry generation
onto one load path removes this class of bug structurally instead of
relying on catching it in review. It is also the prerequisite for ever
letting an operator define a custom table: `dataset-table-provisioning`
currently states custom tables "SHALL NOT be provisioned" because there is
no generic pipeline a tenant-supplied definition could be resolved through
— only hand-registered built-in signal types exist.

## What Changes

- Extend `schemas.toml`'s field model with the metadata the query/registry
  layer needs per field (filterability, defaulting to filterable; explicit
  opt-out for retrieval-only fields like logs' `body`) so a field's
  `LogicalField` registration is fully derivable from its `schemas.toml`
  declaration.
- Generate each signal's Flight wire schema
  (`create_trace_schema`/`create_log_schema`/`create_metric_schema`/
  `create_profile_schema`/`create_span_batch_schema`) from that signal's
  `physical-v1` resolution instead of a hand-written field list.
  `physical-v1` is already documented as "matching OTLP Flight conversion"
  and its fields are already non-`physical_only`, so this closes a gap that
  already exists structurally rather than inventing new plumbing.
- Generate `LogicalSchema::core()`'s per-source `RecordMetadata`/
  `Attribute` entries from `schemas.toml` for every field with a physical
  column, via an explicit physical-type → `LogicalType` mapping. Fields with
  no physical backing at all (e.g. the synthetic `resource.identity`
  `SignalDbDefined` field) remain hand-registered, since they are not part
  of any table's schema.
- Fold `ExponentialHistogram`/`Summary` metrics into `schemas.toml` (shared
  prerequisite with `iceberg-schema-evolution`; do once, consumed by both).
- Add a load-time/test-level consistency check: every non-computed field a
  signal's resolved schema declares SHALL have a corresponding read and
  write path in that signal's conversion code, so a field declared but
  never populated (this change's motivating bug) fails loudly instead of
  silently.
- **Not implemented in this change**: operator-defined custom tables
  themselves. This change only removes the structural blocker; authoring a
  custom table is a follow-up.

## Capabilities

### New Capabilities

- `table-schema-consistency`: guarantees that a signal's declared schema
  (physical columns, wire format, and query-registry entries) and its
  actual conversion-code behavior cannot silently disagree — a mismatch is
  a startup/validation failure, not silent data loss.

### Modified Capabilities

(none — existing signals' physical columns, wire formats, and query
results are unchanged; this change alters how those are generated, not
what they are. Enforced by task-level golden tests asserting the generated
schemas are byte-for-byte identical to today's hand-written ones.)

## Impact

- **common**: `schema/schema_parser.rs` (new per-field metadata,
  physical-type → `LogicalType` mapping), `schema/logical.rs` (`core()`
  becomes generated for physical-backed fields, hand-registration only for
  synthetic ones), `flight/schema.rs` (wire schema functions become thin
  wrappers over `SCHEMA_DEFINITIONS` resolution), `iceberg/schemas.rs`
  (`ExponentialHistogram`/`Summary` resolve from `schemas.toml`, shared
  work with `iceberg-schema-evolution`).
- **querier**: none functionally — `ir_planner.rs` keeps resolving through
  `LogicalSchema`, which now has a generated (not hand-written) source.
- **router**: `endpoints/management.rs`'s schema-introspection endpoint is
  unaffected in output, only in how the data backing it is produced.
- Depends on / shares scope with the already-proposed
  `iceberg-schema-evolution` change (both touch `schemas.toml` and
  `schema_parser.rs`); the metrics-schema consolidation task should land
  once, not twice — coordinate ordering during `/opsx:apply` rather than
  duplicate it.
- Not **BREAKING**: no wire format, Iceberg column, or query result
  changes for any existing signal — this is an internal source-of-truth
  consolidation, verified by golden-output tests.
