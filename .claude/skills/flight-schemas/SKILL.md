---
name: flight-schemas
description: SignalDB Flight schemas and schema versioning - Flight wire format vs physical-vN storage format vs the logical schema version, schema inheritance, write-time transformations, traces/logs/metrics table schemas, and Flight RPC methods per service. Use when working with Arrow schemas, OTLP conversion, schema transforms, or Iceberg table schemas.
user-invocable: false
sources:
  - schemas.toml
  - src/common/src/flight/schema.rs
  - src/writer/src/schema_transform.rs
  - src/common/src/schema/schema_parser.rs
  - src/common/src/iceberg/schemas.rs
  - src/common/src/schema/logical.rs
  - src/common/src/schema/typed_attributes.rs
  - src/common/src/schema/type_authority.rs
  - src/common/src/schema/type_authority/**
---

# SignalDB Flight Schemas & Schema Versioning

Read `schemas.toml` for the versioned field definitions (current physical
versions, inheritance, renames, additions, removals, computed/physical-only
fields) and `src/common/src/flight/schema.rs` for the actual Arrow wire
structs. Read `docs/architecture/flight-communication.md` for Flight RPC
methods per service, ticket grammar, the OTLP write/query flow, and the
"Field-Coverage Check".

Read `docs/architecture/storage-layout.md`'s "Schema Versioning and Evolution"
section for:

- the **three version axes** — Flight wire vs storage (`*_v1_to_*`
  transforms), the `physical-vN` chain, and the logical schema version — which
  must never be read as one another;
- the **logical schema** (`LogicalSchema::core()`): field identity and
  qualifier resolution, logical types, join keys, retrieval-only and
  `physical_only` fields, and the `VERSION` / `logical_schema_version` pin;
- the Flight-wire-vs-Iceberg field table, write-time transformation (traces/
  logs/profiles in `do_put`, metrics at commit), and the generic typed split at
  commit;
- per-signal table schemas, the typed attribute layout, the declared sort
  order, and live-table schema evolution (including why evolution diffs by
  field name rather than the positional IDs `to_iceberg_schema()` assigns).

Attribute type precedence and scoping: `docs/users/schema-registry.md#canonical-types`.
Adding a signal or table end to end: the `adding-new-signal` skill.

## Gotchas not fully covered by the docs

- `transform_trace_v1_to_v2()` is misnamed: it targets a hardcoded
  `"physical-v4"` literal, **not** `current_trace_version()` (`physical-v5`,
  the typed layout). Its only job is the wire shape → the last version with
  plain attribute columns; `create_arrow_schema_from_resolved` has no case for
  typed map/binary columns, so it can never target `physical-v5` directly.
  `transform_logs_v1_to_iceberg` pins logs' last pre-typed version the same
  way. Bump these literals deliberately, not alongside `current_*_version`.
- It is the only **compiled-plan-based** transform (`compiled-schema-materializer`):
  a `TraceV1ToV2Plan` of per-field extractor closures is resolved once
  (`warm_trace_v1_to_v2_plan()`, from `IcebergTableWriter::new`) instead of
  re-matched per batch. Plan construction returns `Err`, never panics, on an
  unmatched field. Logs/profiles/metrics transforms stay hand-written
  per-field, wire-to-physical directly.
- `transform_metrics_to_wide`/`transform_metric_exemplars` build columns
  positionally with no runtime field check, so the
  `writer::schema_transform::schema_consistency` test is their only guard
  against a physical field nobody writes.
