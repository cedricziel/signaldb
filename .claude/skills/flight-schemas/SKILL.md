---
name: flight-schemas
description: SignalDB Flight schemas and schema versioning - v1 wire format vs v2 storage format, schema inheritance, write-time transformations, traces/logs/metrics table schemas, and Flight RPC methods per service. Use when working with Arrow schemas, OTLP conversion, schema transforms, or Iceberg table schemas.
user-invocable: false
---

# SignalDB Flight Schemas & Schema Versioning

Read `schemas.toml` for the versioned field definitions (current physical
versions, inheritance, renames, additions, removals, computed/physical-only
fields) and `src/common/src/flight/schema.rs` for the actual Arrow wire
structs. Read `docs/architecture/flight-communication.md` for Flight RPC
methods/ticket grammar and the OTLP write/query flow. Read
`docs/architecture/storage-layout.md`'s "Schema Definition System" through
"An existing table's schema tracks..." sections for schema resolution order,
the Flight-v1-vs-Iceberg-storage field-rename table, per-table Iceberg
schemas, and live-table schema evolution (including why evolution diffs by
field name rather than the positional IDs `to_iceberg_schema()` assigns).

## Gotchas not fully covered by the docs

- `transform_trace_v1_to_v2()` (misnamed: current target is physical-v3, a
  hardcoded literal bumped alongside `schemas.toml`'s `current_trace_version`)
  is the only **compiled-plan-based** transform (`compiled-schema-materializer`):
  a `TraceV1ToV2Plan` of per-field extractor closures is resolved once
  (`warm_trace_v1_to_v2_plan()`, from `IcebergTableWriter::new`) instead of
  re-matched per batch. Plan construction returns `Err`, never panics, on an
  unmatched field. Logs/profiles/metrics transforms stay hand-written
  per-field, wire-to-physical directly — none has a v1->v2 split like traces.
- Field-coverage guard against silently-unused physical fields
  (`writer::schema_transform::schema_consistency`) is detailed in
  `flight-communication.md`'s "Field-Coverage Check" subsection.
