---
name: flight-schemas
description: SignalDB Flight schemas and schema versioning - v1 wire format vs v2 storage format, schema inheritance, write-time transformations, traces/logs/metrics table schemas, and Flight RPC methods per service. Use when working with Arrow schemas, OTLP conversion, schema transforms, or Iceberg table schemas.
user-invocable: false
sources:
  - schemas.toml
  - src/common/src/flight/schema.rs
  - src/writer/src/schema_transform.rs
  - src/common/src/schema/schema_parser.rs
  - src/common/src/iceberg/schemas.rs
---

# SignalDB Flight Schemas & Schema Versioning

## Schema System Overview

Schemas are defined in `schemas.toml` (compiled into binary via `include_str!`) and support:

- **Versioning**: Each signal type tracks a current physical version (traces=physical-v2, logs=physical-v1, metrics=physical-v1). A separate `logical_schema_version` (`otel-2026-08`) tracks the client-visible OTel logical schema, independent of the physical Iceberg realization.
- **Inheritance**: `inherits = "physical-v1"` pulls all parent fields
- **Field renames**: `{ from = "name", to = "span_name" }`
- **Computed fields**: `{ name = "timestamp", computed = "start_time_unix_nano" }`
- **Physical-only fields**: `{ physical_only = true }` marks fields that exist in the Iceberg table but are not part of the client-visible logical schema. Computed fields and partition-by fields are automatically marked `physical_only` during resolution.

Schema resolution in `SchemaDefinitions` (`src/common/src/schema/schema_parser.rs`):

1. Load base version fields
2. If `inherits`, recursively resolve parent
3. Apply `field_renames`
4. Append `field_additions`

## Flight Schema (v1) vs Iceberg Schema (v2)

The wire format and storage format differ intentionally. Writer applies `transform_trace_v1_to_v2()` at ingestion. The v2 transform resolves the physical schema via `resolve_trace_schema("physical-v2")`.

| Aspect           | Flight v1 (wire)              | Iceberg v2 (storage)                                      |
| ---------------- | ----------------------------- | --------------------------------------------------------- |
| Span name        | `name`                        | `span_name`                                               |
| Duration         | `duration_nano` (UInt64)      | `duration_nanos` (Int64)                                  |
| Attributes       | `attributes_json`             | `span_attributes` (Map on new tables; JSON string legacy) |
| Resource         | `resource_json`               | `resource_attributes`                                     |
| Time fields      | UInt64 (nanos)                | Long/Int64 (nanos)                                        |
| Events/Links     | `List<Struct>` (nested Arrow) | `String` (JSON)                                           |
| Partition fields | None                          | `timestamp`, `date_day`, `hour`                           |

## Write-Time Transformation

`transform_trace_v1_to_v2()` in `src/writer/src/schema_transform.rs`:

1. **Detection**: Check for `name` field (v1) vs `span_name` (v2)
2. **Field renames**: Map v1 -> v2 names
3. **Type conversions**: `UInt64` -> `Int64` for Iceberg compatibility
4. **Complex type serialization**: `List<Struct>` events/links -> JSON strings
5. **Computed fields**: Generate `timestamp`, `date_day`, `hour` from `start_time_unix_nano`

Applied in Writer's Flight `do_put` handler before WAL write -- all WAL data is in v2 format.

`service_name` is non-nullable in every Iceberg table. A resource without `service.name` (OTLP allows it; a Collector hostmetrics pipeline without a resource processor is the classic producer) is stored as `common::flight::conversion::UNKNOWN_SERVICE_NAME` (`"unknown"`) — the acceptor's OTLP conversion does this for traces and logs (their v1 batches carry `service_name`), the writer's `extract_resource_context` for the metrics transforms, which re-derive `service_name` from `resource_json` — so such batches are never dead-lettered with "Column 'service_name' is declared as non-nullable but contains null values".

## Traces Table Schema (physical-v2 -- current)

| #     | Field                                     | Iceberg Type | Required | Notes                        |
| ----- | ----------------------------------------- | ------------ | -------- | ---------------------------- |
| 1     | `trace_id`                                | String       | Yes      |                              |
| 2     | `span_id`                                 | String       | Yes      |                              |
| 3     | `parent_span_id`                          | String       | No       |                              |
| 4     | `span_name`                               | String       | Yes      | Renamed from `name`          |
| 5     | `service_name`                            | String       | Yes      |                              |
| 6     | `start_time_unix_nano`                    | Long         | Yes      |                              |
| 7     | `end_time_unix_nano`                      | Long         | Yes      |                              |
| 8     | `duration_nanos`                          | Long         | Yes      | Renamed from `duration_nano` |
| 9     | `span_kind`                               | String       | Yes      |                              |
| 10    | `status_code`                             | String       | Yes      |                              |
| 11    | `status_message`                          | String       | No       |                              |
| 12    | `is_root`                                 | Boolean      | Yes      |                              |
| 13    | `span_attributes`                         | String       | No       | JSON                         |
| 14    | `resource_attributes`                     | String       | No       | JSON                         |
| 15    | `events`                                  | String       | No       | JSON serialized              |
| 16    | `links`                                   | String       | No       | JSON serialized              |
| 17-22 | trace_state, resource_schema_url, scope_* | String       | No       |                              |
| 23    | `timestamp`                               | Timestamp    | Yes      | Computed, partition key      |
| 24    | `date_day`                                | Date         | Yes      | Computed                     |
| 25    | `hour`                                    | Int          | Yes      | Computed                     |

## Logs Table Schema (physical-v1)

Key fields: `timestamp` (partition), `trace_id`, `span_id`, `severity_text`, `severity_number`, `service_name`, `body`, `resource_attributes`, `log_attributes`, `date_day`, `hour`. On tables created since the typed-attribute change, `log_attributes`/`resource_attributes`/`scope_attributes` are Iceberg `Map<String,String>` (schemas.toml `map<string,string>`; nested key/value field IDs allocated after all top-level IDs); legacy tables have JSON strings. Transforms still emit JSON strings — `coerce_batch_to_schema` converts to `MapArray` (`json_strings_to_map_array`) when the table schema declares a map.

Plus, when `[schema.materialized_labels].<signal>` is configured, a nullable `label_<key>` column per key. All four `transform_*_v1_to_iceberg` transforms append these via `extend_schema_with_labels` — value from resource→scope→record attributes, first non-null. Logs/traces/profiles use the batch-level `materialized_label_columns`; the 5 exploded metrics transforms use `materialized_label_columns_from_json` (per data point). Rust-built metrics/profiles schemas append columns via `append_materialized_label_fields`; TOML logs/traces via `to_iceberg_schema_with_labels`. Default empty ⇒ unchanged schema. Per-tenant: transforms and schema creation take the tenant-resolved `MaterializedLabels` (tenant schema override replaces global; resolved in `CatalogManager::ensure_table` and `IcebergTableWriter::new`/`transform_for_signal`). See `docs/architecture/storage-layout.md#materialized-labels`.

## Metrics Schemas

On new tables the metric/profile attribute columns are `Map<String,String>` (`mapify_attr_fields`, nested IDs after labels); legacy JSON strings. Two definitions coexist:

- `schemas.toml` defines physical-v1 schemas for `metrics_gauge`, `metrics_sum`, and
  `metrics_histogram` (used by the TOML schema parser / wire side).
- The **Iceberg** metrics table schemas are built by hardcoded Rust functions in
  `src/common/src/iceberg/schemas.rs` (`create_metrics_*_schema()`) that do
  **not** read the TOML. `metrics_exponential_histogram` and `metrics_summary`
  exist only in Rust -- they have no schemas.toml entry.

Tables:

- `metrics_gauge`: timestamp, service_name, metric_name, value, attributes
- `metrics_sum`: extends gauge with `aggregation_temporality`, `is_monotonic`
- `metrics_histogram`: count, sum, min, max, bucket_counts, explicit_bounds
- `metrics_exponential_histogram`: scale, zero_count, positive/negative buckets (Rust only)
- `metrics_summary`: count, sum, quantile_values (Rust only)

All partitioned by `Hour(timestamp)`.

## Flight RPC Methods by Service

| Method          | Router | Querier | Writer      |
| --------------- | ------ | ------- | ----------- |
| `Handshake`     | Yes    | Yes     | Yes         |
| `ListFlights`   | Yes    | Yes     | Yes (empty) |
| `GetFlightInfo` | Yes    | No      | No          |
| `GetSchema`     | Yes    | Yes     | No          |
| `DoGet`         | Yes    | Yes     | No          |
| `DoPut`         | No     | No      | Yes         |

Writer `ListFlights` succeeds with an empty stream (no predefined flights) rather than returning `Unimplemented`; its `GetSchema`/`DoGet` do return `Status::unimplemented`.

## Flight Schemas Code Location

- Schema definitions: `src/common/src/flight/schema.rs`
- Conversions: `src/common/src/flight/conversion/` subdirectory
- Schema transform: `src/writer/src/schema_transform.rs`
