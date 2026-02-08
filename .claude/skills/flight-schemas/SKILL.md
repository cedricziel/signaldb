---
name: flight-schemas
description: SignalDB Flight schemas and schema versioning - v1 wire format vs v2 storage format, schema inheritance, write-time transformations, traces/logs/metrics table schemas, and Flight RPC methods per service. Use when working with Arrow schemas, OTLP conversion, schema transforms, or Iceberg table schemas.
user-invocable: false
---

# SignalDB Flight Schemas & Schema Versioning

## Schema System Overview

Schemas are defined in `schemas.toml` (compiled into binary via `include_str!`) and support:
- **Versioning**: Each signal type tracks a current version (traces=v2, logs=v1, metrics=v1)
- **Inheritance**: `inherits = "v1"` pulls all parent fields
- **Field renames**: `{ from = "name", to = "span_name" }`
- **Computed fields**: `{ name = "timestamp", computed = "start_time_unix_nano" }`

Schema resolution in `SchemaDefinitions` (`src/common/src/schema/schema_parser.rs`):
1. Load base version fields
2. If `inherits`, recursively resolve parent
3. Apply `field_renames`
4. Append `field_additions`

## Flight Schema (v1) vs Iceberg Schema (v2)

The wire format and storage format differ intentionally. Writer applies `transform_trace_v1_to_v2()` at ingestion.

| Aspect | Flight v1 (wire) | Iceberg v2 (storage) |
|--------|-------------------|---------------------|
| Span name | `name` | `span_name` |
| Duration | `duration_nano` (UInt64) | `duration_nanos` (Int64) |
| Attributes | `attributes_json` | `span_attributes` |
| Resource | `resource_json` | `resource_attributes` |
| Time fields | UInt64 (nanos) | Long/Int64 (nanos) |
| Events/Links | `List<Struct>` (nested Arrow) | `String` (JSON) |
| Partition fields | None | `timestamp`, `date_day`, `hour` |

## Write-Time Transformation

`transform_trace_v1_to_v2()` in `src/writer/src/schema_transform.rs`:
1. **Detection**: Check for `name` field (v1) vs `span_name` (v2)
2. **Field renames**: Map v1 -> v2 names
3. **Type conversions**: `UInt64` -> `Int64` for Iceberg compatibility
4. **Complex type serialization**: `List<Struct>` events/links -> JSON strings
5. **Computed fields**: Generate `timestamp`, `date_day`, `hour` from `start_time_unix_nano`

Applied in Writer's Flight `do_put` handler before WAL write -- all WAL data is in v2 format.

## Traces Table Schema (v2 -- current)

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1 | `trace_id` | String | Yes | |
| 2 | `span_id` | String | Yes | |
| 3 | `parent_span_id` | String | No | |
| 4 | `span_name` | String | Yes | Renamed from `name` |
| 5 | `service_name` | String | Yes | |
| 6 | `start_time_unix_nano` | Long | Yes | |
| 7 | `end_time_unix_nano` | Long | Yes | |
| 8 | `duration_nanos` | Long | Yes | Renamed from `duration_nano` |
| 9 | `span_kind` | String | Yes | |
| 10 | `status_code` | String | Yes | |
| 11 | `status_message` | String | No | |
| 12 | `is_root` | Boolean | Yes | |
| 13 | `span_attributes` | String | No | JSON |
| 14 | `resource_attributes` | String | No | JSON |
| 15 | `events` | String | No | JSON serialized |
| 16 | `links` | String | No | JSON serialized |
| 17-22 | trace_state, resource_schema_url, scope_* | String | No | |
| 23 | `timestamp` | Timestamp | Yes | Computed, partition key |
| 24 | `date_day` | Date | Yes | Computed |
| 25 | `hour` | Int | Yes | Computed |

## Logs Table Schema (v1)

Key fields: `timestamp` (partition), `trace_id`, `span_id`, `severity_text`, `severity_number`, `service_name`, `body`, `resource_attributes`, `log_attributes`, `date_day`, `hour`.

## Metrics Schemas

Defined in `src/common/src/iceberg/schemas.rs` (hardcoded, not in schemas.toml):
- `metrics_gauge`: timestamp, service_name, metric_name, value, attributes
- `metrics_sum`: extends gauge with `aggregation_temporality`, `is_monotonic`
- `metrics_histogram`: count, sum, min, max, bucket_counts, explicit_bounds
- `metrics_exponential_histogram`: scale, zero_count, positive/negative buckets
- `metrics_summary`: count, sum, quantile_values

All partitioned by `Hour(timestamp)`.

## Flight RPC Methods by Service

| Method | Router | Querier | Writer |
|--------|--------|---------|--------|
| `Handshake` | Yes | Yes | Yes |
| `ListFlights` | Yes | Yes | No |
| `GetFlightInfo` | Yes | No | No |
| `GetSchema` | Yes | Yes | No |
| `DoGet` | Yes | Yes | No |
| `DoPut` | No | No | Yes |

## Flight Schemas Code Location

- Schema definitions: `src/common/flight/schema.rs`
- Conversions: `src/common/flight/conversion/` subdirectory
- Schema transform: `src/writer/src/schema_transform.rs`
