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
---

# SignalDB Flight Schemas & Schema Versioning

## Schema System Overview

Schemas are defined in `schemas.toml` (compiled into binary via `include_str!`) and support:

- **Versioning**: Each signal type tracks a current physical version (traces=physical-v5, logs=physical-v4, metrics=physical-v4, profiles=physical-v3) — the typed attribute layout (see below), landed as a one-shot cutover: a table still in the legacy single-map layout is dropped and recreated, never evolved, the next time `IcebergTableManager::ensure_table` loads it. Metrics' `physical-v4` is additionally a table-shape cutover (`otel-native-schema` layer 7): the five legacy per-type tables are replaced by `metrics`/`metric_exemplars`. A separate `logical_schema_version` (`otel-2026-09`) tracks the client-visible OTel logical schema, independent of the physical Iceberg realization.
- **Three version axes, not one**: (1) the Flight **wire** format vs Iceberg storage — the `*_v1_to_*` transforms in `src/writer/src/schema_transform.rs`; "v1"/"v2" in those names is historical and means wire→storage, nothing else; (2) the **physical** chain `physical-v1..vN` in `schemas.toml`, one per signal; (3) the **logical** schema version (`logical_schema_version`), which describes `common::schema::logical`. A storage migration moves (2) only; a logical field change moves (3) only.
- **Inheritance**: `inherits = "physical-v1"` pulls all parent fields
- **Field renames**: `{ from = "name", to = "span_name" }`
- **Field removals**: `{ name = "deprecated_field" }` drops a field inherited from a parent version
- **Computed fields**: `{ name = "timestamp", computed = "start_time_unix_nano" }`
- **Physical-only fields**: `{ physical_only = true }` marks fields that exist in the Iceberg table but are not part of the client-visible logical schema. Computed fields and partition-by fields are automatically marked `physical_only` during resolution.

Schema resolution in `SchemaDefinitions` (`src/common/src/schema/schema_parser.rs`):

1. Load base version fields
2. If `inherits`, recursively resolve parent
3. Apply `field_renames`
4. Append `field_additions`
5. Apply `field_removals`

`SchemaDefinitions::version_chain` separately computes the forward hop order between two named versions by walking `inherits` backward from the target and reversing — version _names_ carry no ordering of their own, only `inherits` pointers do. This is what drives live-table schema evolution (see `docs/architecture/storage-layout.md`'s "Schema Evolution" section) — `resolve_table_schema`'s own step-list above is for resolving one version's field set, not for sequencing versions.

**Positional field IDs, and why evolving a live table can't use them**: `ResolvedSchema::to_iceberg_schema()` assigns Iceberg field IDs by position (`idx + 1`) every time it's called — safe for a table being created fresh, but unsafe to diff against an existing table's live schema (a version that removes a field in the middle would shift every later field's ID, corrupting the mapping already burned into that table's Parquet files). `common::iceberg::evolution`'s live-table functions diff by field _name_ against the table's actual persisted schema instead, reusing existing IDs untouched and minting new ones only for genuine additions.

## Flight Schema (v1) vs Iceberg Schema (physical-v4 intermediate shape)

The wire format and storage format differ intentionally. Writer applies `transform_trace_v1_to_v2()` at ingestion, resolving against a fixed `"physical-v4"` literal — **not** `SCHEMA_DEFINITIONS.current_trace_version()` (now `physical-v5`, the typed layout). This transform's only job is bridging the wire's v1 shape to the last version with `span_attributes`/`resource_attributes`/`scope_attributes` as plain `map<string,string>` columns; `IcebergTableWriter::append_batches_with_marker`'s typed-container splitting (via the attribute type authority) handles the v4 → v5 hop generically afterward, from whatever the table's actual current schema is — `create_arrow_schema_from_resolved` has no case for a typed map/binary column, so this plan can never target `physical-v5` directly. `transform_logs_v1_to_iceberg` is the same shape: a hardcoded `"physical-v3"` literal (logs' own last pre-typed version), not `current_log_version` (now `physical-v4`).

| Aspect           | Flight v1 (wire)              | Iceberg physical-v4 (pre-typed intermediate shape)                                       |
| ---------------- | ----------------------------- | ---------------------------------------------------------------------------------------- |
| Span name        | `name`                        | `span_name`                                                                              |
| Duration         | `duration_nano` (UInt64)      | `duration_nanos` (Int64)                                                                 |
| Attributes       | `attributes_json`             | `span_attributes` (JSON-string carrier; split into the typed layout below before commit) |
| Resource         | `resource_json`               | `resource_attributes`                                                                    |
| Time fields      | UInt64 (nanos)                | Long/Int64 (nanos)                                                                       |
| Events/Links     | `List<Struct>` (nested Arrow) | `String` (JSON)                                                                          |
| Partition fields | None                          | `timestamp`, `date_day`, `hour`                                                          |

## Write-Time Transformation

`transform_trace_v1_to_v2()` in `src/writer/src/schema_transform.rs` is
**compiled-plan-based** (`compiled-schema-materializer`): a
`TraceV1ToV2Plan` — one extractor closure per physical-v4 field, selecting
field renames, `UInt64`→`Int64` casts, `List<Struct>` events/links → JSON
serialization, and the `timestamp`/`date_day`/`hour` computed fields — is
resolved once (`warm_trace_v1_to_v2_plan()`, called from
`IcebergTableWriter::new` for the traces table) rather than re-matched by
field name on every batch. Plan construction returns `Err` (never panics)
on a field with no matching rule, so a bad extraction-rule reference fails
before the writer serves traffic. Only this trace v1→v2 step is
plan-based; `transform_logs_v1_to_iceberg`/`transform_profiles_v1_to_iceberg`/
`transform_metrics_to_wide`/`transform_metric_exemplars` stay hand-written
per-field code (none of them have a v1→v2 split the way traces does — they
go wire-to-physical directly).

Applied in Writer's Flight `do_put` handler before WAL write -- all WAL data is in the current physical format. On the wire, a `WriteMetrics` batch still carries `data_json` unchanged; the writer turns it into both the `metrics` and `metric_exemplars` rows, and one WAL entry commits to both tables (replay-safe via per-table idempotency markers).

Non-finite metric doubles (NaN, ±Inf) are carried in `data_json` as the strings `"NaN"`/`"+Inf"`/`"-Inf"` (`common::flight::conversion::{f64_to_json, json_to_f64}`), never `null`, so a NaN reading stays distinct from a JSON `null` (which the writer leaves as a null `metrics.value`, nullable in `physical-v4`) (#1061). The querier's histogram bounds parser accepts the same sentinels.

`service_name` is non-nullable in every Iceberg table. A resource without `service.name` (OTLP allows it; a Collector hostmetrics pipeline without a resource processor is the classic producer) is stored as `common::flight::conversion::UNKNOWN_SERVICE_NAME` (`"unknown"`) — the acceptor's OTLP conversion does this for traces and logs (their v1 batches carry `service_name`), the writer's `extract_resource_context` for the metrics transforms, which re-derive `service_name` from `resource_json` — so such batches are never dead-lettered with "Column 'service_name' is declared as non-nullable but contains null values".

`writer::schema_transform::schema_consistency` (`unified-table-schema`'s `table-schema-consistency` capability) asserts, per table, that `schemas.toml`'s current non-computed field names exactly match a hand-maintained "fields this transform touches" set — the failure mode it exists to catch is a field declared physical but never actually read or written, the way `dropped_*_count` went silent before #1208. `transform_trace_v1_to_v2`/`transform_logs_v1_to_iceberg`/`transform_profiles_v1_to_iceberg` also self-check this at runtime (each iterates its own resolved schema's field list with an exhaustive match, erroring on an unhandled name); `transform_metrics_to_wide`/`transform_metric_exemplars` build columns positionally against their own resolved `metrics.physical-v4`/`metric_exemplars.physical-v4` schemas with no such runtime check, so the test-level check is these two tables' only guard.

## Traces Table Schema (physical-v4, the write-transform's target -- current storage is physical-v5, the typed attribute layout; see `docs/architecture/storage-layout.md`)

| #     | Field                                     | Iceberg Type | Required | Notes                                                                                             |
| ----- | ----------------------------------------- | ------------ | -------- | ------------------------------------------------------------------------------------------------- |
| 1     | `trace_id`                                | String       | Yes      |                                                                                                   |
| 2     | `span_id`                                 | String       | Yes      |                                                                                                   |
| 3     | `parent_span_id`                          | String       | No       |                                                                                                   |
| 4     | `span_name`                               | String       | Yes      | Renamed from `name`                                                                               |
| 5     | `service_name`                            | String       | Yes      |                                                                                                   |
| 6     | `start_time_unix_nano`                    | Long         | Yes      |                                                                                                   |
| 7     | `end_time_unix_nano`                      | Long         | Yes      |                                                                                                   |
| 8     | `duration_nanos`                          | Long         | Yes      | Renamed from `duration_nano`                                                                      |
| 9     | `span_kind`                               | String       | Yes      | Derived from `span_kind_number`, never the reverse                                                |
| 10    | `status_code`                             | String       | Yes      | Derived from `status_code_number`, never the reverse                                              |
| 11    | `status_message`                          | String       | No       |                                                                                                   |
| 12    | `is_root`                                 | Boolean      | Yes      |                                                                                                   |
| 13    | `span_attributes`                         | String       | No       | JSON                                                                                              |
| 14    | `resource_attributes`                     | String       | No       | JSON                                                                                              |
| 15    | `events`                                  | String       | No       | JSON serialized                                                                                   |
| 16    | `links`                                   | String       | No       | JSON serialized                                                                                   |
| 17-22 | trace_state, resource_schema_url, scope_* | String       | No       |                                                                                                   |
| 23    | `timestamp`                               | Timestamp    | Yes      | Computed, partition key                                                                           |
| 24    | `date_day`                                | Date         | Yes      | Computed                                                                                          |
| 25    | `hour`                                    | Int          | Yes      | Computed                                                                                          |
| 26    | `span_kind_number`                        | Int          | No       | v3: numeric OTel source of truth for `span_kind`, written verbatim from `Span.kind` (issue #1208) |
| 27    | `status_code_number`                      | Int          | No       | v3: numeric OTel source of truth for `status_code`, written verbatim from `Status.code`           |
| 28    | `dropped_attributes_count`                | Long         | No       | v3: preserved verbatim from the OTel span (previously discarded despite being query-registered)   |
| 29    | `dropped_events_count`                    | Long         | No       | v3: as above                                                                                      |
| 30    | `dropped_links_count`                     | Long         | No       | v3: as above                                                                                      |
| 31    | `resource_identity`                       | String       | No       | v4: digest of the span's resource attribute set, from `common::schema::resource_identity` (#1340) |

The five v3 columns and `resource_identity` are nullable, so rows written before their version have no value for them; `arrow_to_otlp_traces` falls back to deriving `span_kind`/`status_code`'s int from the string columns, and defaults the dropped counts to 0, only when the v3 column is absent or null. `resource_identity` is null on any row written before the column existed.

## Logs Table Schema (physical-v3, the write-transform's target -- current storage is physical-v4, the typed attribute layout)

Key fields: `timestamp` (partition), `trace_id`, `span_id`, `severity_text`, `severity_number`, `service_name`, `body`, `resource_attributes`, `log_attributes`, `date_day`, `hour`. `transform_logs_v1_to_iceberg` emits `log_attributes`/`resource_attributes`/`scope_attributes` as JSON strings at this intermediate `physical-v3` shape; the table's actual current schema (`physical-v4`) declares each as five typed columns (`{container}_str/_int/_double/_bool/_residue` — see `docs/architecture/storage-layout.md`'s "Typed attribute layout" section), and `apply_typed_attribute_containers` splits the JSON into them before commit, resolving each key's canonical type through the attribute type authority. v2 (#1340) adds a nullable `resource_identity` string column -- same digest and same null-before-the-column-existed rule as traces'. v3 (#1743) adds nullable `event_name` (String) and `dropped_attributes_count` (Long, cast from the wire batch's UInt32) columns, mirroring `traces.physical-v3`'s dropped counts.

Plus, when `[schema.materialized_labels].<signal>` is configured, a nullable `label_<key>` column per key — except when two configured keys sanitize to the same candidate name (e.g. `http.method`/`http_method`), in which case `common::iceberg::evolution::resolve_label_columns_canonical`/`resolve_label_columns_fresh` assign collision-safe suffixes (`label_http_method`, `label_http_method_2`, ...) deterministically from the full configured key _set_ (order-independent), stamping each column's `doc` as the authoritative key→column record (#1448; same doc-authoritative mechanism as auto-promotion's `resolve_label_columns`, #814). The writer reconciles a batch's label columns against the table's `doc`-authoritative assignment before commit, so a config edit that grows the key set on an already-existing table doesn't misroute one key's values into another's column; matching is by the origin key stamped into each label column's Arrow field metadata (`LABEL_ORIGIN_KEY_METADATA`), which survives the WAL's IPC round trip, not by column name (#1534) — a pre-#1534 WAL entry has no such metadata and falls back to the old name-based guard. Every write-transform appends these via `extend_schema_with_labels` — value from resource→scope→record attributes, first non-null. Logs/traces/profiles use the batch-level `materialized_label_columns`; `transform_metrics_to_wide` uses `materialized_label_columns_from_json` (per data point). Schema creation for all five built-in table types appends label columns via `ResolvedSchema::to_iceberg_schema_with_labels` (`schemas.toml`-sourced for every one of them since #1237 — no hand-written label-appending function remains). Default empty ⇒ unchanged schema. Per-tenant: transforms and schema creation take the tenant-resolved `MaterializedLabels` (tenant schema override replaces global; resolved in `CatalogManager::ensure_table` and `IcebergTableWriter::new`/`transform_for_signal`). See `docs/architecture/storage-layout.md#materialized-labels`.

## Metrics Schemas

The current metrics layout (`otel-native-schema` layer 7, D10) is two wide
tables, `metrics.physical-v4` and `metric_exemplars.physical-v4`
(`schemas.toml`), replacing the five legacy per-type tables
(`metrics_gauge`, `metrics_sum`, `metrics_histogram`,
`metrics_exponential_histogram`, `metrics_summary`). `MetricsLayout::current()`
(`common::iceberg::schemas`) reads `current_metric_version` and returns
`Wide` for `physical-v4`; `Legacy` for anything else. Under `Wide`, the writer
runs two transforms per wire metrics batch instead of the five hand-written
per-type ones:

- `transform_metrics_to_wide` (`src/writer/src/schema_transform.rs`) fans
  each OTLP data point into one `metrics` row, typed by `metric_type`
  (`gauge`/`sum`/`histogram`/`exponential_histogram`/`summary`): scalar
  `value` for gauge/sum, `count`/`sum`/`min`/`max` plus `explicit_bounds`/
  `bucket_counts` (`List<Double>`/`List<Int64>`) for histograms,
  `scale`/`zero_count`/`zero_threshold`/positive-negative bucket lists for
  exponential histograms, and parallel `quantiles`/`quantile_values`
  (`List<Double>`) for summaries — no JSON-string columns. `series_id` is a
  stable digest of the metric name, `metric_type`, resource identity,
  instrumentation scope, and record attributes.
- `transform_metric_exemplars` fans the same batch's exemplars into one
  `metric_exemplars` row each, carrying `series_id` and `point_timestamp`
  (together identifying the `metrics` row it belongs to — `series_id` alone
  identifies only the series), `value`, and flat hex `trace_id`/`span_id`
  (same encoding as traces).

Both are typed-attribute-layout tables from creation (`attributes`/
`resource_attributes`/`scope_attributes` and `filtered_attributes` are each
five typed columns, not one `Map<String,String>`), so there is no
legacy-layout hop to evolve for them the way traces/logs/profiles have.
`iceberg::schemas`'s `create_metrics_schema_with()`/
`create_metric_exemplars_schema_with()` resolve from `schemas.toml` via
`ResolvedSchema::to_iceberg_schema_with_labels`.

One WAL entry commits to both tables, replay-safe via per-table idempotency
markers; the writer's table reconciler drops the five legacy tables on the
cutover (dropped data is not migrated) — see
`docs/operations/table-provisioning.md`.

All partitioned by `Hour(timestamp)`.

## Declared sort order

Beyond the partition spec, every signal table declares an Iceberg sort order (order id `1`) built by `TableSchema::sort_order_for()` in `iceberg/schemas.rs`: traces `(timestamp, trace_id)`, logs `(timestamp, service_name, severity_text)`, metrics `(timestamp, metric_name, service_name)`, profiles `(timestamp, service_name)` — ascending, nulls first. Sort fields carry schema **field ids**, so they must be resolved against the same schema being declared (materialized labels shift ids per tenant); `sort_key_columns()` is the name-level source of truth every producer sorts by. See `docs/architecture/storage-layout.md#declared-sort-order`.

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
