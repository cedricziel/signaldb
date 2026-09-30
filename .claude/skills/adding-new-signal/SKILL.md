---
name: adding-new-signal
description: Step-by-step guide for adding a new signal type or table to SignalDB - logical schema declaration, schemas.toml physical realization, attribute type authority, typed attribute containers, OTLP conversion, WAL operation, writer/acceptor/querier/router updates, IR source registration, and testing. Use when adding new signal types, metric types, or tables.
sources:
  - schemas.toml
  - src/common/src/schema/**
  - src/writer/src/schema_transform.rs
  - src/common/src/iceberg/schemas.rs
---

# Guide: Adding a New Signal Type or Table

A signal has one client-visible shape (the **logical schema**) and one storage
shape (the **physical schema**, `physical-vN` in `schemas.toml`). Queries,
dialects and ingest bind to the logical shape; only the writer and the planner
know the physical one. Do steps 1 and 2 first: their tests fail until the
logical and physical halves agree. The three version axes (Flight wire,
`physical-vN`, logical) are explained in the `flight-schemas` skill.

## Step 1: Declare the logical schema

Edit `LogicalSchema::core()` in `src/common/src/schema/logical.rs`:

- Record metadata: `LogicalField::record_metadata(source, name, LogicalType)`
  with dotted OTel names. Add `.retrieval_only()` for values that can be read
  but not filtered (arrays, kvlists, bags).
- Join keys: `LogicalField::join_key(source, name)` for ids other signals join
  on (`trace_id`/`span_id`; `trace.id`/`span.id` on exemplars). One key has one
  type and filterability everywhere it appears.
- Resource identity: `LogicalField::signaldb_resource_identity(source)` when
  the signal carries the `resource_identity` digest (flagged `non_native`).
- Attribute levels: `LogicalField::attribute(source, AttributeLevel, name, ..)`
  for the resource and scope fields the signal exposes.

Every physical column must be a logical field (by its own name or an alias), an
attribute container, or `physical_only`. `src/common/tests/schema_realization.rs`
enforces this against each signal's current `physical-vN`; add the signal's
alias table there.

Bump `LogicalSchema::VERSION` and `logical_schema_version` in `schemas.toml`
together. `logical_schema_fingerprint_is_pinned` fails until you do, and
reports the new `FIELD_SET_FINGERPRINT` to paste in.

## Step 2: Realize it physically in `schemas.toml`

Every built-in table, `metrics` and `metric_exemplars` included, is resolved
from `schemas.toml`. There are no hand-written schema functions.

- Add a `[{table}.physical-v1]` section and point `[metadata]`
  `current_{signal}_version` at it. Later versions use `inherits` with renames,
  additions and removals. Version names carry no order; only `inherits` does.
- Declare each attribute container (`resource_attributes`, `scope_attributes`,
  the record-level one, `filtered_attributes`, ...) with the `typed_attributes`
  field type. The parser expands it into `{container}_str/_int/_double/_bool`
  typed maps plus a binary `{container}_residue`. A new table starts in this
  layout, so there is nothing to evolve from.
- Computed and partition columns become `physical_only` automatically.
- In `src/common/src/iceberg/schemas.rs` add a `TableSchema` variant and wire
  it into `resolved_schema()`, `schema()`, `from_table_name()`, `table_name()`,
  `all()`, `all_from_config()`, `materialized_labels_of()` and
  `attribute_type_signal()`. Add `create_{table}_schema_with()` (it calls
  `to_iceberg_schema_with_labels` on the resolved schema) and
  `create_{table}_partition_spec()` (hour on `timestamp`).
- `sort_key_columns()` is the sort order every producer (writer, compactor)
  honours; give the table a time-leading key.
- Bloom filters: `bloom_filter_properties_for_table()` in
  `src/common/src/schema/mod.rs` decides which columns get one (label columns
  on every table, `trace_id`/`span_id` on traces and logs). Add the table there
  if it has a point-lookup id.

## Step 3: Give attributes a type authority

The attribute type authority (`src/common/src/schema/type_authority/`) holds one
canonical type per tenant, dataset, signal, level and key.

- Add the signal to `AttributeTypeSignal` in `src/common/src/config/mod.rs`
  (logs, traces, metrics, profiles today; `metrics` and `metric_exemplars`
  share `Metrics`) and to `TableSchema::attribute_type_signal()`. This is what
  makes `[[schema.attribute_types]]` pins and the `attribute_types` catalog
  table cover the new signal.
- Decide each container's level. `typed_attributes::container_level()` treats
  `resource_attributes` and `scope_attributes` as resource and scope level and
  every other container as record level.

## Step 4: Wire format, OTLP conversion, acceptor

- Flight wire schema: `src/common/src/flight/schema.rs`. Attributes stay
  JSON-in-Utf8 on the wire and the WAL stays byte-unchanged; typing happens in
  the writer.
- OTLP to Arrow conversion in `src/common/src/flight/conversion/`. Go through
  `extract_value` in `conversion_common.rs` so `AnyValue` fidelity is kept
  (bytes stay bytes) rather than stringifying.
- Acceptor: add the OTLP service under `src/acceptor/src/services/` and the
  HTTP handler under `handler/`. Pass it the shared `TypeSnapshots` with
  `with_type_snapshots()` and build the warning through `type_warning.rs`. The
  acceptor only reads a cached type snapshot and reports off-type values to the
  sender in OTLP `partial_success`; it rejects nothing and writes no types.

## Step 5: WAL operation and routing

- Add a variant to `WalOperation` in `src/common/src/wal/mod.rs`, and to
  `signal()` and `from_signal()`.
- `src/writer/src/routing.rs` turns a batch's metadata into its `(tenant,
dataset, table)` destination. Both `do_put` and the WAL processor call it, so
  add the variant's `target_table` routing there and nowhere else. A
  one-table signal needs no `target_table`; today only metrics honour it.

## Step 6: Writer

- `src/writer/src/schema_transform.rs`: transform the wire batch to the
  physical shape (renames, casts, computed columns), and add the table to the
  `schema_consistency` tests' touched-field sets.
- Typed attributes: `src/writer/src/storage/iceberg.rs` splits each container
  into the typed layout. It resolves each distinct key once per batch with
  `SignalScope::canonical` and places each value with `place()`. A value of the
  canonical type goes to its typed home; an off-type scalar, array, kvlist or
  bytes value goes to the residue. Nothing is coerced, and the first observed
  scalar type becomes canonical unless a config pin or semconv hint says
  otherwise.
- `src/writer/src/flight_iceberg.rs` (`do_put`) and `processor.rs` handle the
  new operation.
- Tables are provisioned by the writer's table reconciler
  (`src/writer/src/reconcile.rs`: startup pass, then every
  `[writer].table_reconcile_interval`), so a dataset is queryable before its
  first write. The ingest path still load-or-creates on demand as a fallback.
  The reconciler takes its table set from `all_from_config()`, gated by
  `default_schemas.{signal}_enabled`. See
  `docs/operations/table-provisioning.md`.

## Step 7: Querier, IR, dialects

- Register the IR source in `SourceRegistry::core()`
  (`src/query-ir/src/source.rs`: name, grain, whether `extract` is legal) and
  add a `SourcePlan` in `src/querier/src/query/ir_planner.rs` (table, time
  column, containers, prefixes, aliases). Queries use logical names only; the
  planner rejects physical column names. If the IR cannot express something,
  extend the IR rather than adding a dialect-only path.
- TraceQL, LogQL and PromQL are projections onto the same logical schema, lowered
  to IR documents in `src/ql-ir/`. Add lowering there if a dialect must reach
  the new signal.
- Flight ticket handling, if the signal needs its own: `src/querier/src/flight.rs`.

## Step 8: Router and configuration

- HTTP endpoints under `src/router/src/endpoints/`. First-party readers use
  `POST /api/v1/query` (Query IR); compatibility endpoints are for external
  clients.
- Signal toggle in `[schema.default_schemas]` (`src/common/src/config/mod.rs`).

## Step 9: Tests

- Unit tests in each modified crate, including the fingerprint and realization
  tests from steps 1 and 2.
- Integration test in `tests-integration/`: OTLP ingest -> WAL -> Iceberg -> IR
  query, with an off-type value that must land in the residue.

## Key patterns

- Partition by `Hour(timestamp)`; namespace is `[tenant_slug, dataset_slug]`.
- The logical schema is the only surface a query sees. Typed maps, promoted
  `attr_<level>_<key>` columns and the warm index never appear in a query.
- Arrow IPC for WAL data; `List<Struct>` values (events, links) are stored as
  JSON strings.

## Reference: existing signals

- **Traces**: the most complete write path, `src/acceptor/` through
  `src/writer/` to `src/querier/`.
- **Logs**: same shape; `body` is an `AnyValue`.
- **Metrics**: one signal, two tables. `transform_metrics_to_wide` writes the
  wide `metrics` table and `transform_metric_exemplars` writes
  `metric_exemplars`; one WAL entry commits to both, each with its own
  idempotency marker. The IR exposes them as the `metrics` and `exemplars`
  sources.
