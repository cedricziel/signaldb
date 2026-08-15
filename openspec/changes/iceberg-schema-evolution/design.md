## Context

See `proposal.md` for motivation. Load-bearing facts gathered while scoping
this change:

**Schema layers today.** `schemas.toml` (workspace root) is the physical
source of truth per signal, parsed by `SchemaDefinitions`
(`common::schema::schema_parser`) into per-version `TableSchemaDefinition`s
that chain via `inherits` + `field_renames` + `field_additions`. It has no
`field_removals` yet. `ResolvedSchema::to_iceberg_schema()`
(`schema_parser.rs`, `build_iceberg_schema`) assigns Iceberg field IDs
**positionally** — `idx as i32 + 1` over `self.fields` — every time it runs,
not from any ID persisted anywhere. `TableManager::ensure_table`
(`common::iceberg::table_manager`) only creates a table that doesn't exist;
its existing-table branch does nothing but
`backfill_metadata_pruning_properties` (a precedent for "mutate a loaded
table's metadata on every `ensure_table` call") and returns early.

**The positional-field-ID trap.** Because IDs are recomputed by position on
every resolve, `to_iceberg_schema()` is only safe to call for a table being
created _for the first time_ at its current version — the numbering is
self-consistent because there's no prior Parquet data to map against. It is
**not** safe to call for an existing table: if a later schema version
removes a field in the middle of the list and we resolve that version fresh,
every field after the removed one gets a different ID than what's already
recorded in the table's own Iceberg metadata and burned into its existing
Parquet files' column mapping. Evolving a live table therefore cannot start
from `schemas.toml`'s resolved output — it must start from the table's own
current `iceberg_rust::spec::schema::Schema` (loaded from the catalog) and
mutate it minimally.

**Iceberg mechanism available (pinned fork, `cedricziel/iceberg-rust`,
rev in root `Cargo.toml`).** `TableMetadata` already versions schema shape
natively: `current-schema-id` plus the `schemas` array is schema history for
free, and every data file records the schema-id that wrote it — no external
version store needed for the shape itself. Table properties (already used
here: `write.metadata.delete-after-commit.enabled`,
`write.metadata.previous-versions-max`) are the natural place to stamp a
SignalDB-owned label, `signaldb.schema.version`, mirroring `schemas.toml`'s
own version strings.

The high-level `Transaction::add_schema(schema)` builder
(`table/transaction/mod.rs`) only emits `TableUpdate::AddSchema` — it never
emits `TableUpdate::SetCurrentSchema`, so calling it alone registers a new
schema without ever activating it for reads or writes. The primitives needed
to actually activate it exist one layer down, in `catalog::commit`:
`TableUpdate::SetCurrentSchema { schema_id: -1 }` ("use the schema just
added"), `TableUpdate::SetProperties`, and
`TableRequirement::AssertCurrentSchemaId { current_schema_id }` for
optimistic concurrency. `Catalog::update_table(self: Arc<Self>, commit:
CommitTable) -> Result<Table, Error>` is the raw primitive
(`CommitTable { identifier, requirements: Vec<TableRequirement>, updates:
Vec<TableUpdate> }`) — it must be called directly, bypassing the
`Transaction` builder for this one operation.

## Goals / Non-Goals

**Goals:**

- Bring an existing table's Iceberg schema forward to match `schemas.toml`'s
  current version for its signal, additively, without touching existing
  data files.
- Make `schemas.toml` the complete physical-schema source of truth for every
  signal table, including the two metrics representations
  (`ExponentialHistogram`, `Summary`) it doesn't cover today.
- Ship `span_kind_number`, `status_code_number`, and the three
  `dropped_*_count` fixes as the first real use of the mechanism.

**Non-Goals:**

- Backfilling historical row values for any column, old or new (#1209,
  closed).
- Column rename or type-widening as live-table operations. `schemas.toml`
  already models renames for schema _definition_ purposes, but this change
  does not add a live-table rename operation — only add/remove. Renaming a
  live column safely (matching by old name, keeping the same field ID) is a
  straightforward future extension of the same mechanism, deferred until a
  concrete need exists.
- Reclaiming storage from a removed column's historical Parquet data
  (requires compaction/rewrite; out of scope).
- A user-facing "run this migration now" API. Evolution runs implicitly as
  part of the existing reconcile pass; this change does not add an admin
  endpoint beyond what `POST /api/v1/tenants/{id}/tables/create` already
  triggers (which will now also catch up an existing table's schema as a
  side effect of calling `ensure_table`).

## Decisions

### Version tracking: table property, not a migrations table

Store `signaldb.schema.version` (e.g. `"physical-v3"`) as an Iceberg table
property, set in the same atomic commit as any schema change. Rejected
alternative: a per-tenant/dataset "migrations" tracking table. It would add
a second source of truth that can drift from what's actually committed (the
tracking row could say "done" while the real Iceberg commit failed or was
rolled back), and the existing writer reconciler already walks every
tenant/dataset/table on a timer — reading one property per table during that
same walk is strictly less machinery than a new store. A table with no
recorded property is treated as being at the oldest version this mechanism
knows about for its signal (see the `dataset-table-provisioning` delta
spec's scenario for pre-existing tables).

### Evolution commit shape: one atomic multi-update `CommitTable` per version hop

For each version hop, build:

```
CommitTable {
    identifier: <table ident>,
    requirements: vec![TableRequirement::AssertCurrentSchemaId {
        current_schema_id: <table's schema id when loaded>,
    }],
    updates: vec![
        TableUpdate::AddSchema { schema: <new schema>, last_column_id: <max field id> },
        TableUpdate::SetCurrentSchema { schema_id: -1 },
        TableUpdate::SetProperties {
            updates: [("signaldb.schema.version", "<target version>")],
        },
    ],
}
```

committed via `catalog.update_table(commit)`. All three updates land or none
do. The `AssertCurrentSchemaId` requirement means a concurrent evolver (two
writer replicas both reconciling the same table) fails the commit instead of
racing; the loser reloads the table and re-evaluates from its new state,
which naturally converges (the requirement in the retry now matches).

### Building the new `Schema`: mutate the live schema, don't regenerate

The migration engine takes the table's current live
`iceberg_rust::spec::schema::Schema` (from the loaded `Table`, not from
`schemas.toml`) and the target version's _field list_ (names + types +
required, from `ResolvedSchema`, ignoring its positional `field_id`) and
computes:

- **Additions**: names present in the target but absent from the live
  schema. Each gets a fresh ID, `live_schema.highest_field_id() + 1`,
  incrementing for each addition in the same hop. Always nullable.
- **Removals**: names present in the live schema but declared in
  `schemas.toml`'s new `field_removals` for this hop. Dropped from the
  schema by ID lookup; every other field's ID is left untouched.

The result is a full `Schema` (Iceberg requires the complete field list per
`AddSchema`, not a delta), but every field ID in it is either carried over
unchanged from the live schema or newly minted — never recomputed
positionally. This is the one rule that makes this safe: never call
`ResolvedSchema::to_iceberg_schema()` against an existing table.

### Walking versions one hop at a time, not straight to head

If a table is N versions behind, apply each intervening version's diff as
its own commit, in order, rather than computing one big diff from the
table's current version straight to head. This mirrors `schemas.toml`'s own
`inherits` chain (already sequential) and means a partial failure leaves the
table at a valid, previously-reached version rather than in an ambiguous
partially-applied state — each hop is independently a complete, self
consistent version.

### Where it plugs in

`TableManager::ensure_table`'s existing-table branch, next to
`backfill_metadata_pruning_properties` — same call site, same "mutate a
table we just loaded" shape. Triggered by whatever already calls
`ensure_table`: the writer's periodic reconciler (`writer::reconcile`, no
new background job) and the manual `POST /api/v1/tenants/{id}/tables/create`
endpoint.

### `schemas.toml` gets `field_removals` and full metrics coverage

Add `field_removals: Vec<FieldRemoval>` (`{ name: String }`) to
`TableSchemaDefinition`, parsed the same way as `field_renames`, applied
after renames/additions when resolving a version (remove by name from
`resolved_fields`). Separately — and as a prerequisite, since the evolution
engine needs `schemas.toml` to actually describe every table it's
responsible for — add `metrics_exponential_histogram` and `metrics_summary`
schema blocks to `schemas.toml`, generated to match the fields
`iceberg::schemas::create_metrics_exponential_histogram_schema_with`/
`create_metrics_summary_schema_with` already produce by hand, then make
those two functions resolve from `schemas.toml` like `metrics_gauge`/
`metrics_sum`/`metrics_histogram` already do. This is a like-for-like
consolidation (same fields, same types) with no behavior change on its own;
it exists so item 6 below isn't scoped against half the metrics tables.

### span_kind_number / status_code_number / dropped_*_count: one-off uses of the mechanism, not new mechanism

These three fixes are ordinary version bumps through the machinery above:
add nullable `span_kind_number`/`status_code_number` (int32) and the three
`dropped_*_count` (int64, already declared in `LogicalSchema::core()` but
missing physically) to `schemas.toml`'s next `traces.physical-vN`. The
conversion-layer changes (`conversion_traces.rs`: stop hardcoding
`dropped_*_count` to 0, read/write the two `_number` columns directly from
the OTel proto ints instead of only deriving strings) are ordinary
application code, not part of the versioning mechanism itself — the
mechanism's job ends at "the column exists on the table"; what the writer
puts in it is the conversion layer's job as it always has been.

## Risks / Trade-offs

- **[Risk]** A table stuck mid-catch-up if a hop's commit keeps failing
  (e.g. persistent `AssertCurrentSchemaId` conflicts under heavy concurrent
  reconciliation) → **Mitigation**: each hop is independently retryable and
  the table is left at its last successfully-reached version, never
  half-applied (see the "partial failure" scenario in
  `table-schema-evolution`'s spec); the existing reconcile pass simply tries
  again next cycle.
- **[Risk]** `schemas.toml`'s positional field-ID assignment remains a trap
  for anyone touching `schema_parser.rs` later without knowing this
  constraint → **Mitigation**: this design document and a code comment at
  `build_iceberg_schema` (added as part of this change) state explicitly
  that its output must never be diffed against a live table by field ID,
  only by name/type for computing the target field _set_.
- **[Risk]** Removing a column changes what future `INSERT`/append paths
  must stop writing; if the writer's RecordBatch construction isn't updated
  in the same version bump, the write path could still try to write to a
  column Iceberg no longer knows about → **Mitigation**: `schema_transform.rs`
  changes ship in the same task/commit as the `schemas.toml` version bump
  that removes the field, same discipline already required for additions.
- **[Trade-off]** No cross-table/cross-dataset audit trail of "what
  migrations have ever run" independent of the tables themselves (a
  dedicated migrations table would give this) → accepted: nothing today
  needs history that outlives a table, and the reconciler already has
  visibility into every table's current property on each pass.

## Migration Plan

1. `schemas.toml`: add `field_removals`, add
   `metrics_exponential_histogram`/`metrics_summary` blocks, bump
   `traces.physical-v3` with the four new nullable columns.
2. `common`: `FieldRemoval` in `schema_parser.rs`; the schema-evolution
   engine (diff + `CommitTable` builder) in `iceberg::table_manager`; wire
   `ExponentialHistogram`/`Summary` schema functions to resolve from
   `schemas.toml`; register `span_kind_number`/`status_code_number` in
   `LogicalSchema::core()`.
3. `common::flight`: wire-schema columns; `conversion_traces.rs` read/write
   fixes.
4. `writer`: `schema_transform.rs` v1→v2 mapping for the new columns.
5. Deploy: the next reconcile pass on any existing deployment (hive
   included) evolves every traces table forward one hop, additive-only, no
   downtime, no data rewrite. No rollback machinery is needed beyond
   redeploying the previous binary — an added-but-unused nullable column is
   harmless to read with older code that doesn't know about it (Iceberg
   projects by schema; a reader on the old schema id/version simply doesn't
   request the new column, an application-level column drop for it).

## Open Questions

None — the concurrency, versioning-store, and field-ID questions that
would otherwise be open were resolved above based on what the pinned
`iceberg-rust` fork actually supports.
