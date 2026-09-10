---
name: storage-layout
description: SignalDB storage layout - WAL directory structure, Iceberg catalog, object store paths, table types, segment lifecycle, and per-dataset storage overrides. Use when working with WAL, Iceberg tables, Parquet files, or storage configuration.
user-invocable: false
sources:
  - docs/architecture/storage-layout.md
  - src/common/src/storage.rs
  - src/common/src/wal/**
  - src/common/src/catalog_manager.rs
  - src/common/src/iceberg/**
---

# SignalDB Storage Layout Reference

## Three-Tier Storage Model

```
WAL (local disk) -> Iceberg SQL Catalog (SQLite metadata) -> Object Store (Parquet data)
```

## Object Store Layout

Path structure: `{storage_base}/{tenant_slug}/{dataset_slug}/{table_name}/`

```
.data/storage/
  acme/
    prod/
      traces/
        metadata/v1.metadata.json
        data/00000-0-{uuid}.parquet
      logs/
      metrics_gauge/
      metrics_sum/
      metrics_histogram/
    archive/
      traces/
```

### Storage Backends

| Scheme      | Backend             | Example                 |
| ----------- | ------------------- | ----------------------- |
| `file://`   | Local filesystem    | `file:///.data/storage` |
| `memory://` | In-memory (testing) | `memory://`             |
| `s3://`     | S3-compatible       | `s3://bucket/prefix`    |

Path resolution in `src/common/src/storage.rs` (`storage_dsn_to_path()`):

- `file:///.data/storage` -> `.data/storage` (relative to the working directory)
- `file:///tmp/data` -> `/tmp/data`
- `s3://bucket/prefix` -> kept as-is

`file://` storage directories are auto-created at startup (`ensure_file_dsn_dir()`), so a fresh checkout works without pre-creating them.

### Per-Dataset Storage Override

Datasets can override global storage:

```toml
[[auth.tenants.datasets]]
id = "archive"
slug = "archive"
[auth.tenants.datasets.storage]
dsn = "s3://acme-archive/signals"
```

Resolution chain in `Configuration::get_dataset_storage_config()`:

1. Check `dataset.storage` -- if `Some`, use it
2. Fall back to global `config.storage`

## WAL Layout

Path: `{wal_dir}/{tenant_id}/{dataset_id}/{signal_type}/`

One `Wal` per `(tenant, dataset, signal)`, in the acceptor and the writer alike
(`common::wal::manager::WalManager`, #932): separate segments, flush mutex and
dead-letter dir per tenant, created lazily on first write, plus
`discover_existing_wals()` at startup so a previous run's pending entries drain
without new traffic. A writer upgrading from the pre-#932 single WAL has its
root segments adopted drain-only by `adopt_root_segments()`.

```
.data/wal/
  acme/
    production/
      traces/
        wal-0000000000.log    # "SDBW"+version header, then [u32 len][u32 crc32][bincode WalEntry] records
        wal-0000000000.data   # ["SDBR"][u32 len][u32 crc32][Arrow IPC stream] records
        wal-0000000000.index  # Processed entry tracking (UUID list)
        dead-letter/          # Retired entries (unreadable/rejected/corrupt), see below
```

Record framing v1 (`src/common/src/wal/framing.rs`, #946): every record in
both files carries its length and a CRC-32 of its payload; `data_offset`
points at the `.data` record header, `data_size` is the payload length. A
`.data` record failing magic/length/CRC on read is quarantined as
`dead-letter/<entry_id>.corrupt.bin`, logged with tenant/dataset/signal/offset,
counted in `signaldb.wal.corrupt_entries{record="data"}`, and retired by the
reader (`dead_letter_unreadable`) without touching its neighbours. Legacy
unframed segments (no `SDBW` header) are read on the old layout, sealed
against appends (the WAL rotates onto a fresh v1 segment on open), and
rewritten to v1 by compaction — no operator step.

On replay, a `.log` record whose payload fails its CRC or deserialize (framing
intact, content corrupted) is skipped rather than aborting the whole
segment — the offset of the next record is still known from the length
prefix, so `offset += entry_len` resyncs onto it. The corrupt bytes are
quarantined to `dead-letter/segment-<id>-offset-<offset>.corrupt.bin` (keyed
by physical location, not `<entry_id>`, since the id lives inside the bytes
that failed to decode — every other `dead-letter/` file is keyed by
`<entry_id>`) and `signaldb.wal.corrupt_entries` is incremented. This is a
permanent loss of that entry, not just a delay: aborting instead of skipping
would turn one corrupted record into a permanent crash loop, since the
same bytes are hit again on every restart (issue #1033). Full detail:
`docs/operations/wal-persistence.md#corrupted-entry-records-during-replay`.

### WAL Entry Structure

```rust
pub struct WalEntry {
    pub id: Uuid,
    pub timestamp: u64,
    pub operation: WalOperation,    // WriteTraces | WriteLogs | WriteMetrics | Flush
    pub data_size: u64,             // payload length (record header excluded)
    pub data_offset: u64,           // offset of the .data record header
    pub processed: bool,
    pub tenant_id: String,
    pub dataset_id: String,
    pub metadata: Option<String>,   // JSON with schema_version, signal_type, target_table
}
```

### WAL Config

```rust
pub struct WalConfig {
    pub wal_dir: PathBuf,               // Default: ".wal"
    pub max_segment_size: u64,          // Default: 64 MB
    pub max_buffer_entries: usize,      // Default: 1000
    pub flush_interval_secs: u64,       // Default: 30s
    pub tenant_id: String,              // Required, non-empty
    pub dataset_id: String,             // Required, non-empty
    pub retention_secs: u64,            // Default: 3600 (1h) — keep processed entries
    pub cleanup_interval_secs: u64,     // Default: 300 (5m) — cleanup cadence
    pub compaction_threshold: f64,      // Default: 0.5 — processed fraction that triggers segment compaction
}
```

Note: the runtime `WalConfig::default()` uses `.wal` as the base directory, but
the services resolve their WAL directory from the TOML-level `[wal] wal_dir`
(see the `configuration` skill): acceptor `{wal_dir}/acceptor`, writer
`{wal_dir}/writer` (default `.data/wal/{service}`), overridable per service via
`ACCEPTOR_WAL_DIR` / `WRITER_WAL_DIR`.

### Segment Lifecycle

1. **Write**: Append a metadata record to the `.log` and write the payload to the `.data` file at the entry's recorded `data_offset` (the writer seeks to that offset — not a blind `O_APPEND` — so a short write can't shift following entries)
2. **Rotation**: When **either** the `.log` or the `.data` file would exceed `max_segment_size`, seal the segment and create a new one (the `.data` file dominates size, so it usually drives rotation; this bounds offsets clear of 2³²)
3. **Processing**: WalProcessor reads unprocessed entries, writes to Iceberg, marks in `.index`
4. **Cleanup**: Fully-processed segments deleted; partial segments compacted past `compaction_threshold`, keeping each survivor's original timestamp

5. **Eviction**: after the sweep, a WAL idle for `WalManager::DEFAULT_IDLE_TIMEOUT` (15m) with no unprocessed entries is closed and dropped; the next write reopens it. `Wal::close` must _abort_ the flush task — it holds clones of the buffer/segments, so dropping the `Arc<Wal>` frees nothing. Eviction holds the same per-key init guard `get_wal` uses, so no second instance can open a directory still being closed (#883). A closed segment refuses appends rather than skipping the write while advancing offsets. Idleness alone can't bound an actively-written fleet, so the cache also carries a soft `[wal].max_instances` cap (default 256, `0` = unbounded): on a miss, `get_wal` evicts the least-recently-appended drained, unreferenced WAL to make room, or lets the write proceed over the cap if nothing qualifies.

Cleanup runs via `WalManager::cleanup_all_if_due` at a **pass boundary** — end of the writer's background drain, end of the acceptor's retry pass — throttled by the manager to the smallest per-signal `cleanup_interval_secs`. The boundary is load-bearing: compaction moves surviving entries to new offsets, so it must not run while a consumer holds entries it listed earlier in the pass (`Wal::read_entry_data` re-resolves entries from the segment's own index as the backstop). The writer's `do_action("flush")` deliberately does not sweep — it is deadline-bounded for the client. See `docs/architecture/storage-layout.md#segment-reclamation`.

## Iceberg Catalog

- SQLite-only `SqlCatalog` named `"signaldb"` (PostgreSQL not supported for Iceberg catalog)
- Namespace: `[tenant_slug, dataset_slug]`
- **Resolved dataset invariant**: `ResolvedTenant.datasets` always contains the tenant's `default_dataset`, whether or not a `datasets` row names it (`catalog_manager.rs::ensure_default_dataset`, applied by both the config and database descriptors). Do not re-add this fallback at consumers: compaction planning, retention, orphan cleanup, and table reconciliation all iterate `.datasets` and get it for free (#1066). Every write path materializes the row (`upsert_tenant_with_default_dataset` for tenant create/update, `ensure_dataset` for config sync), so a row-less default is a legacy state that `Catalog::backfill_default_datasets` clears at boot — the invariant covers the window before it runs
- Tables are provisioned ahead of ingest by the writer's reconciler (`writer/src/reconcile.rs`, startup pass + `[writer] table_reconcile_interval`), which calls `CatalogManager::ensure_dataset_tables` for every registered tenant/dataset. The write path still load-or-creates on demand via the same `ensure_table`, so a dataset converges either way and a failing reconciler degrades to create-on-first-write. A dataset therefore normally holds every enabled signal table (empty, no snapshot) before its first write — see `docs/operations/table-provisioning.md`
- Config: `[schema] catalog_type = "sql"`, `catalog_uri = "sqlite::memory:"`
- Every catalog connection gets `journal_mode = wal` + `busy_timeout = 30000` from the catalog itself (upstream JanKaul/iceberg-rust#381), plus `synchronous = normal` from SignalDB via `sqlite_session_statements` in `iceberg/mod.rs` — so concurrent trace/log commits don't serialize behind a rollback lock and stall first-time table creation, and a commit contending with a compaction doesn't give up after sqlx's 5s default. The pool is lazy, so they land on first use. Session statements need the catalog's support (upstream JanKaul/iceberg-rust#386, currently a fork pin — see `Cargo.toml`)
- **Metadata retention**: every table gets `write.metadata.previous-versions-max = 100` + `write.metadata.delete-after-commit.enabled = true` (`table_manager.rs`) — set at creation and backfilled by `ensure_table` on load for pre-existing tables that lack them (#959; only absent keys are added, so operator-set values survive) — so superseded `metadata.json` files are reclaimed on commit instead of accumulating; honored by the SQL catalog's delete-after-commit support (upstream JanKaul/iceberg-rust#382, currently a fork pin — see `Cargo.toml`). Files already aged out before the backfill stay orphaned (one-time cleanup needed).
- **Output file size**: `write.target-file-size-bytes` is _not_ set at creation. Compaction reconciles it against `[compactor].target_file_size_mb` immediately before each rewrite via `common::iceberg::table_manager::ensure_target_file_size_property` — a metadata-only commit, a no-op once it matches, logged-not-fatal if it loses a CAS race. Setting it at creation would drift when an operator changed the config, and a planner selecting files under `target_file_size_mb` while the writer rolls at a stale value never converges. The pinned writer rolls on **real encoded bytes** (`bytes_written() + in_progress_size()`, upstream JanKaul/iceberg-rust#388), not an in-memory estimate — decoded/encoded diverge ~5–10×. Unset (a table compaction has never touched) falls back to the writer's 512 MiB default, which no ingest flush approaches. See `docs/architecture/storage-layout.md#output-file-size`.

## Table Types (up to 7 per tenant-dataset)

| Signal   | Table Name                                                                                              | Schema Source                                |
| -------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| Traces   | `traces`                                                                                                | `schemas.toml` (v3, inherits v2 inherits v1) |
| Logs     | `logs`                                                                                                  | `schemas.toml` (v1)                          |
| Metrics  | `metrics_gauge`, `metrics_sum`, `metrics_histogram`, `metrics_exponential_histogram`, `metrics_summary` | `schemas.toml` (v1)                          |
| Profiles | `profiles`                                                                                              | `schemas.toml` (v1)                          |

All tables partitioned by `Hour(timestamp)` as `timestamp_hour`.

### Declared sort order

Every signal table declares an Iceberg sort order (order id `1`; id `0` is Iceberg's reserved unsorted order) — traces `(timestamp, trace_id)`, logs `(timestamp, service_name, severity_text)`, metrics `(timestamp, metric_name, service_name)`, profiles `(timestamp, service_name)`, all ascending nulls-first. `TableSchema::sort_key_columns()` in `iceberg/schemas.rs` is the single source of truth; the keys are frozen from what the compactor already produced, so declaring them rewrites nothing. Sort fields bind to the field ids of the schema they're declared against (materialized-label columns shift ids per tenant). Set at creation, and added to pre-existing tables by `table_manager.rs::backfill_sort_order` on load (metadata-only, idempotent, at most one commit per table). Declaration is table-level **intent**; per-file honesty is a separate fact carried by the file itself (Parquet row-group `sorting_columns` + `iceberg.sort-order-id` footer entry → manifest `sort_order_id`), and the DataFusion provider claims the ordering only for attested files, so any unattested file in range keeps the explicit sort. **Attesting an unsorted file yields wrong results, not slow ones** — a producer that can't guarantee the order must write unattested. Legacy files converge through compaction; no backfill job. Both producers resolve the key through one shared decision, `common::iceberg::sort::write_sort_key`: ingest concatenates and columnar-sorts each commit group before writing (whole group, not per batch — files roll from the stream in order; debug builds re-derive the permutation and assert identity), and compaction takes the key from the declaration instead of its own list. They differ deliberately on a table with **no** declaration: ingest leaves it unsorted (nothing could attest it), compaction still sorts by the canonical key resolved by name — neither attests. That asymmetry is an `UndeclaredFallback` argument, not drift; sorted-but-unattested must stay unattested. See `docs/architecture/storage-layout.md#declared-sort-order`.

### Typed attribute maps

New tables across all four signals store their attribute columns as Iceberg `Map<String,String>` — any attribute matches exactly (incl. regex/ordered) via `get_field` extraction; legacy tables keep JSON strings with the substring approximation. The querier detects the form per table (`attr_context_of`); labels/values/detected_fields read either form (`attr_documents`; `distinct()` skipped for maps — Arrow row format can't sort them). Epic #737 L1 (#730), logs first.

### Materialized labels

`[schema.materialized_labels]` (per signal) promotes attribute keys from the `*_attributes` JSON into nullable `label_<key>` columns at ingest, so they match exactly / by regex / with ordered comparisons instead of the JSON substring approximation. Naming via `common::schema::materialized_column_name` (non-alphanumeric → `_`, `label_` prefix). Writer populates from resource→scope→record attributes (first non-null); `coerce_batch_to_schema` drops columns a table lacks and null-fills nullable ones it has. New tables carry the configured columns from creation; existing tables can gain `label_<key>` columns post-creation via `common::iceberg::evolution::add_label_columns` (metadata-only `AddSchema`+`SetCurrentSchema` commit via `Catalog::update_table`; idempotent; collision-proof — two keys sanitizing to the same candidate name get distinct columns via a deterministic suffix, resolved by each column's origin-key `doc`, the authoritative key→column record, not by re-deriving the candidate name; field ids continue past nested map/list ids; no Parquet rewrite — old files null-fill on read, snapshot-pinned schemas stay reachable; needs iceberg-rust rev >= 96f28c18). Querier reads of a promoted-but-possibly-unbackfilled column always `COALESCE` with the attribute-map fallback rather than trusting the column alone, so results stay correct through the null-fill window (#816) — at the cost of the exclusive-column fast path for that column's lifetime. Tables not yet evolved fall back to JSON. Allowlists resolve per tenant (tenant schema override replaces global) at table creation (`ensure_table`) and in the writer transforms. Querier routes via the column when the table schema has it (LogQL `attribute_expr`, trace search `Condition::to_expr`, PromQL `matcher_expr`). Implemented for all four signals: **logs** (exact/regex/ordered), **traces** (exact), **metrics** (exact/regex filter; `by`/`without` grouping; part of the natural series identity — PromQL scan projects the label columns, null-filled across the gauge/sum union), **profiles** (columns populated; no attribute-filter query surface yet). Metrics/profiles table schemas now resolve from `schemas.toml` the same way traces/logs do (`ResolvedSchema::to_iceberg_schema_with_labels` appends label columns for all six built-in table types uniformly); metrics transforms extract per-exploded-data-point label values via `materialized_label_columns_from_json`. See `docs/architecture/storage-layout.md#materialized-labels`.

### Schema evolution (traces/logs)

For the two signals this live-evolution mechanism covers (traces, logs — metrics/profiles resolve their physical schema from `schemas.toml` too since #1237, but aren't wired into `ensure_schema_evolved` yet), an existing table's schema is brought forward to the current `schemas.toml` version on every `ensure_table` load, not just at creation: `common::iceberg::evolution::ensure_schema_current` reads the table's `signaldb.schema.version` property, walks `SchemaDefinitions::version_chain` to the current version, and applies each hop via `apply_schema_migration` (additive `AddSchema`+`SetCurrentSchema`+`SetProperties` through `Catalog::update_table`, same commit/verify shape as `add_label_columns` below). A table with **no recorded version** (pre-dates this mechanism) skips hop-by-hop walking and migrates directly to current in one step, additions only, never removing a field — hop-by-hop removal assumes the starting version is trusted, which an inferred baseline isn't (a table already ahead of an early hop would otherwise lose fields the diff can't tell it legitimately has). No backfill of historical row values; `field_removals` (schemas.toml) only ever drops a column going forward. Field-created-fresh tables get their version stamped at creation, not left to the evolution path to catch up on first load.

### Parquet bloom filters

Enabled per column at table creation via the standard Iceberg property `write.parquet.bloom-filter-enabled.column.<col> = "true"` (honored by the pinned iceberg-rust writer on ingest + compaction); dispatch by table type lives in `common::schema::bloom_filter_properties_for_table`. Point lookups on high-cardinality columns can't use row-group min/max (every file spans the full random range), so a bloom filter is the only structure that prunes them. Enabled columns: **traces and logs** `trace_id`/`span_id` (`common::schema::bloom_filter_properties_for_trace_columns` — traces for single-trace/-span lookups, logs for logs-for-a-trace correlation, where the columns are optional but named identically), **logs** additionally `attr_tokens` list-leaf (`bloom_filter_property_for_attr_tokens`), and every materialized `label_<key>` column (`bloom_filter_properties_for_labels`). `trace_id`/`span_id` also carry `bloom-filter-fpp.column = "0.01"`; `trace_id` alone additionally carries an explicit `bloom-filter-ndv.column` (`BLOOM_FILTER_TRACE_ID_NDV`, an estimate) since it repeats across every span of a trace, unlike `span_id` which is unique per row and fits parquet-rs's default ndv as-is. Metadata-only + creation-time: pre-existing tables don't gain a filter retroactively. Read path: `datafusion_iceberg` reports `Inexact` and builds `ParquetSource` without a predicate, but DataFusion's physical filter-pushdown injects the predicate into `DataSourceExec`, so with `bloom_filter_on_read` (default on) row groups are skipped. Verified by `tests-integration/tests/querier/trace_bloom_pruning.rs`. Compactor gap: `TODO(#731)` — bloom props for _newly promoted_ label columns aren't re-set on rewrite. See `docs/architecture/storage-layout.md#parquet-bloom-filters`.

## Key Implementation Files

| File                                      | Purpose                                                                                                     |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `schemas.toml`                            | Schema definitions with versioning                                                                          |
| `src/common/src/iceberg/mod.rs`           | Iceberg catalog creation, object store builders                                                             |
| `src/common/src/iceberg/schemas.rs`       | Schema creation functions for traces/logs/metrics, partition specs                                          |
| `src/common/src/iceberg/names.rs`         | Naming utilities for table identifiers, namespaces, locations                                               |
| `src/common/src/iceberg/table_manager.rs` | IcebergTableManager for table operations                                                                    |
| `src/common/src/iceberg/evolution.rs`     | `add_label_columns()`/`remove_label_columns()` + generic `ensure_schema_current()` schema-evolution helpers |
| `src/common/src/schema/mod.rs`            | Schema registry, re-exports iceberg modules                                                                 |
| `src/common/src/schema/schema_parser.rs`  | TOML schema parser                                                                                          |
| `src/common/src/catalog_manager.rs`       | CatalogManager singleton                                                                                    |
| `src/common/src/storage.rs`               | Object store creation from DSN                                                                              |
| `src/common/src/wal/mod.rs`               | WAL implementation                                                                                          |
| `src/writer/src/storage/iceberg.rs`       | IcebergTableWriter                                                                                          |
| `src/writer/src/processor.rs`             | WalProcessor                                                                                                |
| `src/writer/src/schema_transform.rs`      | v1->v2 schema transformation                                                                                |
