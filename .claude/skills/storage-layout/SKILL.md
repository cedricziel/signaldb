---
name: storage-layout
description: SignalDB storage layout - WAL directory structure, Iceberg catalog, object store paths, table types, segment lifecycle, and per-dataset storage overrides. Use when working with WAL, Iceberg tables, Parquet files, or storage configuration.
user-invocable: false
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

| Scheme | Backend | Example |
|--------|---------|---------|
| `file://` | Local filesystem | `file:///.data/storage` |
| `memory://` | In-memory (testing) | `memory://` |
| `s3://` | S3-compatible | `s3://bucket/prefix` |

Path resolution in `src/common/src/storage.rs` (`storage_dsn_to_path()`):
- `file:///.data/storage` -> `.data/storage`
- `file:///tmp/data` -> `/tmp/data`
- `s3://bucket/prefix` -> kept as-is

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

```
.data/wal/
  acme/
    production/
      traces/
        wal-0000000000.log    # Entry metadata (bincode)
        wal-0000000000.data   # Raw data (Arrow IPC StreamWriter)
        wal-0000000000.index  # Processed entry tracking (UUID list)
```

### WAL Entry Structure
```rust
pub struct WalEntry {
    pub id: Uuid,
    pub timestamp: u64,
    pub operation: WalOperation,    // WriteTraces | WriteLogs | WriteMetrics | Flush
    pub data_size: u64,
    pub data_offset: u64,
    pub processed: bool,
    pub tenant_id: String,
    pub dataset_id: String,
    pub metadata: Option<String>,   // JSON with schema_version, signal_type, target_table
}
```

### WAL Config
```rust
pub struct WalConfig {
    pub wal_dir: PathBuf,
    pub max_segment_size: u64,       // Default: 64 MB
    pub max_buffer_entries: usize,   // Default: 1000
    pub flush_interval_secs: u64,    // Default: 30s
    pub max_buffer_size: usize,      // Default: 128 MB
    pub tenant_id: String,           // Required, non-empty
    pub dataset_id: String,          // Required, non-empty
}
```

### Segment Lifecycle
1. **Write**: Append to current segment's `.log` and `.data`
2. **Rotation**: When segment exceeds `max_segment_size`, create new segment
3. **Processing**: WalProcessor reads unprocessed entries, writes to Iceberg, marks in `.index`
4. **Cleanup**: Fully-processed segments deleted; partial segments compacted

## Iceberg Catalog

- SQLite-only `SqlCatalog` named `"signaldb"` (PostgreSQL not supported for Iceberg catalog)
- Namespace: `[tenant_slug, dataset_slug]`
- Tables created lazily on first write
- Config: `[schema] catalog_type = "sql"`, `catalog_uri = "sqlite::memory:"`

## Table Types (up to 7 per tenant-dataset)

| Signal | Table Name | Schema Source |
|--------|-----------|---------------|
| Traces | `traces` | `schemas.toml` (v2, inherits v1) |
| Logs | `logs` | `schemas.toml` (v1) |
| Metrics | `metrics_gauge`, `metrics_sum`, `metrics_histogram`, `metrics_exponential_histogram`, `metrics_summary` | `iceberg_schemas.rs` (hardcoded) |

All tables partitioned by `Hour(timestamp)` as `timestamp_hour`.

## Key Implementation Files

| File | Purpose |
|------|---------|
| `schemas.toml` | Schema definitions with versioning |
| `src/common/src/schema/mod.rs` | Iceberg catalog creation |
| `src/common/src/schema/schema_parser.rs` | TOML schema parser |
| `src/common/src/schema/iceberg_schemas.rs` | Metric schemas, partition specs |
| `src/common/src/catalog_manager.rs` | CatalogManager singleton |
| `src/common/src/storage.rs` | Object store creation from DSN |
| `src/common/src/wal/mod.rs` | WAL implementation |
| `src/writer/src/storage/iceberg.rs` | IcebergTableWriter |
| `src/writer/src/processor.rs` | WalProcessor |
| `src/writer/src/schema_transform.rs` | v1->v2 schema transformation |
