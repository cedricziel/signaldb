# Storage Layout Design

## Overview

SignalDB uses a three-tier storage model to balance durability, query performance, and operational flexibility:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Write-Ahead Log (WAL)                       │
│  Durability layer. Segmented files on local disk, per-tenant,      │
│  per-dataset, per-signal type. Data is buffered here before        │
│  being flushed to Iceberg tables.                                  │
└────────────────────────────┬────────────────────────────────────────┘
                             │ WalProcessor (background, 5s interval)
                             v
┌─────────────────────────────────────────────────────────────────────┐
│                      Iceberg SQL Catalog                           │
│  Metadata layer. SQLite-backed catalog named "signaldb" tracking   │
│  table schemas, snapshots, manifest lists, and partition specs.    │
│  Namespace = [tenant_slug, dataset_slug].                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │ Table location pointers
                             v
┌─────────────────────────────────────────────────────────────────────┐
│                         Object Store                               │
│  Data layer. Parquet files + Iceberg metadata JSON files stored    │
│  in local filesystem, S3/MinIO, or in-memory backends.             │
│  Path = {base}/{tenant_slug}/{dataset_slug}/{table_name}/          │
└─────────────────────────────────────────────────────────────────────┘
```

## Object Store Layout

### Physical Directory Structure

All persistent data (Parquet files and Iceberg metadata) is stored in the object store configured via `[storage].dsn`. The path structure is:

```
{storage_base}/
  {tenant_slug}/
    {dataset_slug}/
      {table_name}/
        metadata/
          v1.metadata.json          # Iceberg table metadata
          snap-{id}.avro            # Manifest lists
          {uuid}-m0.avro            # Manifest files
        data/
          {uuid}.parquet            # Data files
```

### Concrete Example

With `dsn = "file:///.data/storage"`, tenant "acme" (slug `acme`), datasets "production" (slug `prod`) and "archive" (slug `archive`):

```
.data/storage/
  acme/
    prod/
      traces/
        metadata/
          v1.metadata.json
        data/
          00000-0-{uuid}.parquet
      logs/
        metadata/
        data/
      metrics_gauge/
        metadata/
        data/
      metrics_sum/
        metadata/
        data/
      metrics_histogram/
        metadata/
        data/
    archive/
      traces/
        metadata/
        data/
  beta/
    staging/
      traces/
        metadata/
        data/
```

### Storage Backends

The `[storage].dsn` config accepts three URL schemes:

| Scheme | Backend | Example | Notes |
|--------|---------|---------|-------|
| `file://` | Local filesystem | `file:///.data/storage` | `LocalFileSystem` with prefix |
| `memory://` | In-memory | `memory://` | For testing only; data lost on restart |
| `s3://` | S3-compatible | `s3://bucket/prefix` | AWS S3, MinIO, or compatible. Uses env vars for credentials. |

**Path resolution** (`storage_dsn_to_path()` in `src/common/src/storage.rs`):

- `file:///.data/storage` -> `.data/storage` (strips leading `/.` for relative paths)
- `file:///tmp/data` -> `/tmp/data` (keeps absolute paths)
- `s3://bucket/prefix` -> kept as-is

### Per-Dataset Storage Override

Each dataset can optionally specify its own storage backend, enabling scenarios like:

- Production data on local NVMe for low latency
- Archive data on S3 for cost efficiency
- Test data in memory

```toml
[[auth.tenants]]
id = "acme"
slug = "acme"

[[auth.tenants.datasets]]
id = "production"
slug = "prod"
# Uses global [storage].dsn (no override)

[[auth.tenants.datasets]]
id = "archive"
slug = "archive"
[auth.tenants.datasets.storage]
dsn = "s3://acme-archive/signals"    # Per-dataset override
```

The resolution chain in `Configuration::get_dataset_storage_config()` (`src/common/src/config/mod.rs`):

1. Check `dataset.storage` -- if `Some`, use it
2. Fall back to global `config.storage`

The Querier registers per-dataset object stores with DataFusion's runtime environment so it can read Parquet files from whichever backend each dataset uses.

## Iceberg Catalog

### Catalog Configuration

The Iceberg metadata catalog is a SQLite-backed `SqlCatalog` (from `iceberg-sql-catalog`) named `"signaldb"`. It is configured via:

```toml
[schema]
catalog_type = "sql"
catalog_uri = "sqlite::memory:"          # In-memory (default, for dev/testing)
# catalog_uri = "sqlite:///.data/catalog.db"  # Persistent (recommended for production)
```

> **Limitation**: Only SQLite is supported for the Iceberg catalog. PostgreSQL URIs are rejected. This is distinct from the service discovery catalog which supports both SQLite and PostgreSQL.

### Namespace Structure

Iceberg namespaces use a two-level hierarchy based on **slugs**:

```
Namespace: [tenant_slug, dataset_slug]
Table:     table_name
Full:      Identifier([tenant_slug, dataset_slug], table_name)
```

Examples:

| Tenant | Dataset | Table | Iceberg Identifier |
|--------|---------|-------|--------------------|
| acme | prod | traces | `Identifier(["acme", "prod"], "traces")` |
| acme | archive | logs | `Identifier(["acme", "archive"], "logs")` |
| beta | staging | metrics_gauge | `Identifier(["beta", "staging"], "metrics_gauge")` |

Namespaces are implicitly created when tables are created -- there is no explicit `create_namespace` call.

### CatalogManager

`CatalogManager` (`src/common/src/catalog_manager.rs`) is the centralized singleton that holds the shared Iceberg catalog instance. All services (Writer, Querier, Router in monolithic mode) use the same `CatalogManager` to ensure consistent metadata access.

```rust
pub struct CatalogManager {
    catalog: Arc<dyn IcebergCatalog>,
    config: Configuration,
}
```

Key methods:

- `catalog()` -- returns `Arc<dyn IcebergCatalog>` for direct Iceberg operations
- `get_tenant_slug(tenant_id)` -- resolves slug from config (falls back to tenant_id)
- `get_dataset_slug(tenant_id, dataset_id)` -- resolves slug from config (falls back to dataset_id)
- `get_dataset_storage_config(tenant_id, dataset_id)` -- resolves per-dataset or global storage config

### DataFusion Integration

The Querier (`src/querier/src/flight.rs`) wraps the Iceberg catalog with a `TenantCatalog` to bridge DataFusion's 3-level naming model to Iceberg's 2-level namespace:

```
DataFusion:   SELECT * FROM acme.prod.traces
                            ^^^^  ^^^^  ^^^^^^
                          catalog schema table

Iceberg:      Identifier(["acme", "prod"], "traces")
                          ^^^^^^^^^^^^^^^   ^^^^^^
                            namespace       table
```

`TenantCatalog` implements `CatalogProvider`:

- `schema_names()`: Filters Iceberg namespaces to those starting with `{tenant_slug}.`, strips the prefix
- `schema(name)`: Prepends `{tenant_slug}.` to look up the full namespace `{tenant_slug}.{dataset_slug}`

At startup, the Querier registers a `TenantCatalog` per enabled tenant, plus a backward-compatible `"iceberg"` catalog.

## Table Types

SignalDB creates up to 7 table types per tenant-dataset combination. Table creation is controlled by the `[schema.default_schemas]` config and happens lazily on first write.

### Signal Type to Table Mapping

| Signal Type | Table Name | WalOperation | Schema Source |
|-------------|-----------|--------------|---------------|
| Traces | `traces` | `WriteTraces` | `schemas.toml` (v2, inherits v1) |
| Logs | `logs` | `WriteLogs` | `schemas.toml` (v1) |
| Metrics (Gauge) | `metrics_gauge` | `WriteMetrics` | `iceberg_schemas.rs` (hardcoded) |
| Metrics (Sum) | `metrics_sum` | `WriteMetrics` | `iceberg_schemas.rs` (hardcoded) |
| Metrics (Histogram) | `metrics_histogram` | `WriteMetrics` | `iceberg_schemas.rs` (hardcoded) |
| Metrics (Exp. Histogram) | `metrics_exponential_histogram` | `WriteMetrics` | `iceberg_schemas.rs` (hardcoded) |
| Metrics (Summary) | `metrics_summary` | `WriteMetrics` | `iceberg_schemas.rs` (hardcoded) |

For metrics, the target table name is extracted from the WAL entry's `metadata` JSON field (`target_table`), defaulting to `metrics_gauge`.

### Partitioning

All tables are partitioned by **hour** using Iceberg's built-in `Hour` transform on the `timestamp` column:

```
PartitionField {
    source_id: <timestamp field id>,
    field_id: 1000 + <timestamp field id>,
    name: "timestamp_hour",
    transform: Transform::Hour,
}
```

Hour-level partitioning automatically enables day/month/year pruning in DataFusion queries.

### Table Schemas

Schema definitions come from two sources:

- **`schemas.toml`** (compiled into the binary): Traces and Logs schemas with versioning and inheritance
- **`iceberg_schemas.rs`** (hardcoded): Metric table schemas

#### Traces Table (v2 -- current)

Defined in `schemas.toml` via v1 base + v2 inheritance with renames and additions.

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1 | `trace_id` | String | Yes | |
| 2 | `span_id` | String | Yes | |
| 3 | `parent_span_id` | String | No | |
| 4 | `span_name` | String | Yes | Renamed from `name` in v2 |
| 5 | `service_name` | String | Yes | |
| 6 | `start_time_unix_nano` | Long | Yes | Nanoseconds since epoch |
| 7 | `end_time_unix_nano` | Long | Yes | Nanoseconds since epoch |
| 8 | `duration_nanos` | Long | Yes | Renamed from `duration_nano` in v2 |
| 9 | `span_kind` | String | Yes | |
| 10 | `status_code` | String | Yes | |
| 11 | `status_message` | String | No | |
| 12 | `is_root` | Boolean | Yes | |
| 13 | `span_attributes` | String | No | JSON. Renamed from `attributes_json` in v2 |
| 14 | `resource_attributes` | String | No | JSON. Renamed from `resource_json` in v2 |
| 15 | `events` | String | No | JSON serialized (nested List<Struct> in Flight) |
| 16 | `links` | String | No | JSON serialized (nested List<Struct> in Flight) |
| 17 | `trace_state` | String | No | |
| 18 | `resource_schema_url` | String | No | |
| 19 | `scope_name` | String | No | |
| 20 | `scope_version` | String | No | |
| 21 | `scope_schema_url` | String | No | |
| 22 | `scope_attributes` | String | No | |
| 23 | `timestamp` | Timestamp | Yes | Computed from `start_time_unix_nano`. Partition key. |
| 24 | `date_day` | Date | Yes | Computed from timestamp |
| 25 | `hour` | Int | Yes | Computed from timestamp |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

#### Logs Table (v1 -- current)

Defined in `schemas.toml`.

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1 | `timestamp` | Timestamp | Yes | Partition key |
| 2 | `observed_timestamp` | Timestamp | No | |
| 3 | `trace_id` | String | No | Correlation with traces |
| 4 | `span_id` | String | No | Correlation with traces |
| 5 | `trace_flags` | Int | No | |
| 6 | `severity_text` | String | No | |
| 7 | `severity_number` | Int | No | |
| 8 | `service_name` | String | Yes | |
| 9 | `body` | String | No | |
| 10 | `resource_schema_url` | String | No | |
| 11 | `resource_attributes` | String | No | JSON |
| 12 | `scope_schema_url` | String | No | |
| 13 | `scope_name` | String | No | |
| 14 | `scope_version` | String | No | |
| 15 | `scope_attributes` | String | No | JSON |
| 16 | `log_attributes` | String | No | JSON |
| 17 | `date_day` | Date | Yes | Computed from timestamp |
| 18 | `hour` | Int | Yes | Computed from timestamp |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

#### Metrics Gauge Table (v1 -- current)

Defined in `iceberg_schemas.rs`.

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1 | `timestamp` | Timestamp | Yes | Partition key |
| 2 | `start_timestamp` | Timestamp | No | |
| 3 | `service_name` | String | Yes | |
| 4 | `metric_name` | String | Yes | |
| 5 | `metric_description` | String | No | |
| 6 | `metric_unit` | String | No | |
| 7 | `value` | Double | Yes | |
| 8 | `flags` | Int | No | |
| 9 | `resource_schema_url` | String | No | |
| 10 | `resource_attributes` | String | No | JSON |
| 11 | `scope_name` | String | No | |
| 12 | `scope_version` | String | No | |
| 13 | `scope_schema_url` | String | No | |
| 14 | `scope_attributes` | String | No | JSON |
| 15 | `scope_dropped_attr_count` | Int | No | |
| 16 | `attributes` | String | No | JSON |
| 17 | `exemplars` | String | No | JSON |
| 18 | `date_day` | Date | Yes | Computed |
| 19 | `hour` | Int | Yes | Computed |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

#### Metrics Sum Table (v1 -- current)

Extends Gauge with aggregation fields.

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1-8 | *(same as Gauge 1-8)* | | | |
| 9 | `aggregation_temporality` | Int | Yes | 0=Unspecified, 1=Delta, 2=Cumulative |
| 10 | `is_monotonic` | Boolean | Yes | |
| 11-21 | *(same as Gauge 9-19)* | | | |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

#### Metrics Histogram Table (v1 -- current)

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1-6 | *(same as Gauge 1-6)* | | | |
| 7 | `count` | Long | Yes | Total count |
| 8 | `sum` | Double | No | |
| 9 | `min` | Double | No | |
| 10 | `max` | Double | No | |
| 11 | `bucket_counts` | String | No | JSON array |
| 12 | `explicit_bounds` | String | No | JSON array |
| 13 | `flags` | Int | No | |
| 14 | `aggregation_temporality` | Int | Yes | |
| 15-25 | *(resource/scope/attributes/exemplars/date_day/hour)* | | | |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

#### Metrics Exponential Histogram Table (v1 -- current)

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1-6 | *(same as Gauge 1-6)* | | | |
| 7 | `count` | Long | Yes | |
| 8 | `sum` | Double | No | |
| 9 | `min` | Double | No | |
| 10 | `max` | Double | No | |
| 11 | `scale` | Int | No | |
| 12 | `zero_count` | Long | No | |
| 13 | `positive_offset` | Int | No | |
| 14 | `positive_bucket_counts` | String | No | JSON array |
| 15 | `negative_offset` | Int | No | |
| 16 | `negative_bucket_counts` | String | No | JSON array |
| 17 | `flags` | Int | No | |
| 18 | `aggregation_temporality` | Int | Yes | |
| 19 | `zero_threshold` | Double | No | |
| 20-30 | *(resource/scope/attributes/exemplars/date_day/hour)* | | | |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

#### Metrics Summary Table (v1 -- current)

| # | Field | Iceberg Type | Required | Notes |
|---|-------|-------------|----------|-------|
| 1-6 | *(same as Gauge 1-6)* | | | |
| 7 | `count` | Long | Yes | |
| 8 | `sum` | Double | Yes | |
| 9 | `quantile_values` | String | No | JSON array of `{quantile, value}` objects |
| 10 | `flags` | Int | No | |
| 11-21 | *(resource/scope/attributes/exemplars/date_day/hour)* | | | |

**Partition**: `Hour(timestamp)` as `timestamp_hour`

## WAL Layout

### Directory Structure

The WAL is organized by tenant, dataset, and signal type under the configured base directory:

```
{wal_dir}/
  {tenant_id}/
    {dataset_id}/
      {signal_type}/
        wal-0000000000.log      # Entry metadata (bincode-serialized WalEntry structs)
        wal-0000000000.data     # Raw data (Arrow IPC StreamWriter format)
        wal-0000000000.index    # Processed entry tracking (UUID list)
        wal-0000000001.log      # Next segment after rotation
        wal-0000000001.data
        wal-0000000001.index
```

### Concrete Example

With default `wal_dir = ".data/wal"`:

```
.data/wal/
  acme/
    production/
      traces/
        wal-0000000000.log
        wal-0000000000.data
        wal-0000000000.index
      logs/
        wal-0000000000.log
        wal-0000000000.data
        wal-0000000000.index
      metrics/
        wal-0000000000.log
        wal-0000000000.data
        wal-0000000000.index
    staging/
      traces/
        ...
  beta/
    staging/
      traces/
        ...
```

### WAL Configuration

```rust
pub struct WalConfig {
    pub wal_dir: PathBuf,
    pub max_segment_size: u64,       // Default: 64 MB (67108864 bytes)
    pub max_buffer_entries: usize,   // Default: 1000
    pub flush_interval_secs: u64,    // Default: 30 seconds
    pub max_buffer_size: usize,      // Default: 128 MB
    pub tenant_id: String,           // Required, non-empty
    pub dataset_id: String,          // Required, non-empty
}
```

The WAL enforces non-empty `tenant_id` and `dataset_id` at construction time.

### Segment Files

Each WAL segment consists of three files:

| File | Format | Content |
|------|--------|---------|
| `.log` | Length-prefixed bincode | Sequence of `WalEntry` structs (8-byte length prefix + bincode bytes) |
| `.data` | Raw bytes | Arrow IPC `StreamWriter` format. Entries reference data by offset and size. |
| `.index` | Binary | 8-byte count + 16-byte UUIDs of processed entries |

### WAL Entry Structure

```rust
pub struct WalEntry {
    pub id: Uuid,
    pub timestamp: u64,
    pub operation: WalOperation,      // WriteTraces | WriteLogs | WriteMetrics | Flush
    pub data_size: u64,               // Size of data in .data file
    pub data_offset: u64,             // Offset into .data file
    pub processed: bool,
    pub tenant_id: String,
    pub dataset_id: String,
    pub metadata: Option<String>,     // JSON with schema_version, signal_type, target_table, etc.
}
```

### Segment Lifecycle

1. **Write**: New entries appended to current segment's `.log` and `.data` files
2. **Rotation**: When segment exceeds `max_segment_size` (64MB default), a new segment is created with incremented ID
3. **Processing**: `WalProcessor` reads unprocessed entries, writes to Iceberg, marks entries in `.index`
4. **Cleanup**: Fully-processed segments are deleted. Partially-processed segments above compaction threshold are compacted.

### Data Serialization

Arrow RecordBatches are serialized to/from bytes using Arrow's IPC StreamWriter/StreamReader:

```rust
// Write
let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
writer.write(batch)?;
writer.finish()?;

// Read
let reader = StreamReader::try_new(&data[offset..offset + size], None)?;
let batch = reader.into_iter().next().unwrap()?;
```

## Schema Versioning and Evolution

### Schema Definition System

Schemas are defined in `schemas.toml` at the repository root and compiled into the binary via `include_str!`. The system supports:

| Feature | Description |
|---------|-------------|
| **Versioning** | Each signal type tracks a current version (e.g., `current_trace_version = "v2"`) |
| **Inheritance** | A version can inherit all fields from a parent: `inherits = "v1"` |
| **Field renames** | Rename fields across versions: `{ from = "name", to = "span_name" }` |
| **Field additions** | Add new fields: `{ name = "timestamp", type = "timestamp_ns", computed = "start_time_unix_nano" }` |
| **Computed fields** | Fields derived from other fields at write time |

### Schema Resolution

The `SchemaDefinitions` struct (`src/common/src/schema/schema_parser.rs`) resolves a versioned schema by:

1. Loading the base version's fields
2. If `inherits` is specified, recursively resolving the parent and starting with its fields
3. Applying `field_renames` to inherited fields
4. Appending `field_additions`

### Flight Schema vs Iceberg Schema

The Flight wire format (v1) and Iceberg storage format (v2) are intentionally different:

| Aspect | Flight Schema (v1) | Iceberg Schema (v2) |
|--------|-------------------|---------------------|
| Span name field | `name` | `span_name` |
| Duration field | `duration_nano` (UInt64) | `duration_nanos` (Long/Int64) |
| Attributes field | `attributes_json` | `span_attributes` |
| Resource field | `resource_json` | `resource_attributes` |
| Time fields | UInt64 (nanoseconds) | Long/Int64 (nanoseconds) |
| Events/Links | `List<Struct>` (nested Arrow) | `String` (JSON serialized) |
| Partition fields | None | `timestamp`, `date_day`, `hour` |

### Write-Time Transformation

The Writer applies `transform_trace_v1_to_v2()` (`src/writer/src/schema_transform.rs`) at ingestion time:

1. **Detection**: Checks if batch has `name` field (v1) or `span_name` field (v2)
2. **Field renames**: Maps v1 field names to v2 names
3. **Type conversions**: `UInt64` -> `Int64` for Iceberg compatibility
4. **Complex type serialization**: `List<Struct>` events/links -> JSON strings
5. **Computed fields**: Generates `timestamp`, `date_day`, `hour` from `start_time_unix_nano`

The transformation is applied in the Writer's Flight `do_put` handler before data is written to the WAL, ensuring all WAL data is in v2 format.

### No Iceberg-Level Schema Evolution

Currently, there is no Iceberg ALTER TABLE or runtime schema evolution. Tables are created with the current schema version (v2 for traces, v1 for logs/metrics). If a table already exists, it is loaded as-is. Incoming v1 data is always transformed to v2 before writing.

## Multi-Tenant Storage Isolation

### Isolation Summary

```
Tenant: "acme" (slug: "acme")
├── Dataset: "production" (slug: "prod")
│   ├── WAL:         .data/wal/acme/production/traces/
│   ├── Iceberg NS:  ["acme", "prod"]
│   ├── Object Path: .data/storage/acme/prod/traces/
│   └── DataFusion:  acme.prod.traces
│
└── Dataset: "archive" (slug: "archive")
    ├── WAL:         .data/wal/acme/archive/traces/
    ├── Iceberg NS:  ["acme", "archive"]
    ├── Object Path: s3://acme-archive/signals/acme/archive/traces/  (override)
    └── DataFusion:  acme.archive.traces
```

### Slug-Based Naming

All storage paths and Iceberg identifiers use **slugs** (URL-friendly identifiers), not raw IDs. Slugs are resolved from the tenant/dataset configuration:

- `CatalogManager::get_tenant_slug(tenant_id)` -- returns slug from config, or `tenant_id` if not found
- `CatalogManager::get_dataset_slug(tenant_id, dataset_id)` -- returns slug from config, or `dataset_id` if not found

### Table Creation Flow

When the `WalProcessor` encounters data for a new tenant/dataset/table combination:

1. Resolve `tenant_slug` and `dataset_slug` from `CatalogManager`
2. Resolve `StorageConfig` (per-dataset override or global)
3. Compute table location: `{storage_base}/{tenant_slug}/{dataset_slug}/{table_name}`
4. Check if table exists in Iceberg catalog: `catalog.tabular_exists(&table_ident)`
5. If not, create with `CreateTableBuilder`:
   - Schema from `iceberg_schemas` (matched by table name)
   - Partition spec (Hour on timestamp)
   - Table location pointing to the object store path
6. Cache the `IcebergTableWriter` for future writes to the same combination

### Configuration Toggles

Table creation can be controlled per-tenant via `[schema.default_schemas]`:

```toml
[schema.default_schemas]
traces_enabled = true
logs_enabled = true
metrics_enabled = true
# custom_schemas = { "custom_table" = "..." }
```

## Key Implementation Files

| File | Purpose |
|------|---------|
| `schemas.toml` | Schema definitions with versioning and inheritance |
| `src/common/src/schema/mod.rs` | Iceberg catalog creation, `TenantSchemaRegistry` |
| `src/common/src/schema/schema_parser.rs` | TOML schema parser with inheritance resolution |
| `src/common/src/schema/iceberg_schemas.rs` | Hardcoded metric schemas, partition specs, `TableSchema` enum |
| `src/common/src/catalog_manager.rs` | `CatalogManager` singleton for shared Iceberg catalog |
| `src/common/src/storage.rs` | Object store creation from DSN, path resolution |
| `src/common/src/wal/mod.rs` | WAL implementation (segments, entries, flush, cleanup) |
| `src/common/src/config/mod.rs` | Configuration structs including tenant/dataset/storage |
| `src/writer/src/storage/iceberg.rs` | `IcebergTableWriter` -- table creation and data writes |
| `src/writer/src/processor.rs` | `WalProcessor` -- background WAL-to-Iceberg processing |
| `src/writer/src/schema_transform.rs` | Flight v1 -> Iceberg v2 schema transformation |
| `src/querier/src/flight.rs` | `TenantCatalog` -- DataFusion/Iceberg namespace bridge |
| `src/querier/src/query/table_ref.rs` | Safe table reference construction with slug validation |
