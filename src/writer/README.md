# SignalDB Writer

The writer is SignalDB's stateful ingestion service (the "Ingester"). It
receives Arrow record batches from acceptors via Apache Arrow Flight `do_put`,
journals them in its own Write-Ahead Log, and a background `WalProcessor`
(running every 5 seconds, with exponential backoff on repeated failures)
persists accumulated entries to Apache Iceberg tables as Parquet files and
marks them processed — turning durable-but-raw WAL records into queryable
columnar storage.

## Endpoint

| Protocol | Port | Purpose |
|----------|------|---------|
| Flight (gRPC) | 50061 | `do_put` ingestion of trace/log/metric batches |

The writer registers itself in the catalog with the `Storage` capability so
acceptors can discover it.

## Design: one verified write path

All data reaches Iceberg through a single entry point,
`IcebergTableWriter::append_batches_with_marker`. It writes Parquet data files
and commits them together with a WAL idempotency marker in **one Iceberg
snapshot** (a single catalog compare-and-swap), then **verifies the commit
against the catalog** before reporting success.

The verification exists because the SQL catalog's CAS
(`UPDATE ... WHERE metadata_location = <previous>`) does not report a lost
race: if a concurrent committer (another writer node, the compactor) moved the
table's metadata pointer first, the losing commit matches zero rows and
`commit()` still returns `Ok`. The writer therefore never trusts the return
value — after every attempt it reloads the table and checks that its marker
landed. Only the marker decides success, so retries can never double-append and
a lost race can never silently drop data.

Consequences:

- **Do not add a write path that trusts `commit()`** — it can silently lose
  data under concurrent commits.
- Replay after a crash consults the marker
  (`IcebergTableWriter::load_committed_marker`) to distinguish "committed but
  not yet marked processed" from "never committed", making WAL processing
  idempotent end to end.

## Architecture

- **`WalProcessor`** (`processor.rs`): drains WAL entries per
  tenant/dataset/table, dedupes against the committed marker, and commits fresh
  entries in bounded chunks (`MAX_ENTRIES_PER_COMMIT`), marking each chunk
  processed before the next commit.
- **`IcebergTableWriter`** (`storage/iceberg.rs`): table handle management,
  wire→storage schema transformation, Parquet writing, and the verified
  append-with-marker commit loop with exponential-backoff retries
  (`RetryConfig`).
- **`IcebergWriterFlightService`** (`flight_iceberg.rs`): Arrow Flight endpoint
  for inter-service ingestion; drives the background `WalProcessor` loop
  (5s base interval, exponential backoff up to 300s on repeated failures).
- **Schema transformation** (`schema_transform.rs`): converts v1 wire-format
  batches (raw OTLP columns) into the Iceberg storage schema per signal type.

## Usage

```rust
use writer::IcebergTableWriter;

let mut writer = IcebergTableWriter::new(
    &catalog_manager,
    object_store,
    "my_tenant".to_string(),
    "my_dataset".to_string(),
    "metrics_gauge".to_string(),
)
.await?;

// Appends the batches and records the WAL entry ids as the idempotency
// marker in one verified Iceberg commit.
writer
    .append_batches_with_marker("wal-writer-id", vec![(entry_id, record_batch)])
    .await?;

// After a crash: ids in the marker are durably committed even if the WAL
// never marked them processed.
let committed = writer.load_committed_marker("wal-writer-id").await?;
```

### Retry configuration

```rust
use std::time::Duration;
use writer::RetryConfig;

writer.set_retry_config(RetryConfig {
    max_attempts: 5,
    initial_delay: Duration::from_millis(200),
    max_delay: Duration::from_secs(10),
    backoff_multiplier: 2.5,
});
```

## Configuration

The writer is configured through the shared SignalDB configuration
(`signaldb.toml` / `SIGNALDB_*` environment variables):

```toml
[schema]
catalog_type = "sql"
# Only SQLite catalog URIs are supported (create_sql_catalog_with_builder
# rejects everything else).
catalog_uri = "sqlite:///.data/catalog.db"

[storage]
dsn = "s3://my-data-bucket/iceberg-tables/"

[wal]
wal_dir = ".data/wal"
```

Key environment variables:

- `WRITER_WAL_DIR`: WAL directory (default: `.wal/writer`)
- `SIGNALDB_*`: standard configuration overrides (see `signaldb.dist.toml`)

## Running

```bash
cargo run --bin signaldb-writer

# Custom port and WAL directory
cargo run --bin signaldb-writer -- --flight-port 50061
WRITER_WAL_DIR=/custom/wal/path cargo run --bin signaldb-writer
```

## Testing

```bash
# Unit + crate tests
cargo test -p writer

# End-to-end write path (append + marker verification)
cargo test -p writer --test test_e2e_simple
cargo test -p writer --test test_retry_logic

# Crash-replay idempotency (workspace integration suite)
cargo test -p tests-integration --test wal_replay_idempotency

# Benchmarks
cargo bench -p writer --bench iceberg_benchmarks --features benchmarks
cargo bench -p writer --bench connection_pool_benchmarks --features benchmarks
```

## Observability

Watch for these log signals in production:

- `Iceberg commit reported an error but the marker landed` — an ambiguous
  commit resolved as success by verification; harmless but worth tracking.
- `Iceberg commit reported success but the marker is absent (catalog CAS
  silently lost)` — a concurrent commit won the race; the writer retries with
  fresh metadata. Sustained occurrences indicate commit contention on a table.

```bash
# Debug logging
RUST_LOG=writer=debug cargo run --bin signaldb-writer
```

## Further reading

- [docs/architecture/overview.md](../../docs/architecture/overview.md) — write path
- [docs/architecture/storage-layout.md](../../docs/architecture/storage-layout.md) — WAL and Iceberg table layout
- [docs/operations/wal-persistence.md](../../docs/operations/wal-persistence.md) — WAL durability and recovery
