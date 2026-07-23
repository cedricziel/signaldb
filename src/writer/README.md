# SignalDB Writer

Stateful persistence service. Receives Arrow record batches from acceptors via
Apache Arrow Flight `do_put`, journals them in its own Write-Ahead Log, and a
background `WalProcessor` (running every 5 seconds, with exponential backoff
on repeated failures) commits accumulated entries to Apache Iceberg tables in
object storage.

## Endpoint

| Protocol | Port | Purpose |
|----------|------|---------|
| Flight (gRPC) | 50061 | `do_put` ingestion of trace/log/metric batches |

The writer registers itself in the catalog with the `Storage` capability so
acceptors can discover it.

## Data flow

```mermaid
flowchart LR
    A[Acceptor] -->|Flight do_put| W[Writer WAL]
    W -->|WalProcessor, every 5s| I[Iceberg tables / Parquet]
```

Durability comes from the WAL: batches are acknowledged after the WAL write,
and entries are marked processed only after the Iceberg commit succeeds.

## Running

```bash
cargo run --bin signaldb-writer

# Custom port and WAL directory
cargo run --bin signaldb-writer -- --flight-port 50061
WRITER_WAL_DIR=/custom/wal/path cargo run --bin signaldb-writer
```

Key environment variables:

- `WRITER_WAL_DIR`: WAL directory (default: `.wal/writer`)
- `SIGNALDB_*`: standard configuration overrides (see `signaldb.dist.toml`);
  the writer needs `[storage]` and `[schema]` for the Iceberg catalog

## Testing

```bash
cargo test -p writer

# Benchmarks
cargo bench -p writer --bench iceberg_benchmarks --features benchmarks
cargo bench -p writer --bench connection_pool_benchmarks --features benchmarks
```

## Further reading

- [docs/architecture/overview.md](../../docs/architecture/overview.md) — write path
- [docs/architecture/storage-layout.md](../../docs/architecture/storage-layout.md) — WAL and Iceberg table layout
- [docs/operations/wal-persistence.md](../../docs/operations/wal-persistence.md) — WAL durability and recovery
