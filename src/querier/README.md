# SignalDB Querier

Query execution engine. Serves SQL queries over the stored Iceberg tables
using DataFusion, exposed to other services (primarily the router) via an
Apache Arrow Flight endpoint.

## Endpoint

| Protocol | Port | Purpose |
|----------|------|---------|
| Flight (gRPC) | 50054 | Query execution (`do_get` with a SQL ticket) |

The querier registers itself in the catalog with the `QueryExecution`
capability so routers can discover it.

## Running

```bash
cargo run --bin signaldb-querier

# Custom port
cargo run --bin signaldb-querier -- --flight-port 50054
```

Configuration comes from `signaldb.toml` / `SIGNALDB_*` environment variables
(see `signaldb.dist.toml`). The querier needs access to the same `[storage]`
object store and `[schema]` Iceberg catalog the writer persists to.

## Testing

```bash
cargo test -p querier

RUST_LOG=debug cargo test -p querier -- --nocapture
```

## Further reading

- [docs/users/querying-sql.md](../../docs/users/querying-sql.md) — writing SQL queries against SignalDB
- [docs/architecture/overview.md](../../docs/architecture/overview.md) — query path
- [docs/architecture/storage-layout.md](../../docs/architecture/storage-layout.md) — the tables the querier reads
- [docs/architecture/flight-communication.md](../../docs/architecture/flight-communication.md) — Flight protocol usage
