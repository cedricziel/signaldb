# SignalDB Router

Stateless HTTP gateway. Exposes the Grafana Tempo-compatible query API,
discovers backend services through the catalog, and forwards queries to
queriers over Apache Arrow Flight.

## Endpoints

| Protocol | Port | Purpose |
|----------|------|---------|
| HTTP | 3000 | Tempo-compatible API under `/tempo` (`/tempo/api/traces/{trace_id}`, `/tempo/api/search`, tag endpoints) and tenant admin API |
| Flight (gRPC) | 50053 | Inter-service Arrow Flight |

The full endpoint list, including which Tempo endpoints are implemented versus
stubs, lives in
[docs/users/tempo-api-reference.md](../../docs/users/tempo-api-reference.md).

## Running

```bash
cargo run --bin signaldb-router

# Custom ports
cargo run --bin signaldb-router -- --http-port 3000 --flight-port 50053
```

Configuration comes from `signaldb.toml` / `SIGNALDB_*` environment variables
(see `signaldb.dist.toml`). The router is stateless and scales horizontally;
it needs a shared catalog (`[discovery]`) to find queriers.

## Testing

```bash
cargo test -p router

# Unauthenticated smoke test against a running instance
curl http://localhost:3000/health

# Tempo endpoints require auth headers
curl http://localhost:3000/tempo/api/echo \
  -H "Authorization: Bearer <api-key>" -H "X-Tenant-ID: <tenant>"
curl "http://localhost:3000/tempo/api/search?limit=10" \
  -H "Authorization: Bearer <api-key>" -H "X-Tenant-ID: <tenant>"
```

## Further reading

- [docs/users/tempo-api-reference.md](../../docs/users/tempo-api-reference.md) — HTTP API reference
- [docs/users/grafana-datasource.md](../../docs/users/grafana-datasource.md) — Grafana integration
- [docs/architecture/overview.md](../../docs/architecture/overview.md) — query path
- [docs/architecture/service-discovery.md](../../docs/architecture/service-discovery.md) — capability-based routing
