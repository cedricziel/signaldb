# SignalDB Acceptor

OTLP ingestion service. Receives traces, logs, and metrics via OTLP gRPC
(OTLP HTTP trace ingestion exists only as a non-persisting stub), authenticates
the tenant, writes each request to a Write-Ahead Log for durability, and
forwards batches to writer services over Apache Arrow Flight.

## Endpoints

| Protocol | Port | Path / Service | Notes |
|----------|------|----------------|-------|
| gRPC | 4317 | OTLP `TraceService`, `LogsService`, `MetricsService` | Full OTLP export support |
| HTTP | 4318 | `POST /v1/traces` | Stub: accepts JSON and returns 200, does not persist |
| HTTP | 4318 | `POST /api/v1/write` | Prometheus remote write (protobuf + snappy) |
| HTTP | 4318 | `GET /health` | Liveness check |

The OTLP gRPC services and the Prometheus remote-write endpoint require
authentication headers (`Authorization: Bearer <api-key>`, `X-Tenant-ID`,
optional `X-Dataset-ID`); the `/v1/traces` HTTP stub is unauthenticated.
See [docs/users/authentication.md](../../docs/users/authentication.md).

## Running

```bash
# Standalone service
cargo run --bin signaldb-acceptor

# With custom ports and WAL directory
cargo run --bin signaldb-acceptor -- --grpc-port 4317 --http-port 4318
ACCEPTOR_WAL_DIR=/custom/wal/path cargo run --bin signaldb-acceptor
```

Key environment variables:

- `ACCEPTOR_WAL_DIR`: WAL directory (default: `.wal/acceptor`)
- `SIGNALDB_*`: standard configuration overrides (see `signaldb.dist.toml`)

## Testing

```bash
cargo test -p acceptor

# Send a test trace via HTTP
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <api-key>" \
  -H "X-Tenant-ID: <tenant>" \
  -d '{"resourceSpans":[]}'
```

## Further reading

- [docs/architecture/overview.md](../../docs/architecture/overview.md) — where the acceptor sits in the write path
- [docs/users/sending-otlp.md](../../docs/users/sending-otlp.md) — client setup for OTLP export
- [docs/users/prometheus-remote-write.md](../../docs/users/prometheus-remote-write.md) — metrics via remote write
- [docs/operations/wal-persistence.md](../../docs/operations/wal-persistence.md) — WAL durability and recovery
