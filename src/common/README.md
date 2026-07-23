# SignalDB Common

Shared library crate used by every SignalDB service. It owns configuration
loading, service discovery, authentication, the Write-Ahead Log, Flight
schemas and transport, and the Iceberg catalog integration.

## Modules

| Module | Purpose |
|--------|---------|
| `auth/` | API key authentication, `TenantContext`, gRPC interceptor and HTTP middleware |
| `catalog.rs` | Service registry backed by PostgreSQL/SQLite with heartbeats |
| `catalog_manager.rs` | Shared Iceberg catalog lifecycle management |
| `cli.rs` | Common CLI arguments and subcommands for all service binaries |
| `config/` | `Configuration`: defaults → TOML → `SIGNALDB_*` environment variables |
| `dataset.rs` | Tenant/dataset model |
| `flight/` | Arrow Flight schemas, OTLP-to-Arrow conversions, transport with pooling |
| `iceberg/` | Iceberg table helpers and writers |
| `model/` | Shared data models (spans, traces) |
| `ratelimit.rs` | Per-tenant rate limiting |
| `schema/` | Iceberg table schemas and catalog creation |
| `self_monitoring/` | Telemetry for SignalDB's own services |
| `service_bootstrap.rs` | Service registration and capability advertisement |
| `storage.rs` | DSN-based object store creation (`file://`, `memory://`, `s3://`) |
| `tenant_api.rs` | Tenant admin API handlers |
| `testing/` | Test fixtures (behind the `testing` feature) |
| `wal/` | Write-Ahead Log: durable segments, replay, processed-marking |

## Configuration

Configuration is not documented here — the annotated reference is
[`signaldb.dist.toml`](../../signaldb.dist.toml) at the repo root, and the
struct definitions live in [`src/config/mod.rs`](src/config/mod.rs).

## Testing

```bash
cargo test -p common

# Focused module runs
cargo test -p common -- wal
cargo test -p common -- flight
```

## Further reading

- [docs/architecture/overview.md](../../docs/architecture/overview.md) — how services use these modules
- [docs/architecture/service-discovery.md](../../docs/architecture/service-discovery.md) — catalog and bootstrap
- [docs/architecture/flight-communication.md](../../docs/architecture/flight-communication.md) — Flight schemas and transport
- [docs/architecture/storage-layout.md](../../docs/architecture/storage-layout.md) — WAL and Iceberg layout
- [docs/users/authentication.md](../../docs/users/authentication.md) — tenant and API key model
