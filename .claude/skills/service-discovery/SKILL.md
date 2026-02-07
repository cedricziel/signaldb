---
name: service-discovery
description: SignalDB service discovery - capability-based routing, ServiceBootstrap pattern, catalog schema, connection pooling, and heartbeat mechanism. Use when working with service registration, capability routing, or inter-service communication.
user-invocable: false
---

# SignalDB Service Discovery

## Capability-Based Routing

Services register with specific capabilities for automatic routing:

| Service | Capabilities | Discovery Pattern |
|---------|-------------|------------------|
| Acceptor | `TraceIngestion` | Clients connect directly via OTLP |
| Writer | `TraceIngestion`, `Storage` | Acceptors discover via `Storage` capability |
| Router | `Routing` | Clients connect directly via HTTP |
| Querier | `QueryExecution` | Routers discover via `QueryExecution` capability |

## ServiceBootstrap Pattern

Every service uses `ServiceBootstrap` at startup:

1. Connects to service catalog (SQLite or PostgreSQL from `[discovery]` or `[database]` DSN)
2. Generates unique UUID `service_id`
3. Registers in `ingesters` table with service_type, address, capabilities (comma-separated)
4. Spawns background heartbeat task updating `last_seen`
5. On shutdown: deregisters and stops heartbeat

```rust
let bootstrap = ServiceBootstrap::new(config).await?;
bootstrap.register().await?;
// ... service runs ...
// Automatic deregistration on drop/shutdown
```

## Service Catalog Schema

```sql
CREATE TABLE ingesters (
    id UUID PRIMARY KEY,
    address TEXT NOT NULL,
    last_seen TIMESTAMP WITH TIME ZONE,
    stopped_at TIMESTAMP WITH TIME ZONE
);
```

## Discovery Mechanism

- **InMemoryFlightTransport**: Connection pooling (max 50 connections, 30s timeout, 5min expiry) + capability-based client lookup
- **ServiceRegistry** (Router-specific): Cached HashMap of services, polls catalog at configurable interval
- **Service selection**: Currently picks first available (round-robin planned)
- **TTL-based cleanup**: Stale services auto-removed

## Configuration

```toml
[database]
dsn = "sqlite://.data/signaldb.db"

[discovery]
dsn = "sqlite://.data/signaldb.db"   # Falls back to [database].dsn
heartbeat_interval = "30s"
poll_interval = "60s"
ttl = "300s"
```

## Key Implementation Files

| File | Purpose |
|------|---------|
| `src/common/src/catalog.rs` | Catalog trait + implementations |
| `src/common/src/service_bootstrap.rs` | ServiceBootstrap registration |
| `src/common/src/flight/transport.rs` | InMemoryFlightTransport, connection pooling |
| `src/router/src/service_registry.rs` | Router's cached service registry |
