---
name: service-discovery
description: SignalDB service discovery - capability-based routing, ServiceBootstrap pattern, catalog schema, connection pooling, and heartbeat mechanism. Use when working with service registration, capability routing, or inter-service communication.
user-invocable: false
sources:
  - docs/architecture/service-discovery.md
  - src/common/src/catalog.rs
  - src/common/src/service_bootstrap.rs
  - src/common/src/flight/transport.rs
---

# SignalDB Service Discovery

Read `docs/architecture/service-discovery.md` for the full design:
capability-based routing table, the `ingesters`/`shards`/`shard_owners`
catalog schema (plus the multi-tenancy, user-identity, `attribute_stats`,
`attribute_value_stats`, `attribute_types`, and `schema_registries` tables
sharing the same database), the ServiceBootstrap registration/heartbeat/reaper
sequence, on-disk SQLite pool sizing and busy retries, and the
connection-pooling + round-robin discovery mechanics (the acceptor→writer
forward uses rendezvous hashing on `ingest_id` instead; see
`docs/architecture/flight-communication.md`).

Register through `ServiceBootstrap::from_bind_addr()`, which applies the
`<SERVICE>_ADVERTISE_ADDR` override; `ServiceBootstrap::new()` registers its
address verbatim. Advertised addresses for containers and multi-host setups:
`docs/operations/binaries.md#advertised-addresses`.

Key files: `src/common/src/catalog.rs` (`Catalog` trait + SQL),
`src/common/src/service_bootstrap.rs` (`ServiceBootstrap`),
`src/common/src/flight/transport.rs` (`InMemoryFlightTransport`,
`ServiceCapability`).
