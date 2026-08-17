---
name: service-discovery
description: SignalDB service discovery - capability-based routing, ServiceBootstrap pattern, catalog schema, connection pooling, and heartbeat mechanism. Use when working with service registration, capability routing, or inter-service communication.
user-invocable: false
---

# SignalDB Service Discovery

Read `docs/architecture/service-discovery.md` for the full design:
capability-based routing table, the `ingesters`/`shards`/`shard_owners`
catalog schema (plus the multi-tenancy, `attribute_stats`, and
`schema_registries` tables sharing the same database), the
ServiceBootstrap registration/heartbeat/reaper sequence, and the
connection-pooling + round-robin discovery mechanics.

Key files: `src/common/src/catalog.rs` (`Catalog` trait + SQL),
`src/common/src/service_bootstrap.rs` (`ServiceBootstrap`),
`src/common/src/flight/transport.rs` (`InMemoryFlightTransport`,
`ServiceCapability`).
