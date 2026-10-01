---
name: architecture
description: SignalDB architecture reference - FDAP stack, write/query data flow, service components, deployment models, and dual catalog system. Use when understanding how components fit together, data flow, or system design.
user-invocable: false
sources:
  - docs/architecture/overview.md
  - src/signaldb-bin/src/**
  - src/query-ir/src/**
  - src/querier/src/query/ir_planner.rs
  - src/router/src/endpoints/query.rs
  - Cargo.toml
---

# SignalDB Architecture Reference

Read `docs/architecture/overview.md` for the workspace crate table, the
write/query data flow (telemetry processors, per-`(tenant, dataset, signal)`
WALs, the typed attribute split, the writer's table reconciler), per-service
ports/capabilities, the router's query surfaces and UI/session/OIDC endpoints,
the compactor's lifecycle tasks and per-table locking, deployment models,
multi-tenancy, and the dual catalog system (service catalog vs. Iceberg
catalog, the latter on SQLite or PostgreSQL).

Read `docs/architecture/fdap.md` for what each of Flight/DataFusion/Arrow/
Parquet does and where SignalDB deviates from canonical FDAP (Iceberg table
format, WAL in front of the columnar path, semconv-based semantics).

For the native Query IR (`POST /api/v1/query`), read
`docs/users/querying-ir.md` (document shape, pipeline stages, envelopes,
multi-query formulas, pagination) and
`docs/architecture/flight-communication.md`'s "Query IR Execution Notes"
(the `query_ir` Flight ticket and `src/querier/src/query/ir_planner.rs`
lowering internals: Metric Series via `metric_ops`, `safe_ident` result
aliasing, exception-event UDF resolution, physical-name rejection).

Schema evolution, the logical schema, and the declared sort order live in
`docs/architecture/storage-layout.md`; signal-table provisioning (the writer's
reconciler) lives in `docs/operations/table-provisioning.md`; compactor
behavior in `docs/operations/compactor/`.
