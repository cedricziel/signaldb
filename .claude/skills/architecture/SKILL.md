---
name: architecture
description: SignalDB architecture reference - FDAP stack, write/query data flow, service components, deployment models, and dual catalog system. Use when understanding how components fit together, data flow, or system design.
user-invocable: false
---

# SignalDB Architecture Reference

Read `docs/architecture/overview.md` for the write/query data flow diagrams,
per-service ports/capabilities, deployment models, multi-tenancy, and the dual
catalog system (service catalog vs. Iceberg catalog).

Read `docs/architecture/fdap.md` for what each of Flight/DataFusion/Arrow/
Parquet does and where SignalDB deviates from canonical FDAP (Iceberg table
format, WAL in front of the columnar path, semconv-based semantics).

For the native Query IR (`POST /api/v1/query`), read
`docs/users/querying-ir.md` (document shape, pipeline stages, envelopes) and
`docs/architecture/flight-communication.md`'s "Query IR Execution Notes"
(the `query_ir` Flight ticket and `src/querier/src/query/ir_planner.rs`
lowering internals: multi-table union via `CoercedTableProvider`, the
`histogram_quantile` collect-and-reinject, exception-event UDF resolution).

Schema evolution (traces/logs schema tracks `schemas.toml`, hop-by-hop vs.
additions-only) lives in `docs/architecture/storage-layout.md`; signal-table
provisioning (the writer's reconciler) lives in
`docs/operations/table-provisioning.md`.
