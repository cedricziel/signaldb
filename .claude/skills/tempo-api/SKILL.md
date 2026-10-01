---
name: tempo-api
description: SignalDB Tempo API compatibility - implemented/stub endpoints, query flow, admin API, Grafana native plugin, and built-in Tempo datasource support. Use when working with HTTP API, Grafana integration, or query endpoints.
user-invocable: false
sources:
  - src/router/src/endpoints/tempo.rs
  - src/router/src/endpoints/tenants.rs
  - src/router/src/endpoints/pyroscope.rs
  - src/querier/src/query/trace.rs
  - src/querier/src/query/tags_to_ir.rs
  - src/querier/src/query/ir_planner.rs
  - src/ql-ir/**
  - src/querier/src/flight.rs
  - src/grafana-plugin/src/**
  - src/grafana-plugin/backend/src/**
---

# SignalDB Tempo API Compatibility

Read `docs/users/tempo-api-reference.md` for the endpoint list, status
(implemented/partial/501), the TraceQL subset and its 400-vs-501 rejection
classes, tag-discovery time-window semantics, span-field extras, error
mapping, and the standalone Tempo gRPC querier protocol (`tempopb.Querier` on
the Flight port). Read `docs/users/grafana-datasource.md` for wiring Grafana's
built-in Tempo/Loki datasources and building/installing the native SignalDB
plugin (pnpm, `pnpm run build:backend`) — including its current limitation
(fixed Flight tickets, router answers with empty placeholder results).

Admin API and tenant self-service API endpoints are the `multi-tenancy`
skill's domain. Flight ticket grammar (`find_trace:...`, `search_traces:...`,
`query_ir:...`) and the querier's DataFusion session options
(`querier::session_config_from`) are documented in
`docs/architecture/flight-communication.md`.

SignalDB's native, non-dialect query surface is the Query IR
(`POST /api/v1/query`, `docs/users/querying-ir.md`; the supported `irVersion`
range is `MAX_IR_VERSION` in `src/query-ir/src/version.rs`) — Tempo/LogQL/
Prometheus are compatibility dialects that sit alongside it. They are
projections onto the same logical schema: `ql-ir` lowers TraceQL, LogQL and
PromQL to IR documents, Tempo `tags` lower through
`querier/query/tags_to_ir.rs`, and the querier's `ir_planner` runs them all;
LogQL keeps a fallback lowering only for what `ql-ir` refuses.
