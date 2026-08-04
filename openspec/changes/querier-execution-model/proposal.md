# Querier Execution Model: Streaming, Deadlines, Snapshot Pinning, Resource Fairness

## Why

The 2026-08-04 audit found the querier's execution semantics are accidental rather than contractual (epic #953/#954-adjacent findings #938, #931, #949, #941): results are fully materialized then fully IPC-buffered before "streaming" (peak memory = result set + encoded copy, buffered again in the router); the transport imposes a 30s per-request deadline from a value named `connection_timeout` while the querier's own budget is 60s, so 30–60s queries die client-side while the querier burns CPU (residual of #919); a query can observe two different Iceberg snapshots for the same table, with isolation resting on the `snapshots_to_keep` config discipline instead of code; and the memory pool is first-come-first-served with unlimited default per-tenant concurrency, so one heavy query starves every other tenant. These are user-observable promises — when bytes arrive, when a query fails and how that's reported, what data consistency a result has, and what one tenant can do to another — and changing them piecemeal produces exactly the incoherence #919 patched once already.

## What Changes

- **Streaming execution**: the querier executes queries as a stream and emits Flight result batches incrementally; the full result is never resident (neither decoded nor encoded) at once. Router HTTP endpoints consume incrementally. **BREAKING** (observable): errors can now surface mid-stream after initial batches, instead of always before the first byte.
- **One deadline, derived everywhere**: a single per-query deadline (the querier's `query_timeout`) governs all hops; client/transport request deadlines are derived from it (plus margin), never configured independently. Connection establishment gets its own separate connect-timeout. Exceeding the deadline cancels execution server-side (frees CPU and permits), and the failure reaches the caller with an attributable timeout error.
- **Snapshot-pinned queries**: each query resolves every table it touches exactly once and executes against that snapshot for its entire lifetime; results are single-snapshot-consistent per table. Removes correctness dependence on `snapshots_to_keep` sizing (which remains as the lifecycle knob it is).
- **Resource fairness with real defaults**: fair memory sharing across concurrent queries (spill-capable), bounded default per-tenant concurrency, bounded default memory limit. **BREAKING** default-behavior change: deployments relying on unlimited defaults must configure limits explicitly.

## Capabilities

### New Capabilities

- `query-execution-contract`: the querier's user/operator-observable execution semantics — incremental result delivery and mid-stream error reporting, the single-deadline model with server-side cancellation, per-query snapshot consistency, and multi-tenant resource-fairness guarantees with bounded defaults.

### Modified Capabilities

_None — no existing spec in `openspec/specs/` covers query execution semantics (query-ir-core covers the IR language surface, not execution)._

## Impact

- **Crates**: `querier` (do_get streaming, session/runtime construction, per-query deadline + cancellation, snapshot resolution), `router` (incremental Flight consumption in tempo/logql/promql/query endpoints; error mapping for mid-stream failures), `common` (flight transport deadline derivation, config: connect-timeout vs request deadline, new defaults), `tests-integration`.
- **Issues**: implements #938, #931, #949, #941; complements merged #919; interacts with #921 (bodyless Tempo errors — mid-stream error mapping must not regress it) and #939 (caching, orthogonal).
- **API surfaces**: Tempo/LogQL/PromQL HTTP responses keep their formats; failure timing/shape changes (mid-stream aborts) — **BREAKING** in error-observability terms.
- **Config**: `[querier]` gains/repurposes deadline + fairness keys; unlimited defaults for `memory_limit_mb` and `max_concurrent_queries_per_tenant` are replaced with bounded defaults (**BREAKING** behavior for deployments that relied on unlimited).
