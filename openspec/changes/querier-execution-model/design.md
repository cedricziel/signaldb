# Design: Querier Execution Model

## Context

See proposal.md — Why. Load-bearing code facts (audit-verified, 2026-08-04):

- `do_get` path: `df.collect()` (`src/querier/src/flight.rs:1201`) → `batches_to_flight_data` builds a full `Vec<FlightData>` (`flight.rs:2071-2085`) → `stream::iter`; router endpoints collect the whole stream again before decoding (`src/router/src/endpoints/tempo.rs:696-706`, same in promql/logql/query). The router's Flight _proxy_ already streams (`src/router/src/endpoints/flight.rs:210`); client-side `FlightRecordBatchStream` usage exists in the SDK and grafana backend.
- Transport sets `Endpoint::timeout(connection_timeout=30s)` as a per-request deadline (`src/common/src/flight/transport.rs:104`, `:236`); querier `query_timeout` = 60s with a wall-clock wrapper around execution (`flight.rs:2042`); #919 fixed one masking instance, the incoherence remains structural. #921 tracks bodyless Tempo errors.
- Table resolution: `LiveIcebergSchema::table` loads fresh per reference, `DataFusionTable::new(t, None, None, None)` — snapshot bounds unused (`flight.rs:63-96`, `:114-153`).
- Runtime: `RuntimeEnvBuilder::with_memory_limit` ⇒ `TrackConsumersPool<GreedyMemoryPool>` (`flight.rs:337-347`); `memory_limit_mb` default `None`, `max_concurrent_queries_per_tenant` default `None` (`src/common/src/config/mod.rs:1227`, `:1232`); admission semaphore exists but is inert (`flight.rs:605-620`).
- Per-tenant session isolation (catalog-shadowing handling, per-request session clone) is correct and has regression tests — do not disturb.

Constraints: HTTP response _formats_ must stay Tempo/Loki/Prometheus-compatible; mid-stream failures must map to those APIs' error conventions without regressing #921's fix; ticket handlers are per-query, so streaming can be introduced per ticket type.

## Goals / Non-Goals

**Goals:**

- Bounded querier/router memory independent of result size; time-to-first-byte proportional to first batch.
- One deadline value with derived transport deadlines and true server-side cancellation.
- Per-query snapshot pinning as a code property.
- Fair-by-default multi-tenant resource envelope.

**Non-Goals:**

- Caching (table resolution TTL, file statistics) — #939, separate.
- Changing query languages/lowering or response schemas.
- Cross-table (multi-table) transactional consistency — pinning is per table.
- WebSocket/SSE tail streaming (epic #437).

## Decisions

**D1 — Streaming: `df.execute_stream()` + `FlightDataEncoderBuilder` in `do_get`; router endpoints consume via `FlightRecordBatchStream` and decode incrementally, aggregating only where the HTTP response format forces it.**
Where an endpoint's JSON shape requires full aggregation (e.g. Tempo trace assembly), the router bounds memory via existing row caps but still consumes incrementally (no `Vec<FlightData>` intermediary — decode as batches arrive). Alternative — keep collect() and only cap sizes — rejected: leaves time-to-first-byte and the double-copy in place.

**D2 — Mid-stream error mapping: before the first HTTP byte, errors map to proper status codes exactly as today (#921's contract); after streaming has begun on chunked endpoints, abort the HTTP response with a trailer/connection error and log attributably.**
The spec's "never silently truncated success" is the invariant; per-endpoint mechanics follow each API's conventions. Grafana clients treat aborted transfers as failures, which is the correct observable outcome.

**D3 — Deadline model: `query_timeout` is the single budget. The querier stamps a deadline at admission; execution runs under `tokio::time::timeout_at` with cooperative cancellation (dropping the stream cancels DataFusion tasks). The Flight client derives its request deadline as `remaining budget + margin` per call (gRPC deadline via request-level timeout metadata), and `Endpoint::timeout` is dropped in favor of `Endpoint::connect_timeout` for connection establishment only.**
Router derives its wait from the same budget it received/attached. Alternative — propagate an absolute deadline header cross-service — heavier; the derived-per-hop scheme achieves coherence with local clocks and margins and can evolve to header propagation later without spec change.

**D4 — Snapshot pinning: resolve each referenced table once at query setup (the ticket handler already knows its tables), pass the resolved snapshot id as the `end` bound to `DataFusionTable::new(t, None, Some(snapshot_ts/None), …)` equivalents, and reuse that provider instance for every reference in the plan.**
This rides the existing fork provider capability (snapshot bounds parameters exist and are simply unused). Composes with #939 later: a TTL cache stores resolved (table, snapshot) pairs; pinning semantics are unchanged by caching.

**D5 — Resource envelope: `FairSpillPool` instead of `GreedyMemoryPool`; defaults become `memory_limit_mb = min(50% of system RAM, 4096)` and `max_concurrent_queries_per_tenant = 8`; `None`/unlimited only by explicit config (`"unlimited"` sentinel or explicit large value).**
FairSpillPool divides the budget across active consumers and reserves headroom for spillable operators — directly the "heavy query does not starve tenants" scenario. Defaults are release-noted BREAKING behavior.

**D6 — Sequencing inside the change: D3 (deadline) and D5 (envelope) first — small, independently shippable, immediately fix #931/#941; then D4 (pinning); D1/D2 (streaming) last, endpoint by endpoint (raw SQL/query-IR ticket first, Tempo trace assembly last).**

## Risks / Trade-offs

- [Mid-stream aborts confuse HTTP clients] → D2 keeps pre-first-byte behavior identical (most failures — planning, admission, bad requests — still fail early); mid-stream abort is strictly better than today's silent 30s bodyless death; per-endpoint tests pin the mapping. #921 regression tests stay green.
- [Streaming holds Iceberg/DataFusion resources for the transfer duration] → Deadline (D3) bounds total lifetime; slow-consumer backpressure is bounded by gRPC flow control + deadline.
- [FairSpillPool changes perf profile of single-query workloads] → Benchmarks before/after; single-tenant homelab with one active query sees the full pool anyway (fair share of 1).
- [Bounded default concurrency throttles existing dashboards] → Default 8/tenant is above observed Grafana parallelism; admission rejections are attributable and the knob is documented.
- [Pinning races snapshot expiration] → Pinned snapshot is held only for query lifetime (≤ deadline); expiration retains `snapshots_to_keep` ≥ query horizon, and `compactor-partition-scoped-lifecycle`'s cleanup grace period covers execution — cross-referenced there.

## Migration Plan

1. Ship D3 + D5 (config/runtime changes; BREAKING defaults release-noted; rollback = config).
2. Ship D4 (pinning) — behavior-only, rollback by revert.
3. Ship D1/D2 per ticket type behind a `querier.streaming` flag defaulting on; rollback = flag off (collect path retained until the flag is removed one release later).

## Open Questions

- Exact default numbers for memory/concurrency (D5 proposes min(50% RAM, 4 GiB) and 8): tune from hive + benchmark data before release; changing the numbers does not change the contract.
- Whether to later propagate an absolute deadline header end-to-end (D3 note) — deferrable, contract-compatible.
