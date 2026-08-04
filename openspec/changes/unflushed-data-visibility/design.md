# Design: Unflushed Data Visibility (LSM Stage 2)

## Context

See proposal.md — Why. Builds on the `lsm-writer-memtable` change (resident
double-buffered groups, insert-time schema coercion, coercion helpers in
`common`). Facts that shape this design (verified in code during expert
review):

- The querier pins Iceberg metadata at table-resolution time:
  `LiveIcebergSchema::table()` (src/querier/src/flight.rs) loads the
  tabular, and `datafusion_iceberg`'s scan reads that in-memory copy — the
  cold scan is effectively a point-in-time snapshot whose `properties` the
  querier can read. Every query surface (Tempo, LogQL, PromQL, IR, raw
  SQL, profiles) funnels through this one resolution point.
- Iceberg snapshots record no WAL entry ids. The existing per-writer
  idempotency marker in table properties is _replaced_ every commit (and
  within a drain chunked at `MAX_ENTRIES_PER_COMMIT`), so it cannot answer
  "is entry X covered by snapshot S". Entry-id-based boundary resolution is
  not implementable; a monotonic watermark is.
- `datafusion_iceberg` reports `Inexact` filter pushdown unconditionally,
  so DataFusion always re-applies filters above the scan — writer-side
  predicate pushdown is an optimization, never a correctness requirement.
- Full schema coercion (timestamp ns→µs, JSON→Map attributes, `label_*`
  null-fill, `attr_tokens`) is what makes hot and cold arms union-able; a
  null-filled `attr_tokens` on the hot arm would make
  `array_has(attr_tokens, …)` NULL and silently drop every hot row from
  filtered LogQL queries. Stage 1 coerces at insert; the querier must still
  guarantee arm-schema equality against _its_ pinned schema.
- `ensure_table` runs at commit time, so before a group's first commit the
  Iceberg table does not exist and today's resolution returns
  table-not-found even though the writer holds the data.
- Writer discovery (`discover_services_by_capability`) issues a fresh
  catalog SQL query per call; the acceptor round-robins ingest across all
  Storage-capable writers, so one table's hot data spans **all** writers —
  fan-out and per-writer watermarks are load-bearing, not scale-out polish.
- The monolith is not in-process for Flight: `signaldb-bin` runs writer and
  querier as separate Flight servers over localhost TCP.
- There is no warnings channel in the router/tempo-api responses; only
  PromQL has a standard `warnings` field.

## Goals / Non-Goals

**Goals:**

- Read-your-writes at ack time, on every query surface, including before a
  table's first commit.
- Provable no-duplication _and_ no-omission across the flush boundary.
- Commit interval becomes a storage-shape knob (raising it is a follow-up
  config change, not part of this change).
- Hot scans bounded in bytes and authenticated; degradation observable.

**Non-Goals:**

- No removal of `do_action("flush")` (operational primitive; also there is
  no production query-path caller to remove).
- No cross-ingest dedup: at-least-once redelivery upstream can still
  produce duplicate rows, exactly as today.
- No writer-side query engine; the writer streams batches, the querier
  plans and filters.
- No in-process monolith fast path in this change (worth doing later; the
  localhost hop is acceptable first).

## Decisions

### D1: Per-group monotonic sequence + per-writer watermark in table properties

The writer assigns each memtable insert a `u64` sequence per
`(writer_id, tenant, dataset, table)`; groups drain FIFO, and each commit
writes `signaldb.hot.<writer_id>.seq = W` (last contiguously committed
sequence) via `update_properties` in the **same transaction** as
`append_data` — the existing idempotency marker already proves properties
and data commit under one catalog CAS. The marker itself is untouched; the
watermark is an additional cumulative key, one per writer (writer ids are
stable per WAL directory), so growth is bounded and multi-writer composes.
Rejected: entry-id sets (unbounded, and nothing in Iceberg can store them
cumulatively — the marker is replaced every commit).

### D2: Hot-first ordering makes the boundary airtight

The hybrid provider's scan (a) fans out hot scans and buffers results
tagged `(writer_id, seq)`, then (b) loads the Iceberg table, pinning
snapshot S, reads `W_S[writer]` from S's properties, and (c) drops hot
batches with `seq ≤ W_S[writer]`. Because W_S is read _after_ the hot scan,
`W_S ≥ W_at_hot_scan_time`: anything at or below W_S is in S (drop from hot
— no duplicate); anything above W_S was uncommitted and therefore still
resident when the hot scan ran (returned by hot — no omission; stage 1
already retains batches until `mark_processed`). A writer with no watermark
key yields `W = 0` (keep all hot). The boundary is derived per provider
instance — deriving it from a separately loaded catalog copy reopens the
race. Rejected: plan-time pin + entry-id filtering (loses rows committed
between planning and execution — strictly worse than duplicating, and
invisible in tests); per-query snapshot-reload avoidance via a writer-side
committed-batch ring (viable fallback if the extra `load_tabular` per scan
proves expensive on S3, but it adds retention semantics — start simple).

### D3: One chokepoint — a hybrid provider from `LiveIcebergSchema::table()`

The union is built where every surface already resolves tables, so Tempo,
LogQL, PromQL, IR, profiles, and raw SQL get hot data without per-path
work, and future surfaces cannot miss it. Shape: `schema()` = the pinned
Iceberg Arrow schema verbatim; `scan()` = hot fetch (async, filters
available → mandatory time bounds + projection extracted here), then cold
resolution per D2, then `UnionExec` of the two arms with identical schemas;
`supports_filters_pushdown` = `Inexact` for everything (DataFusion keeps a
FilterExec above — correctness never depends on writer-side filtering);
`statistics()` = unknown/inexact (never report cold stats as exact — join
ordering would exclude hot rows). Eager hot fetch inside `scan()` also
gives degradation a clean home: failures are known before the plan
executes, avoiding the unanswerable "downgrade a running stream" problem.
Missing Iceberg table + resident hot data → hot-only provider on the
canonical schema from `common::iceberg::schemas` with `W = 0`.

### D4: Arm-schema equality enforced querier-side

Stage 1 coerces at insert, but the table's schema can drift between insert
and query (attribute promotion adds `label_*` columns), and different
writers may momentarily hold different views. The querier therefore
re-coerces hot batches against **its pinned schema** using the shared
helpers from `common`, and the provider asserts field-for-field equality
(including nullability and derived columns like `attr_tokens`) before
building the union. Tests must include a LogQL attribute-equality query
proving hot rows survive the `attr_tokens` conjunct, and hot/cold
`date_bin` bucketing agreement (µs vs ns).

### D5: Bounded hot scans, cached discovery

Time bounds are mandatory in the ticket (extracted from `scan()` filters;
observability queries always carry them) and the writer prunes
non-overlapping batches via min/max timestamps tracked per batch at insert;
responses carry a hard byte cap. Buffered hot bytes are registered against
the querier's DataFusion memory pool — the hot buffer must not be invisible
to `session_context_with_limits`. Writer discovery is cached in the querier
with a TTL tied to the heartbeat interval (today it is a catalog SQL query
per call — unacceptable per user query, especially on SQLite). Fan-out
covers **all** Storage-capable writers (ingest round-robins, so every
writer may hold any table's hot data) with a per-writer timeout;
timeout/failure → degrade per spec. A per-request table-resolution cache
keeps multi-reference queries (self-joins, multi-statement trace flows)
from repeating hot scans.

### D6: Hot scans are authenticated and scoped

The `do_get` ticket is honored only with valid internal-service
authentication (same requirement as `do_put`), and the ticket's tenant is
validated against the authenticated caller's scope — presence of _a_
tenant is not enough. Without this, a writer deployed without
`internal_service_key` (warned-but-served today) would expose every
tenant's unflushed data to any network peer. The `_system` tenant's
anti-loop guard (#760) applies to the scan path.

### D7: Degradation surfaces through telemetry, not invented response fields

Tempo search and Loki `query_range` have no standard partial-result field,
and Grafana would not render one; only PromQL has `warnings`. So the
observable contract is: `querier_hot_scan_failures_total` metric +
`signaldb.query.hot_scan_degraded` span attribute on every surface, plus
the `warnings` field on the PromQL path. A writer still replaying after
restart reports "warming" and is treated as degraded (its hot data is
incomplete; committed data still serves).

### D8: Rejected simpler alternatives (recorded so they are not re-proposed)

- **Time-watermark visibility** (cold excludes last N seconds, hot serves
  them exclusively): observability data is event-timestamped, not
  ingest-timestamped — a backdated span landing in Iceberg would be
  excluded by the cold filter and absent from hot: permanently invisible.
  Also converts writer unavailability into a data hole.
- **Row-identity dedup (DISTINCT over the union)**: logs have no identity;
  two identical lines are legitimately two rows. Plus a full hash
  aggregate over every result.
- **Writer-side union**: moves the query engine into the writer.
- **Stage 1 only (queryable-at-commit)**: viable fallback, but forfeits
  raising `commit_interval` — the honest framing of this change's value.

## Risks / Trade-offs

- [Extra `load_tabular` per scan (hot-first ordering)] → measure; fallback
  is a bounded writer-side committed-batch ring covering the
  planning→execution window (D2).
- [Hot buffer memory in the querier] → mandatory time bounds, response
  byte cap, registration in the DataFusion memory pool (D5).
- [Watermark property lost by a lifecycle commit would resurrect committed
  rows as duplicates until the next writer commit] → compactor regression
  test that compaction/expiration preserves `signaldb.hot.*` (properties
  merge on update, but pin it with a test).
- [Query-path dependency on writers] → degrade-not-fail with per-writer
  timeouts; committed data always serves; degradation observable (D7).
- [Restart/rollout windows: acked data in neither memtable (replaying) nor
  Iceberg] → replaying writers report warming → degraded, and stage 1's
  incremental replay bounds the window; documented in ops docs.

## Migration Plan

- Depends on `lsm-writer-memtable` being deployed. Land writer-side
  sequence + watermark + `do_get` first (additive, inert), then the
  querier hybrid provider (tolerates writers without the endpoint via
  degrade-not-fail), then migrate the five flush-barrier test suites.
- Raising `commit_interval` defaults is a deliberate follow-up config
  change after bake-in, not part of this change.
- Rollback: disable the hybrid provider (config flag) → behavior returns
  to queryable-at-commit; no data implications either way.

## Open Questions

- Whether general predicate pushdown (beyond time bounds + projection) to
  the writer is worth adding later — pure optimization under `Inexact`
  semantics; measure first.
- Hot-scan response byte cap default — pick against querier memory-pool
  defaults during implementation.
