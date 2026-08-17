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
watermark is an additional cumulative key, one per writer, so growth is
bounded and multi-writer composes.

`writer_id` here is the WAL-persisted identity
(`Wal::load_or_create_writer_id`), stable across restarts — not the
per-incarnation ServiceBootstrap UUID. Note that this identity is per **WAL
directory**, which since #1299 means per `(tenant, dataset, signal)`, not
per writer process: `signaldb.hot.<writer_id>.seq` is therefore one key per
WAL feeding the table (still bounded — a tenant's own WAL plus, at most,
the adopted legacy root one), and a querier fanning out to a single writer
node will see several `writer_id`s from it. Stage 2 must size
its fan-out and its watermark handling for that, not for one id per node. **Sequences stay strictly
increasing across restarts via an epoch**: a counter persisted in
the WAL directory alongside `writer_id` is incremented once per writer
start, and sequences are `(epoch << 32) | counter`. Replay reassigns
sequences in the new epoch, so every replayed or fresh batch numbers above
any watermark a previous incarnation committed — a restart can never
produce a resident batch whose sequence falls at or below an existing
watermark. Allocation is atomic per group; the contiguous-prefix rule for
chunked commits holds within an epoch, which suffices because a drain
never spans a restart. **Overflow is handled, not assumed away**: when a
group's counter approaches saturation (2³² allocations in one epoch — a
long-lived writer can reach this), the writer persists an epoch increment
and rolls forward mid-run, keeping sequences monotonic; if the epoch space
itself is ever exhausted, sequence allocation fails closed and ingest is
rejected retryably rather than wrapping below an existing watermark.
Boundary tests cover counter saturation and the epoch roll.

**Replay reconciles already-committed entries before serving them**: a
crash after an Iceberg commit but before `mark_processed` leaves WAL
entries pending whose rows are already in the table. Blindly reassigning
those entries new-epoch sequences (above every watermark) would make the
hot filter keep them — a guaranteed duplicate. During startup replay,
before a group becomes servable, the writer reconciles pending entries
against its durable commit evidence (the idempotency marker, which by
construction covers exactly the commit whose marks may be missing) and
marks covered entries processed instead of inserting them as scannable.
The "warming" state (D7) keeps the scan surface degraded until this
reconciliation completes. A crash-after-commit-before-mark test asserts no
duplicate rows are served.

**Concurrent watermark commits**: on a catalog CAS conflict, the writer
reloads the latest table metadata, reapplies only its own
`signaldb.hot.<writer_id>.seq` key plus its data files, and retries — it
never rewrites other writers' keys — and after commit verifies the
committed metadata carries its new watermark. A two-writer race test
asserts both watermarks and both data sets survive interleaved commits.

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
already retains batches until `mark_processed`). The proof leans on one
explicit freshness assumption: the catalog load in (b) observes every
commit that completed before the load began (read-after-commit). The SQL
catalog provides this — snapshot resolution is a single-row read of the
current table-metadata pointer — and the spec states it as a requirement
so an alternative catalog backend cannot silently weaken it.

**Missing-watermark semantics (fail closed on inconsistency):** each hot
scan response also carries the writer's own last-committed watermark per
group, `W_writer`. The querier filters against
`max(W_S[writer], W_writer)` — using the writer's self-report is always
safe because it only ever excludes rows the writer itself has already
committed. The two sources also cross-check: no table key and
`W_writer = 0` means the writer genuinely never committed to this table
(common in multi-writer: keep all hot — required for first-run
visibility); no table key but `W_writer > 0` means the table lost its
watermark metadata — the boundary is unresolvable, that writer's hot
batches are dropped, and degradation is recorded. `W = 0` is therefore
reserved for the true never-committed state, never inferred from missing
metadata alone.

The boundary is derived per provider instance — deriving it from a
separately loaded catalog copy reopens the race. Rejected: plan-time pin +
entry-id filtering (loses rows committed between planning and execution —
strictly worse than duplicating, and invisible in tests); per-query
snapshot-reload avoidance via a writer-side committed-batch ring (viable
fallback if the extra `load_tabular` per scan proves expensive on S3, but
it adds retention semantics — start simple).

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
non-overlapping batches via min/max timestamps tracked per batch at
insert. **When `scan()` cannot derive finite time bounds** — raw SQL
without a time predicate can reach it, and `max_sql_rows` applies after
planning so it bounds nothing here — the provider contacts no writers:
the query serves committed data only and records degradation telemetry
(tested with an unbounded-query case). Responses carry a hard byte cap
with fail-closed semantics: a writer that would exceed the cap (including
a single over-cap batch) signals truncation instead of sending a partial
set, and the querier treats a truncated response as an unresolvable
boundary for that writer — drop its hot data, record degradation — never
merging a silently partial hot arm. Continuation/pagination is a possible
follow-up optimization, not part of this change. On top of the per-writer
cap there is a **query-wide hot-buffer budget** — a fixed absolute
configuration value, deliberately independent of writer count (without
it, exposure would scale as `writer_count × per_writer_cap`). Admission
is atomic per writer arm: the provider reserves a response's bytes before
buffering it and admits a writer's hot arm only if it fits completely
within the remaining budget; an arm that does not fit is discarded whole
and marked unresolved (degradation recorded) — a partially buffered arm
would silently omit acknowledged rows without marking that writer
degraded. DataFusion memory-pool registration accounts the bytes but does
not define admission, so the budget does. Writer discovery avoids the per-query catalog discovery SQL
(unacceptable per user query, especially on SQLite) **without a staleness
window**: registrations bump a monotonic routing generation in the
catalog; the querier caches the writer set keyed by that generation and
re-reads only the cheap generation scalar per query, refetching the full
set when it changes. A newly joined writer is therefore included in the
first fan-out after it registers — TTL-only caching was rejected because
its staleness window contradicts the no-omission contract for data
acknowledged before query execution. A writer-joins-then-immediate-query
test pins this. Fan-out covers **all** Storage-capable writers (ingest
round-robins, so every writer may hold any table's hot data) with a
per-writer timeout; timeout/failure → degrade per spec. A per-request
table-resolution cache keeps multi-reference queries (self-joins,
multi-statement trace flows) from repeating hot scans.

### D6: Hot scans are authenticated and scoped

The `do_get` ticket is honored only with valid internal-service
authentication (same requirement as `do_put`), and the ticket's tenant is
validated against the authenticated caller's scope — presence of _a_
tenant is not enough. Without this, a writer deployed without
`internal_service_key` (warned-but-served today) would expose every
tenant's unflushed data to any network peer. The `_system` tenant's
anti-loop guard (#760) applies to the scan path. Transport encryption is
out of scope here: inter-service Flight channels are plaintext today for
`do_put` and every other RPC, and the hot scan inherits that
deployment-wide posture rather than forking it per-endpoint — requiring
TLS/mTLS across all service-to-service Flight links is a real hardening
item, but it belongs to a dedicated transport-security change covering
every channel, not a rider on this one. The dependency is stated
explicitly as a deployment prerequisite: hot scans return tenant data, so
inter-service Flight links MUST run on a trusted private network segment
(the same boundary `do_put` already requires for confidentiality) until
the transport-security change lands, and that change must cover `do_get`;
the ops docs (task 5.2) state this boundary requirement.

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
