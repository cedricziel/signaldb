# Design: LSM-Style Writer Memtable (Stage 1)

## Context

See proposal.md — Why. Current state that shapes the approach (verified
against the code; corrected after expert review):

- `WalProcessor::drain_pending` (src/writer/src/processor.rs) rebuilds its
  pending groups from disk every tick: `get_unprocessed_entries` →
  `read_entry_data` → deserialize. `read_entry_data` linearly scans every
  segment's entry list under the WAL lock (src/common/src/wal/mod.rs), so a
  P-entry backlog over E-entry segments costs O(P·E) per tick. Entries
  deferred by the coalescing floor are re-decoded each tick (in-code NOTE
  at processor.rs:222).
- Today's per-tick full re-read is also the writer's _failure handling_:
  failed commits retry because everything unprocessed is re-read; poison
  entries accumulate failure counts (`MAX_ENTRY_FAILURES = 10`) until
  dead-lettered. Any memtable design must preserve this self-healing.
- `Wal::append` only buffers in memory; durability comes from
  `wal.flush()` (invoked on the `do_put` path in flight_iceberg.rs).
  `mark_processed` fails for entries that exist in no flushed segment.
- `do_put` applies the v1→v2 `transform_for_signal`, but full coercion to
  the Iceberg table's Arrow schema (timestamp ns→µs, JSON→Map attributes,
  materialized `label_*` null-fill, `attr_tokens`) happens only at commit
  time in `coerce_batch_to_schema` (src/writer/src/storage/iceberg.rs).
- `do_action("flush")` has no production caller — only the test-gated
  `common::testing::flush_storage_writers` used by five integration tests.
  Query paths do not force commits today; this change does not claim
  otherwise and does not alter visibility semantics.
- The background loop holds the processor mutex across the entire drain,
  including Iceberg commits; the acceptor has its own WAL and retry
  consumer (src/acceptor/src/handler/wal_retry.rs), so a retryable
  rejection from the writer is absorbed upstream without client data loss.
- The acceptor is _not_ part of this change: its hot path already forwards
  the decoded in-memory batch (handler/forward.rs); only the retry consumer
  reads its WAL back.
- WAL format and offset-authoritative append semantics (#883) are
  load-bearing and unchanged.

## Goals / Non-Goals

**Goals:**

- Decode-once ingest: steady-state commits never read WAL payloads back;
  payload reads are labeled recovery / reconcile / dead-letter and
  observable.
- Preserve today's failure semantics exactly: failed commits retry, poison
  entries dead-letter after the same failure budget, WAL segments still
  become fully-processed and reclaimable.
- Bounded memory under every condition: steady state, bursts, catalog
  outage, and startup replay of an arbitrarily large backlog.
- Ingest ack latency never couples to catalog/object-store latency.

**Non-Goals:**

- No visibility changes: data remains queryable at commit, and the
  force-commit primitive is untouched. Querier-visible hot data is the
  follow-up change `unflushed-data-visibility`.
- No acceptor changes; no WAL on-disk format changes; no change to
  per-entry `mark_processed` granularity.
- No sorting/dedup-at-flush improvements (natural follow-up once batches
  are resident).

## Decisions

### D1: Memtable = per-group batches, not a sorted tree

Ingest arrives as columnar Arrow batches and queries are scans, not keyed
lookups — a RocksDB-style skiplist/B-tree would tear batches into rows for
no read benefit. Groups keyed by `(tenant, dataset, table)` (matching the
existing coalescer and idempotency-marker granularity) hold decoded
`RecordBatch`es plus per-entry bookkeeping (WAL entry ids, trace links —
what `TableBatch` carries today).

### D2: Insert after `wal.flush()`, via one shared routing function

The memtable insert happens on the `do_put` path only after the durable WAL
flush returns Ok — insert-after-append would let the memtable hold entries
that exist in no segment, making `mark_processed` fail _after_ a successful
Iceberg commit (the state the marker protocol exists to avoid). Routing to
`(tenant, dataset, table)` is extracted into a single function used by both
the ingest path and replay (today `do_put` and `determine_target_table`
compute it independently, including the metadata tenant override and
`metrics_gauge` fallback — two implementations would let the same batch
land in different tables across a restart).

### D3: The memtable is a reconciled cache, not the sole source of truth

The drain reads resident groups, but the WAL remains authoritative for
"what is unprocessed." Each tick the processor fetches unprocessed entry
_metadata_ (cheap — no payload reads), diffs ids against resident ids, and
lazily loads payloads for the difference. Eviction happens only on
`mark_processed` success or dead-letter (dead-letter also releases byte
accounting). Consequences: a commit that fails after draining retries from
memory; an entry that failed routing/decoding at replay is revisited and
accumulates failures toward dead-lettering; existing processor tests that
append directly to the WAL and expect a drain keep passing. Alternative
rejected: pure drain-from-memory ("WAL recovery-only"), which silently
strands data on any commit failure and breaks segment reclamation for
poison entries.

### D4: Soft budget signals the loop; hard ceiling backpressures ingest

Config: `[writer] memtable_soft_bytes` (pressure target),
`memtable_hard_bytes` (reject threshold, e.g. 2× soft),
`max_uncommitted_bytes` per group (byte sibling of the existing
`max_uncommitted_rows`, so the global budget is a safety net rather than
the steady-state trigger — dozens of groups at the row ceiling would
otherwise sit in permanent pressure). Accounting via Arrow
`get_array_memory_size` (approximate; ops docs say to leave headroom).
Crossing the soft budget notifies the commit loop, which flushes
largest-group-first until under budget; pressure work never runs inline in
`do_put`. At the hard ceiling, `do_put` returns a retryable gRPC error
(`RESOURCE_EXHAUSTED`) — the acceptor's WAL retry consumer redelivers, so
this is flow control, not data loss. Alternatives rejected: inline
pressure flush in `do_put` (couples ack latency to the catalog — the exact
coupling async ack removed) and unbounded "loop until under budget" (spins
or OOMs during a catalog outage; today's writer survives outages precisely
because deferred data lives on disk).

### D5: Bounded incremental replay

Startup replay alternates load-and-commit: insert decoded batches until
resident bytes reach the soft budget, drain, continue. Peak replay memory
is bounded by the budget regardless of backlog size (this repo has seen
multi-GB writer backlogs; replay-all-then-start would crash-loop). Payloads
are read by iterating segments sequentially, not via per-entry
`read_entry_data` scans (O(P·E) → O(P + segments)). Poison entries follow
the normal failure path via D3's reconciliation. The writer serves `do_put`
during replay (WAL durability is independent of the memtable); the replay
metric exposes progress.

### D6: Double-buffered groups; memtable state off the processor mutex

The drain swaps a group's active `Vec<RecordBatch>` into an immutable
flushing slot under a short lock, commits outside the lock, and clears the
slot only after `mark_processed` succeeds (restoring it into active on
failure). Inserts land in a fresh active vector meanwhile, so a
seconds-long catalog commit never blocks ingest for that group. Memtable
state lives behind its own lock, not the processor mutex — otherwise every
insert serializes behind the whole drain.

### D7: Coerce to the table's Arrow schema at insert

Insert-time coercion (via `coerce_batch_to_schema` +
`json_strings_to_map_array` moved into `common`) makes byte accounting
truthful, commits cheaper, and hands the follow-up visibility change hot
batches already in the cold schema. The commit path keeps a final coercion
check for schema drift between insert and commit (attribute promotion can
change the table's materialized columns while a batch is resident).

### D8: Self-monitoring within cardinality rules

Metrics: resident bytes (total; per-group attribution capped to top-N to
respect the bounded-cardinality rule), pressure-flush counter, hard-ceiling
rejection counter, replay volume, and a WAL payload-read counter labeled by
reason (`recovery` / `reconcile` / `dead_letter`) — the observable proof of
decode-once. The #760 anti-loop guard (suppress self-telemetry when
processing the `_system` tenant) extends to memtable code paths. The
`Flush`-marker scope bug (marker scopes built from WAL-default tenant ids
rather than metadata-derived routing) is fixed while the marker queue is
reworked.

## Risks / Trade-offs

- [Writer RSS grows by the budget] → explicit soft/hard bytes config,
  pressure flush, rejection backstop; metrics to size it; conservative
  defaults.
- [Arrow size accounting undercounts allocator overhead] → treat budgets as
  approximate; document headroom guidance in ops docs.
- [Reconciliation diff cost on huge backlogs] → metadata-only listing (no
  payload reads); id-set diff is linear in unprocessed count, the same list
  the loop already fetches today.
- [Insert-time coercion changes where schema errors surface (ingest instead
  of commit)] → coercion failures follow the existing poison/dead-letter
  path; a batch that cannot be coerced today would have failed at commit
  anyway.
- [Restart replay lengthens startup on a large backlog] → incremental
  replay keeps memory bounded; time is bounded by sequential segment reads;
  replay metric exposes progress; `do_put` stays available throughout.
- [Graceful shutdown with resident data] → shutdown drains the memtable
  (bounded by a timeout) and logs residual counts; WAL replay covers the
  remainder.

## Migration Plan

- Invisible to clients; deploy like any writer release. Rollback =
  redeploy previous image; WAL semantics identical in both directions
  (resident data not yet committed is replayed from the WAL by either
  version).
- Ops docs gain memtable sizing guidance; dashboards gain resident-bytes /
  pressure-flush / rejection panels.

## Open Questions

- Default values for `memtable_soft_bytes` / `memtable_hard_bytes` /
  `max_uncommitted_bytes` — pick after measuring typical batch sizes on a
  live deployment; does not affect specs or tasks.
