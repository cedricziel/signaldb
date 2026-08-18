# Design: LSM-Style Writer Memtable (Stage 1)

## Context

See proposal.md — Why. Current state that shapes the approach (verified
against the code; corrected after expert review):

- `WalProcessor::drain_pending` (src/writer/src/processor.rs) rebuilds its
  pending groups from disk every tick: `get_unprocessed_entries` →
  `read_entry_data` → deserialize. The cost per pending entry is a `stat`,
  an `open`, a `seek`, a `read_exact`, a per-record CRC validation (#1294)
  and an Arrow decode — I/O and CPU, not lookup: since #1112 every segment
  carries an `entry_index` hash map, so `read_entry_data` probes each
  segment instead of scanning its entry list (src/common/src/wal/mod.rs).
  Entries deferred by the coalescing floor are re-decoded each tick
  (in-code NOTE in `drain_pending`).
- `Wal::get_unprocessed_entries` walks every entry of every segment,
  processed ones included. Since #1305 both services sweep their WALs
  between passes, so that per-tick listing is bounded by the live backlog
  rather than by every entry ever written. This change does not address the
  remaining scan: the memtable removes payload reads and decodes, not the
  metadata scan.
- The writer holds **one WAL per (tenant, dataset, signal)** (#1299), not a
  global WAL. `IcebergWriterFlightService::do_put` resolves the WAL through
  `WalManager` on every call, and `drain_pending` groups by (WAL identity,
  tenant, dataset, table). Wherever this document says "the writer's WAL"
  it means one of many.
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
  `common::testing::flush_storage_writers`, used across the integration
  suite (nine test files at the time of writing).
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
  entries dead-letter after the same failure budget, and WAL entries still
  become fully processed. Segment _reclamation_ is not among the semantics
  to preserve: it does not happen today (#1305) and this change neither
  restores nor further blocks it.
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
no read benefit. Groups are keyed by
`(WAL identity, tenant, dataset, table)` and hold decoded `RecordBatch`es
plus per-entry bookkeeping (WAL entry ids, trace links — what `TableBatch`
carries today).

**Amendment (a correctness fix, not a restatement).** This document
originally keyed groups by `(tenant, dataset, table)`. The WAL must be part
of the key. `IcebergTableWriter::load_committed_marker` and
`append_batches_with_marker` are keyed by `wal.writer_id()`, and two WALs
can feed one table — a tenant's own WAL alongside the adopted legacy root
one (`WalManager::adopt_root_segments`). A group mixing two WALs would
write one WAL's idempotency marker over entries belonging to another, which
presents as exact row duplication on the retry path, never as an error.
`drain_pending` already groups this way today; the memtable must not
regress it.

The WAL identity in the key is `wal.writer_id()` — the identity persisted
in the WAL directory (`Wal::load_or_create_writer_id`), stable across
restarts and already the marker's key. It is deliberately **not** the
`Arc::as_ptr(&wal) as usize` that today's `drain_pending` `GroupKey` uses.
That pointer is sound only because the manager holds every `Arc<Wal>` alive
for the single cycle in which the key exists. A memtable key outlives the
cycle, so a WAL evicted from and re-created in the manager's cache could
reuse a freed address and silently merge two WALs' entries (ABA).

### D2: Insert after `wal.flush()`, via one shared routing function

The memtable insert happens on the `do_put` path only after the durable
flush of the batch's own WAL (its tenant/dataset/signal WAL, #1299) returns
Ok — insert-after-append would let the memtable hold entries
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
lazily loads payloads for the difference. The diff runs per WAL, against
that WAL's own unprocessed set. Eviction happens only on `mark_processed`
success or dead-letter (dead-letter also releases byte accounting).
Consequences: a commit that fails after draining retries from memory; an
entry that failed routing/decoding at replay is revisited and accumulates
failures toward dead-lettering; existing processor tests that append
directly to the WAL and expect a drain keep passing. Alternative rejected:
pure drain-from-memory ("WAL recovery-only"), which silently strands data
on any commit failure and loses the failure-budget accounting that
dead-letters poison entries. (An earlier revision also justified this by
segment reclamation. That leg was void when written — nothing called
`Wal::cleanup` — and is now moot: since #1305 both services reclaim
segments regardless of which drain shape this change picks.)

### D4: Soft budget signals the loop; hard ceiling backpressures ingest

Config: `[writer] memtable_soft_bytes` (pressure target),
`memtable_hard_bytes` (reject threshold, e.g. 2× soft),
`max_uncommitted_bytes` per group (byte sibling of the existing
`max_uncommitted_rows`, so the global budget is a safety net rather than
the steady-state trigger — dozens of groups at the row ceiling would
otherwise sit in permanent pressure). Accounting via Arrow
`get_array_memory_size`. Both budgets are defined over **accounted Arrow
bytes** — the sum of `get_array_memory_size` across resident (active and
flushing) batches — not process RSS: group keys, bookkeeping vectors,
trace links, and decode/coercion temporaries are deliberately excluded, so
the ceiling is a bound on the dominant term, and ops docs instruct sizing
with headroom (budgets well below container limits) to absorb the
unaccounted remainder and allocator overhead. The hard-ceiling admission
check runs at the top of `do_put`, **before** the WAL append, and counts
in-flight puts via a reservation for the incoming payload size — a
rejected put therefore leaves no durable WAL entry, so acceptor
redelivery cannot create duplicates.
Crossing the soft budget notifies the commit loop, which flushes
largest-group-first; pressure work never runs inline in `do_put`. **Each
tick does a bounded amount of pressure work** — a small fixed number of
extra groups, never "loop until under budget". Nothing in the writer backs
off: the background loop absorbs failures per entry and per group and ticks
at a fixed interval whatever the outcome, so an unbounded pressure loop
during a catalog outage would spin at full rate — cheaper I/O than today,
identical catalog pressure. Being under budget is therefore not guaranteed
within any one tick; the hard ceiling, not the pressure loop, is what
bounds memory. At the hard ceiling, `do_put` returns a retryable gRPC error
(`RESOURCE_EXHAUSTED`) — the acceptor's WAL retry consumer redelivers, so
this is flow control, not data loss. Once a batch is durably in the WAL,
`do_put` no longer fails for memtable reasons: a post-durability failure
(e.g. coercion) routes the entry to the poison path while the export is
still acknowledged, precisely so the acceptor does not redeliver a
durable entry. Alternatives rejected: inline
pressure flush in `do_put` (couples ack latency to the catalog — the exact
coupling async ack removed) and unbounded "loop until under budget" (spins
or OOMs during a catalog outage; today's writer survives outages precisely
because deferred data lives on disk).

### D5: Bounded incremental replay

Startup replay alternates load-and-commit: insert decoded batches until
resident bytes reach the soft budget, drain, continue. Peak replay memory
is bounded by the budget regardless of backlog size (this repo has seen
multi-GB writer backlogs; replay-all-then-start would crash-loop). Payloads
are read by iterating each WAL's segments sequentially rather than
re-opening the data file per entry, so replay pays one open per segment
instead of a `stat` plus an `open` per entry. Poison entries follow
the normal failure path via D3's reconciliation, and a failed replay
commit retains its chunk and retries under the normal failure budget
rather than blocking replay progress. The writer serves `do_put`
concurrently during replay (WAL durability is independent of the
memtable): live inserts and replay loads share the same budget accounting
and admission check, so their combined residency stays bounded; replay is
complete when the startup backlog has been loaded once, after which D3's
reconciliation owns any remainder. The replay metric exposes progress.

### D6: Double-buffered groups; memtable state off the processor mutex

The drain swaps a group's active `Vec<RecordBatch>` into an immutable
flushing slot under a short lock, commits outside the lock, and evicts
from the flushing slot at **entry granularity**: after a commit, each
successfully `mark_processed` entry's batches are evicted; entries whose
mark failed (or whose chunk never committed — commits are chunked at
`MAX_ENTRIES_PER_COMMIT`) are retained and merged back into the group
without overwriting inserts that landed in the fresh active vector
meanwhile. A whole-slot clear or restore cannot represent these partial
outcomes. A commit that succeeded before its mark failed is covered by
the existing idempotency marker on the retry path, unchanged. Inserts
land in a fresh active vector during the flush, so a seconds-long catalog
commit never blocks ingest for that group. Memtable state lives behind
its own lock, not the processor mutex — otherwise every insert serializes
behind the whole drain. Because a group is single-WAL by construction (D1), every `mark_processed`
in a drain targets that one WAL and its idempotency marker stays exact.
Failure-injection tests cover commit failure, partial mark-processed
failure, and concurrent insert during flush.

### D7: Coerce to the table's Arrow schema at insert

Insert-time coercion (via `coerce_batch_to_schema` +
`json_strings_to_map_array` moved into `common`) makes byte accounting
truthful, commits cheaper, and hands the follow-up visibility change hot
batches already in the cold schema. Coercion on the `do_put` path reads
the target Arrow schema from a **process-local schema cache only** —
populated by the commit path (which already resolves tables via
`ensure_table`) and by replay; `do_put` never touches the catalog or
object store, keeping ack latency decoupled. On a cache miss (first batch
for a group since startup) the batch is inserted in v2-transformed form
and coerced by the commit path exactly as today; on schema drift the
commit path's final coercion remains authoritative (attribute promotion
can change the table's materialized columns while a batch is resident).
A cache refresh failure therefore degrades to today's commit-time
coercion, never to an ingest error.

### D8: Self-monitoring within cardinality rules

Metrics: resident bytes (total; per-group attribution capped to top-N to
respect the bounded-cardinality rule), pressure-flush counter, hard-ceiling
rejection counter, replay volume, and a WAL payload-read counter labeled by
reason (`recovery` / `reconcile` / `dead_letter`) — the observable proof of
decode-once. The #760 anti-loop guard (suppress self-telemetry when
processing the `_system` tenant) extends to memtable code paths.

An earlier revision folded in a `Flush`-marker scope fix (marker scopes
built from WAL-default tenant ids rather than from metadata-derived
routing). That fix is dropped, because the bug is dead. Since #1299 a
`Flush` entry is stamped with its own WAL's configured tenant/dataset,
which for every live WAL is the real tenant; the sole WAL whose config
still reads `default`/`default` is the adopted legacy root one, and that is
deliberate (`WalManager::adopt_root_segments`). More decisively, no
production code has ever appended a `Flush` marker to the writer WAL —
`git log -S` places its only appends in the processor's own tests, from
#891 onwards.

## Risks / Trade-offs

- [Writer RSS grows by the budget] → explicit soft/hard bytes config,
  pressure flush, rejection backstop; metrics to size it; conservative
  defaults.
- [Arrow size accounting undercounts allocator overhead] → treat budgets as
  approximate; document headroom guidance in ops docs.
- [Reconciliation diff cost on huge backlogs] → metadata-only listing (no
  payload reads); the id-set diff is linear in the unprocessed count. The
  _listing_ it diffs against is not — see Context: it is linear in the total
  entries ever written (#1305). That cost is unchanged by this change,
  today's loop already pays it every tick, but this change does not fix it
  and the ops docs must not imply that it does.
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
