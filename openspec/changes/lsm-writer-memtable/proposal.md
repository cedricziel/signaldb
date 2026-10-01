# LSM-Style Writer Memtable (Stage 1)

## Why

The writer already has an LSM shape — WAL for durability, Parquet/Iceberg as
immutable sorted files, the compactor as compaction — but is missing the
memtable. Every processing tick, `WalProcessor::drain_pending` re-reads and
re-deserializes the entire unprocessed WAL backlog from disk to rebuild its
pending groups (entries deferred by the coalescing floor are decoded again
each tick). The cost is the disk I/O and the decode themselves, paid per
entry per tick and itemized in design.md's Context; it is not lookup, which
has been a hash probe since #1112 gave each segment an `entry_index`.
Keeping decoded batches resident removes the per-tick read-back and
decode entirely, bounds writer memory explicitly instead of implicitly, and
is the prerequisite for a follow-up change (`unflushed-data-visibility`)
that decouples query visibility from commit cadence so the commit interval
can later be raised for larger files and fewer snapshots.

What this change does NOT remove is the per-tick _metadata_ scan.
`Wal::get_unprocessed_entries` walks every entry of every segment,
processed ones included, so that scan stays linear in the number of entries
the WAL still holds. Since #1305 both services sweep their WALs between
passes, so fully-processed segments are reclaimed and the scan no longer
grows with every entry ever written — but it is still linear in the live
backlog. The memtable removes payload reads and decodes; the metadata scan
is untouched.

Note this change does NOT claim query traffic currently forces small
Parquet files: the writer's `do_action("flush")` has no production caller
(only the test-gated `common::testing::flush_storage_writers`). Visibility
semantics are unchanged by this stage.

## What Changes

- **Resident memtable as a reconciled cache over the WALs.** `do_put`
  appends to the WAL of the batch's own tenant/dataset/signal (#1299: the
  writer holds one WAL per such triple, never a global one), and only after
  that WAL's flush succeeds inserts the decoded RecordBatch into an
  in-memory pending group keyed by
  `(WAL identity, tenant, dataset, table)`. The commit loop drains resident
  groups; batches are evicted only after `mark_processed` succeeds on their
  own WAL, so a failed commit retries from memory. A per-tick
  reconciliation compares each WAL's unprocessed entry ids (metadata only,
  no payload reads) against resident ids and lazily loads any difference —
  preserving today's self-healing for failed commits, poison entries, and
  dead-lettering.
- **Shared routing.** One routing function computes
  `(tenant, dataset, table)` for both the `do_put` insert path and WAL
  replay, so a batch lands in the same table before and after a restart.
- **Memory budget: soft signal + hard ceiling.** Crossing the soft budget
  signals the commit loop to flush the largest group first — never inline
  catalog work on the `do_put` path. A hard ceiling rejects ingest with a
  retryable error (the acceptor's WAL retry consumer absorbs it), so
  sustained commit failure cannot grow memory without bound. A per-group
  byte ceiling complements the existing per-group row ceiling so the
  global budget is a safety net, not the steady-state trigger.
- **Bounded, incremental startup replay.** Replay inserts until the budget
  is reached, commits, and continues — replay memory is bounded by the
  budget regardless of backlog size, and payloads are read by iterating
  segments rather than per-entry scans.
- **Schema coercion at insert.** Batches are coerced to the Iceberg table's
  Arrow schema (timestamp units, JSON→Map attributes, materialized label
  columns, `attr_tokens`) when inserted, via helpers shared through
  `common`, making byte accounting truthful and commits cheaper.
- **Double-buffered groups.** Draining swaps a group's active batches into
  an immutable flushing slot under a short lock and commits outside it, so
  a slow Iceberg commit never blocks ingest for that group.
- No changes to visibility semantics, the WAL on-disk format, at-least-once
  delivery, per-entry `mark_processed` granularity, the force-commit
  primitive, OTLP surfaces, or the acceptor (its hot path already forwards
  decoded in-memory batches; its WAL read-back is retry-only).

## Capabilities

### New Capabilities

- `writer-memtable`: the writer's in-memory cache of decoded pending
  batches — insert-after-durability, WAL reconciliation and eviction rules,
  bounded incremental replay, soft/hard memory budget, and observability.

### Modified Capabilities

- `writer-commit-coalescing`: commit triggers gain a memory-pressure
  trigger (soft budget exceeded → commit largest group first), and the
  "at most one commit per interval" bound is amended to admit pressure
  commits as a rate-limited, observable exception.

## Impact

- **writer**: new memtable module; `flight_iceberg.rs` (`do_put` inserts
  after WAL flush; hard-ceiling rejection); `processor.rs` (drain from
  resident groups + reconciliation; bounded replay; shared routing;
  dead-letter eviction).
- **common**: `[writer]` memory config (`memtable_soft_bytes`,
  `memtable_hard_bytes`, per-group `max_uncommitted_bytes`);
  schema-coercion helpers moved from the writer for reuse;
  self-monitoring metrics (resident bytes, pressure flushes, hard-ceiling
  rejections, WAL payload reads by reason, replay volume).
- **tests-integration**: crash-recovery replay, budget-pressure,
  hard-ceiling backpressure, and commit-failure retry tests.
- Not breaking: WAL on-disk format, Iceberg layout, Flight wire schemas,
  and all ingest/query surfaces are unchanged.

## Follow-up change

Stage 2 — querier-visible unflushed data (hot/cold union with a
per-writer commit watermark) — is deliberately a separate change:
`unflushed-data-visibility`. This stage ships alone and changes no
externally observable behavior beyond memory bounds and backpressure.
