## Context

See `proposal.md` — Why. The relevant current-state facts that shape the approach:

- Writer `do_put` (`writer/src/flight_iceberg.rs`) writes each batch to the
  writer WAL, flushes, then **synchronously** loops `process_single_entry` →
  `append_batches_with_marker` → Iceberg `commit()` before returning `PutResult`.
  The inline comment already notes this "could be made async".
- A background loop is already spawned in both serving modes
  (`signaldb-bin/src/main.rs`, `writer/src/main.rs` →
  `start_background_processing`, 5s base interval with backoff). It calls
  `WalProcessor::process_pending_entries`, which reads all unprocessed WAL
  entries, groups them by `(tenant, dataset, table)`, and commits each group
  (chunked at `MAX_ENTRIES_PER_COMMIT = 1024`).
- `WalOperation::Flush` entries already exist in the WAL enum and are currently
  skipped by the processing loop (`processor.rs`) — a no-op today.
- The acceptor already provides durability and at-least-once delivery
  (`ingest-durability` spec): it acks on its own WAL flush and re-forwards
  unprocessed entries via `WalRetryConsumer`.
- One Iceberg `commit()` = one new `metadata.json` + one `UPDATE iceberg_tables`
  catalog write. Snapshot expiration runs against every active tenant hourly and
  caps the _snapshots list_, not the cumulative metadata-version count.

FDAP constraint: this change alters only commit **timing**, not data. It touches
no Arrow/Parquet types (all via DataFusion re-exports already), no Flight v1 wire
/ v2 storage schema transform, and no on-disk WAL or Iceberg layout. `Flush` is a
pre-existing WAL operation, so repurposing it is not a format change.

## Goals / Non-Goals

**Goals:**

- Make the background loop the sole Iceberg commit path; `do_put` acks on WAL
  flush.
- Cap commit rate at O(`commit_interval`) per `(tenant, dataset, table)` via a
  time-OR-size coalescing floor, for all tenants uniformly.
- Provide a deterministic force-commit so read-your-writes is available on demand.
- Keep the Iceberg metadata chain bounded independent of commit rate.

**Non-Goals:**

- Full ingest backpressure / flow control on writer-WAL depth (monitor only for
  now; existing backoff stands).
- Per-tenant divergent commit policy — one uniform model (the reserved
  `_system` tenant is fixed by the same mechanism, not a special case).
- Changing the acceptor→writer durability contract or WAL/Iceberg formats.

## Decisions

### D1 — `do_put` acks on WAL flush; background loop is the sole committer

Remove the synchronous `process_single_entry` loop from `do_put`; return
`PutResult` right after `wal.flush()`. Deferred entries stay unprocessed in the
writer WAL and are drained by `process_pending_entries`.

_Why:_ eliminates the export↔catalog latency coupling (#889) at the source and
is a deletion, not a new path. The background loop is already running in both
serving modes, so nothing new needs wiring for delivery.

_Alternative considered:_ keep `do_put` synchronous but move only `_system` to
async. Rejected — the user chose the all-tenants model; a tenant-conditional
write path adds divergence for a benefit the uniform model already delivers.

### D2 — Coalescing floor keyed on `(tenant, dataset, table)`

`WalProcessor` keeps an in-memory `last_commit: HashMap<writer_key, Instant>`
(a `CommitCoalescer`). Per tick, for each group it commits iff
`now - last_commit >= commit_interval` **OR** `pending_rows >= max_uncommitted_rows`;
otherwise it leaves the group unprocessed (does not mark) and revisits next tick.
`pending_rows` is summed from the grouped `RecordBatch`es (the loop already
deserializes entries to group them); deferred entries are re-read next tick, a
cost bounded to roughly one extra read at the default `commit_interval ≈ tick`.
Pre-deserialization gating (grouping on entry metadata, summing `data_size`
bytes) is a later optimization worth doing only if `commit_interval` is
configured much larger than the tick. `last_commit` is updated on a successful
commit; a fresh (never-committed) key is treated as immediately eligible so
first data isn't delayed a full interval.

_Why:_ the OR guarantees liveness (time) and bounded commit size (rows). In-memory
state is safe to lose on restart — a lost `last_commit` only means the first
post-restart tick may commit slightly early, which is harmless.

_Alternative considered:_ a byte-size ceiling instead of row count. Deferred —
row count is already available cheaply and is good enough; a byte estimator can
be added later without a spec change.

### D3 — Force-commit via a writer Flight `do_action("flush")`, tenant-scoped

Expose `WalProcessor::force_commit_pending(scope)` that force-commits the groups
matching a `FlushScope { tenant_id, dataset_id: Option }` (floor bypassed for
that scope only; other tenants keep coalescing), and surface it as a writer
Flight `do_action("flush")` (today `do_action` returns `unimplemented`) whose
body is a **required** JSON `{"tenant_id", "dataset_id"?}`. An unscoped flush is
rejected so one caller cannot force-commit — and amplify catalog writes for —
every tenant on the writer. The integration-test harness and any read-your-writes
client call it after ingest. The pre-existing `WalOperation::Flush` marker is
honored with the same drain semantics, scoped to the marker entry's own
tenant/dataset.

_Scoping added in response to review (rust-code-reviewer + CodeRabbit): an
unscoped global flush would bypass coalescing for unrelated tenants and
reintroduce the write amplification this change exists to remove._

_Why:_ a `do_action` is synchronous and directly awaitable from tests, giving
deterministic waits in place of `sleep`. Repurposing `Flush` keeps a WAL-level
path for callers that only speak the WAL.

_Alternative considered:_ keep tests on `sleep` with a longer bound. Rejected —
slow and flaky, and it wouldn't serve real RYW clients.

### D4 — Bound metadata via table properties, with an expiration-tick fallback

Prefer setting Iceberg table properties (`write.metadata.previous-versions-max`,
`write.metadata.delete-after-commit.enabled`) at table creation so old
`metadata.json` are pruned on commit. If the pinned iceberg-rust (JanKaul) does
not honor them (see Open Questions), fall back to a short-interval snapshot-
expiration + metadata-cleanup tick for the affected tables.

_Why:_ property-driven pruning is the least code and runs inline with commits;
the fallback keeps the guarantee even if upstream support is absent.

## Risks / Trade-offs

- **Read-your-writes breaks for all tenants (BREAKING).** → Force-commit primitive
  (D3) plus migrating ingest-then-query tests to call it; document the new
  eventual-visibility semantics for clients.
- **Writer WAL grows if the background loop stalls** (it keeps acking while not
  committing). → Existing failure backoff; emit a writer-WAL-depth / pending-group
  gauge so stalls are observable. Full backpressure is a Non-Goal.
- **Repeated re-scan of deferred groups each tick.** → Evaluate the floor from
  metadata/row-counts before deserializing; only deserialize groups that will
  commit this tick.
- **In-memory `last_commit` lost on restart.** → Harmless (at worst one early
  commit post-restart); no persistence needed.
- **Metadata-pruning support uncertain in pinned iceberg-rust.** → D4 fallback
  (expiration tick) guarantees the bound regardless.

## Migration Plan

- Config-gated defaults: `commit_interval = 5s`, `max_uncommitted_rows ≈ 100k`.
  Setting `commit_interval = 0` commits every tick (near-current cadence) as an
  escape hatch / staged rollout knob.
- Rollout order matches the PR stack in `tasks.md`: floor + force-commit
  primitive first (inert while `do_put` stays synchronous), then flip `do_put`
  to async, then metadata bounding.
- Rollback: revert the `do_put` flip (D1) to restore synchronous commits; the
  floor/force-commit code is inert without it.

## Open Questions

- Does the pinned iceberg-rust (JanKaul) honor `write.metadata.previous-versions-max`
  / `delete-after-commit.enabled`? A short spike in the metadata-bounding PR
  decides D4's primary vs fallback path; it does not affect the spec or the other
  PRs.
