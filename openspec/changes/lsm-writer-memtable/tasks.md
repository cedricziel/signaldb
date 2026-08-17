# Tasks: LSM-Style Writer Memtable (Stage 1)

## 1. Foundations

- [ ] 1.1 Extract one shared routing function computing
      `(tenant, dataset, table)` from batch metadata, used by both the
      `do_put` path and WAL replay (covers the metadata tenant override and
      `metrics_gauge` fallback from `determine_target_table`); unit tests
      pinning ingest/replay agreement. The `Flush`-marker scope fix this
      task originally carried is dropped as a dead bug: #1299 stamps every
      live WAL's entries with that WAL's own tenant, and no production code
      has ever appended a `Flush` marker to the writer WAL (evidence in
      design.md D8). Also add a comment at `drain_pending`'s `GroupKey`
      recording why its
      `Arc::as_ptr` identity is sound for a value confined to one cycle and
      would not be for one that outlives it
- [ ] 1.2 Move `coerce_batch_to_schema` and `json_strings_to_map_array`
      from the writer into `common` (no behavior change); writer commit
      path consumes them from there
- [ ] 1.3 Add `memtable` module: per-`(wal writer_id, tenant, dataset,
      table)` groups with active/flushing double buffering behind their own
      lock, byte accounting via `get_array_memory_size`, per-entry
      bookkeeping (WAL entry ids, trace links); unit tests for insert/swap/
      restore-on-failure/evict accounting, plus one pinning that two WALs
      feeding the same table stay in separate groups

## 2. Ingest and drain paths

- [ ] 2.1 `do_put` ordering: hard-ceiling admission (with in-flight
      reservations) before the WAL append; after `wal.flush()` returns Ok,
      coerce via the process-local schema cache (never the catalog) and
      insert into the memtable before acking; tests — rejected ingest
      leaves no WAL entry, WAL flush failure leaves no memtable entry,
      post-durability coercion failure follows the poison path while the
      ack still succeeds
- [ ] 2.2 Rework `drain_pending`: drain resident groups (swap-out under
      short lock, commit outside it), keep coalescing floor and
      `Flush`-marker handling; entry-granularity eviction — evict each
      entry only after its `mark_processed` succeeds, retain/merge
      unprocessed entries back without overwriting concurrent inserts;
      failure-injection tests for commit failure, partial mark failure,
      and concurrent insert during flush; adapt processor tests, keeping
      the WAL-append-then-drain tests green via 2.3
- [ ] 2.3 Per-tick WAL reconciliation, per WAL: diff that WAL's unprocessed
      entry ids (metadata only) against resident ids, lazily load payloads
      for the difference; dead-letter evicts the resident copy and releases
      bytes; tests: failed-commit retry without restart, poison entry
      reaches dead-letter after the existing failure budget,
      direct-WAL-append entries get drained. The listing itself stays
      linear in the total entries ever written until #1305 gives segment
      cleanup a caller; claim no better, in docs or in metric descriptions
- [ ] 2.4 Bounded incremental startup replay: alternate load-and-commit at
      the soft budget, sequential segment iteration instead of per-entry
      `read_entry_data`; integration tests — restart with un-committed
      entries loses nothing; replay of a backlog larger than the budget
      stays within budget; live ingest during replay shares the budget and
      both datasets commit; a failed replay-chunk commit retries without
      halting replay

## 3. Memory budget and backpressure

- [ ] 3.1 Config in `common`: `[writer] memtable_soft_bytes`,
      `memtable_hard_bytes`, per-group `max_uncommitted_bytes`; defaults +
      signaldb.dist.toml documentation
- [ ] 3.2 Soft-budget pressure signal to the commit loop
      (largest-group-first, never inline in `do_put`, bounded work per tick
      — never "loop until under budget", since nothing in the loop backs
      off) and per-group byte ceiling as a commit trigger; tests for budget
      breach, byte-ceiling commit, noisy-tenant scenarios, and one pinning
      that a tick's pressure work stays bounded when every commit fails
- [ ] 3.3 Hard-ceiling backpressure: `do_put` returns retryable
      `RESOURCE_EXHAUSTED` at the ceiling; integration test — sustained
      commit failure (catalog down) bounds resident memory, acceptor WAL
      retry redelivers after recovery

## 4. Observability and lifecycle

- [ ] 4.1 Metrics via common::self_monitoring: resident bytes (total +
      top-N group attribution), pressure-flush counter, hard-ceiling
      rejection counter, replay volume, WAL payload-read counter labeled
      recovery/reconcile/dead-letter; extend the #760 `_system` anti-loop
      guard to memtable paths
- [ ] 4.2 Graceful shutdown: drain memtable with a bounded timeout, log
      residual counts; test that residuals are recovered by replay
- [ ] 4.3 Ops docs under docs/operations/: sizing guidance (soft/hard/per-
      group, headroom for accounting drift), dashboard panels for the new
      metrics

## 5. Wrap-up

- [ ] 5.1 Full workspace test run, clippy, fmt, cargo machete; update
      CLAUDE.md / storage-layout & architecture skill docs where the write
      path description changed
- [ ] 5.2 `openspec validate --strict` passes; sync deltas / archive per
      workflow
