# Tasks: Trace-ID Point Index

> Implementation waits on design sign-off, in particular Open Questions 1-3
> in design.md. Every task begins with its failing test.

## 1. Measure first (tests-integration)

- [ ] 1.1 Extend `tests-integration/benches/querier_read_paths.rs` with a
      cold-cache bare-id lookup over 30 days of hour partitions on the local
      object store: baseline files opened and latency. This is the number the
      change has to beat.
- [ ] 1.2 Ingest bench: sustained trace ingest with and without an index
      append per commit cycle (a prototype behind a test-only flag). Record
      the regression against the 10% bar from design.md D3.

## 2. Schema and config (common)

- [ ] 2.1 Failing test (`cargo test -p common trace_index`): the
      `trace_index` schema is `trace_id string`, `hour timestamp_ns`,
      partitioned `truncate(1, trace_id)`, sorted `(trace_id, hour)`, with a
      bloom filter on `trace_id`. Then add it to `schemas.toml`.
- [ ] 2.2 Failing test: `[trace_index]` parses `enabled` (default `false`),
      `hot_window` (humantime, default `2h`) and `refresh_interval` (default
      `1m`), with `SIGNALDB_TRACE_INDEX_*` overrides. Then implement it, and document it in `signaldb.dist.toml`.

## 3. Writer (writer)

- [ ] 3.1 Failing test (`cargo test -p writer trace_index`): for a batch of
      spans, the writer derives the distinct `(trace_id, hour)` pairs and
      commits them to `trace_index` before it commits `traces`.
- [ ] 3.2 Failing test: an injected `traces` commit failure after a
      successful index commit leaves the index superset. A WAL replay commits
      the data, and the index has duplicate rows, not missing ones.
- [ ] 3.3 Failing test: an injected index commit failure fails the cycle
      before any `traces` commit, and replay recovers.
- [ ] 3.4 Failing test (`cargo test -p writer reconcile`): when the index
      is enabled, the reconciler creates and activates `trace_index` wherever
      it ensures `traces`, and sets `active_since = T` and
      `complete_from = ceil_hour(T + refresh_interval)`. An activation at
      14:40 with `R = 1m` gives a watermark of 15:00, not 14:00.
- [ ] 3.5 Failing test: a writer runs D2 only while the table is active. It
      sees activation and deactivation within `refresh_interval`.
      Reactivation resets `complete_from` and `seeded`.

## 4. Compactor (compactor)

- [ ] 4.1 Failing test (`cargo test -p compactor trace_index`): a shard
      rewrite dedups rows, drops hours older than the retention cutoff, sorts
      by `(trace_id, hour)`, and leaves untouched any writer file younger than
      the settle age that was appended concurrently.
- [ ] 4.2 Failing test: the backfill job indexes closed `traces` hours older
      than `complete_from`, newest first. It lowers the watermark in the same
      commit as each hour's rows, and stops at the retention cutoff. A crash
      mid-run leaves the watermark at the last complete hour.
- [ ] 4.3 Failing test: the seed step indexes data files added before
      `T + R` whose `timestamp` upper bound reaches `complete_from`, including
      a future-dated span, then sets `seeded`. The querier ignores the index
      until `seeded` is set.
- [ ] 4.4 Tenant and dataset deletion drop `trace_index` (extend the
      existing deletion test).

## 5. Querier (querier)

- [ ] 5.1 Failing test (`cargo test -p querier find_by_id`): a bare-id
      lookup scans only the indexed hours, the hot window, and hours older
      than `complete_from`. Assert this on the physical plan's partition
      predicates.
- [ ] 5.2 Failing test: index missing, disabled, or erroring gives the same
      spans as today. The test also asserts the warn log and the fallback
      counter.
- [ ] 5.3 Failing test: lookups with `start`/`end` produce the same plan as
      today.
- [ ] 5.4 Integration (`tests-integration/tests/querier/trace_point_lookup.rs`):
      a trace with spans in three hours across two days, plus one late span
      written a day after its event hour, comes back whole with no range,
      both before and after compaction and backfill.
- [ ] 5.5 Tenant isolation: the same trace id in two tenants. Each lookup
      sees only its own index and spans.

## 6. Surfaces (router, OpenAPI, SDK, MCP, UI), gated on Open Question 1

- [ ] 6.1 If the IR gains a bare-id lookup: a failing IR validation test for
      omitting `range` on a `trace_id = <literal>` + `trace` result. Then
      implement it, and update the OpenAPI spec and the generated UI/SDK
      clients.
- [ ] 6.2 Surface parity: wherever the MCP `get_trace`, the CLI or the
      Explore UI trace view fetches a trace by id, it uses the bare-id IR form
      instead of a padded time window.

## 7. Docs and skills

- [ ] 7.1 `docs/operations/table-provisioning.md`: document `trace_index` and
      its lifecycle.
- [ ] 7.2 `docs/architecture/` storage layout: document the companion table,
      index-first ordering, and the watermark.
- [ ] 7.3 `docs/users/querying-ir.md` (if 6.1 lands) and the
      `storage-layout` skill.
- [ ] 7.4 Ticks and notes on #880, plus a pointer from #936 for the sort lever.
