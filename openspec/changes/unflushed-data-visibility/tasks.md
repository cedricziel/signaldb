# Tasks: Unflushed Data Visibility (LSM Stage 2)

## 1. Writer — watermark and hot-scan surface

- [ ] 1.1 Per-group monotonic sequence assigned at memtable insert
      (`writer_id, tenant, dataset, table`); FIFO drain advances a
      contiguously-committed high-water mark, chunked commits advance only
      the contiguous prefix; unit tests for sequence/watermark accounting
- [ ] 1.2 Write `signaldb.hot.<writer_id>.seq` via `update_properties` in
      the same transaction as `append_data` (alongside the existing
      idempotency marker, unchanged); test: no snapshot has the data
      without the covering watermark
- [ ] 1.3 Track per-batch min/max timestamps at insert for scan pruning
- [ ] 1.4 Hot-scan `do_get`: ticket types in `common::flight` (tenant/
      dataset/table + mandatory time bounds); internal-service auth
      identical to `do_put`, ticket tenant validated against caller scope,
      unscoped/unauthorized rejected; batches streamed in the table's
      Arrow schema tagged `(writer_id, seq)`; response byte cap; `_system`
      anti-loop guard; tests for isolation, auth, pruning, cap
- [ ] 1.5 Replay-in-progress writers report "warming" on the scan surface

## 2. Compactor — watermark preservation

- [ ] 2.1 Regression test: compaction and snapshot-expiration commits
      preserve `signaldb.hot.*` table properties

## 3. Querier — hybrid provider

- [ ] 3.1 Cached Storage-writer discovery (TTL tied to heartbeat interval)
      in the querier; per-request table-resolution cache so multi-reference
      queries scan hot data once per table
- [ ] 3.2 `HybridTableProvider` returned from `LiveIcebergSchema::table()`:
      eager hot fan-out to all Storage-capable writers (per-writer
      timeout), then cold resolution pinning snapshot S and reading
      `W_S[writer]` from the same instance, drop hot batches with
      `seq ≤ W_S`; `UnionExec` arms with identical schemas; `Inexact`
      pushdown; unknown statistics; hot bytes registered against the
      DataFusion memory pool
- [ ] 3.3 Querier-side arm-schema equality: re-coerce hot batches against
      the pinned schema via the shared `common` helpers; assert
      field-for-field equality including nullability and derived columns;
      tests: LogQL attribute-equality query keeps hot rows (`attr_tokens`
      conjunct), hot/cold `date_bin` bucketing agreement
- [ ] 3.4 Hot-only provider when the Iceberg table does not exist but hot
      data does (canonical schema, `W = 0`); integration test: new tenant's
      first data queryable before first commit
- [ ] 3.5 Degradation: writer unreachable/warming/boundary-unresolvable →
      drop hot, serve cold; `querier_hot_scan_failures_total` metric +
      `signaldb.query.hot_scan_degraded` span attribute; `warnings` on the
      PromQL path (router plumbing); integration test with no writer
      running

## 4. Correctness under races

- [ ] 4.1 Integration test: query concurrent with a group flush returns
      each row exactly once (no-dup) — loop the race window
- [ ] 4.2 Integration test: commit landing between hot scan and cold
      resolution loses no rows (no-omission)
- [ ] 4.3 Read-your-writes integration test: acknowledged data visible via
      Tempo, LogQL, and PromQL surfaces immediately after ack, no
      force-commit involved

## 5. Test migration and wrap-up

- [ ] 5.1 Migrate the five flush-barrier integration suites
      (promql_queries, logql_queries, router_tempo_endpoints, e2e
      logs/metrics, and the remaining `flush_storage_writers` caller) to
      memtable visibility; keep `flush_storage_writers` and the flush
      action for operational/targeted use
- [ ] 5.2 Config flag to disable the hybrid provider (rollback path);
      document in signaldb.dist.toml and ops docs, including degradation
      semantics and the deliberate follow-up of raising `commit_interval`
- [ ] 5.3 Full workspace test run, clippy, fmt, cargo machete; update
      architecture/tempo-api skill docs for the new query flow
- [ ] 5.4 `openspec validate --strict` passes; sync deltas / archive per
      workflow
