# Tasks: Querier Execution Model

Sequenced per design D6: deadline + resource envelope first (small, ship immediately), then snapshot pinning, then streaming endpoint-by-endpoint. Groups ≈ PRs. TDD throughout.

## 1. Deadline coherence (D3, #931)

- [ ] 1.1 Tests: a query running 45s with a 60s budget completes (no 30s transport kill); a query exceeding the budget returns an attributable timeout AND stops server-side work (permit released, execution task cancelled)
- [ ] 1.2 Transport: replace `Endpoint::timeout(connection_timeout)` with `connect_timeout`; add per-request deadline derivation (remaining budget + margin) at the Flight client call sites
- [ ] 1.3 Querier: stamp deadline at admission; run execution under `timeout_at` with cooperative cancellation; ensure permit/memory-reservation release on cancel
- [ ] 1.4 Config: separate `connect_timeout` from the query budget; remove independent request-deadline knobs; update dist config + docs

## 2. Resource fairness with bounded defaults (D5, #941)

- [ ] 2.1 Tests: concurrent small query completes while a heavy query saturates the pool (fairness); pool exhaustion yields resource-exhausted error, not OOM; default config boots with bounded memory + per-tenant concurrency visible in startup logs
- [ ] 2.2 Swap `GreedyMemoryPool` → `FairSpillPool`; enable spill configuration
- [ ] 2.3 Bounded defaults: `memory_limit_mb = min(50% RAM, 4096)`, `max_concurrent_queries_per_tenant = 8`; explicit unlimited opt-out; release-note BREAKING defaults; benchmark before/after
- [ ] 2.4 Expose `target_partitions`/`batch_size` under `[querier.datafusion]` (monolithic-mode oversubscription fix)

## 3. Snapshot pinning (D4, #949)

- [ ] 3.1 Tests: commit landing mid-query is invisible to that query; a plan referencing the same table twice observes one snapshot; pinned query outlives a concurrent snapshot expiration within grace
- [ ] 3.2 Resolve tables once per query in ticket handlers; construct providers with the pinned snapshot bound; reuse the provider instance across plan references
- [ ] 3.3 Cross-reference test with compactor lifecycle: cleanup grace period covers a maximum-deadline pinned query (documentation + assertion in tests-integration)

## 4. Streaming: querier do_get (D1, #938)

- [ ] 4.1 Tests: first batch arrives before execution completes (time-to-first-byte); peak querier memory bounded under a near-row-cap result; mid-stream failure terminates the Flight stream with an attributable error
- [ ] 4.2 Replace `collect()` + `batches_to_flight_data` with `execute_stream()` + `FlightDataEncoderBuilder` in `do_get`, behind `querier.streaming` (default on); raw-SQL/query-IR tickets first
- [ ] 4.3 Roll out to remaining ticket types (trace, logs, metrics, profiles)

## 5. Streaming: router consumption + error mapping (D1, D2)

- [ ] 5.1 Tests per endpoint family (tempo/logql/promql/query): pre-first-byte failures keep today's status-code mapping (#921 regression tests stay green); mid-stream failure aborts the HTTP response attributably (no silent truncation); memory bounded while assembling format-required aggregations
- [ ] 5.2 Replace collect-then-decode with incremental `FlightRecordBatchStream` consumption in router endpoints; stream JSON where the format allows, bounded assembly where it does not
- [ ] 5.3 Remove the collect fallback + flag one release later (tracked follow-up)

## 6. Close-out

- [ ] 6.1 Full workspace lint/format/machete; tests-integration + benchmark suites green; verify no regression on `Server-Timing`/traceresponse (#918) and error bodies (#921)
- [ ] 6.2 Update GitHub: close #931/#938/#941/#949 via PRs; tick epic #953; note interaction outcomes on #921
