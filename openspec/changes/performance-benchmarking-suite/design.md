## Context

See `proposal.md` — Why. Today only `src/writer/benches/` exists (Criterion, gated by the `benchmarks` feature, in-memory catalog + `object_store::memory::InMemory`), and nothing runs it. The querier exposes its query surface through `SignalDBQuerier` (Tempo) and `QuerierFlightService`; the `query` module (`trace.rs`, `logql.rs`, `promql.rs`, …) is private. Parquet bloom filters over trace columns and materialized-label columns already exist in `common::iceberg::table_manager` via `bloom_filter_properties_for_trace_columns()` / `bloom_filter_properties_for_labels()`, applied at table-creation time — so a bloom on/off comparison is a matter of writing the same data into two tables with and without those properties.

FDAP constraint applies to the benchmark data-generation code exactly as to production: Arrow/Parquet types MUST come from DataFusion's re-exports (`datafusion::arrow::…`), mirroring the existing writer benches, to avoid version skew. Benchmarks touch no Flight wire path and no on-disk migration — the v1-wire/v2-storage transform and WAL/Iceberg layout are out of scope.

## Goals / Non-Goals

**Goals:**

- Query-path Criterion coverage (trace-lookup, one query per surface, bloom on/off) seeded from an in-memory Iceberg dataset, reusing the writer's in-memory write approach so no external services are needed.
- Per-PR compile guard for all bench targets, and a nightly release run whose results are tracked over time with a regression threshold.
- One documented local baseline/compare workflow.

**Non-Goals:**

- No macro/end-to-end throughput harness across running services — the existing `test-matrix.yml` ignored-test load job is separate and stays (only its misleadingly-named Criterion-less job is renamed).
- No per-PR _timing_ gate. Criterion on shared CI runners is too noisy; per-PR CI only compiles, nightly tracks the trend.
- No change to how bloom filters are configured in production — the bench only exercises the existing on/off table properties.
- Explicitly deferred benches (documented so the boundary is intentional, not an oversight): the agnostic query-IR lowering path (still a multi-change stack in flux — benching a moving target is wasted work), Flight RecordBatch serialization round-trip, and the v1-wire→v2-storage schema transform. Add these once the code stabilizes or a path becomes a measured bottleneck.

## Decisions

**Read benches live in `tests-integration`, exercised as DataFusion SQL over the registered table.** `tests-integration` already depends on `writer` (to seed), `querier`, `common/testing`, `datafusion_iceberg`, and the trace/log/metric generators, so the read benches need no new dep edges and no exposing of the querier's private `query` module. They register the seeded Iceberg table as a `DataFusionTable` and run the lookup/search SQL — the exact recipe proven in `tests/querier/trace_bloom_pruning.rs` — which measures the dominant scan + pruning cost the UI waits on. Rejected alternatives: a `benchmarks`-gated re-export seam in `src/querier` (more coupling, exposes internals) and benchmarking through `SignalDBQuerier`/Flight (drags in transport framing noise). CAVEAT: faithful PromQL/LogQL benches still need the querier's private metric/log engines — raw SQL over those tables is only a scan proxy — so those two either get a narrow `benchmarks` re-export of `MetricsService`/`LogsService` or stay proxies (open decision, see tasks 3.3).

**Seed data via the in-memory Iceberg write path, generated with a fixed seed.** Reuse the writer's `InMemory` object store + memory catalog to materialize Parquet, then point the querier at it. Data generation is deterministic (fixed seed, no `rand` per-iteration in the measured closure) so runs are comparable across revisions. Alternative — hand-built `RecordBatch`es queried directly through DataFusion — was rejected because it skips file pruning, which is exactly what trace-lookup and bloom benches must measure.

**Querier read benches cover the UI's actual actions.** Single-trace lookup (`find_by_id_with_tenant`), trace search / groups (`find_traces_with_tenant`), a PromQL metric query, and a LogQL log query — each over a fixed seeded dataset, measuring the dominant data-scan cost the UI waits on. A/B comparisons (bloom on/off, materialized-vs-JSON) are explicitly out of scope here: they are one-off optimization _investigations_, not steady-state latency/regression measurements, and the bloom-off side has no `ensure_table` toggle (would force hand-built Iceberg tables). Revisit as a separate investigation if needed.

**OTLP decode is benched in isolation, before WAL/Iceberg.** The acceptor bench measures only protobuf decode + Arrow conversion over a fixed in-memory payload, with no WAL or object-store write in the measured closure, so ingest CPU is attributed to the conversion step and not masked by I/O. Representative payloads for traces, logs, and metrics.

**WAL round-trip and compaction reuse the in-memory substrate.** The WAL bench serializes a representative batch and replays it back; the compaction bench generates a fixed set of input Parquet files via the in-memory write path and times the compactor rewrite. Both are `benchmarks`-gated seams in `common` (WAL) and `compactor`, mirroring the writer/querier pattern.

**WAL bench targets the post-#865 framing, not today's.** #865 is actively changing WAL Arrow-IPC framing (framed + CRC data records). The WAL round-trip bench is written against the fixed framing so it does not have to be rewritten when the fix lands — implementation is sequenced after, or alongside, that fix stack rather than benching soon-to-change encode logic.

**Compile guard runs for every crate with benches.** `cargo check --benches --features benchmarks` covers `writer`, `querier`, `acceptor`, `compactor`, and `common`.

**Trend tracking via `benchmark-action/github-action-benchmark`.** It ingests Criterion's JSON output, commits a historical series to a `gh-pages`-style data branch, renders a trend chart, and supports `alert-threshold` + `fail-on-alert`. Chosen over rolling our own storage: it is purpose-built for Criterion, needs no external service, and keeps history in-repo. The alert threshold is a workflow input (start ~150–200% i.e. 1.5–2× to absorb runner noise, tune from observed variance).

**Compile guard = `cargo check --benches --features benchmarks` per PR.** `check` not `build` — we only need to prove the targets compile, not produce runnable bench binaries, keeping the PR step cheap. Crate list is enumerated in the compile-guard decision above.

**Nightly workflow: `schedule` cron, release mode, `--test-threads`/`-j 1` semantics via Criterion defaults, single job (not the light/medium/heavy matrix).** The existing `performance-benchmark` matrix in `test-matrix.yml` is renamed/removed since it runs ignored tests, not Criterion — keeping the name invites confusion about what is actually gated.

## Risks / Trade-offs

- **Criterion noise on GitHub-hosted runners produces false regression alerts** → nightly-only (never per-PR), a generous initial `alert-threshold`, and `fail-on-alert` tuned after observing a week of variance; the trend chart stays even when alerts are silenced.
- **In-memory `InMemory` object store has different I/O characteristics than S3/local disk**, so absolute numbers are not production latencies → benches are for _relative_ regression detection ("are we making it worse"), stated explicitly in the docs; not marketed as SLA numbers.
- **Bench-only public seam risks drift from real query code paths** → keep the `benchmarks` feature re-exporting the same functions production uses (thin re-export, no bench-specific logic), so a signature change breaks the bench compile — which the per-PR guard catches.
- **`gh-pages` data-branch write needs a token with contents write** → use the workflow's `GITHUB_TOKEN` with `contents: write` scoped to the nightly job only.
- **WAL bench coupled to the in-flight #865 framing change** → sequence WAL bench implementation after/alongside the #865 fix stack so it targets the fixed framing; if built earlier it must be revisited when framing changes.

## Migration Plan

Purely additive tooling — no runtime, schema, or data migration, no rollback concern beyond reverting the added CI files and bench crates. Order: (1) querier `benchmarks` feature + read-path benches (trace-lookup, trace search, PromQL, LogQL), (2) acceptor OTLP-decode + compactor rewrite benches, (3) WAL round-trip bench (sequenced with the #865 fix), (4) per-PR compile guard across all bench crates, (5) nightly workflow + trend branch, (6) rename/remove the stale `performance-benchmark` job, (7) contributor docs. Each step is independently revertable.

## Open Questions

- Exact initial `alert-threshold` and whether `fail-on-alert` is on from day one or after a variance-observation period — deferrable; it is a workflow input that does not change the specs, the bench code, or the task breakdown.
