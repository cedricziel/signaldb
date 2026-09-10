# Design: Declared Sort Orders End-to-End

## Context

See proposal.md — Why. Load-bearing code facts (audit-verified, 2026-08-04):

- Table creation passes name/schema/partition-spec/location/properties only (`src/common/src/iceberg/table_manager.rs:126-152`); zero workspace hits for `SortOrder`.
- Compactor already sorts per signal (`src/compactor/src/rewriter.rs:442-467`); profiles have no sort entry and warn.
- Ingest writes batches in arrival order (`src/writer/src/storage/iceberg.rs:289-305`).
- Fork writer sets only compression+blooms (`iceberg-rust/src/arrow/write.rs:516-518`) — no `sorting_columns`; fork provider sets `ordering: None` per `PartitionedFile` and never calls `.with_output_ordering` (`datafusion_iceberg/src/table/mod.rs:1121-1131`, `:959-964`), but _does_ attach per-file min/max statistics (`:1124`).
- Querier session is a bare `SessionConfig::new()` (`src/querier/src/flight.rs:339`); `split_file_groups_by_statistics` defaults false.
- DataFusion 54.1 classifies scans Exact/Inexact/Unsupported for sort pushdown; Exact removes the sort entirely — which is where a dishonest declaration becomes a wrong-results bug.

Constraints: post-1.0 BC breaks fine (no migration shims); fork changes should be upstreamable (precedent #379/#382); `compactor-partition-scoped-lifecycle` defines how compaction jobs are scoped — this change only adds the attribution obligations to its output.

## Goals / Non-Goals

**Goals:**

- One canonical, declared, machine-readable sort order per signal table, honored by every producer.
- Honest per-file attribution so the engine's Exact-vs-Inexact classification is trustworthy by construction.
- Measured TopK/ordered-scan wins on the standard benchmarks.

**Non-Goals:**

- Changing partition specs or sort _keys_ beyond defining one for profiles (layout redesign belongs to `otel-native-schema`).
- Enabling `pushdown_filters`/`reorder_filters` (remains in #937 — orthogonal to ordering).
- Backfilling/rewriting legacy files eagerly (compaction converges them; no one-off migration job).
- Z-order or multi-dimensional clustering.

## Decisions

**D1 — Attribution carrier: Iceberg table `SortOrder` (table-level intent) + per-file Parquet footer `sorting_columns` (file-level honesty).**
The provider derives scan `output_ordering` from the table SortOrder but claims it per file only when the file's footer attests it (legacy files lack the footer entry ⇒ Inexact). This makes the mixed-population rule mechanical: attribution travels with the file, intent with the table. Alternative — table-level declaration only, assume all files comply — rejected: pre-change files exist, and one dishonest file under an Exact claim yields wrong results.

**D2 — Sort keys: freeze the compactor's existing keys as canonical (traces `(timestamp, trace_id)`, logs `(timestamp, service_name, severity_text)`, metrics `(timestamp, metric_name, service_name)`); define profiles as `(timestamp, service_name)`.**
Rationale: these keys are already produced today, so declaring them costs no rewrite; time-leading matches the dominant query shape and the hour partitioning. Revisit keys only inside `otel-native-schema`'s layout work, which must then amend this spec.

**D3 — Ingest sorts per write, inside the writer's persist path, before `write_parquet_partitioned`.**
A per-commit-group Arrow sort (kernel `lexsort`/`sort_to_indices` + `take`) on batches that are typically seconds of data — cheap, bounded, and it makes ingest files honestly attributable immediately rather than waiting for compaction. Alternative — leave ingest files unattributed (Inexact) and let compaction attribute — rejected: the hot recent data is exactly what `ORDER BY time DESC LIMIT n` queries touch, so deferring attribution defers most of the win.

**D4 — Fork changes, both upstream-first: (a) `set_sorting_columns` derived from table SortOrder in the writer-properties builder (mirror of #379's bloom plumbing); (b) `datafusion_iceberg` maps table SortOrder → `FileScanConfigBuilder::with_output_ordering`, claiming per-file ordering only for footer-attested files (D1).**
Carried on the cedricziel fork until merged, extending the delta tracked in #950/#955.

**D5 — Engine flag: enable `split_file_groups_by_statistics` in the querier session as part of this change; leave `pushdown_filters`/`reorder_filters` to #937.**
The flag's benefit depends on ordering+stats, so it belongs here with the benchmark gate; the filter flags have independent risk/benefit and their own measurement plan.

**D6 — Verification of honesty: debug/test-time sortedness assertion at the writer boundary (feature-gated batch check), plus an integration test that plans `ORDER BY … LIMIT` over a deliberately mixed table and diffs against optimization-disabled results.**
No production-time full-file verification (cost); honesty is enforced at producers + tested, per the spec's "defect, not tolerated state" stance.

## Risks / Trade-offs

- [Dishonest attribution ⇒ silently wrong results] → D1's per-file attestation + D6 tests + the mixed-population correctness scenario as a permanent regression test. This is the risk the whole design is shaped around.
- [Ingest sort adds write-path latency] → Sort is per commit group (seconds of data) and columnar; measured in the write benchmarks. If a pathological group is huge, the sort is still O(group), not O(table).
- [Fork PRs stall upstream] → Same posture as #379/#382: carry on the fork, keep the delta small and listed in #950.
- [DF 55 changes sort-pushdown APIs mid-flight] → The declaration (SortOrder + footers) is engine-version-independent; only the provider mapping might need rebasing at the coordinated DF 55 bump.
- [Old binaries reading new table metadata] → Accepted per post-1.0 policy; release-noted as BREAKING.

## Migration Plan

1. Land fork changes (D4) and pin.
2. Declare SortOrder on _new_ table creation + add-sort-order on existing tables at startup (idempotent metadata update); ingest sort (D3) ships in the same release so new files are attributed from day one.
3. Enable D5 flag; run benchmark gates.
4. Legacy files converge via compaction (`compactor-partition-scoped-lifecycle`); no explicit backfill. Rollback: disable the session flag and stop attributing (metadata declarations are inert without the provider claim).

## Open Questions

- Whether metrics' key should lead with `metric_name` before `timestamp` for series-scan locality — deferrable: changing the key later is a compaction-rewrite away and does not alter this contract; evaluate with benchmark data (feeds `otel-native-schema` layout work).
