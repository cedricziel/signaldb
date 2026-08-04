# Declared Sort Orders End-to-End

## Why

SignalDB's Parquet files end up **physically sorted but declared unsorted** (audit epic #936): the compactor sorts output by per-signal time-leading keys, but no Iceberg `SortOrder` is declared on any table, no Parquet `sorting_columns` reach file footers, and the DataFusion scan surfaces `ordering: None` on every file. DataFusion 54 ships statistics-based file reordering and TopK sort pushdown worth 27×–49× on `ORDER BY timestamp DESC LIMIT n` — the dominant observability query shape — but only for scans that _declare_ ordering and statistics. Today SignalDB gets none of it, and `sort → limit` query plans re-sort data that is already ordered on disk.

The hard part is not plumbing but the correctness contract: declaring an ordering that files do not actually satisfy produces **wrong query results** (sorts silently elided over unsorted data), so the declaration must be provably honest at every producer.

## What Changes

- **Iceberg `SortOrder` declared at table creation** for all signal tables, matching the compactor's existing per-signal sort keys (traces `(timestamp, trace_id)`, logs `(timestamp, service_name, severity_text)`, metrics `(timestamp, metric_name, service_name)`; profiles gain a defined key).
- **Every file producer honors the declared order**: ingest (writer) sorts each batch group before writing; compaction already sorts and keeps doing so. Producers that cannot guarantee the order MUST NOT declare it on their files.
- **Parquet `sorting_columns` written into file footers** (fork writer-properties extension, same pattern as the per-column bloom-filter support already upstreamed in JanKaul/iceberg-rust#379).
- **Scan surfaces ordering and statistics**: the DataFusion table provider reports file-level `output_ordering` derived from table sort-order metadata (fork `datafusion_iceberg` extension), enabling sort elimination and ordered file grouping.
- **Engine flags enabled**: `split_file_groups_by_statistics` on in the querier session (per-file min/max statistics are already attached by the provider), so TopK queries read most-promising files first (#937's flag subset that depends on ordering; the pushdown_filters flags stay in #937).
- **One-shot cutover for pre-existing files**: tables created before this change get the sort order declared going forward; old unsorted files are handled by the Exact/Inexact scan classification (Inexact until compacted), consistent with the post-1.0 no-migration-shims policy. **BREAKING** on-disk metadata addition (table metadata gains sort-order; files gain footer metadata) — old binaries reading new tables are unsupported, per policy.

## Capabilities

### New Capabilities

- `declared-data-ordering`: the contract between file producers and the query engine — which tables declare which sort order, the honesty invariant (declared ⇒ physically true per file), how mixed sorted/unsorted file populations are classified, and the observable query-behavior guarantees (correct results always; sort elimination/TopK acceleration when ordering is declared).

### Modified Capabilities

_None — no existing spec in `openspec/specs/` covers storage layout or scan ordering._

## Impact

- **Crates**: `common` (iceberg table creation/`table_manager`, schemas), `writer` (ingest-path sort before write), `compactor` (already sorts; declaration hookup + regression guard), `querier` (session flag, plan expectations), `tests-integration` (ordering + plan-shape tests, `querier_read_paths` benchmarks).
- **Fork**: two `iceberg-rust` fork extensions to upstream — `sorting_columns` in writer properties, and `output_ordering` surfaced from table sort order in `datafusion_iceberg`. Adds to the fork-delta accounting tracked in #950.
- **Issues**: implements #936; enables the ordering-dependent half of #937; interacts with `compactor-partition-scoped-lifecycle` (compaction is the mechanism that converges old files to sorted+declared) and with `otel-native-schema` (sort keys are layout; that change's layout decisions must reference this contract).
- **Benchmarks**: `tests-integration/benches/querier_read_paths.rs` and `trace_read_analysis.rs` gate before/after.
