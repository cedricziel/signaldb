# Design: Compactor Partition-Scoped Lifecycle Rework

## Context

See proposal.md — Why. Load-bearing code facts (audit-verified, 2026-08-04):

- Planner groups all live files under one `"all"` key and its comment couples planning scope to rewrite/commit scope (`src/compactor/src/planner.rs:245-253`); executor calls `rewrite_table` which registers the whole table, globally sorts, and `.collect()`s into an unbounded default `SessionContext` (`src/compactor/src/rewriter.rs:470-508`); commit is a whole-table `replace` guarded on snapshot-id equality (`src/compactor/src/commit.rs:78-95`), while ingest commits per `(tenant, dataset, table)` every ~5s.
- Orphan detection filters snapshots by age before building the live set (`src/compactor/src/orphan/detector.rs:237-266`); `revalidate_before_delete` already rebuilds it correctly via `build_live_file_set(&table, None)` (`detector.rs:456`) — the correct algorithm exists and is currently the optional safety net. #475 notes its scalability limits.
- Partition values are path-parsed in two places (`src/compactor/src/iceberg/partition.rs:155-158`, `src/compactor/src/retention/enforcer.rs:512-517`); manifest entries already expose `data_file.partition()`.
- The rewrite write side already consumes a stream (`rewriter.rs:197-205` feeds `write_parquet_partitioned` a stream), so `collect()` → `execute_stream()` is a bounded change.
- Fork transaction surface in use today: `append_data`, `replace`, `update_properties`, `expire_snapshots` (JanKaul iceberg-rust via cedricziel fork).
- Row-count parity verification exists on both sides (`rewriter.rs:216-221`, `executor.rs:378-387`) and must be preserved.

Constraints: multi-tenant, homelab-first (single-digit-GB RAM boxes must compact 50 GB+ tables); post-1.0 BC breaks are acceptable but must be labeled; `otel-native-schema` layers 7–8 will reuse the scoped-rewrite primitive for layout migration.

## Goals / Non-Goals

**Goals:**

- Compaction cost and peak memory proportional to the compaction unit (one partition), not table size.
- Commits that survive concurrent ingest by construction (delta semantics + input-scoped conflict detection).
- Orphan detection whose _detection_ algorithm is the safe one, with re-validation as defense-in-depth rather than the only correct pass.
- Default config that both compacts and reclaims.
- A reusable "rewrite this partition's files and swap them atomically" primitive that later layout-migration work can drive.

**Non-Goals:**

- Declaring sort orders in table metadata (companion change `declared-sort-orders`; this change only keeps output physically sorted).
- Attribute/layout schema migration itself (`otel-native-schema`).
- Snapshot-pinned queries (`querier-execution-model`); this change only respects retained snapshots during cleanup.
- Cross-partition rewrite optimizations (e.g. merging adjacent hours into day files) — future work once the scoped primitive exists.

## Decisions

**D1 — Partition scope = one Iceberg partition value of the existing `Hour(timestamp)` spec; "closed" = partition end < now − lateness window (default 2× the WAL flush/commit horizon, config `compactor.partition_lateness`).**
Rationale: hour partitions already exist on all signal tables; closed-only selection removes the ingest/compaction race on the hot partition entirely, which is cheaper and more robust than fine-grained conflict handling there. Alternative — compact the open partition with retries — rejected: it reintroduces the starvation the audit found, for marginal benefit (the open hour is small by definition).

**D2 — Delta commit via the fork's transaction API: `overwrite`-style operation carrying explicit `removed_files` + `added_files`, validated against the job's input set only.**
Conflict rule: re-load table at commit time; the commit proceeds iff every input file is still live in the current snapshot's target partition. Appends elsewhere never conflict; retention dropping the target partition does. If the fork's transaction surface only offers whole-table `replace`, extend it upstream (precedent: #379, #382) rather than emulating deltas with `replace` — emulation would re-import the global race. Alternative — optimistic whole-table replace with rebase-on-conflict — rejected: rebase logic is strictly harder than a scoped commit and still O(table) in metadata.

**D3 — Candidacy: per-partition trigger `small_file_count >= file_count_threshold` where small = `size < target_file_size`; job inputs = all files in the partition below target size, capped by an enforced `max_files_per_job` (largest-count-first). `min_input_file_size_kb` is deleted, not repurposed.**
Deleting (vs inverting) the key makes the **BREAKING** config change loud at parse time for anyone who set it. Unknown keys already fail config parsing, which is the desired surfacing.

**D4 — Rewrite executes per job as `execute_stream` through a `RuntimeEnv` with `FairSpillPool(compactor.memory_limit_mb)` and spill enabled; sort remains per-partition (bounded), output rolls files at target _encoded_ size using the writer's actual bytes-written feedback rather than `get_array_memory_size()` estimates.**
Rationale: the in-memory-size estimate under-delivers file sizes ~5–10× (audit finding); the Parquet writer knows real encoded bytes. Alternative — static compression-ratio factor — rejected as data-shape-dependent guesswork.

**D5 — Orphan detection builds the live set as: manifests reachable from current snapshot ∪ manifests of all retained snapshots (retained = not expired by `snapshots_to_keep`/expiration policy). The snapshot-age filter is removed. Pre-delete re-validation stays, unconditionally (config flag removed), as defense-in-depth.**
This makes detection correct rather than "wrong but rescued". Scalability (#475): stream manifest entries into a hashed set keyed by path; do not materialize per-file structs. The age knob (`max_snapshot_age_hours`) survives only as an _expiration_ input, never a liveness input.

**D6 — Serialization between lifecycle actors: retention drops, compaction commits, and snapshot expiration for the _same table_ run under a per-table async mutex inside the compactor service; cross-process safety still rests on catalog CAS + D2's input-scoped validation.**
Rationale: the compactor owns all three loops in-process today; a cheap local ordering removes most self-conflicts, and CAS remains the true guard for multi-writer deployments.

**D7 — Defaults: `orphan_cleanup.enabled = true`, `dry_run = false`, `grace_period_hours = 24`, `revalidate` unconditional. Flip lands in the same release as D5, sequenced after it in the task order, and is called out **BREAKING** (behavioral).**

## Risks / Trade-offs

- [Fork lacks a scoped remove/add transaction op] → Upstream contribution before the executor lands (tasks front-load it); fallback is carrying it in the cedricziel fork as with #382. Migration to apache/iceberg-rust later must re-check this surface (tracked in #950/#955).
- [Closed-only selection leaves the current hour fragmented] → Accepted: the open hour is bounded (≤1h of ingest); it becomes eligible one lateness-window later. Lateness window is config for high-skew clocks.
- [Deleting `min_input_file_size_kb` breaks existing TOML] → Intentional and loud (parse error). Release notes + `signaldb.dist.toml` updated in the same PR.
- [Default-on orphan cleanup meets a pre-existing broken live-set in mixed-version rollouts] → D5 and D7 ship in the same release and tasks order D5 first; grace period + unconditional re-validation bound the blast radius even if operators toggle early.
- [Per-partition sort spills on huge partitions] → FairSpillPool + spill path; worst case the job fails attributably (spec: resource error, no OOM), and `max_files_per_job` shrinks the unit.
- [Manifest-derived partition reads cost more than path parsing] → Manifest entries are already read for planning; incremental cost is negligible against a rewrite job.

## Migration Plan

1. Ship D5 (live-set correctness) + D3 config change + D2/D1/D4 executor rework behind the existing compactor enable flag; dry-run remains available for validation.
2. Flip D7 defaults in the same release, release-noted as BREAKING behavior.
3. Rollback: re-disable orphan cleanup via config; the scoped executor has no on-disk format change, so reverting the binary reverts behavior. Delta commits are ordinary Iceberg snapshots — time-travel/rollback semantics are unchanged.

## Open Questions

- Whether `expire_snapshots` cadence should tighten once cleanup is default-on (today `snapshots_to_keep = 10`); safe to tune post-landing with metrics.
- Whether the scoped-rewrite primitive should expose a "rewrite with transform" hook now for `otel-native-schema` layer 7, or be extended when that layer lands. Default: extend later; keep the primitive minimal.
