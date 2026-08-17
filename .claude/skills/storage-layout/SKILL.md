---
name: storage-layout
description: SignalDB storage layout - WAL directory structure, Iceberg catalog, object store paths, table types, segment lifecycle, and per-dataset storage overrides. Use when working with WAL, Iceberg tables, Parquet files, or storage configuration.
user-invocable: false
---

# SignalDB Storage Layout Reference

Read `docs/architecture/storage-layout.md` for the three-tier storage model,
object-store path layout and backends, per-dataset storage overrides, Iceberg
catalog/namespace/pragma configuration, table types, per-signal Iceberg
schemas, typed attribute maps, materialized labels, Parquet bloom filters and
compression, and live-table schema evolution.

Read `docs/operations/wal-persistence.md` for WAL directory/segment layout,
entry structure, replay behavior (including corrupted-record handling and
`dead-letter/` artifact kinds), write-integrity guarantees, and deployment
sizing/capacity planning.

## Gotchas not fully covered by the docs

- Iceberg fork pins (WAL-mode pragmas, delete-after-commit, real-encoded-bytes
  file-size rolling) each name their upstream JanKaul/iceberg-rust PR in the
  relevant doc section — cross-check `Cargo.toml` before assuming a stock
  `iceberg-rust` has the behavior; SignalDB is temporarily pinned to a fork.
- `[wal]`/`[schema]` TOML fields and their env-var forms are the
  `configuration` skill's job; this skill covers storage/WAL _behavior_, not
  the config surface.
