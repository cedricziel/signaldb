---
name: storage-layout
description: SignalDB storage layout - WAL directory structure, Iceberg catalog, object store paths, table types, segment lifecycle, and per-dataset storage overrides. Use when working with WAL, Iceberg tables, Parquet files, or storage configuration.
user-invocable: false
sources:
  - docs/architecture/storage-layout.md
  - src/common/src/storage.rs
  - src/common/src/wal/**
  - src/common/src/catalog_manager.rs
  - src/common/src/iceberg/**
  - schemas.toml
  - src/common/src/attrs/**
---

# SignalDB Storage Layout Reference

Read `docs/architecture/storage-layout.md` for the three-tier storage model
(WAL → Iceberg SQL catalog on SQLite or PostgreSQL → object store), object-store
path layout and backends, per-dataset storage overrides, Iceberg catalog/
namespace/pragma configuration, metadata retention, table types (traces, logs,
`metrics` + `metric_exemplars`, profiles), the declared sort order and its
per-file attestation rule, per-signal Iceberg schemas, the typed attribute
layout (one-shot cutover), attribute storage tiers (typed maps, opt-in warm
index, promoted `attr_*` columns), materialized labels, Parquet bloom filters
and compression, the WAL directory/segment layout, and live-table schema
evolution (including why the legacy → typed hop is a drop-and-recreate, not an
evolution).

Read `docs/operations/wal-persistence.md` for WAL directory/segment layout,
record framing, entry structure, replay behavior (including corrupted-record
handling, the pre-#932 single-WAL adoption, and `dead-letter/` artifact kinds),
write-integrity guarantees, the instance cap, and deployment sizing/capacity
planning.

## Gotchas not fully covered by the docs

- `iceberg-rust`/`datafusion_iceberg`/`iceberg-sql-catalog` are git-pinned to
  an upstream JanKaul/iceberg-rust `main` rev (declared sort orders have no
  crates.io release yet). Behaviors the docs attribute to a specific upstream
  PR (WAL-mode pragmas, delete-after-commit, encoded-bytes file-size rolling,
  sort orders) — cross-check the rev in `Cargo.toml` before assuming a
  published `iceberg-rust` has them.
- `[wal]`/`[schema]` TOML fields and their env-var forms are the
  `configuration` skill's job; this skill covers storage/WAL _behavior_, not
  the config surface.
