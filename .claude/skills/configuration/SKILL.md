---
name: configuration
description: SignalDB configuration reference - all TOML sections, environment variables, database/discovery/storage/WAL/schema/auth/queue settings, and service ports. Use when working with configuration, environment variables, or TOML settings.
user-invocable: false
sources:
  - src/common/src/config/mod.rs
  - signaldb.dist.toml
---

# SignalDB Configuration Reference

`signaldb.dist.toml` is the annotated reference config — every section
(`[database]`, `[auth]` incl. `oidc`/`default_limits`, `[storage]`,
`[schema]` incl. `materialized_labels`/`attribute_types`/`warm_index`,
`[discovery]`, `[wal]`, `[querier]` incl. `datafusion`/`warm_index` and the
Query IR `correlate`/`match`/`graph`/`page_*`/`tail_*` limits, `[writer]`,
`[processors]`, `[acceptor]`, `[compactor]` incl. `retention`/
`orphan_cleanup`/`attr_promotion`, `[self_monitoring]` incl. `frontend`,
`[profiling]`, `[tenants]`, `[mcp]` incl. `oauth`, `[public]`, `[github]`,
`[demo]`) is documented inline with defaults and rationale.
`src/common/src/config/mod.rs` has the struct definitions and, on nearly
every field, an `Env:` doc comment giving its exact environment variable
name. Precedence: defaults -> TOML file (`signaldb.toml`) -> env vars.

Behavior behind a section lives in its doc: `docs/operations/wal-persistence.md`
(`[wal]`), `docs/operations/table-provisioning.md` (`[writer]` reconciler and
WAL markers), `docs/operations/compactor/configuration.md` (`[compactor]`),
`docs/users/schema-registry.md` (`[[schema.attribute_types]]`),
`docs/architecture/storage-layout.md#attribute-storage-tiers` (warm index,
promotion), `docs/users/authentication.md` (`[auth]` limits and quotas),
`docs/operations/oidc-sso.md` (`[auth.oidc]`), `docs/operations/github-app.md`
(`[github]`), `docs/operations/demo-mode.md` (`[demo]`),
`docs/users/processors.md` (`[processors]`), `docs/users/sending-otlp.md`
(`[acceptor]`, `[public]`), `docs/users/mcp.md` (`[mcp]`), and
`docs/users/querying-ir.md` (Query IR limits).

## Gotchas not fully covered by the docs

- Env var derivation is two rules, not one: a TOML key with no underscore
  takes `SIGNALDB_<SECTION>_<FIELD>`; a key containing an underscore (most
  `[wal]`/`[compactor]`/`[discovery]` fields, and every nested section such
  as `SIGNALDB__AUTH__OIDC__*` or `SIGNALDB__GITHUB__*`) needs
  `SIGNALDB__<SECTION>__<FIELD>` instead. Single-underscore on a multi-word
  field silently resolves to the wrong path and does nothing — no error. See
  the warning at the top of `signaldb.dist.toml`.
- `[schema].catalog_uri` (Iceberg metadata catalog) accepts `sqlite://`/
  `sqlite:file:` or `postgres://`/`postgresql://`
  (`create_sql_catalog_with_builder`, `src/common/src/iceberg/mod.rs`). It is
  a separate catalog instance from the `[database]`/`[discovery]` service
  catalog, even when both point at the same PostgreSQL server.
- `ACCEPTOR_WAL_DIR` / `WRITER_WAL_DIR` (or `--wal-dir`) are read directly by
  the binaries, not via figment, name the full service directory, and win
  over `[wal].wal_dir` (whose `acceptor`/`writer` subdirectories are the
  default).

## Service Ports (Defaults)

Acceptor gRPC 4317, HTTP 4318. Writer Flight 50061 (standalone) / 50051
(monolithic). Router HTTP 3000, Flight 50053. Querier Flight 50054.
Compactor Flight 50055 (`COMPACTOR_FLIGHT_ADDR`), HTTP 9091
(`metrics_addr`).
