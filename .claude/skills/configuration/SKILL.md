---
name: configuration
description: SignalDB configuration reference - all TOML sections, environment variables, database/discovery/storage/WAL/schema/auth/queue settings, and service ports. Use when working with configuration, environment variables, or TOML settings.
user-invocable: false
---

# SignalDB Configuration Reference

`signaldb.dist.toml` is the annotated reference config — every section
(`[database]`, `[auth]`, `[storage]`, `[schema]`, `[discovery]`, `[wal]`,
`[querier]`, `[writer]`, `[compactor]` incl. `retention`/`orphan_cleanup`/
`attr_promotion`, `[mcp]` incl. `oauth`, `[self_monitoring]` incl. `frontend`,
`[profiling]`, `[tenants]`) is documented inline with defaults and rationale.
`src/common/src/config/mod.rs` has the struct definitions and, on nearly
every field, an `Env:` doc comment giving its exact environment variable
name. Precedence: defaults -> TOML file (`signaldb.toml`) -> env vars.

## Gotchas not fully covered by the docs

- Env var derivation is two rules, not one: a TOML key with no underscore
  takes `SIGNALDB_<SECTION>_<FIELD>`; a key containing an underscore (most
  `[wal]`/`[compactor]`/`[discovery]` fields) needs
  `SIGNALDB__<SECTION>__<FIELD>` instead. Single-underscore on a multi-word
  field silently resolves to the wrong path and does nothing — no error. See
  the warning at the top of `signaldb.dist.toml`.
- `[schema].catalog_uri` (Iceberg metadata catalog) accepts SQLite only and
  rejects `postgres://` at startup — unlike `[database]`/`[discovery]`,
  which do support PostgreSQL.

## Service Ports (Defaults)

Acceptor gRPC 4317, HTTP 4318. Writer Flight 50061 (standalone) / 50051
(monolithic). Router HTTP 3000, Flight 50053. Querier Flight 50054.
Compactor Flight 50055 (`COMPACTOR_FLIGHT_ADDR`), HTTP 9091
(`metrics_addr`).
