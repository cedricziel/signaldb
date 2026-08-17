---
name: crate-map
description: SignalDB crate map - workspace members, module locations within common/writer/querier/router crates, and key root files. Use when navigating the codebase, finding where code lives, or understanding module boundaries.
user-invocable: false
---

# SignalDB Crate Map

Workspace members: `cat Cargo.toml` (`[workspace].members`) — don't hand-copy
that list here, it goes stale (e.g. the `logql` LogQL-parser crate is easy to
miss). Module layout within a crate: `ls -R src/<crate>/src` (nested dirs like
`query/`, `endpoints/`, `iceberg/`, `retention/`, `orphan/` hold most of the
interesting code in querier/router/compactor).

Orientation:

- `src/common/` is the shared foundation — config, auth, WAL, Flight schemas/
  transport, Iceberg catalog integration, schema parsing, service discovery.
  Read it first when unsure where shared logic lives.
- Every service crate (`acceptor`, `writer`, `router`, `querier`, `compactor`)
  exposes `cli::Args` + `cli::run(common, args)`; `signaldb-bin` wires them as
  clap subcommands of the one `signaldb` binary — there are no per-service
  `[[bin]]`s.
- `src/grafana-plugin/backend` is excluded from the root workspace: its
  `grafana-plugin-sdk` pin drags in a second Arrow major version. It has its
  own `Cargo.toml` and CI.
- `schemas.toml` (repo root) is the physical-schema source of truth for all
  six built-in table types, compiled in via `include_str!`.
- `vendor/otel-semconv/` and `otel/registry/` are the sources for the bundled
  `otel`/`signaldb` schema registries (see the `schema-registry` docs).
