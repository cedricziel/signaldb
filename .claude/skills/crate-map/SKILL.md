---
name: crate-map
description: SignalDB crate map - workspace members, module locations within common/writer/querier/router crates, and key root files. Use when navigating the codebase, finding where code lives, or understanding module boundaries.
user-invocable: false
sources:
  - Cargo.toml
---

# SignalDB Crate Map

Workspace members: `cat Cargo.toml` (`[workspace].members`) for the
authoritative list; `docs/architecture/overview.md`'s workspace table says
what each crate is for. Module layout within a crate: `ls -R src/<crate>/src`
(nested dirs like `query/`, `endpoints/`, `iceberg/`, `retention/`,
`orphan/`, `metric_ops/` hold most of the interesting code in
querier/router/compactor).

Read `docs/contributing/compat-crates.md` for the dependency rules on the
query-language and leaf crates (`logql`/`traceql` parse only, published as
`logql-parser`/`traceql-parser`; `ql-ir` lowers them onto the IR with nothing
FDAP; `query-ir` and `eval-model` are leaf crates checked by
`scripts/check-leaf-purity.sh`). Read `docs/architecture/openapi-codegen.md`
for `signaldb-api` (admin DTOs), the progenitor-generated `signaldb-sdk`, and
the `xtask` generators.

Orientation:

- `src/common/` is the shared foundation — config, auth, WAL, Flight schemas/
  transport, Iceberg catalog integration, schema parsing, service discovery,
  processors, eval sets. Read it first when unsure where shared logic lives.
- Every service crate (`acceptor`, `writer`, `router`, `querier`, `compactor`)
  exposes `cli::Args` + `cli::run(common, args)`; `signaldb-bin` wires them as
  clap subcommands of the one `signaldb` binary — there are no per-service
  `[[bin]]`s.
- `src/grafana-plugin/backend` is excluded from the root workspace: its
  `grafana-plugin-sdk` pin drags in a second Arrow major version. It has its
  own `Cargo.toml` and CI.
- `workspace-hack/` is managed by `cargo hakari` (one unified feature set for
  shared deps); see `workspace-hack/README.md`.
- `schemas.toml` (repo root) is the physical-schema source of truth for every
  built-in table type, compiled in via `include_str!`.
- `vendor/otel-semconv/`, `vendor/otel-semconv-genai/`, `otel/registry/`, and
  `otel/registry-genai/` are the sources for the bundled `otel`/`otel-genai`/
  `signaldb` schema registries (see `docs/users/schema-registry.md`).
