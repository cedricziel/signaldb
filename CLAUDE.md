# CLAUDE.md

SignalDB: distributed observability database on the FDAP stack (Flight, DataFusion, Arrow, Parquet) with native OTLP ingest and Tempo API compatibility. Architecture, configuration, storage, and API docs live in `docs/` — read those instead of guessing. The `architecture`, `crate-map`, `storage-layout`, `service-discovery`, `configuration`, `flight-schemas`, `multi-tenancy`, `tempo-api`, and `http-api` skills route into them.

## Non-obvious facts

- One binary: `signaldb` is the monolith, `signaldb <service>` runs one service (acceptor, router, writer, querier, compactor, mcp). Ports are in `signaldb.dist.toml`.
- Local runs: `./scripts/run-dev.sh` (monolith), `services` (microservices, logs to `.data/logs/`), `--sqlite` (default, no dependencies), `--with-deps --postgres` (PostgreSQL via docker compose). WAL in `.data/wal/`, Parquet in `.data/storage/`, SQLite in `.data/*.db`.
- Write path: Acceptor (OTLP) → WAL → Writer (Flight) → Iceberg/Parquet. Query path: Router (HTTP) → Querier (Flight) → DataFusion → Iceberg.
- Config precedence: defaults → `signaldb.toml` → `SIGNALDB_*` env. Key sections: `[database]`, `[storage]`, `[discovery]`, `[wal]`, `[schema]`, `[auth]`; `signaldb.dist.toml` is the annotated reference.
- Tenancy: API-key requests send `Authorization: Bearer <api-key>` plus an explicit `X-Tenant-ID: <tenant>` header (intentional; don't infer the tenant from the key) and optionally `X-Dataset-ID: <dataset>`. OAuth access tokens carry their tenant grant from consent: a single-tenant grant ignores `X-Tenant-ID`, a multi-tenant grant requires it as the selector. WAL is laid out `{tenant}/{dataset}/{signal}/`, Iceberg tables are namespaced per tenant.
- Table lifecycle: the writer runs a signal-table reconciler (startup pass plus every `[writer].table_reconcile_interval`, default 5m) that ensures every registered tenant/dataset holds a table for each signal type enabled for that tenant, so a dataset is queryable before its first write. The ingest path still load-or-creates on demand, so a failing reconciler degrades to create-on-first-write. `POST /api/v1/tenants/{id}/tables/create` is the manual trigger. Queries against a signal with no table return an empty result, never an error. See `docs/operations/table-provisioning.md`.
- JS tooling is pnpm (root workspace + `pnpm-lock.yaml`); `npm install` desyncs the lockfile.
- Fresh worktrees: run `git submodule update --init opentelemetry-proto` or tempo-api fails to build.

## Query IR is our own query surface

Everything first-party that reads data — the Explore UI, CLI, MCP tools,
tests, benchmarks, debugging — goes through the Query IR
(`POST /api/v1/query`, see `docs/users/querying-ir.md`), never the
Tempo/Loki/Prometheus/Pyroscope compatibility APIs. Those exist for external
clients (Grafana) and are lossy by design. If the IR can't express something
we need, extend the IR (a logical field, a stage) rather than reaching for a
compat endpoint.

## Verifying a change

When a commit stages Rust files, the cargo-husky pre-commit hook runs `cargo fmt --check` and `cargo clippy --workspace --all-targets --all-features`, so it compiles the whole workspace (UI files trigger the pnpm checks instead). Run the scoped checks yourself first:

```bash
cargo fmt
cargo clippy -p <crate> --all-targets --all-features -- -D warnings
cargo test -p <crate> <filter>
cargo machete --with-metadata        # when a Cargo.toml changed
cargo deny check                     # when dependencies changed
pnpm --filter ./src/ui typecheck && pnpm --filter ./src/ui lint && pnpm --filter ./src/ui test   # UI changes
```

Build with `CARGO_INCREMENTAL=0` (keeps sccache hits high, matches CI) and stop building below ~8 GB free disk.

## Rust rules (full guide: `docs/contributing/rust.md`, read it when writing or reviewing Rust)

- Use Arrow & Parquet types re-exported by DataFusion, never the crates directly (version skew).
- Use `tracing`, never `log::` macros — CI rejects them (span-construction guard).
- Boundary spans (HTTP/gRPC/Flight servers and clients, SQL catalog, background jobs) come from the factories in `common::self_monitoring::spans`. No bare `#[tracing::instrument]` — always `skip_all` plus explicit bounded fields — and `otel.kind` never appears outside `common::self_monitoring`.
- Setting `RUSTFLAGS` _replaces_ the per-target `rustflags` in `.cargo/config.toml` instead of merging; see `docs/operations/binaries.md`.
- No `.unwrap()`/`.expect()` in production paths; `thiserror` for library code, `anyhow` with context for application code.
- No emoji in logs (CLI output is fine). Add deps to `[workspace.dependencies]`.

## Workflow

- Test Driven Development: write tests before implementing features; all tests pass before committing. Use testcontainers for integration tests involving external services.
- Delegate implementation to the `oss:coder` subagent: any scoped "write/change code and make it pass" task — feature, fix, refactor, test. The orchestrating session plans, reviews the result (`rust-code-reviewer` for Rust), and integrates. Keep investigation, architecture, and gnarly debugging out of it (route those to `model: fable`). The task prompt must state the acceptance test, files in scope, and whether to push — a prompt checklist overrides inherited rules, so keep it complete or omit it.
- Use semantic commits for all changes.
- Docs owe updates when behavior changes; the `docs` skill and the TaskCompleted hook say which file.
