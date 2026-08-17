# CLAUDE.md

SignalDB: distributed observability database on the FDAP stack (Flight, DataFusion, Arrow, Parquet) with native OTLP ingest and Tempo API compatibility. Architecture, configuration, storage, and API docs live in `docs/` — read those instead of guessing; `.claude/skills/` are routers into them.

## Non-obvious facts

- One binary: `signaldb` runs the monolith, `signaldb <service>` (acceptor, router, writer, querier, compactor, mcp) runs one service. `./scripts/run-dev.sh` wraps the common local setups; local state lives under `.data/` (WAL, Parquet, SQLite).
- Write path: Acceptor (OTLP) → WAL → Writer (Flight) → Iceberg/Parquet. Query path: Router (HTTP) → Querier (Flight) → DataFusion → Iceberg.
- Config precedence: defaults → `signaldb.toml` → `SIGNALDB_*` env. `signaldb.dist.toml` is the annotated reference.
- Tenancy: `Authorization: Bearer <key>` plus an explicit `X-Tenant-ID` header (intentional; don't infer tenant from the key). WAL is laid out `{tenant}/{dataset}/{signal}/`, Iceberg tables are namespaced per tenant.
- Tables: the writer reconciles signal tables per tenant/dataset (startup + `[writer].table_reconcile_interval`), so a dataset is queryable before its first write; ingest still load-or-creates. Queries against a signal with no table return empty, never an error. See `docs/operations/table-provisioning.md`.
- JS tooling is pnpm (root workspace + lockfile); never `npm install`.
- Fresh worktrees: run `git submodule update --init opentelemetry-proto` or tempo-api fails to build.

## Rust rules (full text: `docs/contributing/rust.md`)

- Import Arrow/Parquet types via DataFusion's re-exports, never the crates directly (version skew).
- `tracing` only, never `log::`; boundary spans (HTTP, gRPC/Flight, SQL catalog, jobs) come from `common::self_monitoring::spans` factories; `#[instrument]` always with `skip_all` + explicit fields; `otel.kind` never set outside `common::self_monitoring`. CI enforces all of this.
- `thiserror` for library-style errors, `anyhow` + `.context()` at application boundaries; no `unwrap`/`expect` outside tests.
- No emoji in logs (CLI output is fine).
- Add deps to `[workspace.dependencies]`. The cargo-husky pre-commit hook runs fmt, clippy `-D warnings`, `cargo machete`, and `cargo deny check` when Rust files are staged (and UI typecheck/lint for `src/ui/`); commits touching neither skip both.
- Setting `RUSTFLAGS` replaces the per-target `rustflags` in `.cargo/config.toml` instead of merging.

## Workflow

- TDD: failing test first; all tests pass before committing. Use testcontainers for external services.
- Semantic commits.
- Docs owe updates when behavior changes; the `docs` skill and the TaskCompleted hook say which file.
