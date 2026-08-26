# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SignalDB is a distributed observability signal database built on the FDAP stack (Flight, DataFusion, Arrow, Parquet). It's designed for cost-effective storage and querying of metrics, logs, and traces with native OTLP support and Tempo API compatibility.

## Development Commands

One binary: `signaldb` is the monolith, `signaldb <service>` runs one service (acceptor, router, writer, querier, compactor, mcp). Ports are in `signaldb.dist.toml`.

### Local Development

```bash
./scripts/run-dev.sh              # Monolithic mode with local file storage
./scripts/run-dev.sh services     # Microservices mode (logs to .data/logs/)
./scripts/run-dev.sh --sqlite     # SQLite mode (default, no dependencies)
./scripts/run-dev.sh --with-deps --postgres  # With PostgreSQL via docker compose
```

Storage locations: WAL files in `.data/wal/`, Parquet data in `.data/storage/`, SQLite in `.data/*.db`

### Pre-Commit Workflow

The project uses cargo-husky for pre-commit hooks that automatically run:

```bash
cargo fmt                  # Format code (runs automatically on commit)
cargo clippy --workspace --all-targets --all-features  # Lint (runs automatically on commit)
cargo machete --with-metadata  # Check for unused dependencies (run manually before commit)
cargo deny check           # License and security auditing
```

JS tooling is pnpm (root workspace + `pnpm-lock.yaml`); `npm install` desyncs the lockfile.

## Architecture

**Key Principle**: Use Arrow & Parquet types re-exported by DataFusion to ensure version compatibility.

Configuration precedence: defaults → TOML file (`signaldb.toml`) → environment variables (`SIGNALDB_*`). Key sections: `[database]`, `[storage]`, `[discovery]`, `[wal]`, `[schema]`, `[auth]`.

The `architecture`, `crate-map`, `storage-layout`, `service-discovery`, `configuration`, and `flight-schemas` skills route into `docs/` for everything else.

## Multi-Tenancy & Authentication

**Request Headers**:

- `Authorization: Bearer <api-key>`
- `X-Tenant-ID: <tenant>`
- `X-Dataset-ID: <dataset>` (optional)

**Isolation**: WAL organized by tenant/dataset (`.wal/{tenant}/{dataset}/{signal}/`), Iceberg tables namespaced per tenant.

**Table lifecycle**: the writer runs a signal-table reconciler (startup pass plus every `[writer].table_reconcile_interval`, default 5m) that ensures every registered tenant/dataset holds a table for each signal type enabled for that tenant, so a dataset is queryable before its first write. The ingest path still load-or-creates on demand, so a failing reconciler degrades to create-on-first-write. `POST /api/v1/tenants/{id}/tables/create` is the manual trigger. Queries against a signal with no table return an empty result, never an error. See `docs/operations/table-provisioning.md`.

## Key Development Patterns

### Query IR is our own query surface

Everything first-party that reads data — the Explore UI, CLI, MCP tools,
tests, benchmarks, debugging — goes through the Query IR
(`POST /api/v1/query`, see `docs/users/querying-ir.md`), never the
Tempo/Loki/Prometheus/Pyroscope compatibility APIs. Those exist for external
clients (Grafana) and are lossy by design. If the IR can't express something
we need, extend the IR (a logical field, a stage) rather than reaching for a
compat endpoint.

### Rust rules CI enforces (full guide: `docs/contributing/rust.md`)

- Use `tracing`, never `log::` macros — CI rejects them (span-construction guard).
- Boundary spans (HTTP/gRPC/Flight servers and clients, SQL catalog, background jobs) come from the factories in `common::self_monitoring::spans`. No bare `#[tracing::instrument]` — always `skip_all` plus explicit bounded fields — and `otel.kind` never appears outside `common::self_monitoring`.
- Setting `RUSTFLAGS` _replaces_ the per-target `rustflags` in `.cargo/config.toml` instead of merging; see `docs/operations/binaries.md`.
- No `.unwrap()`/`.expect()` in production paths; `thiserror` for library code, `anyhow` with context for application code.

## Development Guidelines

- Test Driven Development: write tests before implementing features; all tests pass before committing
- Use testcontainers for integration tests involving external services
- Rust coding standards: `docs/contributing/rust.md` (read it when writing or reviewing Rust)
- Delegate implementation to the `coder` subagent (`.claude/agents/coder.md`, model sonnet): any scoped "write/change code and make it pass" task — feature, fix, refactor, test. The orchestrating session plans, reviews the result (`rust-code-reviewer` for Rust), and integrates. Keep investigation, architecture, and gnarly debugging out of it (route those to `model: fable`). The task prompt must state the acceptance test, files in scope, and whether to push — a prompt checklist overrides inherited rules, so keep it complete or omit it.

## Commit Guidelines

Use semantic commits for all changes.
