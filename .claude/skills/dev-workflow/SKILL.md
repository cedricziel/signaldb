---
name: dev-workflow
description: SignalDB development workflow - build, test, lint, format, run services, Docker, Grafana plugin, health checks, and semantic commits. Use when building, testing, running, or deploying SignalDB.
---

# SignalDB Development Workflow

Read `CLAUDE.md` for build/test/run commands, storage locations, Docker
compose, and configuration — all standard `cargo`/`pnpm` invocations. Use
`commit-discipline` for semantic-commit format.

Read `docs/contributing/benchmarking.md` for the Criterion micro-benchmark
suite: `scripts/run-benches.sh` (all targets, `-p <crate>`, or `-- --baseline
main` to compare against a saved baseline), what each bench target measures,
and the nightly trend/regression workflow.

## Gotchas not in CLAUDE.md

- **Pre-commit hook is staged-file gated, not "run manually."**
  `.cargo-husky/hooks/pre-commit` runs `cargo fmt --check`, `clippy -D
warnings`, `cargo machete`, and `cargo deny check` automatically (all
  fatal) whenever staged files match `*.rs`, `Cargo.{toml,lock}`,
  `deny.toml`, `rustfmt.toml`, or `.cargo-husky/`; it runs `pnpm --filter
signaldb-ui typecheck` + `lint` automatically when `src/ui/` files are
  staged. A commit touching neither skips both — this differs from
  CLAUDE.md's "run manually before commit" note for `cargo machete`.
- **JS/TS tooling is pnpm, not npm.** Workspace = `src/grafana-plugin` +
  `src/ui` (`pnpm-workspace.yaml`); scripts are in `package.json`
  (`grafana:dev`/`grafana:build`/`grafana:test`, `ui:dev`/`ui:build`/
  `ui:test`). `npm install` desyncs `pnpm-lock.yaml`.
- **Grafana plugin backend is a standalone cargo workspace**
  (`src/grafana-plugin/backend`), excluded from the root workspace so its
  pinned Arrow version doesn't collide with SignalDB's. Build it with `pnpm
run build:backend`, not `cargo build --workspace`.
- **`scripts/run-dev.sh` never touches `signaldb.toml`** — it generates
  `signaldb.dev.toml` with a fixed dev tenant (`dev`, key `dev-key-123`) and
  a `_system` self-monitoring tenant, so it's safe to run alongside a real
  local config.
