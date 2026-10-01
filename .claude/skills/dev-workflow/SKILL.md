---
name: dev-workflow
description: SignalDB development workflow - build, test, lint, format, run services, Docker, Grafana plugin, Explore UI, health checks, and semantic commits. Use when building, testing, running, or deploying SignalDB.
sources:
  - CLAUDE.md
  - scripts/run-dev.sh
  - compose.yml
---

# SignalDB Development Workflow

Build and test are plain `cargo build` / `cargo test [-p <crate>]`; the scoped
checks to run before committing are in CLAUDE.md, "Verifying a change". Local
runs go through `scripts/run-dev.sh` (read its header for flags), Docker via
`compose.yml`, JS via the `package.json` scripts, config via
`signaldb.dist.toml`. Use `commit-discipline` for semantic-commit format, and
the `verify` skill to observe a change running (monolith on an isolated data
dir, driven through Query IR, MCP, or the Explore UI).

Read `docs/contributing/benchmarking.md` for the Criterion micro-benchmark
suite: `scripts/run-benches.sh` (all targets, `-p <crate>`, or `-- --baseline
main` to compare against a saved baseline), what each bench target measures,
and the nightly trend/regression workflow.

Explore UI (`src/ui`, see `src/ui/README.md`): `pnpm ui:dev`, `pnpm ui:test`,
`pnpm --filter signaldb-ui lint`, `pnpm --filter signaldb-ui build-storybook`.
Every new routed page ships a `Pages/<Name>` Storybook story and a
design-sync entry — `docs/contributing/ui-page-stories.md` and
`.design-sync/NOTES.md` have the rules.

## Gotchas not in CLAUDE.md

- **Pre-commit hook is staged-file gated, not "run manually."**
  `.cargo-husky/hooks/pre-commit` runs `cargo fmt --check`, `clippy -D
warnings`, `cargo machete`, and `cargo deny check` automatically (all
  fatal) whenever staged files match `*.rs`, `Cargo.{toml,lock}`,
  `deny.toml`, `rustfmt.toml`, or `.cargo-husky/`; it runs `pnpm --filter
signaldb-ui typecheck` + `lint` automatically when `src/ui/` files are
  staged. A commit touching neither skips both.
- **JS/TS tooling is pnpm, not npm.** Workspace = `src/grafana-plugin` +
  `src/ui` (`pnpm-workspace.yaml`, `packageManager` in `package.json`);
  scripts are in `package.json` (`grafana:dev`/`grafana:build`/
  `grafana:test`, `ui:dev`/`ui:build`/`ui:test`). Install with `pnpm install
--frozen-lockfile`; `npm install` desyncs `pnpm-lock.yaml` and breaks CI's
  frozen-lockfile install.
- **Grafana plugin backend is a standalone cargo workspace**
  (`src/grafana-plugin/backend`), excluded from the root workspace so its
  pinned Arrow version doesn't collide with SignalDB's. Build it with `pnpm
-C src/grafana-plugin run build:backend`, not `cargo build --workspace`.
- **`scripts/run-dev.sh` never touches `signaldb.toml`** — it generates
  `signaldb.dev.toml` with a fixed dev tenant (`dev`, key `dev-key-123`) and
  a `_system` self-monitoring tenant, so it's safe to run alongside a real
  local config. Both modes also start `signal-producer --estate all`, which
  sends sample OTLP traffic to `:4317` every 10s.
