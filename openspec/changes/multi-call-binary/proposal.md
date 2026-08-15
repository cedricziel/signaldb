## Why

Every release build links eight binaries (`signaldb`, `signaldb-acceptor`, `-router`, `-writer`, `-querier`, `-compactor`, `-mcp`, `-cli`), and with the release profile's thin LTO each link re-optimises the whole DataFusion/Arrow/Iceberg graph: measured 432 s per binary on an 8-core machine versus 32 s without LTO. On the 4-vCPU CI runner that is the 25-minute tail of the musl image job (38 min warm, 47 min cold) and therefore the main-to-image latency for every deploy. The monolithic `signaldb` binary already links all five services; the per-service binaries are the same code with a different `main`. Shipping one server binary that dispatches on how it was invoked removes six of the eight LTO links while keeping LTO — no runtime performance trade.

## What Changes

- The `signaldb` binary becomes a multi-call binary: it runs the monolith when invoked as `signaldb`, and runs a single service when invoked as `signaldb <service>` (`acceptor`, `router`, `writer`, `querier`, `compactor`, `mcp`) or through a hard/sym-link named `signaldb-<service>` (argv[0] dispatch, busybox-style). Each service keeps its own clap surface (`signaldb acceptor --help` shows the acceptor's flags and `--version`).
- **BREAKING** The per-crate `[[bin]]` targets `signaldb-acceptor`, `signaldb-router`, `signaldb-writer`, `signaldb-querier`, `signaldb-compactor` and `signaldb-mcp` are removed. Their `main.rs` logic moves into each crate as a public `run(args)` entry point that the multi-call binary calls. `cargo run --bin signaldb-acceptor` becomes `cargo run --bin signaldb -- acceptor`.
- `signaldb-cli` stays a separate binary: it is an operator tool distributed on its own (laptops, Windows), links the SDK rather than the server stack, and would otherwise turn every CLI download into a 100+ MB server binary.
- Container images: every per-service image ships the same `signaldb` binary plus a `/usr/local/bin/signaldb-<service>` symlink as its entrypoint, so existing `docker run … signaldb-router` invocations and `ENTRYPOINT`s keep working. Image content is otherwise unchanged; per-service images grow to the size of the monolithic one.
- Release artifacts: the microservices tarballs contain `signaldb` plus `signaldb-<service>` symlinks (tar preserves them); the Windows zip contains `signaldb.exe` only — Windows users invoke `signaldb.exe <service>`.
- CI and release workflows build `--bin signaldb --bin signaldb-cli` only.
- `scripts/run-dev.sh` services mode, `compose.yml`, `Dockerfile*`, `deploy/kubernetes/*`, docs and skills are updated to the new invocation.

Not changing: OTLP ingest, Tempo/LogQL/PromQL surfaces, Flight wire schemas, on-disk layout, configuration, ports, the monolithic `signaldb` behaviour, or the CLI.

## Capabilities

### New Capabilities

- `service-binary-dispatch`: how the `signaldb` binary selects monolith vs single-service mode (subcommand and argv[0]), what each mode's CLI surface is, and what container images and release artifacts ship.

### Modified Capabilities

<!-- none: client-surface-parity and mcp-tool-surface mention `signaldb-mcp` only descriptively; the MCP server's behaviour and tool surface are unchanged. -->

## Impact

- Crates: `signaldb-bin` (dispatch), `acceptor`, `router`, `writer`, `querier`, `compactor`, `mcp-server` (each gains a public `run` entry point and loses its `[[bin]]`), `common` (shared dispatch helper if any). `signaldb-cli`, `signaldb-sdk`, `tests-integration` unaffected except where tests spawn service binaries.
- Build/CI: `.github/workflows/ci.yml` (musl/glibc jobs, deployment test), `.github/workflows/release-please.yml` (build + packaging), `Dockerfile`, `Dockerfile.test`, `docker-compose.test.yml`, `compose.yml`, `scripts/run-dev.sh`, `scripts/test-deployment.sh`, `deploy/kubernetes/*.yaml`.
- Docs/skills: `docs/operations/binaries.md`, `docs/operations/*` deployment pages, `docs/users/*` quick-starts, `README.md`, `CLAUDE.md`, `.claude/skills/dev-workflow`, `crate-map`, `architecture`.
- Expected effect: musl image jobs 8 → 2 LTO links (~38 → ~12 min warm on the 4-vCPU runner; arm64 similar); release-please matrix builds proportionally faster; images unchanged in behaviour.
