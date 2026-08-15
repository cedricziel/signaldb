## Why

Every release build links eight binaries (`signaldb`, `signaldb-acceptor`, `-router`, `-writer`, `-querier`, `-compactor`, `-mcp`, `-cli`), and with the release profile's thin LTO each link re-optimises the whole DataFusion/Arrow/Iceberg graph: measured 432 s per binary on an 8-core machine versus 32 s without LTO. On the 4-vCPU CI runner that is the 25-minute tail of the musl image job (38 min warm, 47 min cold) and therefore the main-to-image latency for every deploy. The monolithic `signaldb` binary already links all five services; the per-service binaries are the same code with a different `main`. Shipping one server binary with the services as subcommands removes six of the eight LTO links while keeping LTO — no runtime performance trade.

## What Changes

- The `signaldb` binary gains one clap subcommand per service — `signaldb acceptor|router|writer|querier|compactor|mcp [flags] [start|config|validate|version]` — next to the existing monolith commands (`start|config|validate|version`); no subcommand still means "run the monolith". The shared options (`--config`, `-v`, `-q`) become global, accepted before or after the service name. Each service's own flags live under its subcommand, so `signaldb acceptor --help` shows the acceptor's flags. The executable's file name is never consulted (no argv[0]/symlink dispatch).
- **BREAKING** The per-crate `[[bin]]` targets `signaldb-acceptor`, `signaldb-router`, `signaldb-writer`, `signaldb-querier`, `signaldb-compactor` and `signaldb-mcp` are removed. Their `main.rs` logic moves into each crate as a public `run(args)` entry point that the multi-call binary calls. `cargo run --bin signaldb-acceptor` becomes `cargo run --bin signaldb -- acceptor`.
- `signaldb-cli` stays a separate binary: it is an operator tool distributed on its own (laptops, Windows), links the SDK rather than the server stack, and would otherwise turn every CLI download into a 100+ MB server binary.
- **BREAKING** Container images: every per-service image ships the same `signaldb` binary with `ENTRYPOINT ["/usr/local/bin/signaldb", "<service>"]`. Manifests and compose files that rely on the image's default entrypoint (ours do) keep working unchanged; anything that overrides the entrypoint or execs `signaldb-<service>` inside a container must switch to `signaldb <service>`. Per-service images grow to the size of the monolithic one.
- **BREAKING** Release artifacts: the microservices archives contain `signaldb` (`signaldb.exe`) as the only server executable; single-service mode is `signaldb <service>` on every platform. The separate `signaldb-mcp-*` archive is folded in.
- CI and release workflows build `--bin signaldb --bin signaldb-cli` only.
- `scripts/run-dev.sh` services mode, `compose.yml`, `Dockerfile*`, `deploy/kubernetes/*`, docs and skills are updated to the new invocation.

Not changing: OTLP ingest, Tempo/LogQL/PromQL surfaces, Flight wire schemas, on-disk layout, configuration, ports, the monolithic `signaldb` behaviour, or the CLI.

## Capabilities

### New Capabilities

- `service-binary-dispatch`: how the `signaldb` binary selects monolith vs single-service mode (service subcommands), what each mode's CLI surface is, and what container images and release artifacts ship.

### Modified Capabilities

<!-- none: client-surface-parity and mcp-tool-surface mention `signaldb-mcp` only descriptively; the MCP server's behaviour and tool surface are unchanged. -->

## Impact

- Crates: `signaldb-bin` (dispatch), `acceptor`, `router`, `writer`, `querier`, `compactor`, `mcp-server` (each gains a public `run` entry point and loses its `[[bin]]`), `common` (shared dispatch helper if any). `signaldb-cli`, `signaldb-sdk`, `tests-integration` unaffected except where tests spawn service binaries.
- Build/CI: `.github/workflows/ci.yml` (musl/glibc jobs, deployment test), `.github/workflows/release-please.yml` (build + packaging), `Dockerfile`, `Dockerfile.test`, `docker-compose.test.yml`, `compose.yml`, `scripts/run-dev.sh`, `scripts/test-deployment.sh`, `deploy/kubernetes/*.yaml`.
- Docs/skills: `docs/operations/binaries.md`, `docs/operations/*` deployment pages, `docs/users/*` quick-starts, `README.md`, `CLAUDE.md`, `.claude/skills/dev-workflow`, `crate-map`, `architecture`.
- Expected effect: musl image jobs 8 → 2 LTO links (~38 → ~12 min warm on the 4-vCPU runner; arm64 similar); release-please matrix builds proportionally faster; images unchanged in behaviour.
