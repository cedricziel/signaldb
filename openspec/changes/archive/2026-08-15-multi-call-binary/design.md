## Context

See proposal.md — Why. Today each of `acceptor`, `router`, `writer`, `querier`, `compactor` and `mcp-server` has a `src/main.rs` (150–280 lines: a clap `Cli` with `CommonArgs`/`CommonCommands` flattened in, config loading, telemetry init, then the service's serve loop) plus a `[[bin]]`; `signaldb-bin` re-implements the monolith over the same library entry points (`acceptor::init_acceptor_resources`, `router::create_router`, …). Every `main.rs` also declares the jemalloc `#[global_allocator]` behind its crate's `jemalloc` feature. The release profile is `lto = "thin"`, `codegen-units = 8`, and cargo runs the LTO step once per binary target; dependency crates are compiled to bitcode only, so the cost sits almost entirely in those per-binary links (measured 432 s each on 8 cores).

Constraints: no behavioural change to any service, port, config key or environment variable; the monolithic `signaldb` CLI must stay byte-for-byte compatible for the hive deployment; the FDAP rule (Arrow/Parquet types via DataFusion re-exports) is unaffected — this change touches no data path, no Flight schema, no WAL/Iceberg layout, so there is no storage migration or rollback of data to plan.

## Goals / Non-Goals

**Goals:**

- Two release links (`signaldb`, `signaldb-cli`) instead of eight, keeping thin LTO.
- Zero-touch upgrade for manifests and compose files that rely on the images' default entrypoints; a one-line change (`signaldb-<svc>` → `signaldb <svc>`) everywhere else.
- Each service's argument surface, help and version output unchanged.
- One clap parser owns the whole command tree; service crates own only their `Args` and `run`.

**Non-Goals:**

- Merging `signaldb-cli` (separate audience, separate dependency set) or `signal-producer` (test tool). Folding the CLI in would save one more LTO link (~7 min) but make every CLI download the 100+ MB server binary and force either `signaldb cli <cmd>` or a rename of the server surface; the ecosystem convention is a separate client (`promtool`, `logcli`, `tempo-cli`). If that link ever matters, link the CLI without LTO via a `release-cli` profile instead.
- Changing which services the monolith runs, or adding `mcp` to the monolith's default set.
- Reworking the shared `CommonArgs`/`CommonCommands` surface.
- Trimming binary size (the per-service images will grow to the monolith's size; accepted).

## Decisions

**D1. Services are clap subcommands of the monolith `Cli`.**
The monolith's `Cli` keeps `CommonArgs` (now marked `global = true` so `--config`/`-v`/`-q` work before or after a service name) and its `Option<Commands>`; `Commands` gains one variant per service — `Acceptor(acceptor::cli::Args)`, `Router(router::cli::Args)`, … — flattened next to `CommonCommands` (`start`, `config`, `validate`, `version`), whose names do not collide with the service names. Each service's `Args` is a `clap::Args` struct holding only that service's flags plus its own `Option<CommonCommands>` subcommand, so `signaldb acceptor validate` and `signaldb acceptor --grpc-port 4319` parse as before; `#[command(version)]` on the variants keeps `signaldb querier --version` working. One parser therefore owns help, errors and (later) shell completions for the whole tree, and the executable's file name is never consulted.
_Alternatives rejected:_ (a) argv[0]/symlink dispatch — implicit, platform-dependent behaviour that gives a mis-named copy a different personality; (b) a raw arg-vector hand-off to each service's former top-level parser — keeps help byte-identical but leaves two parsers, no unified `--help`, and a hand-rolled selector to test.

**D2. Each service crate exposes `pub struct Args` (clap) and `pub async fn run(common: &CommonArgs, args: Args) -> anyhow::Result<()>`.**
The body of today's `main.rs` moves verbatim into the library (`src/cli.rs`): its former `Cli` becomes `Args` minus the flattened `CommonArgs` (now supplied by the monolith parser), and `run` receives the parsed values instead of calling `Cli::parse()`. Telemetry service names stay literals per service. `#[tokio::main]` and the allocator declaration are removed from the service crates: `signaldb-bin`'s single `#[tokio::main]` matches on `Commands` and awaits the selected `run`. All eight mains use the default multi-thread runtime today, so no per-service runtime tuning is lost. Anything the mains kept private (helper fns, `impl Default for XCommands`) moves along.
_Alternative rejected:_ keeping thin `main.rs` wrappers per crate calling `run`. A thin bin is still a full LTO link, and `cargo build --release` in release-please builds every bin unless filtered — the targets must go, not just shrink.

**D3. Allocator and feature graph.**
Only `signaldb-bin` declares `#[global_allocator]` (already the case for the monolith). The service crates' `jemalloc` / `jemalloc-profiling` features are removed together with their mains, except where a feature gates library code (`router`/`common` `jemalloc-profiling` for the heap-profile endpoints) — those stay and `signaldb-bin/jemalloc-profiling` continues to imply them. The `CARGO_FEATURES` list in `Dockerfile` and the `--features` list in `ci.yml` collapse to `signaldb-bin/jemalloc` (+ `signaldb-bin/jemalloc-profiling` for the glibc image). `cargo machete` and `cargo deny` runs confirm nothing dangling.

**D4. Images: same binary, per-service entrypoint.**
Each service stage in `Dockerfile` copies `signaldb` and sets `ENTRYPOINT ["/usr/local/bin/signaldb", "<svc>"]`; container arguments append to the service's arguments as before. The `builder-source` and `builder-prebuilt` stages build/copy only `signaldb` (+ `signaldb-cli` for the monolithic image). No symlinks anywhere. The `mcp` image gets the same treatment (it grows from a small binary to the full one — accepted, see Non-Goals).

**D5. Release archives.**
`release-please.yml` builds `--bin signaldb --bin signaldb-cli` per target. The microservices archive contains `signaldb` (`signaldb.exe`) only, on every platform; the docs state `signaldb <service>`. The monolithic archive is unchanged. The `signaldb-mcp-*` archive is dropped in favour of the microservices archive.

**D6. Local development.**
`scripts/run-dev.sh services` and `Dockerfile.test`/`docker-compose.test.yml` switch to `cargo run --bin signaldb -- <svc>` / `signaldb <svc>`. Side benefit: services mode compiles one binary instead of five.

**D7. Verification.**

- Unit tests in `signaldb-bin` with `Cli::try_parse_from` covering every spec scenario: each service subcommand parses to its variant, shared options before and after the service name, `validate`/`config` under a service, unknown subcommand is a clap error, no subcommand → monolith.
- An integration test in `signaldb-bin` runs `env!("CARGO_BIN_EXE_signaldb")` with `<svc> --version` and `<svc> --help` for every service and asserts the service name and the shared version appear (this replaces the deleted per-binary `--help`/`--version` smoke checks in CI's deployment test).
- The CI `deployment-test` job runs `signaldb` and `signaldb acceptor` / `signaldb router` for a few seconds each from the musl artifact.
- Image smoke: `docker run <router image> --version` (entrypoint appends → router's version) in the docker job.

## Risks / Trade-offs

- [Per-service images grow to the monolith's size (~2×; the mcp image ~5×)] → accepted; homelab positioning values one image family over minimal images, and layer sharing means one pull of the binary layer per host if the same digest is reused across service images (verify by building all service stages from one `COPY` of the identical file so the layer digest matches).
- [A hidden per-`main` difference (runtime flavour, allocator, telemetry service name) changes behaviour when moved into `run`] → move bodies verbatim, diff `--help` output before/after for every service, keep telemetry service names as literals in each `run`.
- [Anything that execs `signaldb-<svc>` by name — hand-written systemd units, `docker exec`, custom entrypoint overrides — breaks] → the change is BREAKING and the release note gives the one-line migration; our own manifests and compose files rely on the image entrypoint and need no change.
- [`kubectl exec … signaldb-router --version` habits] → `signaldb router --version`; documented in binaries.md.

## Migration Plan

1. Land the crate refactor (D2/D3) and dispatcher (D1) with tests, keeping the old `[[bin]]` targets for one commit so `cargo run --bin signaldb-acceptor` still works while the rest of the stack is switched. (Note: the CI `images` gate does not fire for `src/**` changes; the PR should carry the `build-images` label so the image chain is exercised.)
2. Switch `Dockerfile*`, `compose.yml`, `deploy/kubernetes`, `scripts/*`, `ci.yml`, `release-please.yml`, docs and skills; then remove the `[[bin]]` targets and the per-crate `jemalloc` allocator declarations. Same PR or a stacked one.
3. Deploy: images keep their default behaviour (the entrypoint runs the same service), so hive, compose and the k8s manifests need no change. Rollback is a plain revert — no data, config or wire format is involved.

## Open Questions

- Whether to keep publishing a separate `signaldb-mcp` container image (it is the same binary; the image only differs by entrypoint). Not spec-relevant — either answer keeps `ghcr.io/…/mcp` working — so it can be settled during implementation based on how the hive MCP sidecar is deployed.
