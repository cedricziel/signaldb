## Context

See proposal.md — Why. Today each of `acceptor`, `router`, `writer`, `querier`, `compactor` and `mcp-server` has a `src/main.rs` (150–280 lines: a clap `Cli` with `CommonArgs`/`CommonCommands` flattened in, config loading, telemetry init, then the service's serve loop) plus a `[[bin]]`; `signaldb-bin` re-implements the monolith over the same library entry points (`acceptor::init_acceptor_resources`, `router::create_router`, …). Every `main.rs` also declares the jemalloc `#[global_allocator]` behind its crate's `jemalloc` feature. The release profile is `lto = "thin"`, `codegen-units = 8`, and cargo runs the LTO step once per binary target; dependency crates are compiled to bitcode only, so the cost sits almost entirely in those per-binary links (measured 432 s each on 8 cores).

Constraints: no behavioural change to any service, port, config key or environment variable; the monolithic `signaldb` CLI must stay byte-for-byte compatible for the hive deployment; the FDAP rule (Arrow/Parquet types via DataFusion re-exports) is unaffected — this change touches no data path, no Flight schema, no WAL/Iceberg layout, so there is no storage migration or rollback of data to plan.

## Goals / Non-Goals

**Goals:**

- Two release links (`signaldb`, `signaldb-cli`) instead of eight, keeping thin LTO.
- Zero-touch upgrade for images, manifests and scripts that invoke `signaldb-<service>`.
- Each service's argument surface, help and version output unchanged.
- One place (`signaldb-bin`) owns dispatch; service crates own their CLI definitions.

**Non-Goals:**

- Merging `signaldb-cli` (separate audience, separate dependency set) or `signal-producer` (test tool).
- Changing which services the monolith runs, or adding `mcp` to the monolith's default set.
- Reworking the shared `CommonArgs`/`CommonCommands` surface.
- Trimming binary size (the per-service images will grow to the monolith's size; accepted).

## Decisions

**D1. Dispatch lives in `signaldb-bin`, selection is `argv[0]` first, then first argument.**
`main` computes `select(argv0_basename, args) -> Target` as a pure function (unit-testable): strip a trailing `.exe`, if the basename is `signaldb-<svc>` for a known service → `Target::Service(svc, args[1..])`; else if `args[1]` is a known service name → `Target::Service(svc, args[2..])`; else `Target::Monolith(args)`. argv[0] wins over the argument so a mis-named link can never run a different service than its name says (spec scenario "Both selectors present"). Unknown first arguments simply fall through to the monolith's clap parser, which reports the usage error and, per the spec, the error text lists the service names — implemented by giving the monolith `Cli` the service names as documented `after_help` text rather than as real clap subcommands, so they cannot collide with `CommonCommands` (`start`, `config …`) and the monolith's own parse tree stays untouched.
_Alternative rejected:_ modelling services as clap subcommands of the monolith `Cli`. That would nest each service's `Cli` under a shared parser and change `--help`/`--version` output and error formatting for every service; a plain arg-vector hand-off keeps them identical.

**D2. Each service crate exposes `pub async fn run(args: Vec<OsString>) -> anyhow::Result<()>`.**
The body of today's `main.rs` moves verbatim into the library (`src/cli.rs` or the crate root), with `Cli::parse()` replaced by `Cli::parse_from(args)` where `args[0]` is the display name the dispatcher passes (`signaldb-<svc>` for argv[0] dispatch, `signaldb <svc>` for subcommand dispatch, so usage lines read naturally). `#[tokio::main]` and the allocator declaration are removed from the service crates: `signaldb-bin`'s single `#[tokio::main]` awaits whichever `run` was selected. All eight mains use the default multi-thread runtime today, so no per-service runtime tuning is lost. Anything the mains kept private (helper fns, `impl Default for XCommands`) moves along.
_Alternative rejected:_ keeping thin `main.rs` wrappers per crate calling `run`. A thin bin is still a full LTO link, and `cargo build --release` in release-please builds every bin unless filtered — the targets must go, not just shrink.

**D3. Allocator and feature graph.**
Only `signaldb-bin` declares `#[global_allocator]` (already the case for the monolith). The service crates' `jemalloc` / `jemalloc-profiling` features are removed together with their mains, except where a feature gates library code (`router`/`common` `jemalloc-profiling` for the heap-profile endpoints) — those stay and `signaldb-bin/jemalloc-profiling` continues to imply them. The `CARGO_FEATURES` list in `Dockerfile` and the `--features` list in `ci.yml` collapse to `signaldb-bin/jemalloc` (+ `signaldb-bin/jemalloc-profiling` for the glibc image). `cargo machete` and `cargo deny` runs confirm nothing dangling.

**D4. Images: same binary, per-service symlink entrypoint.**
Each service stage in `Dockerfile` copies `signaldb` and does `RUN ln -s signaldb /usr/local/bin/signaldb-<svc>`; `ENTRYPOINT` lines are unchanged. The `builder-source` and `builder-prebuilt` stages build/copy only `signaldb` (+ `signaldb-cli` for the monolithic image). Symlinks are created inside the image, not in the CI `dist/` artifact: `actions/upload-artifact` dereferences symlinks and would upload six copies of a ~90 MB binary. The `mcp` image gets the same treatment (it grows from a small binary to the full one — accepted, see Non-Goals).

**D5. Release archives.**
`release-please.yml` builds `--bin signaldb --bin signaldb-cli` per target. The microservices archive for Linux/macOS is `signaldb` plus `ln -s signaldb signaldb-<svc>` links (tar keeps them); Windows ships `signaldb.exe` only and the docs state `signaldb.exe <service>`. The monolithic archive is unchanged. The `signaldb-mcp-*` archive becomes a link inside the microservices archive rather than its own artifact.

**D6. Local development.**
`scripts/run-dev.sh services` and `Dockerfile.test`/`docker-compose.test.yml` switch to `cargo run --bin signaldb -- <svc>` / `signaldb <svc>`. Side benefit: services mode compiles one binary instead of five.

**D7. Verification.**

- Unit tests for `select(...)` covering every spec scenario (argv[0], subcommand, both, `.exe`, unknown).
- An integration test in `signaldb-bin` runs `env!("CARGO_BIN_EXE_signaldb")` with `<svc> --version` and `<svc> --help` for every service and asserts the service name and the shared version appear (this replaces the deleted per-binary `--help`/`--version` smoke checks in CI's deployment test).
- The CI `deployment-test` job runs `signaldb`, `signaldb acceptor`, and a `signaldb-router` symlink for a few seconds each from the musl artifact.
- Image smoke: `docker run <router image> --version` and `docker run --entrypoint signaldb <router image> router --version` in the docker job.

## Risks / Trade-offs

- [Per-service images grow to the monolith's size (~2×; the mcp image ~5×)] → accepted; homelab positioning values one image family over minimal images, and layer sharing means one pull of the binary layer per host if the same digest is reused across service images (verify by building all service stages from one `COPY` of the identical file so the layer digest matches).
- [A hidden per-`main` difference (runtime flavour, allocator, telemetry service name) changes behaviour when moved into `run`] → move bodies verbatim, diff `--help` output before/after for every service, keep telemetry service names as literals in each `run`.
- [Operators with `cargo install`-style or hand-copied `signaldb-acceptor` files] → not a supported distribution channel; release notes call out the link layout.
- [Windows loses `signaldb-<svc>.exe`] → the subcommand form is documented; Windows service binaries have never been part of any image or deployment guide.
- [`actions/upload-artifact` and symlinks] → symlinks are created in Dockerfile stages and packaging steps only, never in uploaded artifacts.
- [Two dispatch mechanisms invite drift] → one pure `select` function with tests, and both paths call the same `run`.

## Migration Plan

1. Land the crate refactor (D2/D3) and dispatcher (D1) with tests, keeping the old `[[bin]]` targets for one commit so `cargo run --bin signaldb-acceptor` still works while the rest of the stack is switched. (Note: the CI `images` gate does not fire for `src/**` changes; the PR should carry the `build-images` label so the image chain is exercised.)
2. Switch `Dockerfile*`, `compose.yml`, `deploy/kubernetes`, `scripts/*`, `ci.yml`, `release-please.yml`, docs and skills; then remove the `[[bin]]` targets and the per-crate `jemalloc` allocator declarations. Same PR or a stacked one.
3. Deploy: images keep their entrypoint names, so hive and compose users need no change. Rollback is a plain revert — no data, config or wire format is involved.

## Open Questions

- Whether to keep publishing a separate `signaldb-mcp` container image (it is the same binary; the image only differs by entrypoint). Not spec-relevant — either answer keeps `ghcr.io/…/mcp` working — so it can be settled during implementation based on how the hive MCP sidecar is deployed.
