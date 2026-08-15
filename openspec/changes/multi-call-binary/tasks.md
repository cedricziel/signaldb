## 1. Dispatcher in signaldb-bin

- [x] 1.1 Add failing `Cli::try_parse_from` unit tests in `src/signaldb-bin`: `signaldb router --config x` and `signaldb --config x -v router` both parse to `Commands::Router` with the shared options set, `signaldb acceptor validate` and `signaldb acceptor --grpc-port 4319` parse into the acceptor `Args`, `signaldb frobnicate` is a clap error, bare `signaldb` and `signaldb config` still parse as before
- [x] 1.2 Make `CommonArgs` fields `global = true`; add the six service variants (`Acceptor(acceptor::cli::Args)`, …) to the monolith `Commands` next to the flattened `CommonCommands`, with `#[command(version)]`; wire `main` to match on them (2.x supplies the `Args`/`run` targets — land 1.x with the crates in the same PR)
- [x] 1.3 Assert `signaldb --help` lists the six services and `signaldb acceptor --help` lists the acceptor's flags plus the shared options (spec: Service help / Unknown subcommand) via `Cli::command().render_help()` in a unit test
- [x] 1.4 Add a failing integration test in `src/signaldb-bin/tests/` that runs `env!("CARGO_BIN_EXE_signaldb")` with `<svc> --version` and `<svc> --help` for every service and checks the service name and the shared version appear (this depends on 2.x, mark it `#[ignore]` until then if needed)

## 2. Service crates expose `run(args)`

- [x] 2.1 acceptor: move `main.rs` body into `src/cli.rs` as `pub struct Args` (former `Cli` minus `CommonArgs`) and `pub async fn run(common: &CommonArgs, args: Args)`; keep the telemetry service name; snapshot the flag list before/after in a test (`cargo test -p acceptor`)
- [x] 2.2 router: same as 2.1 (`cargo test -p router`)
- [x] 2.3 writer: same as 2.1 (`cargo test -p writer`)
- [x] 2.4 querier: same as 2.1 (`cargo test -p querier`)
- [x] 2.5 compactor: same as 2.1 (`cargo test -p compactor`)
- [x] 2.6 mcp-server: same as 2.1, preserving stdio/HTTP transport selection (`cargo test -p mcp-server`)
- [x] 2.7 signaldb-bin: dispatch each `Commands::<Service>` variant to that crate's `run`; un-ignore and pass 1.4
- [x] 2.8 Remove the six `[[bin]]` targets and their `main.rs`; remove the per-crate `#[global_allocator]` declarations and the `jemalloc` features that only existed for them (keep `jemalloc-profiling` where it gates library code); `cargo machete --with-metadata`, `cargo deny check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## 3. Build, images, release

- [x] 3.1 `Dockerfile`: builder stages build/copy only `signaldb` (+ `signaldb-cli`); each service stage `COPY`s the same file and sets `ENTRYPOINT ["/usr/local/bin/signaldb", "<svc>"]`; `CARGO_FEATURES` reduced to `signaldb-bin/jemalloc`; verify the binary layer digest is shared across service images
- [x] 3.2 `.github/workflows/ci.yml`: musl amd64/arm64 and glibc-profiling jobs build `--bin signaldb --bin signaldb-cli` with the reduced feature list; stage only those into `dist/`; `deployment-test` runs `signaldb`, `signaldb acceptor` and `signaldb router` for a few seconds each; docker job adds a `docker run <router image> --version` smoke check
- [x] 3.3 `.github/workflows/release-please.yml`: build `--bin signaldb --bin signaldb-cli`; microservices archive = `signaldb` (`signaldb.exe`) only; drop the `signaldb-mcp-*` archive
- [x] 3.4 `Dockerfile.test`, `docker-compose.test.yml`, `compose.yml`, `deploy/kubernetes/*.yaml`, `scripts/run-dev.sh` (services mode → `cargo run --bin signaldb -- <svc>`), `scripts/test-deployment.sh`: switch to the new invocation and verify `./scripts/run-dev.sh services` boots all services
- [x] 3.5 Open the PR with the `build-images` label so the image chain runs; confirm the musl amd64 job's link tail drops from ~25 min to the two-link cost and record the before/after job durations in the PR

## 4. Docs and skills

- [x] 4.1 Route via the docs skill: update `docs/operations/binaries.md` (`signaldb <service>` form, image entrypoints, archive contents), the deployment/quick-start pages under `docs/operations/` and `docs/users/` that invoke `signaldb-<svc>`, `README.md`, and `CLAUDE.md` "Running Services"; run `scripts/check-doc-freshness.sh` after committing
- [x] 4.2 Update `.claude/skills/dev-workflow`, `crate-map` and `architecture` where they list the per-service binaries or `cargo run --bin signaldb-<svc>`
- [x] 4.3 Release notes entry (BREAKING: per-service executables and image entrypoint names replaced by `signaldb <svc>`; migration one-liner)
