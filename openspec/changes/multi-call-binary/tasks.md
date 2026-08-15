## 1. Dispatcher in signaldb-bin

- [ ] 1.1 Add failing unit tests for `select(argv0, args) -> Target` in `src/signaldb-bin`: argv[0] `signaldb-writer`, `.exe` suffix, subcommand `signaldb router …`, both selectors (argv[0] wins), unknown first arg falls through to Monolith, bare `signaldb` → Monolith
- [ ] 1.2 Implement `select` and `Target` (`Monolith(args)` / `Service(name, args)`) as a pure function; wire `main` to call it before the monolith's clap parse
- [ ] 1.3 Add the service names to the monolith `Cli`'s `after_help` so `signaldb frobnicate` prints a usage error that lists them (spec: Unknown selector); assert this in a test using `Cli::try_parse_from`
- [ ] 1.4 Add a failing integration test in `src/signaldb-bin/tests/` that runs `env!("CARGO_BIN_EXE_signaldb")` with `<svc> --version` and `<svc> --help` for every service and checks the service name and the shared version appear (this depends on 2.x, mark it `#[ignore]` until then if needed)

## 2. Service crates expose `run(args)`

- [ ] 2.1 acceptor: move `main.rs` body into `pub async fn run(args: Vec<OsString>) -> anyhow::Result<()>` (`Cli::parse_from(args)`), keep the display name and telemetry service name; snapshot `--help` output before/after and diff in a test (`cargo test -p acceptor`)
- [ ] 2.2 router: same as 2.1 (`cargo test -p router`)
- [ ] 2.3 writer: same as 2.1 (`cargo test -p writer`)
- [ ] 2.4 querier: same as 2.1 (`cargo test -p querier`)
- [ ] 2.5 compactor: same as 2.1 (`cargo test -p compactor`)
- [ ] 2.6 mcp-server: same as 2.1, preserving stdio/HTTP transport selection (`cargo test -p mcp-server`)
- [ ] 2.7 signaldb-bin: dispatch `Target::Service` to each crate's `run`; un-ignore and pass 1.4
- [ ] 2.8 Remove the six `[[bin]]` targets and their `main.rs`; remove the per-crate `#[global_allocator]` declarations and the `jemalloc` features that only existed for them (keep `jemalloc-profiling` where it gates library code); `cargo machete --with-metadata`, `cargo deny check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## 3. Build, images, release

- [ ] 3.1 `Dockerfile`: builder stages build/copy only `signaldb` (+ `signaldb-cli`); each service stage `COPY`s the same file and adds `RUN ln -s signaldb /usr/local/bin/signaldb-<svc>`; `CARGO_FEATURES` reduced to `signaldb-bin/jemalloc`; `ENTRYPOINT`s unchanged; verify layer digest of the binary layer is shared across service images
- [ ] 3.2 `.github/workflows/ci.yml`: musl amd64/arm64 and glibc-profiling jobs build `--bin signaldb --bin signaldb-cli` with the reduced feature list; stage only those into `dist/`; `deployment-test` runs `signaldb`, `signaldb acceptor`, and a `signaldb-router` symlink for a few seconds each; docker job adds `docker run <router image> --version` and `--entrypoint signaldb … router --version` smoke checks
- [ ] 3.3 `.github/workflows/release-please.yml`: build `--bin signaldb --bin signaldb-cli`; microservices archive = `signaldb` + `signaldb-<svc>` symlinks on Linux/macOS, `signaldb.exe` only on Windows; fold the `signaldb-mcp-*` archive into the microservices archive
- [ ] 3.4 `Dockerfile.test`, `docker-compose.test.yml`, `compose.yml`, `deploy/kubernetes/*.yaml`, `scripts/run-dev.sh` (services mode → `cargo run --bin signaldb -- <svc>`), `scripts/test-deployment.sh`: switch to the new invocation and verify `./scripts/run-dev.sh services` boots all services
- [ ] 3.5 Open the PR with the `build-images` label so the image chain runs; confirm the musl amd64 job's link tail drops from ~25 min to the two-link cost and record the before/after job durations in the PR

## 4. Docs and skills

- [ ] 4.1 Route via the docs skill: update `docs/operations/binaries.md` (dispatch, symlink layout, Windows form, image contents), the deployment/quick-start pages under `docs/operations/` and `docs/users/` that invoke `signaldb-<svc>`, `README.md`, and `CLAUDE.md` "Running Services"; run `scripts/check-doc-freshness.sh` after committing
- [ ] 4.2 Update `.claude/skills/dev-workflow`, `crate-map` and `architecture` where they list the per-service binaries or `cargo run --bin signaldb-<svc>`
- [ ] 4.3 Release notes entry (BREAKING: per-service executables replaced by `signaldb <svc>` / `signaldb-<svc>` links; migration one-liner)
