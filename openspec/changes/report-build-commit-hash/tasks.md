## 1. Shared build-info crate

- [ ] 1.1 Write a failing unit test for the git-capture logic (short SHA + dirty-flag formatting + `unknown` fallback when `git` output is unavailable).
- [ ] 1.2 Create `src/build-info` (build-dependency-only workspace member, `publish = false`): implement `git rev-parse --short HEAD` + `git status --porcelain` capture, dirty-suffix formatting, and the `unknown` fallback, making the test in 1.1 pass.
- [ ] 1.3 Register `src/build-info` in the root `Cargo.toml` workspace `members` (not `default-members`).
- [ ] 1.4 Exclude `src/build-info` from the `align-core-component-versions` linked-versions group (tooling crate, not a shipped runtime component).

## 2. Fix `common::cli` name/version attribution and add commit hash

- [ ] 2.1 Write a failing test asserting `handle_common_command`'s `Version` output reflects an explicitly-passed `BuildInfo` (name/version/rust_version/commit), not `common`'s own crate metadata.
- [ ] 2.2 Add a `BuildInfo { name, version, rust_version, commit }` struct to `src/common/src/cli.rs`; change `version_info()` and `handle_common_command` to accept it as a parameter instead of resolving `env!("CARGO_PKG_NAME")`/`env!("CARGO_PKG_VERSION")` internally, making 2.1 pass.
- [ ] 2.3 Add `build.rs` (using `build-info`) to `signaldb-bin`, `acceptor`, `router`, `writer`, `querier`, `compactor`, each emitting `cargo:rustc-env=SIGNALDB_BUILD_COMMIT=...` and `cargo:rerun-if-changed=` hints against `.git/HEAD`/the resolved ref.
- [ ] 2.4 Update the `main.rs` of each of those 6 binaries to construct its own `BuildInfo` via its own `env!(...)` calls and pass it to `handle_common_command`.
- [ ] 2.5 `cargo build --workspace` and manually run `<binary> version` for each of the 6 to confirm correct self-identification (name/version no longer read `common`) and a populated commit hash.

## 3. signaldb-cli

- [ ] 3.1 Write a failing test (or snapshot) asserting `signaldb-cli --version` includes a commit hash.
- [ ] 3.2 Add `build.rs` (using `build-info`) to `signaldb-cli`; wire the commit hash into its existing clap-derived version output, making 3.1 pass.

## 4. MCP server

- [ ] 4.1 Write a failing test asserting `server_info`'s response includes a `commit` field alongside `version`.
- [ ] 4.2 Add `build.rs` (using `build-info`) to `mcp-server`; update `server_info` in `src/mcp-server/src/server.rs` to include the commit hash, making 4.1 pass.

## 5. Verify non-git and dirty-build fallbacks

- [ ] 5.1 Test (or manually verify) that building from a tree without `.git` produces `unknown` rather than a build failure.
- [ ] 5.2 Test (or manually verify) that building with uncommitted local changes produces a dirty-marked commit hash.

## 6. Release environment check

- [ ] 6.1 Confirm `.github/workflows/release-please.yml`'s `build-release`/`build-musl-*` jobs and any Dockerfile build context retain `.git` (they already run `git submodule update --init --recursive`) so release builds don't silently fall back to `unknown`.

## 7. Docs

- [ ] 7.1 Check via the docs skill whether an operations/troubleshooting doc should mention using `<binary> version` / MCP `server_info` to identify the exact build running (useful for support/debugging, similar in spirit to the existing compactor troubleshooting docs); add a short note if warranted.
