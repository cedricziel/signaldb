## Context

See proposal.md - Why/What Changes for motivation. Relevant mechanics, verified directly against this codebase (not assumed):

- `env!("CARGO_PKG_NAME")`/`env!("CARGO_PKG_VERSION")` resolve at the compile time of the crate whose source file contains the macro call, using the `CARGO_PKG_*` environment variables Cargo sets for _that crate's own compilation_ — not whichever crate ends up linking it in. `common::cli::utils::version_info()`/`handle_common_command` live in `common`'s own source, so every binary built on that scaffolding reports `common`'s name/version. Confirmed: `cargo run -p signaldb-bin --bin signaldb -- version` prints `common 0.2.1`, not `signaldb-bin 0.1.3`. `signaldb-cli`'s own clap-derived `--version` is unaffected (macro expands in its own crate).
- A custom env var set via `println!("cargo:rustc-env=KEY=VALUE")` in a crate's `build.rs` is visible via `env!("KEY")` only within that same crate's own compilation — it does not propagate to dependent crates. So a git-commit value captured once in `common`'s build script cannot be read via `env!()` from `acceptor`'s or `signaldb-cli`'s source.
- The git commit hash itself is a property of the whole source tree at build time, not of any individual crate — every crate compiled in the same `cargo build` invocation sees the same commit. What varies is not the _value_, but _which crate's compilation the `env!()` call needs to resolve inside_, per binary's own `main.rs`.
- No `/version` HTTP endpoint exists today on router/acceptor/querier/writer (confirmed via `Bash` grep for the route). Nothing to touch there.

## Goals / Non-Goals

**Goals:**

- Every binary's version output correctly names itself (fixes the `common`-misattribution bug) and includes the git commit it was built from.
- One shared implementation of the git-capture logic (short SHA + dirty flag + non-git fallback), not copy-pasted ad hoc per crate.
- No new runtime dependency; git-capture is a build-time concern only.

**Non-Goals:**

- Not adding a new HTTP `/version` endpoint — none exists today; out of scope here (see proposal.md).
- Not adopting `vergen`/`shadow-rs` unless the plain `build.rs` + `git` shell-out proves insufficient (e.g. needing Windows compatibility without a `git` binary on PATH — assessed below and judged acceptable).
- Not restructuring `CommonCommands`/`handle_common_command`'s other subcommands (`Config`, `Validate`, `Start`) beyond the signature change needed to pass `BuildInfo` through.

## Decisions

**Fix the name/version misattribution as part of this change, not separately.** Correctly attributing a build hash _per binary_ requires the exact same fix (resolve identity at the calling binary's own compile time) as correctly attributing name/version. Doing the hash without the fix would just add a correct commit hash next to an incorrect binary name — not worth shipping half-fixed.

**Mechanism: `handle_common_command` takes a `BuildInfo` struct instead of resolving identity internally.** `BuildInfo { name: &'static str, version: &'static str, rust_version: &'static str, commit: &'static str }`, constructed by each binary's own `main.rs` via its own `env!(...)` calls (which now correctly resolve against that binary's Cargo metadata) plus its own `env!("SIGNALDB_BUILD_COMMIT")` (set by that binary's own `build.rs`). Alternative considered: convert `version_info()` into a `macro_rules!` macro so `env!` resolves at the call site automatically. Rejected — `handle_common_command` is async and branches over four subcommands; turning that into a declarative macro loses IDE support and readability for marginal benefit over an explicit struct parameter, and the struct approach generalizes cleanly to the MCP server (which doesn't use `CommonCommands` at all).

**One `build.rs` per binary crate, sharing logic via a new build-dependency-only crate (`src/build-info`).** Since the custom env var must be set inside each binary's own compilation (per Context above), each of `signaldb-bin`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `signaldb-cli`, `mcp-server` (8 crates) needs its own `build.rs`. Duplicating ~15 lines of `git rev-parse`/`git status --porcelain` shell-out logic 8 times crosses from "acceptable duplication" into "extract it" — unlike the common project guidance to avoid speculative traits, this is a concrete, present need shared by 8 call sites with identical behavior. `src/build-info` is `[build-dependencies]`-only (never a runtime dependency of anything), workspace member, `publish = false`, excluded from the `align-core-component-versions` linked group (it's tooling, not a shipped runtime component — same reasoning as `signal-producer`).
Alternative considered: a single build.rs in `common` with the value re-exported as a `pub const`. Rejected — a `pub const &str` computed at `common`'s compile time is fine for a value that's genuinely workspace-invariant (the commit hash _value_ is), but the goal here is also to keep `common::cli` decoupled from directly hardcoding env-var plumbing per caller; passing `BuildInfo` explicitly is simpler to reason about and test than crates reaching into `common::build_info::BUILD_COMMIT` implicitly. It also sidesteps a subtlety: incremental rebuilds only re-run a crate's `build.rs` when that crate's own inputs change, so a `common`-only build script could go stale (report an old commit) if only a downstream crate's source changed and `common` wasn't recompiled — per-binary build scripts avoid this because they always re-run for whichever binary is actually being rebuilt. (`build.rs` re-run staleness is mitigated the standard way: emit `cargo:rerun-if-changed=../../.git/HEAD` and the relevant ref file so it's checked on every build regardless of which crate triggered the rebuild — noted in Migration Plan below.)

**Plain `build.rs` shelling out to `git`, not `vergen`/`shadow-rs`.** No new external dependency, minimal surface (short SHA + dirty flag + fallback is all that's needed — not full build-timestamp/rustc-version tooling). `git` is already a hard requirement for building this repo (submodule init, `cargo-husky` hooks), so assuming it's on PATH is safe. Fallback to `unknown` when `git rev-parse` fails (no `.git`, no `git` binary) rather than failing the build — satisfies the `build-provenance-reporting` spec's non-git-build scenario.

**MCP server_info gets its own `BuildInfo` construction, not routed through `common::cli`.** `mcp-server` doesn't depend on `common` at all today (confirmed via Cargo.toml) and doesn't use `CommonCommands`. It gets its own `build.rs` (via `build-info`) and constructs its `BuildInfo` (or just the two fields it needs: version + commit) directly in `server.rs` using its own `env!(...)` calls. No need to introduce a `common` dependency just for this.

## Risks / Trade-offs

- **[Risk]** Changing `handle_common_command`'s signature touches every binary crate's `main.rs` that calls it (6 crates). → **Mitigation**: mechanical, compiler-guided change — `cargo build --workspace` after the signature change will fail-to-compile every call site that needs updating; no risk of silently missing one.
- **[Risk]** `git rev-parse` inside `build.rs` running in a sandboxed/network-restricted CI environment or a Docker build context without `.git` (e.g. a `COPY` of source only, no `.git` dir) → build falls back to `unknown` rather than failing, but that silently degrades observability in exactly the environments (release builds) where it matters most. → **Mitigation**: verify the release build path (`.github/workflows/release-please.yml`'s `build-release`/`build-musl-*` jobs, and any `Dockerfile`) actually has `.git` available — they already run `git submodule update --init --recursive`, implying `.git` is present; confirm Docker build contexts also include it (task to check `Dockerfile`/`.dockerignore` for `.git` exclusion).
- **[Risk]** Stale build script caching: if only one crate's `build.rs` re-runs on incremental rebuild, other already-built binaries in the same `target/` keep reporting an older commit until they're also rebuilt. → **Mitigation**: this is expected/correct behavior, not a bug — a binary's reported commit should reflect when _it_ was last built, not the workspace's current HEAD. Document this in the commit-hash's user-facing meaning (it's "the commit this binary was built from," not "the commit currently checked out").

## Migration Plan

1. Create `src/build-info` crate (build-dependency only): git-capture function, `unknown`/dirty-flag fallback logic, register in root `Cargo.toml` workspace `members` (and add to `default-members`? — no, build-dependency-only crates don't need to be in `default-members`, only `members`).
2. Add `build.rs` to each of the 8 binary crates, each depending on `build-info` as a `[build-dependencies]` entry and emitting `cargo:rustc-env=SIGNALDB_BUILD_COMMIT=...` plus `cargo:rerun-if-changed=` hints against `.git/HEAD` and the resolved ref.
3. Change `common::cli`'s `BuildInfo` struct + `handle_common_command`/`version_info()` signatures; update the 6 `main.rs` call sites in `signaldb-bin`/`acceptor`/`router`/`writer`/`querier`/`compactor`.
4. Update `signaldb-cli`'s own version-printing path (independent of `common`) to include the commit hash.
5. Update `mcp-server`'s `server_info` tool to include its own commit hash.
6. `cargo build --workspace` to confirm every call site compiles; manually run `--version`/`version` on each binary to confirm correct self-identification (the bug this surfaces) and presence of a commit hash.
7. Rollback: revert the commit — build scripts and struct signatures have no persisted state or migration to undo.

## Open Questions

None — mechanism, scope, and the discovered name/version bug's fix are all decided above.
