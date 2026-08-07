## Why

None of SignalDB's binaries or version-reporting surfaces currently expose the git commit they were built from — `signaldb --version` and the MCP `server_info` tool only print `CARGO_PKG_VERSION`. Once main-component versions are aligned via a shared version number (see `align-core-component-versions`), the version string alone can no longer disambiguate two different builds cut between releases (e.g. two hotfix builds off the same unreleased version, or a dev build vs. a tagged release) — a build-time commit hash is needed to tell them apart.

## What Changes

- Embed the build git commit (short SHA, plus a dirty-tree marker when the working tree had uncommitted changes at build time) into every binary at compile time, via `build.rs` reading `git rev-parse`/`git status` (no new dependency required; `vergen`/`shadow-rs` are acceptable alternatives if the plain `build.rs` approach proves awkward — decided in design.md).
- **Fixes a pre-existing bug found while scoping this work**: `common::cli::utils::version_info()` and the `Version` CLI subcommand (`src/common/src/cli.rs`) build their output from `env!("CARGO_PKG_NAME")`/`env!("CARGO_PKG_VERSION")` _inside `common`'s own source file_. Since `env!` resolves at the compile time of the crate that contains the macro call — `common`, not whichever binary links it — every binary built on this scaffolding (`signaldb-bin`, `acceptor`, `router`, `writer`, `querier`, `compactor`) currently reports itself as `common 0.2.1` regardless of which one is actually running. Verified directly: `cargo run -p signaldb-bin --bin signaldb -- version` prints `common 0.2.1`. (`signaldb-cli` is unaffected — its `--version` is a separate clap-derived macro expanded in its own crate, and already reports `signaldb-cli 0.1.3` correctly.) Fixing this is unavoidable groundwork for this change: correctly attributing a build hash _per binary_ requires the same fix as correctly attributing the name/version per binary — both need to resolve in each binary crate's own compilation unit, not `common`'s.
- Extend `common::cli::utils::version_info()` and the `Version` CLI subcommand to include the commit hash, once fixed to resolve name/version/commit per calling binary. This flows to every binary built on `common`'s CLI scaffolding: `signaldb-bin`, `acceptor`, `router`, `writer`, `querier`, `compactor`.
- Extend `signaldb-cli`'s own version output the same way (it does not share `common`'s CLI scaffolding — separate wiring).
- Extend the MCP server_info tool (`src/mcp-server/src/server.rs`) to include `mcp-server`'s own build commit hash in its response, alongside the existing `version` field.
- No new HTTP `/version` endpoint is introduced by this change — none exists today (confirmed: no `/version` route on router/acceptor/querier/writer). If one is added later, it inherits this same build-metadata convention, but that's separate follow-on work, not scoped here.
- Builds outside a git checkout (e.g. a source tarball with no `.git`) fall back to a clear placeholder (e.g. `unknown`) rather than failing the build.

## Capabilities

### New Capabilities

- `build-provenance-reporting`: defines the observable behavior that every SignalDB binary's version-reporting surface (CLI `--version`/`version` subcommand, MCP `server_info` tool) includes the git commit hash it was built from, with a defined fallback for non-git builds.

### Modified Capabilities

- `mcp-tool-surface`: `server_info`'s response shape gains a build-commit field alongside the existing `version` field.

## Impact

- New workspace member `src/build-info` (build-dependency only, not part of the runtime dependency graph): shared git-commit-capture logic (short SHA, dirty-tree marker, `unknown` fallback outside a git checkout), since the custom `SIGNALDB_BUILD_COMMIT` env var it emits must be set per-crate — a Cargo `cargo:rustc-env` value from one crate's build script is not visible to any other crate's compilation, so every binary crate needs its own build script invoking this shared logic. See design.md for why one build.rs per binary is necessary here, versus computing this once centrally.
- `build.rs` (new) in each of: `signaldb-bin`, `acceptor`, `router`, `writer`, `querier`, `compactor`, `signaldb-cli`, `mcp-server` — each a few lines calling into `build-info`.
- `src/common/src/cli.rs`: `handle_common_command`/`version_info()` change signature to accept a `BuildInfo { name, version, rust_version, commit }` supplied by the caller (each binary's `main.rs`, using its own `env!(...)` calls so they resolve against that binary's own crate metadata) instead of resolving `env!("CARGO_PKG_NAME")`/`env!("CARGO_PKG_VERSION")` internally — this is the fix for the pre-existing misattribution bug described above, and the mechanism the commit hash rides along on.
- `src/signaldb-bin/src/main.rs`, `src/acceptor/src/main.rs`, `src/router/src/main.rs`, `src/writer/src/main.rs`, `src/querier/src/main.rs`, `src/compactor/src/main.rs`: construct and pass their own `BuildInfo` at the `handle_common_command` call site.
- `src/signaldb-cli/...`: same mechanism, independent wiring since `signaldb-cli` doesn't consume `common`'s CLI scaffolding at all.
- `src/mcp-server/Cargo.toml`, `src/mcp-server/build.rs` (new), `src/mcp-server/src/server.rs`: `server_info` tool response gains the commit hash.
- No changes to OTLP ingest, query surfaces, Flight wire schemas, or on-disk Iceberg/WAL layout — not BREAKING in that sense. The `server_info` MCP tool response shape changes (additive field), which is a backward-compatible surface change, not a breaking one. The `common::cli` function signature change is an internal API change within the workspace, not an external one.
