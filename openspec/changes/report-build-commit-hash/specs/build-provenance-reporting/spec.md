## Purpose

Lets operators and support engineers identify the exact commit a running SignalDB binary was built from, so a reported version number (especially once main components share one aligned version) can be disambiguated between builds cut at different points in history.

## ADDED Requirements

### Requirement: CLI version output includes the build commit

Every SignalDB CLI binary built on the shared CLI scaffolding (`signaldb`, `signaldb-acceptor`, `signaldb-router`, `signaldb-writer`, `signaldb-querier`, `signaldb-compactor`) and the standalone `signaldb-cli` SHALL include the git commit hash it was built from in its version output (`--version` flag and `version` subcommand), alongside the existing package name, semantic version, and Rust version.

#### Scenario: Version command reports the build commit

- **WHEN** a user runs `signaldb version` (or the equivalent on any of the service binaries, or `signaldb-cli --version`)
- **THEN** the output includes a short git commit hash identifying the exact source commit the binary was compiled from

#### Scenario: Working tree had uncommitted changes at build time

- **WHEN** a binary is built from a git checkout with uncommitted local changes
- **THEN** the reported commit hash is marked as a dirty build (e.g. a `-dirty` suffix), distinguishing it from a build of the exact committed state

#### Scenario: Build outside a git checkout

- **WHEN** a binary is built from a source tree with no `.git` directory available (e.g. an extracted source tarball)
- **THEN** the version output reports a clear placeholder (e.g. `unknown`) for the commit hash instead of failing the build or omitting the field silently
