## Purpose

Defines how the single `signaldb` server executable selects between running the monolith and running one service, what each mode's command-line surface is, and what container images and release artifacts an operator can rely on receiving.

## ADDED Requirements

### Requirement: One server executable runs monolith or single service

The `signaldb` executable SHALL run all services in one process when its first argument is not a service name, and SHALL run exactly one service when the first argument names it: `signaldb <service> [args…]`. Recognised services are `acceptor`, `router`, `writer`, `querier`, `compactor` and `mcp`. Service selection SHALL depend only on the arguments, never on the executable's file name, so a copied or renamed binary behaves identically.

#### Scenario: Monolithic start

- **WHEN** an operator runs `signaldb --config signaldb.toml`
- **THEN** the acceptor, router, writer, querier and compactor start in the one process exactly as the monolithic binary did before

#### Scenario: Single-service start

- **WHEN** an operator runs `signaldb router --config signaldb.toml`
- **THEN** only the router starts, with the same behaviour, ports and configuration handling as the former `signaldb-router` binary

#### Scenario: Renamed executable

- **WHEN** the executable is copied to a file named `signaldb-writer` and run with `--flight-port 50051`
- **THEN** it behaves as `signaldb --flight-port 50051` (the monolith's parser rejects the writer-only flag) — the file name is never consulted

#### Scenario: Unknown selector

- **WHEN** an operator runs `signaldb frobnicate`
- **THEN** the process exits non-zero with a usage error that lists the recognised services, and no service starts

### Requirement: Each service keeps its own command-line surface

In single-service mode the argument parser SHALL be that service's own: `--help` describes that service's flags and subcommands, `--version` reports the workspace version, and the common commands (`config validate`, `config show`, `start`) SHALL behave as they did in the per-service binaries. Environment-variable overrides SHALL be honoured unchanged.

#### Scenario: Service help

- **WHEN** an operator runs `signaldb acceptor --help`
- **THEN** the output is the acceptor's usage, naming `--grpc-port`, `--http-port`, `--bind` and `--wal-dir`, and does not list flags of other services

#### Scenario: Service version

- **WHEN** an operator runs `signaldb querier --version`
- **THEN** the output names the service and reports the same version string as `signaldb --version`

#### Scenario: Common command in single-service mode

- **WHEN** an operator runs `signaldb compactor config validate --config signaldb.toml`
- **THEN** the configuration is validated and the process exits without starting the compactor

### Requirement: Container images ship the multi-call binary with a service entrypoint

Every per-service container image SHALL contain the `signaldb` executable and SHALL have `/usr/local/bin/signaldb <service>` as its entrypoint, so orchestration manifests and compose files that rely on the image's default entrypoint keep working unchanged. Arguments given to the container SHALL be appended to that service's arguments. The monolithic image's entrypoint SHALL remain `/usr/local/bin/signaldb`.

#### Scenario: Service image default command

- **WHEN** an operator runs the `router` image with no command override
- **THEN** the router starts

#### Scenario: Service image with arguments

- **WHEN** an operator runs the `router` image with `--version` as the container command
- **THEN** the router's version is printed and the container exits, and `docker exec <container> signaldb router --help` prints the router's usage

#### Scenario: Monolithic image unchanged

- **WHEN** an operator runs the monolithic image as before
- **THEN** all services start in one process and the UI is served, with no change to ports, configuration or environment variables

### Requirement: Release artifacts contain the multi-call binary

Release archives SHALL contain the `signaldb` executable (`signaldb.exe` on Windows) as the only server executable; single-service mode is reached through `signaldb <service>` on every platform. The `signaldb-cli` archive SHALL remain a separate artifact.

#### Scenario: Extracted archive

- **WHEN** an operator extracts a release archive and runs `./signaldb acceptor --version` (or `signaldb.exe acceptor --version`)
- **THEN** the acceptor's version is printed, and no `signaldb-acceptor` executable is present or expected
