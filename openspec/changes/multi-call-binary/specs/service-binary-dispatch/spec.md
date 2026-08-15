## Purpose

Defines how the single `signaldb` server executable selects between running the monolith and running one service, what each mode's command-line surface is, and what container images and release artifacts an operator can rely on receiving.

## ADDED Requirements

### Requirement: One server executable runs monolith or single service

The `signaldb` executable SHALL run all services in one process when invoked with no service selector, and SHALL run exactly one service when a selector names it. The selector SHALL be either the first command-line argument (`signaldb <service> [args…]`) or the executable's own name (`signaldb-<service> [args…]`, as via a symlink or hard link). Recognised services are `acceptor`, `router`, `writer`, `querier`, `compactor` and `mcp`.

#### Scenario: Monolithic start

- **WHEN** an operator runs `signaldb --config signaldb.toml`
- **THEN** the acceptor, router, writer, querier and compactor start in the one process exactly as the monolithic binary did before

#### Scenario: Subcommand selector

- **WHEN** an operator runs `signaldb router --config signaldb.toml`
- **THEN** only the router starts, with the same behaviour, ports and configuration handling as the former `signaldb-router` binary

#### Scenario: argv[0] selector

- **WHEN** the executable is invoked through a link named `signaldb-writer` (e.g. `/usr/local/bin/signaldb-writer --flight-port 50051`)
- **THEN** only the writer starts and the remaining arguments are the writer's arguments, unchanged

#### Scenario: Both selectors present

- **WHEN** a link named `signaldb-writer` is invoked as `signaldb-writer querier`
- **THEN** the argv[0] selector wins: the writer starts and `querier` is passed through to the writer's own argument parser (which rejects it), so a mis-linked binary never silently runs a different service

#### Scenario: Unknown selector

- **WHEN** an operator runs `signaldb frobnicate`
- **THEN** the process exits non-zero with a usage error that lists the recognised services, and no service starts

### Requirement: Each service keeps its own command-line surface

In single-service mode the argument parser SHALL be that service's own: `--help` describes that service's flags and subcommands, `--version` reports the workspace version, and the common commands (`config validate`, `config show`, `start`) SHALL behave as they did in the per-service binaries. Environment-variable overrides SHALL be honoured unchanged.

#### Scenario: Service help

- **WHEN** an operator runs `signaldb acceptor --help` or `signaldb-acceptor --help`
- **THEN** the output is the acceptor's usage, naming `--grpc-port`, `--http-port`, `--bind` and `--wal-dir`, and does not list flags of other services

#### Scenario: Service version

- **WHEN** an operator runs `signaldb querier --version`
- **THEN** the output names the service and reports the same version string as `signaldb --version`

#### Scenario: Common command in single-service mode

- **WHEN** an operator runs `signaldb compactor config validate --config signaldb.toml`
- **THEN** the configuration is validated and the process exits without starting the compactor

### Requirement: Container images ship the multi-call binary with a service entrypoint

Every per-service container image SHALL contain the `signaldb` executable and a link at `/usr/local/bin/signaldb-<service>` that is the image's entrypoint, so existing invocations, health probes and orchestration manifests keep working unchanged. The monolithic image's entrypoint SHALL remain `/usr/local/bin/signaldb`.

#### Scenario: Service image default command

- **WHEN** an operator runs the `router` image with no command override
- **THEN** the router starts, and `docker exec <container> signaldb-router --version` and `docker exec <container> signaldb router --version` both succeed with the same output

#### Scenario: Monolithic image unchanged

- **WHEN** an operator runs the monolithic image as before
- **THEN** all services start in one process and the UI is served, with no change to ports, configuration or environment variables

### Requirement: Release artifacts contain the multi-call binary

Release archives for Linux and macOS SHALL contain the `signaldb` executable and `signaldb-<service>` links for every recognised service; the Windows archive SHALL contain `signaldb.exe`, and single-service mode on Windows SHALL be reached through the subcommand selector. The `signaldb-cli` archive SHALL remain a separate artifact.

#### Scenario: Linux archive

- **WHEN** an operator extracts the Linux microservices archive and runs `./signaldb-acceptor --version`
- **THEN** the acceptor's version is printed, and `./signaldb acceptor --version` prints the same

#### Scenario: Windows archive

- **WHEN** an operator extracts the Windows archive
- **THEN** `signaldb.exe acceptor --help` prints the acceptor's usage and no `signaldb-acceptor.exe` is expected
