## Purpose

Guarantees that every operator- and user-facing capability of SignalDB is
reachable identically through the API, the SDK, the CLI, and the MCP server, so
the four surfaces cannot drift apart in what they can do.

## ADDED Requirements

### Requirement: SDK is the sole client access path

The CLI (`signaldb-cli`) and the MCP server (`signaldb-mcp`) SHALL reach every
SignalDB capability exclusively through `signaldb-sdk`. Neither SHALL construct
its own HTTP client to a SignalDB endpoint, its own Arrow Flight client, or any
other direct transport to a SignalDB service.

#### Scenario: CLI issues a query

- **WHEN** the CLI executes any query (SQL, TraceQL, LogQL, or PromQL)
- **THEN** the request is dispatched through a `signaldb-sdk` client method
- **AND** the CLI crate contains no direct `FlightServiceClient` or raw HTTP
  construction against a SignalDB service

#### Scenario: MCP tool performs an operation

- **WHEN** an MCP tool handles a call that reaches SignalDB
- **THEN** it invokes a `signaldb-sdk` client method
- **AND** the `mcp-server` crate depends on SignalDB only via `signaldb-sdk`

### Requirement: SDK covers the full API surface

`signaldb-sdk` SHALL expose every capability the SignalDB API offers, spanning
its HTTP surface (admin/management, operational control, and the
PromQL/LogQL/TraceQL query-compat endpoints) and its Arrow Flight surface (SQL
query). A capability reachable through the API but absent from the SDK is a
defect.

#### Scenario: A new API capability lacks SDK coverage

- **WHEN** the API exposes an operation that has no corresponding `signaldb-sdk`
  method
- **THEN** the parity check fails and identifies the uncovered operation

### Requirement: Three-way parity is enforced

An automated check SHALL enumerate the public capability surface of
`signaldb-sdk` and assert that each capability is reachable through both a CLI
verb and an MCP tool. The check SHALL fail when a surface under-exposes a
capability the SDK provides.

#### Scenario: SDK gains a capability the CLI does not surface

- **WHEN** a capability exists in the SDK but no CLI verb reaches it
- **THEN** the parity check fails and names the missing CLI verb

#### Scenario: SDK gains a capability the MCP server does not surface

- **WHEN** a capability exists in the SDK but no MCP tool reaches it
- **THEN** the parity check fails and names the missing MCP tool

#### Scenario: All surfaces are aligned

- **WHEN** every SDK capability has both a CLI verb and an MCP tool
- **THEN** the parity check passes
