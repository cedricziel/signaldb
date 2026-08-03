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

### Requirement: Query-surface parity is enforced

An automated check SHALL assert query-capability parity against a manifest of the
query languages SignalDB supports (SQL, TraceQL, LogQL, PromQL, and Query IR).
Every language SHALL be reachable through a CLI `query` flag. Every language
served over the router's **HTTP** surface (TraceQL, LogQL, PromQL, Query IR)
SHALL additionally be reachable through an MCP tool. **SQL** is served over Arrow
Flight (gRPC); because the MCP server is an HTTP forwarder that holds no Flight
client, SQL is intentionally CLI-only, and this boundary SHALL be asserted
explicitly so it cannot erode silently. The check SHALL fail when a surface
under-exposes a language its transport requires.

#### Scenario: An HTTP query language is missing from the CLI or MCP

- **WHEN** a TraceQL/LogQL/PromQL/Query-IR capability lacks either a CLI `query`
  flag or an MCP tool
- **THEN** the parity check fails and names the missing surface

#### Scenario: SQL is CLI-only by design

- **WHEN** the parity check evaluates SQL
- **THEN** it requires a CLI `--sql` flag and requires that no MCP tool claims
  SQL, matching the HTTP-forwarder boundary

#### Scenario: All surfaces are aligned

- **WHEN** every query language has its required CLI flag, every HTTP language
  has an MCP tool, and SQL remains CLI-only
- **THEN** the parity check passes
