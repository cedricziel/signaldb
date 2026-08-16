# client-surface-parity Specification

## Purpose
Guarantees that every operator- and user-facing capability of SignalDB is
reachable identically through the API, the SDK, the CLI, and the MCP server, so
the four surfaces cannot drift apart in what they can do.
## Requirements
### Requirement: SDK is the sole client access path

The CLI (`signaldb-cli`) and the MCP server (`signaldb-mcp`) SHALL reach every
SignalDB capability exclusively through `signaldb-sdk`. Neither SHALL construct
its own HTTP client to a SignalDB endpoint, its own Arrow Flight client, or any
other direct transport to a SignalDB service. Both SHALL obtain their HTTP
client through the SDK's client builder rather than assembling one themselves,
so cross-cutting client policy — retry on throttling, timeouts, default
headers — is defined once in the SDK and cannot drift between consumers.

#### Scenario: CLI issues a query

- **WHEN** the CLI executes any query (SQL, TraceQL, LogQL, PromQL, or Query IR)
- **THEN** the request is dispatched through a `signaldb-sdk` client method
- **AND** the CLI crate contains no direct `FlightServiceClient` or raw HTTP
  construction against a SignalDB service

#### Scenario: MCP tool performs an operation

- **WHEN** an MCP tool handles a call that reaches SignalDB
- **THEN** it invokes a `signaldb-sdk` client method
- **AND** the `mcp-server` crate depends on SignalDB only via `signaldb-sdk`

#### Scenario: Consumers build clients through the SDK

- **WHEN** the CLI or the MCP server needs an HTTP client for SignalDB
- **THEN** it obtains it from the SDK's client builder, and neither crate
  constructs a bare HTTP client for a SignalDB endpoint

#### Scenario: Throttling is retried on every surface

- **WHEN** the parity check evaluates the CLI, the MCP server, and the UI
- **THEN** each is shown to route SignalDB requests through a client that
  applies the shared retry-on-throttle policy

### Requirement: SDK covers the full API surface

`signaldb-sdk` SHALL expose every capability the SignalDB API offers, spanning
its HTTP surface (admin/management, tenant self-management including tables
and schemas, operational control, and the PromQL/LogQL/TraceQL/Pyroscope
query-compat endpoints including trace-to-profile correlation) and its Arrow
Flight surface (SQL query). A capability reachable through the API but absent
from the SDK is a defect. Every HTTP endpoint the router serves to tenants or
administrators SHALL be declared in the OpenAPI document, because that document
is the SDK's only source; an endpoint outside it is invisible to every client
and is therefore a parity defect. The route drift guard's allowlist SHALL name
only routes that are genuinely not part of the client contract (health, the
OpenAPI document itself, browser session and OAuth flows, static UI); a
compat query route SHALL NOT be allowlisted.

#### Scenario: A new API capability lacks SDK coverage

- **WHEN** the API exposes an operation that has no corresponding `signaldb-sdk`
  method
- **THEN** the parity check fails and identifies the uncovered operation

#### Scenario: A router endpoint is missing from the OpenAPI document

- **WHEN** the router registers a tenant- or admin-facing HTTP route that has no
  OpenAPI operation
- **THEN** the OpenAPI drift check fails and names the route

#### Scenario: Pyroscope compat endpoints are in the contract

- **WHEN** the OpenAPI document is inspected
- **THEN** it declares operations for `/pyroscope/render`, `/render-diff`,
  `/label-names`, `/label-values`, `/profile-types`, and
  `/api/profiles/trace/{trace_id}`, and the generated Rust SDK and TypeScript
  client expose them

### Requirement: Query-surface parity is enforced

An automated check SHALL assert capability parity across the CLI and the MCP
server for **every** operation the SDK exposes — derived from the SDK's own
operation list, not a hand-maintained subset — against a reviewed exclusion
list of operations that are intentionally single-surface. That list SHALL be
limited to operations that are inherently tied to a browser or a signed-in
human: the OAuth 2.1 consent endpoints (session-cookie authenticated) and the
human self-serve tenant creation performed by an instance administrator
(API-key clients create tenants through the admin API instead). Every other
operation — including the whole tenant management API, which an API key with
`tenant:manage` may call — SHALL have both a CLI command and an MCP tool.
Query languages keep their specific rules: every language SHALL be reachable
through a CLI `query` flag; every language served over the router's **HTTP**
surface (TraceQL, LogQL, PromQL, Query IR) SHALL additionally be reachable
through an MCP tool; **SQL** is served over Arrow Flight (gRPC) and, because
the MCP server is an HTTP forwarder that holds no Flight client, SQL is
intentionally CLI-only, asserted explicitly. The check SHALL fail and name the
surface and operation whenever a non-excluded SDK operation lacks a CLI command
or an MCP tool, and SHALL fail when the exclusion list names an operation that
no longer exists.

#### Scenario: An HTTP query language is missing from the CLI or MCP

- **WHEN** a TraceQL/LogQL/PromQL/Query-IR capability lacks either a CLI `query`
  flag or an MCP tool
- **THEN** the parity check fails and names the missing surface

#### Scenario: SQL is CLI-only by design

- **WHEN** the parity check evaluates SQL
- **THEN** it requires a CLI `--sql` flag and requires that no MCP tool claims
  SQL, matching the HTTP-forwarder boundary

#### Scenario: A management operation lacks a surface

- **WHEN** the SDK gains an operation (for example a new tenant-management call)
  and either the CLI or the MCP server does not expose it and it is not on the
  exclusion list
- **THEN** the parity check fails and names the operation and the missing
  surface

#### Scenario: Management operations are not excluded

- **WHEN** the parity check evaluates the tenant management operations
  (datasets, API keys, memberships, schema, tenant self view)
- **THEN** each has a CLI command and an MCP tool; only the OAuth consent
  endpoints and human self-serve tenant creation are excluded

#### Scenario: Stale exclusion is rejected

- **WHEN** the exclusion list names an operation the SDK no longer has
- **THEN** the parity check fails so the list cannot silently rot

#### Scenario: All surfaces are aligned

- **WHEN** every non-excluded SDK operation has a CLI command and an MCP tool,
  every query language has its required CLI flag, every HTTP language has an
  MCP tool, and SQL remains CLI-only
- **THEN** the parity check passes

