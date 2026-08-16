## MODIFIED Requirements

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
