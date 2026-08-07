## Purpose

Guarantees that the web UI reaches every SignalDB HTTP capability exclusively
through the generated TypeScript client, so the UI's request/response shapes
cannot drift from the published OpenAPI contract — the UI-side counterpart to
`client-surface-parity`'s guarantee for the CLI and MCP server.

## ADDED Requirements

### Requirement: UI reaches SignalDB exclusively through the generated client

Every SignalDB HTTP capability the web UI consumes — tenant/dataset/API-key
management, Tempo/Loki/Prometheus/Pyroscope query-compat, and UI session/
whoami — SHALL be reached through a function generated into
`src/ui/src/api/gen` from the published OpenAPI document. No module under
`src/ui/src` SHALL construct its own `fetch()` (or other raw HTTP transport)
request against a SignalDB endpoint.

#### Scenario: A query-compat call goes through the generated client

- **WHEN** the UI issues a Tempo, Loki, Prometheus, or Pyroscope query or
  metadata request
- **THEN** the request is dispatched through a function generated in
  `src/ui/src/api/gen`
- **AND** no module in `src/ui/src/api` (or elsewhere) constructs the request
  via a hand-written `fetch()` call

#### Scenario: Session and whoami calls go through the generated client

- **WHEN** the UI creates a session, deletes a session, or fetches the
  authenticated tenant via whoami
- **THEN** the request is dispatched through a function generated in
  `src/ui/src/api/gen`

### Requirement: Domain-shaped adapters may wrap generated calls

A UI module MAY keep a hand-written type or transform function on top of a
generated client call when it performs real computation the generated type
does not encode — such as flattening OTLP attribute wire values, merging and
sorting log streams, converting timestamp units, or delta-decoding a
flamebearer profile. A hand-written type that only renames fields or narrows
a generated type's shape with no computation SHALL be replaced by the
generated type directly.

#### Scenario: A computed transform survives as an adapter

- **WHEN** a UI function derives a value the generated response type doesn't
  provide directly (e.g. the root span of a trace, computed from the
  generated span list)
- **THEN** the function may keep a hand-written return type and transform
  logic, so long as the underlying HTTP call is a generated client function

#### Scenario: A pass-through type is not duplicated

- **WHEN** a hand-written UI type's fields are a direct rename or subset of a
  generated response type with no computed values
- **THEN** UI code consumes the generated type directly instead of
  maintaining a parallel hand-written type
