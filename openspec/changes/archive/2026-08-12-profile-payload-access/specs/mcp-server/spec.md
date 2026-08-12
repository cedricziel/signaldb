## ADDED Requirements

### Requirement: Single profile retrieval tool

The MCP server SHALL expose a `get_profile` tool that retrieves one
profile's actual payload — its aggregated flamegraph (names, per-level
frame data, total sample value, max self value) — by `profile_id`, scoped
to the caller's tenant, following the same single-entity retrieval shape as
`get_trace`. It SHALL accept the same optional `dataset` argument,
dataset-scoping, and payload-cap/truncation rules as the other query tools,
and SHALL return a clean MCP "not found" error — not an empty success or a
transport failure — when the id does not exist for the caller's tenant.

For MCP clients that negotiate the MCP Apps UI extension, `get_profile`'s
result SHALL be rendered as an interactive flamegraph, following the same
mechanism `get_trace` uses to render an interactive waterfall (a
compiled-in UI resource registered for the tool, with the flamegraph
delivered as the call result's structured content). Clients that do not
negotiate the extension SHALL still receive the flamegraph as plain
structured JSON.

#### Scenario: Get profile by id

- **WHEN** an authenticated session calls `get_profile` with a `profile_id`
  that exists for the caller's tenant
- **THEN** the tool returns that profile's flamegraph as structured JSON

#### Scenario: Get profile by id when absent

- **WHEN** a session calls `get_profile` with a `profile_id` that does not
  exist for the caller's tenant
- **THEN** the tool returns a clean MCP "not found" error rather than an
  empty success or a transport failure

#### Scenario: Cross-tenant profile id is not found

- **WHEN** a session authenticated for tenant A calls `get_profile` with a
  `profile_id` that belongs only to tenant B
- **THEN** the tool returns a "not found" error, not tenant B's data

#### Scenario: UI-capable client renders an interactive flamegraph

- **WHEN** an MCP client that has negotiated the MCP Apps UI extension calls
  `get_profile`
- **THEN** the result is delivered so the client can render it as an
  interactive flamegraph, using the same mechanism `get_trace` uses for its
  interactive waterfall

#### Scenario: Non-UI client receives plain structured data

- **WHEN** an MCP client that has not negotiated the MCP Apps UI extension
  calls `get_profile`
- **THEN** the result is the flamegraph as plain structured JSON, with no UI
  resource reference

#### Scenario: Oversized flamegraph is truncated with a flag

- **WHEN** `get_profile`'s underlying flamegraph result would exceed the
  tool's payload cap
- **THEN** the tool returns valid JSON marked `truncated: true` with a
  narrowing hint, not an unbounded blob

#### Scenario: Tools list includes get_profile

- **WHEN** any authenticated tenant session issues `tools/list`
- **THEN** `get_profile` is present in the returned list alongside the other
  query and exploration tools
