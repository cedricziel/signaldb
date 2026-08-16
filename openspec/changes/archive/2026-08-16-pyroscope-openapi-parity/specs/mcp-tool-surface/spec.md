## ADDED Requirements

### Requirement: Profile discovery and query are available as tools

The MCP server SHALL expose the Pyroscope-compatible profile surface as tools,
tenant-scoped like every other tool: `discover_profile_types` (the profile
types with data), `discover_attributes` with `signal: "profiles"` (label names
and, with `tag`, label values), `search_profiles` (a Pyroscope selector plus a
time range → the aggregated flame graph, subject to the same payload cap and
truncation flag as other query tools), `compare_profiles` (two ranges → the
diff flame graph), and `profiles_for_trace` (the profiles correlated with a
trace id). The existing `get_profile` (single profile by id) is unchanged.
Results SHALL be the SDK's native shapes.

#### Scenario: Profile types are discoverable

- **WHEN** a tenant has ingested CPU profiles and a session calls
  `discover_profile_types`
- **THEN** the tool returns the CPU profile type among the types with data,
  scoped to the caller's tenant

#### Scenario: Profile labels through discover_attributes

- **WHEN** a session calls `discover_attributes` with `signal: "profiles"` and
  no `tag`
- **THEN** the tool returns the profile label names for the caller's tenant;
  with a `tag` it returns that label's values

#### Scenario: A selector renders a flame graph

- **WHEN** a session calls `search_profiles` with a selector such as
  `process_cpu:cpu:nanoseconds{service_name="checkout"}` and a range
- **THEN** the tool returns the aggregated flame graph as structured JSON,
  truncated with `truncated: true` if it exceeds the payload cap

#### Scenario: Profiles for a trace

- **WHEN** a session calls `profiles_for_trace` with a trace id that has
  correlated profiles
- **THEN** the tool returns those profiles' identities scoped to the caller's
  tenant
