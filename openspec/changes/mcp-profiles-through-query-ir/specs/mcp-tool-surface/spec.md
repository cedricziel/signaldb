## MODIFIED Requirements

### Requirement: Profile discovery and query are available as tools

The MCP server SHALL expose profile discovery and query as tools,
tenant-scoped like every other tool, reading through the Query IR
(`POST /api/v1/query`) and never through the Pyroscope-compatible endpoints:
`discover_profile_types` (the distinct `sample.type`/`sample.unit` pairs of
the `profiles` source, default all history up to now), `discover_attributes` with
`signal: "profiles"` (the `profiles` source's field names through the Query IR
`describe` stage and, with `tag`, a field's values), `search_profiles` (a
Pyroscope-style selector plus a time range, default the last hour → the
`flamegraph` envelope, subject to the same payload cap and truncation flag as
other query tools), `compare_profiles` (a baseline and a comparison range →
the `flamegraph` envelope with a `baseline`), and `profiles_for_trace` (the
profiles correlated with a trace id). The selector SHALL filter on its sample
type (the profile type's second `:` segment, or a bare name) and on
`service_name` with `=`, `!=`, `=~` or `!~` (regexes fully anchored); any
other label, operator, or a malformed selector SHALL be rejected as invalid
parameters naming it. Blank time parameters SHALL count as unset, a missing
`from` SHALL default to a fixed span before `until`, and a range whose `from`
is not before its `until` SHALL be rejected as invalid parameters. The existing `get_profile`
(single profile by id) is unchanged. `discover_profile_types`,
`search_profiles` and `compare_profiles` SHALL keep the Pyroscope response
shapes they returned before (profile-type entries, and a flamebearer with
`single` or `double` metadata and, for a diff, `leftTicks`/`rightTicks`, zero
when nothing matched), plus the IR's `truncated` flag.

#### Scenario: Profile types are discoverable

- **WHEN** a tenant has ingested CPU profiles and a session calls
  `discover_profile_types`
- **THEN** the tool returns the CPU profile type among the types with data,
  scoped to the caller's tenant

#### Scenario: Profile labels through discover_attributes

- **WHEN** a session calls `discover_attributes` with `signal: "profiles"` and
  no `tag`
- **THEN** the tool returns the `describe: fields` result for the `profiles`
  source for the caller's tenant; with a `tag` it returns that field's
  `describe: values` result

#### Scenario: A selector renders a flame graph

- **WHEN** a session calls `search_profiles` with a selector such as
  `process_cpu:cpu:nanoseconds{service_name="checkout"}` and a range
- **THEN** the tool sends a `flamegraph` document filtering `sample.type` and
  `service.name` to `POST /api/v1/query` and returns the aggregated flame
  graph as structured JSON, truncated with `truncated: true` if it exceeds
  the payload cap

#### Scenario: Two ranges render a differential flame graph

- **WHEN** a session calls `compare_profiles` with a selector, a baseline
  range and a comparison range
- **THEN** the tool sends one `flamegraph` document whose `range` is the
  comparison and whose `baseline` is the baseline range, and returns the
  differential flame graph with both sides' totals

#### Scenario: Profiles for a trace

- **WHEN** a session calls `profiles_for_trace` with a trace id that has
  correlated profiles
- **THEN** the tool returns those profiles' identities scoped to the caller's
  tenant
