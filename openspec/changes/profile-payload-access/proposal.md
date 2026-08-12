## Why

`query-ir-profiles` made profile _summary_ rows (id, timestamp, duration,
sample type, service name) queryable through the native Query IR, but
deliberately excluded the raw `samples_json`/`stacktraces_json` payload —
the actual stack-sample data a flamegraph is built from — as an IR field,
to avoid turning storage JSON into an unbounded query surface. That leaves
only the Pyroscope-compatible HTTP endpoints (and unrestricted raw SQL) able
to produce a flamegraph. The native Query IR, the CLI, and the MCP server
have no way to retrieve actual profile payloads at all: an agent or script
using the generic query surface can _find_ profiles but never see what they
profiled.

## What Changes

- Add a `flamegraph` result envelope to the Query IR, legal only for the
  `profiles` source: a terminal stage aggregates every profile row matched
  by the pipeline into one bounded `Flamegraph{names, levels, total,
max_self}` structure — the same shape and the same
  `aggregate_profiles_to_flamegraph` aggregation the Pyroscope `/render`
  endpoint already produces, not a raw JSON dump of `samples_json`/
  `stacktraces_json`. Filtering to a single `profile.id` yields that one
  profile's flamegraph; filtering to a selector/time range aggregates across
  matches, matching Pyroscope's existing semantics.
- Enforce the same `profiles:read` scope already required for the
  `profiles` source; no new authorization surface.
- Add an MCP `get_profile` tool (single profile by `profile.id`, wrapping
  the same flamegraph path) so agents can retrieve one profile's payload
  directly, mirroring `get_trace`'s shape and its MCP Apps interactive
  rendering for UI-capable clients.
- Document that SQL already exposes the raw payload columns unrestricted
  today (`SELECT samples_json, stacktraces_json FROM profiles`) — no code
  change needed there, just closing the documentation gap so "every
  interface" is verifiably true rather than assumed.
- CLI: the existing generic `query-ir` command gains no new flags — it
  already forwards an arbitrary IR document, so `result: "flamegraph"`
  works once the server accepts it. Regenerate the Rust SDK from the
  extended OpenAPI schema so the SDK's typed result enum includes the new
  envelope.

**Scoped out:** a bespoke flamegraph-rendering widget in the native Explore
UI. The Pyroscope-compatible `/pyroscope/render` endpoint plus the Grafana
datasource plugin's flamegraph panel already render profile payloads
visually today; this change's UI-facing surface is limited to the generic
Query IR client being able to request and receive a `flamegraph` result
(e.g. for programmatic use or a future panel), not a new hand-built
flamegraph component. Native Explore-UI flamegraph rendering is a separate,
larger UI investment left to a future change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `query-ir-core`: add the `flamegraph` result envelope, scoped to the
  `profiles` source, reusing the existing bounded aggregation rather than
  exposing raw payload JSON as a field.
- `mcp-server`: add the `get_profile` tool.

## Impact

- **common**: `query_ir` result-envelope type gains `Flamegraph`; reuses
  `common::profile::aggregation::aggregate_profiles_to_flamegraph` (no
  changes needed to that function — it's already transport-agnostic).
- **querier**: `ir_planner`'s profiles `SourcePlan` gains flamegraph-terminal
  handling that decodes the pipeline's own scanned/windowed/filtered
  DataFrame and calls the existing `aggregate_profiles_to_flamegraph`
  directly, instead of DataFusion row/table/series lowering for that
  envelope.
- **router**: `query.rs` accepts and validates `result: "flamegraph"`,
  rejecting it for any source other than `profiles`.
- **mcp-server**: new `get_profile` tool plus its MCP Apps UI resource
  registration, following the `get_trace` pattern exactly.
- **signaldb-sdk**, **signaldb-cli**: regenerated from the extended OpenAPI
  schema; no hand-written HTTP.
- **docs/users**: `profiles.md` (document the new envelope + raw-SQL
  clarification) and `mcp.md` (document `get_profile`).
- No changes to OTLP profiles ingestion, the Pyroscope-compatible endpoints,
  Flight wire schemas, WAL, or Iceberg layout — this is additive, read-path
  only.
