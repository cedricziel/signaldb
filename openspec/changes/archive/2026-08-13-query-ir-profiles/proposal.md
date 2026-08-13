## Why

Profiles are queryable only through Pyroscope-specific endpoints, preventing the
native Query IR from filtering and aggregating profile summaries alongside logs
and traces. Adding profiles to Query IR provides one structured, tenant-scoped
query model without replacing flamegraph or diff workflows.

## What Changes

- Register `profiles` as a Query IR source with bounded summary-row projections,
  logical profile fields, profile/resource/scope attribute addressing, and
  generic filtering, aggregation, ranking, ordering, and time series.
- Enforce `profiles:read` for native Query IR profile requests so this source
  cannot bypass existing profile-query authorization.
- Add profiles to the Explore Query source selector and render native Query IR
  rows, tables, and series through generated clients.
- Document profile Query IR semantics and explicitly defer sample/frame JSON,
  flamegraph, diff, extraction, and heatmap operations.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `query-ir-core`: Register profiles as a supported typed source and enforce its
  source-specific authorization and projection semantics.

## Impact

- **common**: Query IR source registration and typed field-resolution contracts.
- **querier**: Profile Iceberg source planning and aggregate execution.
- **router**: Source-aware authorization for native Query IR requests.
- **ui**, **signaldb-cli**, **mcp-server**, **signaldb-sdk**: Generic Query IR
  profile queries and source selection using existing generated client flows.
- **docs/users** and architecture skill: Native query source documentation.
- No changes to OTLP Profiles ingestion, Pyroscope compatibility endpoints,
  Flight wire schemas, WAL, or Iceberg layout.
