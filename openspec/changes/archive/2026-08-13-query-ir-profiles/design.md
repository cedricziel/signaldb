## Context

See proposal.md. Query IR currently has registered logs and traces sources,
while profiles are queried through dedicated Pyroscope-compatible tickets and
HTTP handlers. The profile Iceberg table already stores a metadata row per
profile plus JSON payload columns for samples and stacktraces. Query IR lowers
validated documents into DataFusion plans over tenant/dataset-scoped tables.

## Goals / Non-Goals

**Goals:**

- Add a typed, scalar profile-summary source to the native Query IR model.
- Preserve tenant/dataset isolation and enforce source-specific read scopes at
  the router boundary.
- Reuse existing row, table, and series envelopes in generic clients.

**Non-Goals:**

- Replacing Pyroscope flamegraph, diff, label-discovery, or single-profile APIs.
- Exposing samples, stacktraces, or raw JSON as IR fields.
- Adding profile heatmaps, extraction, cross-signal joins, or a new Flight wire
  format.

## Decisions

### Register profiles as a metadata-row source

`profiles` will have a profile-row relation grain and disallow log extraction.
Its registry will map canonical logical fields to scalar profile columns,
including profile identity, timestamp, duration, sample and period metadata,
service name, and trace/span correlation IDs. Resource and scope attributes use
the established logical attribute conventions and are resolved through the same
registry-mediated paths as other sources.

This permits the existing `where`, `aggregate`, `topk`/`bottomk`, `order`,
`limit`, rows/table, and series semantics to lower through DataFusion without
introducing profile-specific IR stages. Treating payload JSON as an IR field was
rejected because it would make arbitrary sample/frame traversal an unbounded,
storage-shaped API rather than a stable query contract.

### Enforce source scopes before Flight dispatch

The router will parse only enough of the request's source selection to map it to
the corresponding read scope, reject unauthorized requests, and derive tenant
and dataset exclusively from the authenticated context. The querier remains
responsible for full IR validation and planning.

This keeps authorization at the public boundary and prevents a native profile
request from bypassing `profiles:read`. A global generic-query scope was
rejected because it would grant access to signals beyond an API key's existing
least-privilege scopes.

### Keep generic clients generic

The OpenAPI request represents `from` as a string, so generated SDK and MCP
clients need no protocol changes. The Explore Query builder will add `profiles`
to its source union and selector; profile defaults use `service.name` for
grouping. The Profiles feature remains the destination for payload-specific
visualizations.

### Preserve FDAP alignment and storage compatibility

The planner uses Arrow and Parquet types re-exported by DataFusion. This change
queries the existing profiles Iceberg schema directly; it requires no Flight v1
wire/schema transform changes, WAL changes, or Iceberg migration.

## Risks / Trade-offs

- [Profile metadata naming differs from existing logical conventions] → Add
  source-registry and planner tests for every supported field and alias.
- [Generic Query UI suggests flamegraph-level functionality] → Label the source
  as profile summaries and retain profile-detail navigation separately.
- [Source authorization drifts from profile endpoints] → Centralize source to
  read-scope mapping and test both allowed and denied request paths.

## Migration Plan

1. Deploy the additive source registration and router scope check.
2. Deploy the querier planner support and generic UI selector together.
3. Roll back by removing the source registration and UI option; existing profile
   data and Pyroscope-compatible APIs remain unchanged.
