## Context

See proposal.md. `query-ir-profiles` registered `profiles` as a Query IR
source but its design explicitly rejected exposing `samples_json`/
`stacktraces_json` as raw IR fields: "Treating payload JSON as an IR field
was rejected because it would make arbitrary sample/frame traversal an
unbounded, storage-shaped API rather than a stable query contract." That
reasoning still holds — this change does not reverse it. Instead it adds a
_bounded, structured_ way to get the actual profile payload: the same
`Flamegraph{names, levels, total, max_self}` aggregation the
Pyroscope-compatible `/pyroscope/render` endpoint already returns, produced
by the already-transport-agnostic `aggregate_profiles_to_flamegraph`
(`src/common/src/profile/aggregation.rs`). Query IR gains a fourth result
envelope alongside `rows`/`series`/`table`, legal only for `profiles`.

Raw SQL access to `samples_json`/`stacktraces_json` already works today
(`src/querier/src/flight.rs` runs `ctx.sql(...)` against the Iceberg table's
native schema with no column filtering) — that surface needs no code
change, only a documentation fix so it isn't mistaken for a gap.

## Goals / Non-Goals

**Goals:**

- Let the native Query IR (and therefore the CLI and the MCP `query_ir`
  tool) request an actual profile payload — as a bounded flamegraph, not raw
  JSON — for one profile or an aggregated selector/range.
- Add an MCP `get_profile` tool that fetches one profile's flamegraph by id,
  matching `get_trace`'s single-entity retrieval shape and its MCP Apps
  interactive rendering for UI-capable clients.
- Reuse the existing `ProfileService`/`aggregate_profiles_to_flamegraph`
  path rather than duplicating flamegraph assembly.
- Close the documentation gap: SQL already exposes the raw payload
  unrestricted; state that explicitly instead of leaving it undiscoverable.

**Non-Goals:**

- Exposing `samples_json`/`stacktraces_json` as selectable/filterable Query
  IR fields. The `query-ir-profiles` rejection of raw payload-as-field
  stands; this change only adds a bounded aggregate result.
- A native Explore-UI flamegraph rendering component. Scoped out in
  proposal.md — existing Pyroscope-compatible rendering (Grafana plugin,
  `/pyroscope/render`) covers the visual case today.
- Profile diffing (`render-diff`) through Query IR. Only single-window/
  single-selector flamegraph retrieval is in scope; diff semantics don't map
  cleanly onto a `where`-filtered pipeline and are left to a future change
  if needed.
- Any change to `profiles:read` authorization, ingestion, Flight wire
  schemas, WAL, or Iceberg layout.

## Decisions

### Flamegraph as a fourth, source-scoped result envelope

`query-ir-core`'s "Declared and validated result envelope" requirement
currently enumerates `rows`, `series`, `table`. This change adds
`flamegraph`, but legality is source-scoped: declaring `flamegraph` for a
`logs` or `traces` source is an envelope mismatch, rejected at validation
exactly like declaring `series` for a non-time-series terminal relation
today. Only `profiles` can terminate in a flamegraph.

Rejected alternative: a dedicated `/api/v1/query/flamegraph` endpoint
outside the IR. Keeping it inside the existing IR means the same
`where`/time-range/source-selection machinery (and its validation, auth,
and tenant-scoping) is reused verbatim — no new parsing, no new auth path,
no new client-generation surface beyond one more enum variant.

### The flamegraph stage bypasses DataFusion row lowering

Unlike `rows`/`table`/`series`, a `flamegraph`-terminated pipeline does not
lower into a DataFusion projection over the profiles table's summary
columns. The planner instead recognizes the envelope, resolves the matching
profile rows through the existing `ProfileService::fetch_models` (still
tenant/dataset-scoped, still honoring any `where` predicates translated to
its existing selector/range parameters), and calls
`aggregate_profiles_to_flamegraph` directly. This means `aggregate`/`topk`/
`order`/`limit` stages are **not legal** before a `flamegraph` envelope —
only `from`/`where` are — because the aggregation itself _is_ the terminal
stage, not a DataFusion-lowered one. This mirrors how `extract` is only
legal for `logs`: legality is enforced by the existing "Legal-stage
enforcement by relation type" requirement, extended with a new relation kind
for the flamegraph-eligible pipeline.

Rejected alternative: lower flamegraph construction into DataFusion
(UDAF over exploded stack frames). Rejected because it would duplicate
`aggregate_profiles_to_flamegraph`'s tree-building logic in SQL/UDAF form
for no behavioral benefit — the existing function is already correct,
tested, and used by Pyroscope; reusing it directly keeps one implementation
of "how a flamegraph is built" instead of two that could drift.

### `get_profile` mirrors `get_trace` exactly

`get_profile(profile_id, dataset?)` calls the same flamegraph path with a
`profile.id` filter and returns the `Flamegraph` as `structured_content`,
registering `ui://signaldb/profile` alongside `ui://signaldb/trace` in
`UI_TOOLS` (`src/mcp-server/src/apps.rs`, `server.rs`) so MCP-Apps-aware
clients render it as an interactive flamegraph and other clients get the
plain JSON. A profile id that doesn't exist for the caller's tenant returns
the same clean "not found" MCP error `get_trace` returns for a missing
trace id — no new error shape introduced.

Rejected alternative: a `search_profiles`-style tool instead of/alongside
`get_profile`. Not needed — the existing `query_ir` MCP tool already finds
profile ids via the `profiles` source's summary fields (shipped by
`query-ir-profiles`); `get_profile` only needs to fetch one payload by id,
same division of labor as `search_traces` (via `query_ir`/dedicated tool)
and `get_trace`.

## Risks / Trade-offs

- [A flamegraph aggregated over a broad, unfiltered time range could be
  very large] → Cap the number of profile rows fed into
  `aggregate_profiles_to_flamegraph` at the same bound the Pyroscope render
  path already uses, and return a truncation flag if the cap is hit — Query
  IR's existing "bounded projection" principle applies to this envelope
  too, not just `rows`/`table`.
- [`get_profile`'s MCP Apps rendering duplicates undocumented behavior
  `get_trace` already has] → Follow the exact same code path
  (`json_result_for_app`, `UI_TOOLS` registration) so there's one rendering
  mechanism, not two; document `get_profile`'s rendering in this change
  even though `get_trace`'s own MCP Apps behavior remains undocumented in
  `mcp-server`'s spec (pre-existing gap, out of scope here).
- [Flamegraph-terminated pipelines silently accepting `aggregate`/`topk`/
  `order` stages that don't apply] → Reject them explicitly at validation
  via the relation-type mechanism, with an error naming the offending stage,
  consistent with the existing "Illegal stage is rejected pre-execution"
  scenario for logs/traces.

## Migration Plan

1. Add the `Flamegraph` envelope type and planner handling
   (additive: existing `rows`/`series`/`table` queries on any source are
   unaffected).
2. Add the `get_profile` MCP tool.
3. Document the raw-SQL payload access and both new retrieval paths.
4. Roll back by removing the envelope variant, planner branch, and MCP tool;
   no data, schema, or existing-query behavior changes to undo.
