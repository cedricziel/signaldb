## Why

The Query IR cannot say "which service called which". Answering that means joining each span to its parent span, and the IR has no join. Today the UI guesses call edges from client-span attributes such as `server.address`, which misses callers entirely and cannot name an instrumented downstream service reliably. A service map (the `service-map` change) and any caller-side view need real parent/child edges, so the join comes first.

`query-cross-signal-correlate` already reserves a `correlate` stage but is a stub with open problems (fan-out across signals, mixed key encodings, time-window bounding against another table). A join within one trace, from a span to its parent, avoids most of those problems: both sides are the same table, the keys share an encoding, and a parent sits in the same trace. This change ships that narrow case first and gives the cross-signal work a tested stage to build on.

## What Changes

- Add a `correlate` pipeline stage to the Query IR (new IR version 8) that joins the current `traces` relation to its parent spans in the same trace, on `trace_id` plus `parent_span_id = span_id`.
- Join kinds `inner` and `left`. A `left` join keeps root spans and spans whose parent is missing from the window.
- Parent-side columns come back under a fixed `parent.` prefix, so both sides' `service_name` stay addressable (`service_name`, `parent.service_name`).
- The joined relation feeds the existing stages: `where` on either side, and `aggregate` grouped by any mix of both sides' fields.
- Validation rejects `correlate` on any source other than `traces`, a second `correlate` in one pipeline, and `correlate` after `aggregate`.
- A fan-out bound: the join is capped by a server-side row limit, reported as a warning when hit, never silently truncated.
- Queries at IR versions 1-7 are unchanged.

## Capabilities

### New Capabilities

- `query-ir-span-join`: the span-to-parent `correlate` stage — its inputs, join kinds, column namespacing, validation rules, and bounds.

### Modified Capabilities

(none — `query-ir-core`'s versioning rules already cover adding a stage in a new IR version)

## Impact

- **query-ir**: new `correlate` stage, relation typing for the joined relation, `parent.` field resolution, validation rules, IR version 8.
- **querier**: lowering to a DataFusion join on the traces table, time-window and row bounds, warning on truncation.
- **router**: OpenAPI schema for the new stage; generated Rust SDK and TypeScript client.
- **docs**: `docs/users/querying-ir.md` section and roadmap update; `query-ir` MCP skill.
- **query-cross-signal-correlate** (stub): builds on this stage instead of defining it from scratch.
- No change to ingest, compatibility APIs, Flight schemas, or on-disk layout. Not breaking.
