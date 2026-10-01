# Proposal

## Why

The `match` stage (IR v12) only sees spans whose start time falls inside the
document's `range`. When a trace straddles the range edge, a `child` or
`descendant` chain that runs through a span outside the range is broken, and
spans that start after `range.to` are never seen. The query then returns
partial witness rows, or misses a trace that would have matched, and the
response looks exactly like a complete answer. `docs/users/querying-ir.md`
already says that only in-range spans are seen. Nothing in the response tells
the caller which of its results this affected. The per-trace span and byte
bounds already turn silent truncation into an explicit outcome. The range edge
is the remaining path where a `match` answer is silently incomplete.

## What Changes

- **IR result change (additive):** a new `QueryWarning` code,
  `match_incomplete_trace`. It is raised on the `rows` and `trace` envelopes of
  a document whose `match` stage declares at least one relation, when at least
  one trace the evaluator buffered has a hierarchy that is visibly broken inside
  the scanned window. A trace counts when:
  - a buffered span names a non-empty `parent_span_id` that no buffered span of
    the same trace carries (a **dangling parent**: the parent started before
    `range.from` or was never ingested), or
  - a buffered span ends after `range.to` (`end_time_unix_nano > range.to`).
    Such a span was still open at the range end, so it can have children that
    start after the range.
- The message gives two counts: matched traces whose witness rows may be
  partial, and evaluated traces that did not match but might have matched over
  a wider range. It also names up to three example trace ids. The result itself
  does not change: the warning is reported next to the result and never
  suppresses or alters rows.
- The querier reports the counts to the router in the existing Flight trailer,
  the one that already carries the `correlate` report. Unknown fields are
  ignored in both directions, so a router and a querier from adjacent releases
  still work together.
- Docs: the `match` semantics bullet about the range and the
  [Warnings](../../../docs/users/querying-ir.md#warnings) section of
  `docs/users/querying-ir.md`, plus the `QueryWarning.code` description in the
  OpenAPI spec and both regenerated clients.

Not breaking: no ingest, Flight ingest schema, WAL, or storage change. The new
trailer field is additive JSON.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `structural-trace-query`: the "Structural span-set matching" requirement
  gains the range-visibility rule and an explicit `match_incomplete_trace`
  warning when an evaluated trace is cut by the range or has a dangling parent.

## Impact

- **querier**: `query/structural_match.rs`. The evaluator computes the two
  conditions per finished trace from columns it already buffers (plus
  `end_time_unix_nano`) and counts traces. `query/ir_planner.rs` passes the
  window end into the evaluator and reads the counts back after `collect()`,
  using the same pattern as the `correlate` row-limit flag.
- **common**: `flight/mod.rs`. The trailer report gains an optional
  `matchIncomplete` member.
- **router**: `endpoints/query.rs` turns the report into the warning. The
  `QueryWarning.code` doc comment and OpenAPI description list the new code.
- **signaldb-sdk / ui**: regenerated clients (doc-string change only). The UI
  already renders every warning generically, so it needs no UI code.
- **docs**: `docs/users/querying-ir.md`.
- No new dependencies.
