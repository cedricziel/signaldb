## Why

The querier lowers a query onto DataFusion **twice, in two unrelated bodies of
code**:

```text
TraceQL ─► traceql::parse ─► search_filter.rs   (411)  ─┐
LogQL   ─► logql::parse   ─► logql.rs           (712)  ─┤
                             logql_metric.rs    (705)  ─┼─► DataFusion
PromQL  ─► promql-parser  ─► promql.rs         (2009)  ─┤
                                                        │
IR doc  ─────────────────────► ir_planner.rs   (5736)  ─┘
```

Both sides implement the same concerns independently: promotion-invariant field
resolution, the materialized-column / attribute-map / JSON-substring fallback,
three-valued absent semantics, identifier quoting. When one is fixed, nothing
makes the other follow.

That is not hypothetical. `ir_planner` carries fixes for a UNION column-ordering
bug (#1348) and a mixed-case identifier 500 (#1070). Whether the compat path has
the equivalents is a question nobody has had to answer, because nothing forces
the comparison.

`ql-ir` now closes the gap that made this impossible: LogQL and TraceQL lower to
IR documents, and `querier/tests/ql_ir_documents_are_planable.rs` pins their
field names against the real `LogicalSchema`. What remains is to _use_ it.

## What Changes

Compat requests become IR documents, planned by the one planner:

```text
TraceQL ─► traceql::parse ─┐
LogQL   ─► logql::parse   ─┼─► ql_ir ─► ir::Document ─► ir_planner ─► DataFusion
IR doc  ──────────────────┘
```

- **`SchemaResolver` and `SourcePlan` become reachable** outside `ir_planner`
  (`pub(crate)`), and the planner grows an entry point that takes a validated
  document and returns a `DataFrame`. Today `IrService` is the only door, and it
  starts from a Flight ticket.
- **Trace search** (`trace.rs`) builds an IR document from `q` instead of
  folding `search_filter::to_expr` per condition. `search_filter.rs` keeps
  `parse_tags` — Tempo's logfmt `tags` parameter is an HTTP encoding, not
  TraceQL — and loses its lowering half.
- **LogQL** (`logql.rs`, `logql_metric.rs`) is replaced by `ql_ir::logql_to_ir`
  for everything `ql-ir` covers, and keeps its existing path for what it does
  not.
- **The compat result assembly is untouched.** Tempo's `SearchResult` and Loki's
  streams/matrix are wire formats; only the plan that feeds them changes.

## What this is not

**Not a PromQL change.** `promql.rs` is the largest file in the table and stays
exactly as it is. We do not own that parser, its lowering targets a `MetricPlan`
rather than expressions directly, and folding it in would double the blast
radius for the least-shared machinery. Revisit once traces and logs have proven
the pattern.

**Not a user-visible change.** Every accepted query stays accepted, every
rejection keeps its status and message, every response keeps its shape. This is
an internal re-plumbing, and the acceptance criterion is that nobody can tell.

**Not a deletion of the old path in one step.** See D3: the old lowering stays
compiled and callable behind a per-signal switch until differential testing says
otherwise.

## Risk, stated plainly

This reroutes a **live, user-facing API** whose behaviour was already changed
once this month (`publishable-ql-crates` reclassified unparseable TraceQL from
501 to 400). The failure mode is not a crash — it is a filter that resolves
differently and returns _more_ or _fewer_ rows while the response still looks
healthy. `ql-ir` was built to refuse rather than approximate for exactly this
reason, but the planner swap is where a silent difference would actually appear.

Hence D2: a differential harness that runs both lowerings over a query corpus
and compares the resulting plans, landing **before** any endpoint moves.

## Capabilities

### Modified Capabilities

- `query-ir-core`: the IR planner becomes the querier's single lowering, so the
  guarantees it already states — promotion-invariant resolution, absent-value
  semantics, physical-name rejection — extend to queries that arrive as TraceQL
  or LogQL rather than as documents.

## Impact

- **Crates**: `querier` (`ir_planner` entry point and visibility; `trace.rs`,
  `logql.rs`, `logql_metric.rs`, `search_filter.rs`), `ql-ir` (gaps found by the
  differential harness), `query-ir` (only if the harness finds an expressiveness
  gap), `tests-integration`.
- **Issues**: none open; file one and link it here.
- **API surfaces**: none intentionally. The change is verified by the absence of
  a difference, not by a new behaviour.
- **Config**: a per-signal switch during rollout (D3), removed when the old path
  is deleted.
