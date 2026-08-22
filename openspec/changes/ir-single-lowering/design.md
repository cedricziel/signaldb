## Context

`publishable-ql-crates` established that parsing is separable from lowering, and
`ql-ir` proved the lowering can target an IR document. This change is the third
step: making the IR document the thing the querier actually plans.

**Constraint:** FDAP version alignment is unaffected — the planner already uses
DataFusion's re-exported Arrow types and keeps doing so.

**Constraint:** no Flight wire schema, WAL, or Iceberg layout changes. There is
nothing to migrate and nothing to roll back beyond a revert.

**Constraint:** the compat wire formats are fixed. Tempo's `SearchResult` and
Loki's stream/matrix shapes are what Grafana parses; only the plan feeding them
moves.

## Goals / Non-Goals

**Goals**

- One lowering from a resolved field to a DataFusion expression.
- Compat behaviour identical, demonstrated rather than asserted.
- Delete the duplicated lowering once that is demonstrated.

**Non-Goals**

- PromQL (proposal, "What this is not").
- Extending what the compat APIs accept. A construct `ql-ir` refuses keeps
  whatever the old path did with it until it is deliberately given an IR form.
- Changing result envelopes or projections.

## Decisions

### D1 — The seam is a validated document plus a planner entry point

`ir_planner` exposes exactly one new door:

```rust
pub(crate) fn plan_document(
    ctx: &SessionContext,
    doc: &Document,
    resolver: &dyn FieldResolver,
    // tenant/dataset scoping as the existing path takes it
) -> Result<DataFrame, QuerierError>
```

`IrService` keeps its Flight-ticket entry and calls the same function, so there
is one planner rather than a planner and a compat-planner.

**Rejected: exposing `lower_predicate` alone.** It is a method needing
`col_of`, `derived_types` and `resolver`, so a caller would have to assemble
planner internals to use it. That is the coupling this change exists to remove,
re-created at a different address.

**Rejected: keeping `SchemaResolver` private and duplicating it.** A second
resolver is a second place for promotion-invariance to be wrong.

### D2 — Differential testing lands before any endpoint moves

A harness runs a corpus through both lowerings and compares the resulting plans:

```text
query ──► old lowering ──► Expr ──┐
     └──► ql_ir ─► document ──────┴──► compare
                   ─► ir_planner ─► Expr
```

Comparison is on the **optimized logical plan**, not the raw expression tree:
the two paths legitimately build different-shaped expressions that DataFusion
normalises to the same plan, and comparing before optimisation would fail on
differences that do not exist at execution.

The corpus is seeded from three places, in this order of value:

1. Every query in the existing compat tests (`router_tempo_endpoints.rs`,
   `logql_queries.rs`, `query_parity.rs`) — these already encode behaviour
   somebody cared about.
2. The `ql-ir` test corpora.
3. Hand-written adversarial cases: promoted vs unpromoted attributes, mixed-case
   labels (#1070), absent values, an attribute whose name collides with a
   physical column.

A difference is a finding to explain, not a failure to route around — and not
automatically a bug. Three outcomes are possible, and the harness cannot tell
them apart on its own:

1. **One lowering is wrong.** Fix it; this is the case the harness exists for.
2. **Both are correct and the plans differ anyway** — DataFusion does not
   normalise every equivalent form. Record why, and pin the pair so the
   exception is deliberate rather than a quietly weakened comparison.
3. **They mean genuinely different things**, and which is right is a product
   question about what the compat surface promises.

What must not happen is loosening the comparison until it passes. Every
difference gets one of those three answers in writing before the surface
moves.

### D3 — Per-signal switch, old path stays callable

Traces and logs move independently, each behind a config switch defaulting to
the old path. The old lowering is deleted only after its signal has run on the
new path with differential testing green.

This is deliberately more ceremony than a straight swap. The justification is
the failure mode: a wrong filter returns a plausible number of rows, and no
alert fires. A switch means the answer to "did this break search?" is a config
change rather than a revert-and-redeploy.

**The switch is temporary and its removal is a task in this change**, not a
permanent knob. A configuration option that outlives its rollout becomes a
second code path nobody tests.

### D4 — Tempo `tags` stays out of it

`parse_tags` handles Tempo's logfmt `tags` parameter, which is an HTTP
parameter encoding that happens to produce `traceql::Condition` values. It has
no TraceQL text to lower. It keeps producing conditions, which are lowered by
whichever path is active — so it needs a `Condition`-to-IR shim rather than a
text one.

That shim lives in the querier, not in `ql-ir`: `ql-ir` lowers _languages_, and
`tags` is not one.

### D5 — What `ql-ir` refuses keeps its current behaviour

`ql-ir` refuses cross-series arithmetic, `without` grouping, `topk`/`bottomk` as
vector aggregations, `ip()`, `unwrap` and `irate`. Some of those the old LogQL
path _does_ support today.

A refusal from `ql-ir` therefore falls back to the old lowering rather than
becoming a user-visible rejection. Turning a working query into a 501 would be
a regression dressed as a refactor.

This means the old LogQL path cannot be deleted wholesale — only the portion
`ql-ir` covers. Closing the remainder is follow-up work, and D6 of
`publishable-ql-crates` describes what the IR would need.

## Risks / Trade-offs

| Risk                                                             | Mitigation                                                                    |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| A filter resolves differently and silently changes result counts | D2 differential harness on optimized plans, landing before any endpoint moves |
| The IR cannot express something the old path did                 | D5: refusal falls back rather than rejecting; the harness surfaces the set    |
| Making planner internals `pub(crate)` invites unrelated coupling | One entry point (D1), not a set of exposed helpers                            |
| The rollout switch becomes permanent                             | Its removal is a task here, not a follow-up                                   |
| Reviewing a large diff at once                                   | Traces first, logs second, deletion third — each independently revertible     |

## Migration Plan

No data migration. Rollout is the per-signal switch (D3); rollback is flipping
it. After deletion, rollback is an ordinary revert.

## Open Questions

1. **Does the optimized-plan comparison in D2 hold for aggregates**, or only for
   filters? Filters are the bulk of the duplication and the obvious starting
   point; if aggregate plans differ structurally in ways DataFusion does not
   normalise, the harness may need a weaker equivalence (row-level results over
   a fixture dataset) for the metric path.
2. **Does anything depend on `search_filter::to_expr`'s exact expression shape**
   — a test asserting `Debug` output, for instance? Those pass today by
   construction and would need rewriting against behaviour rather than shape.
