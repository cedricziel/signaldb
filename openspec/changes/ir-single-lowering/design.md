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

In the shipped signature `resolver` is not actually a parameter: the real code
builds it internally, from the _scanned table's_ schema
(`SchemaResolver::new(base.schema(), &source)`), and that is deliberate — a
resolver built from anything other than the schema DataFusion just returned
could disagree with what got promoted, which is exactly the invariant this
change must not weaken.

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

### D6 — `logs.body` becomes filterable for string operators

The harness found that every LogQL line filter (`|=`, `!=`, `|~`, `!~`)
lowers to a predicate on `body`, which `LogicalSchema::core()` marks
`RetrievalOnly`, so `plan_document` rejects the document outright — and
`ql_ir::logql_to_ir` returns `Ok`, so the D5 fallback never sees it.

Retrieval-only was a foundation default from the Layer-2 logical schema
(#1104), not a product decision, and it leaves the IR — our own query surface
— without full-text log search while the Explore UI's logs view still compiles
`|= "…"` through the Loki compat path. The rule is to extend the IR rather
than reach for a compat endpoint, so `body` becomes **filterable**: the planner
already lowers `AnyValue` as `String`, the physical column is `Utf8` (the
JSON-serialised AnyValue), and the old LogQL path already does
`contains(body, …)` on exactly that column. `contains`, `regex`, `eq`, `ne`,
`exists` apply; ordered and numeric operators are rejected by type coercion
as for any string field. `span_events` stays retrieval-only (filter on the
`exception.*` fields instead).

Lands in the §4 PR, before LogQL is routed: `LogicalSchema::core()` and its
tests, the planner test that used `body` as the retrieval-only example (use
`span_events`), `docs/users/querying-ir.md`'s retrieval-only paragraph, and
any generated schema listing that states `"filterable": false` for `body`.

### D7 — An ungrouped LogQL range aggregation groups by the stream identity

LogQL's `count_over_time({…}[5m])` with no outer `by` is one series per
matching *stream*; the old path implemented that with `logs.rs`'s
`SERIES_COLUMNS` (`service_name`, `severity_text`), which is also what
`get_series` reports as a stream. `ql_ir` emitted `by: []`, collapsing to one
series. The old path is right; `ql_ir` is fixed to emit the stream identity
(`service.name`, `severity_text`, as logical names) as the default grouping.

That mapping belongs in `ql-ir`: "what a Loki stream is in SignalDB" is
exactly the kind of decision the crate exists to own, and the SDK/CLI preview
must produce the same document the querier plans. The querier pins the two
against each other — a test asserts `ql-ir`'s default grouping resolves to
`SERIES_COLUMNS` — so they cannot drift.

### D8 — Unscoped attributes take the IR's combining rule

For a bare attribute (`{ .http.method = "GET" }`, `{k8s_namespace="prod"}`)
the old path ORed a match across every container; the IR coalesces across
containers by priority (span/log, then scope, then resource) and compares
once. They differ only when the same key holds *different* values in two
containers. The compat surfaces adopt the IR's rule: Tempo's own unscoped
lookup is span-first-then-resource, Loki has no container concept at all, and
OR-across-containers was the approximation, not the contract. This is the one
deliberate edge-case behaviour change in this change; the harness pins it as
an explained difference until the old path is deleted.

### D9 — LogQL negative matchers keep Loki's absent-matches semantics

Loki (and Prometheus) treat a missing label as the empty string:
`{foo!="bar"}` matches a stream without `foo`, `{foo=""}` matches absent. The
old LogQL path honoured that (`is_null().or(not_eq(…))`); the IR's `not` is
Kleene, where absent satisfies neither `=` nor `not(=)`. The compat surface
promises Loki's semantics, so `ql_ir::logql_to_ir` encodes them explicitly:
`!=` → `or[ne, not(exists)]`, `!~` → `or[not(regex), not(exists)]`, `=""` →
`or[eq "", not(exists)]`. `=~` stays a plain `regex` (a pattern that matches
the empty string would match absent in Loki; that corner is recorded, not
emulated). TraceQL is unaffected: the old TraceQL path lowers equality only,
and Tempo's own semantics for a missing attribute are "no match", which is the
IR's rule.

### D10 — `ir_planner` resolves promoted columns for scope-qualified fields

The harness found `SchemaResolver::column_for` computes the materialised
column name from the scope-qualified logical field (`span.http.method` →
`label_span_http_method`), which no promoted column is ever named, so a
scope-qualified attribute always takes the map-extraction path even when a
promoted column exists. The old path keyed off the bare attribute key, as the
compactor does. Results agree (promotion duplicates the value), so this is a
promotion-invariance and performance gap, not a correctness bug — but it is
the planner's stated claim, so it is fixed in §3 before traces move: strip the
scope the way `Lowering::qualified_attr` already does, and the harness's
row-level pin for this case becomes a plan-level one.

### Findings left as they are

- The old LogQL metric path still has the #1070 mixed-case grouping bug
  (`logs.rs::execute_plan`). Queries `ql-ir` covers move to the planner, which
  has the fix; the D5 fallback set keeps the old path and the bug. Tracked as
  a separate issue filed in §4, fixed when the fallback set is closed.
- `tests-integration/tests/query_parity.rs` carries no query text (it tests
  CLI/MCP operation parity); the corpus premise in D2 was stale and the
  harness draws from the two files that do.

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

## Open Questions — answered

1. **Does the optimized-plan comparison in D2 hold for aggregates?** No.
   `logs.rs`'s `execute_plan` (grouping-column resolution, topk/sort/
   label_replace post-passes, vector-binary joins) and `ir_planner`'s
   `lower_aggregate` build structurally different plans with no basis for
   DataFusion to normalise them onto each other. The metric path instead
   uses the weaker equivalence anticipated here: execute both over an
   identical fixture, sort, and compare rows
   (`querier::query::differential::logql_metric_corpus_row_level_equivalence`).
2. **Does anything depend on `search_filter::to_expr`'s exact expression
   shape?** Only `search_filter.rs`'s own unit tests do (asserting `{:?}`
   output), and they exercise exactly the lowering half task 5.1 deletes
   along with them. Nothing outside the module depends on the shape, so no
   test needed rewriting for this change.

The differential harness (`src/querier/src/query/differential.rs`, landed by
task 2) also surfaced findings beyond these two questions; each has a decision
above (D6–D10, "Findings left as they are"). The full triage table lives in the
harness's module doc.
