## Why

The dialects cannot express "the logs for the 10 slowest checkout traces" — it is
a join, and each dialect is single-signal. `query-ir-core` establishes the IR and
its type system; this change adds **cross-signal correlation** as a first-class
`correlate` stage that joins the current relation to another signal. This is one
of the two capabilities no compatibility dialect can offer, and the reason the IR
is worth building.

> Status: **stub** — scope + hard problems captured, not yet designed. Depends on
> `query-ir-core` (the relation type system it composes onto).

## What Changes (intended)

- A `correlate` stage taking a target signal and a join key, lowering to a
  DataFusion `Join`. This makes the IR an explicit **DAG** (a `correlate`/`binop`
  node has sub-pipeline children) — the type system from `query-ir-core` is
  extended to type joins and post-join column namespacing.
- `binop` (series formula) is **owned by `query-metrics-model`**, not this
  change — its hard part is PromQL vector-matching (`on()/ignoring()/group_left`),
  which is metric-specific. This change only shares the DAG/sub-pipeline typing
  that `binop` also relies on.

## Hard problems to solve (from design review — do not skip)

- **Fan-out / cardinality safety.** `trace_id → logs` is one-to-many; an
  unbounded `correlate` can explode (hot trace × winners). Needs per-side limits
  and a documented fan-out model (cf. Malloy symmetric aggregates, ES|QL ENRICH).
- **Time-window bounding of the join.** A correct cross-signal join bounds the
  target scan to each source row's `[start,end]`, not just the outer range;
  `on:"trace_id"` alone is the classic unbounded-correlation footgun.
- **Keys beyond `trace_id`.** `span_id` for span-scoped logs/profiles;
  resource-identity (service + resource attrs) for signals lacking `trace_id`;
  exemplars for metric↔trace (absent from today's schema). Metrics correlation is
  gated on `query-metrics-model` (no `trace_id` on the metric row).
- **Encoding & pushdown.** `traces.trace_id` is `Utf8`, `logs.trace_id` is
  `Binary`; canonicalising on the join key must not wrap the wide side in a
  function that kills bloom/predicate pushdown — canonicalise the narrow
  (winners) side, or store a consistent encoding.
- **Join-kind taxonomy.** `inner`/`semi` are not enough — `anti` ("traces with
  _no_ error logs") and `left` (enrichment) are wanted.
- **Ordering rule.** `aggregate → correlate` must enforce that the join key
  survived the `by` set; post-`correlate` column collisions (both sides have
  `service_name`) need defined namespacing.

## Capabilities

### New Capabilities

- `query-cross-signal-correlate`: the `correlate` join stage — its typing,
  fan-out and time-window semantics, key/encoding rules, and join-kind taxonomy.

## Impact

- **common**: extend the relation type system to joins + namespacing; validation
  of key existence and aggregate-survival.
- **querier**: join lowering with time-window bounding and pushdown-preserving
  key canonicalisation.
- **router/ui/cli/tests-integration**: surface + builder flow + E2E. Additive; no
  on-disk changes expected (correlation is query-time).
