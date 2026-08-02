## Why

The real power of trace querying is **structural** — matching spans by their
hierarchy ("a descendant Postgres span under a checkout root"). `query-ir-core`
handles flat single-span filtering; this change adds a `match` stage for named
span-sets related by structure. It is called out as its own change **specifically
because the execution engine is an open question that must be prototyped before
committing** — and because the fast path touches on-disk layout.

> Status: **stub — prototype required before design is finalised.** Depends on
> `query-ir-core`.

## What Changes (intended)

- A `match` stage: named span-sets (each a predicate) related by hierarchy
  (`child`, `descendant`, `ancestor`, `sibling`), returning matching traces or
  span-sets, with a `trace` result envelope (deferred from core).

## The engine question (must prototype first — do not pre-commit)

The pre-restructure draft assumed SQL self-join + `RecursiveQuery` over Iceberg.
Review flagged this as possibly the wrong engine:

- **Recursion cost.** Descendant closure via recursive CTE over columnar
  Parquet with no adjacency index can re-scan the trace partition per level —
  pathological on wide/deep traces. Tempo uses a bespoke **per-trace evaluator**
  because whole traces colocate and fit in memory.
- **Bounded depth changes correctness.** A depth cap silently drops deep
  descendants — TraceQL `>>` has no bound. A perf mitigation must not become a
  wrong-answer.
- **Materialised ancestry** (write an `ancestor_ids`/path column at ingest)
  collapses `descendant` to `array_contains(...)` with no recursion — but touches
  the trace schema + writer + an Iceberg migration with rollback.

**Prototype-first task:** benchmark (a) recursive-CTE lowering, (b) a per-trace
evaluator, and (c) materialised-ancestry, on real deep/wide traces from hive,
before choosing. The IR is identical across choices; only the plan/strategy
differs.

## Parity scope to decide

Named-span-sets + relations is a _subset_ of TraceQL. Decide v1 coverage for:
spanset quantitative conditions (`count() > 3` over a spanset), aggregate-over-
spanset (`avg(duration)` within a trace), `select()` attribute projection,
spanset union, negated structural ops (`!>>`), mixing `&&`/`||` with structure,
and matching on `events`/`links` (nested list columns in the trace row).

## Capabilities

### New Capabilities

- `query-structural-traces`: the `match` stage, the `trace` result envelope, the
  chosen structural-execution strategy, and the decided TraceQL-parity scope.

## Impact

- **querier**: `match` lowering + the chosen engine. **Possibly writer +
  common/schema + an Iceberg migration** if materialised ancestry is chosen
  (BREAKING-adjacent; needs rollback) — hence its own change.
- **router/ui/cli/tests-integration**: surface + waterfall renderer + E2E.
