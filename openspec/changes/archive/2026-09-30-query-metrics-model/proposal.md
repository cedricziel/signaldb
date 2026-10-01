## Why

`query-ir-core` deliberately excludes `metrics` as a source. Metrics do not fit
the flat row-set model the core assumes: samples live in a `data_json` blob with
`metric_type ∈ {gauge,sum,histogram,exponential_histogram,summary}`, carry
`aggregation_temporality` (delta vs cumulative) and `is_monotonic`, and there is
**no `trace_id`** on the metric row. Folding PromQL-style range-vectors into a
generic `range`/`aggregate` stage — as an earlier draft did — is unsound: it
assumes a scalar sample per series per timestamp and keys reset-handling off
`is_monotonic` alone, ignoring temporality; and histograms/summaries are not
scalar at all. This change designs a **metric-native sub-model** for the IR.

> Status: **stub — needs a data model before design.** Depends on
> `query-ir-core`. Relates to the PromQL parity effort (#336) and its raw-sample
> bucket engine.

## What Changes (intended)

- A metric source + metric-native stages that model, correctly:
  - **instant vs range vectors vs scalar** as distinct relation types (the
    PromQL type distinction `query-ir-core` collapses into `series`);
  - **temporality-aware** rate/increase (cumulative with known
    `start_time_unix_nano` resets vs delta sums — not Prometheus scrape-inferred
    resets);
  - **histogram/exponential-histogram** functions (`histogram_quantile`) computed
    over **OTLP** bucket structure (explicit bucket bounds/counts and
    exponential positive/negative + zero buckets — _not_ Prometheus `le` buckets;
    the bucket model itself is an open question below);
  - `binop` with **vector-matching modifiers** (`on()/ignoring()/group_left/
group_right`) — most of real PromQL arithmetic. (Candidate for this change's v1;
    its matching/many-to-many/label-output semantics are unspecified pending
    design — not a committed v1 contract yet.)
  - staleness, `@`, `offset`, subqueries `[5m:1m]` — decide v1 coverage.
- A `scalar` result envelope (deferred from core) and reuse of the existing
  raw-sample bucket engine where it is actually sound.

## Open questions

- Reuse the PromQL bucket engine wholesale vs. a metric relation type layered on
  it. What is the minimum sound v1 (likely gauge/sum rate + `histogram_quantile`)
  and what stays on the PromQL dialect until later.
- `binop` is **owned here** (its hard part — PromQL vector-matching
  `on()/ignoring()/group_left/group_right` — is metric-specific);
  `query-cross-signal-correlate` only provides the shared DAG/sub-pipeline typing.
  Open: exact vector-matching semantics and series alignment (nearest vs
  interpolate).

## Capabilities

### New Capabilities

- `query-metrics-model`: the metric-native source, relation types (instant/range/
  scalar), temporality/histogram-aware functions, vector-matching `binop`, and
  the `scalar` envelope.

## Impact

- **common**: metric relation types + validation.
- **querier**: metric planning over `data_json`, reusing the raw-sample bucket
  engine; temporality-aware reset handling.
- **router/ui/cli/tests-integration**: surface + metric builder + E2E. Additive;
  no on-disk changes expected.
