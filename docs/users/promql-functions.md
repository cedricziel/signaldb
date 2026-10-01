---
audience: user
type: reference
status: living
sources:
  - src/ql-ir/src/promql_lower.rs
  - src/router/src/endpoints/promql.rs
---

# PromQL function & operator support

What SignalDB's `/prometheus` query API supports today, and what it doesn't
yet. See [Query metrics with PromQL](querying-promql.md) for usage.

SignalDB lowers a parsed PromQL expression to a [Query IR](querying-ir.md)
document over the `metrics` source and executes it on the IR's metric
operators. Each series is evaluated at the instants `start + k·step`: an
instant selector reads the series' latest point in the 5-minute lookback, and a
range function reads the window `[range]` ending at the instant, as in
Prometheus. Anything the IR cannot express is rejected before execution with a
`400 bad_data` naming the construct, never a wrong result (see
[below](#constructs-that-return-400)).

Aggregation operators (`sum`, `avg`, `min`, `max`, `count`, … with or without
`by`/`without`) first reduce each series to its latest sample in the 5-minute
lookback ending at the instant, then aggregate across series, as Prometheus
does. Only the `_over_time` and
range functions fold across time within a series.

## Selectors

| Feature                                                     | Status                                                                          |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Instant vector selector `metric{…}`                         | ✅                                                                              |
| Label matchers `=`, `!=`, `=~`, `!~`                        | ✅ (all four operators on every label)                                          |
| `__name__` matcher                                          | ✅ (`=`, `!=`, `=~`, `!~`; regex patterns are fully anchored, as in Prometheus) |
| Range vector selector `metric[5m]` (as a function argument) | ✅                                                                              |
| `offset` modifier (`metric offset 5m`)                      | ✅                                                                              |
| `@` modifier (`metric @ 1600000000`, `@ start()`/`@ end()`) | ✅ (pins to the instant, 5-min lookback, replicated across steps)               |
| Subqueries `expr[5m:1m]`                                    | ✅ (under an `_over_time` reducer; inner evaluated at the resolution)           |

SignalDB stores metric names in their OTel dotted form (`signaldb.wal.entries_pending`, `process.memory.usage`), which is what `discover_metrics` (the `metric.name` values) and `/prometheus/api/v1/label/__name__/values` return. A dotted name can be used bare — `signaldb.wal.entries_pending`, `process.memory.usage{service_name="signaldb"}`, `rate(signaldb.ingest.spans_received[5m])` — and SignalDB rewrites it to the quoted forms standard PromQL already supports: `{"signaldb.wal.entries_pending"}` or `{__name__="signaldb.wal.entries_pending"}`.

## Aggregation operators

| Operator                                                         | Status |
| ---------------------------------------------------------------- | ------ |
| `sum`, `avg`, `min`, `max`, `count` (with/without `by (…)`)      | ✅     |
| `topk(k, …)`, `bottomk(k, …)` (no `by`/`without`)                | ✅     |
| `stddev`, `stdvar` (population), `group` (with/without `by (…)`) | ✅     |
| `without (…)` grouping                                           | ✅     |
| `quantile(phi, …)` (parameterized, with/without `by (…)`)        | ✅     |
| `count_values`                                                   | ✅     |

## Range (`[range]`) functions

| Function                                                                   | Status                                                                                                                    |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `rate`                                                                     | ✅ (counter delta ÷ range seconds; a drop between consecutive samples counts as a reset, so the result is never negative) |
| `increase`                                                                 | ✅ (counter delta, reset-corrected)                                                                                       |
| `delta`                                                                    | ✅ (gauge delta: last − first, no reset correction)                                                                       |
| `deriv`                                                                    | ✅ (per-second slope via linear regression; needs ≥2 samples)                                                             |
| `irate`, `idelta`                                                          | ✅ (from the last two samples in the window; `irate` is reset-corrected)                                                  |
| `avg_over_time`, `sum_over_time`, `min_over_time`, `max_over_time`         | ✅                                                                                                                        |
| `count_over_time`, `last_over_time`                                        | ✅                                                                                                                        |
| `stddev_over_time`, `stdvar_over_time`                                     | ✅ (population)                                                                                                           |
| `<agg>_over_time` under an outer aggregation, e.g. `sum(avg_over_time(…))` | ✅                                                                                                                        |
| `resets`, `changes`                                                        | ✅                                                                                                                        |
| `present_over_time`                                                        | ✅ (1 per bucket with samples)                                                                                            |
| `quantile_over_time(phi, …)`                                               | ✅ (per-series phi-quantile of the bucket)                                                                                |
| `absent_over_time`                                                         | ✅ (1 at each instant with no sample in the window)                                                                       |

## Histograms

| Function                                                 | Status                                                                                                                                                                                                                                                              |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `histogram_quantile(phi, metric)`                        | ✅ (one quantile per stored series, labelled by its labels less `__name__`; each series' latest point in the 5-minute lookback; interpolated from the OTLP buckets, explicit or exponential; the argument is the histogram metric, not `le`-keyed `_bucket` series) |
| `histogram_quantile(phi, rate(metric[5m]))`              | ✅ (one quantile per stored series, over its per-bucket increase in the window)                                                                                                                                                                                     |
| `histogram_quantile(phi, sum by (…) (rate(metric[5m])))` | ✅ (each series differenced against itself, then merged per `by` group; `sum(metric)` merges the latest points)                                                                                                                                                     |
| `histogram_count`, `histogram_sum`                       | ✅ (sum the stored `count`/`sum` columns, including summary and exponential_histogram rows)                                                                                                                                                                         |
| `histogram_fraction(lower, upper, metric)`               | ✅ (estimated fraction of observations in `(lower, upper]`, interpolated within the buckets the bounds fall in; `-Inf`/`+Inf` bounds take in the open-ended buckets; the same operand shapes as `histogram_quantile`)                                                |

`histogram_quantile` and `histogram_fraction` ignore gauge and sum rows, but a
summary point in the queried range is a `400` (`<function> is not supported on
summary metrics`), where Prometheus would drop the series with an annotation.
It fails the whole query, so narrow the selector with `__name__` when a
selection may hold summaries. Without a `sum`, two series left with one label
set once `__name__` is dropped are a `400`, as in Prometheus. The operands they
reject are listed [below](#constructs-that-return-400).

## Binary operators

| Operator                                                                | Status                                                               |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Arithmetic `+ - * / % ^` with a scalar (`metric * 8`, `1024 / metric`)  | ✅ (drops `__name__`)                                                |
| Comparison `== != > < >= <=` with a scalar (`metric > 5`, `5 < metric`) | ✅ (filters series; with `bool` maps to 1/0 and drops `__name__`)    |
| Nested expressions (`a + b + c`, `(a / b) * 100`)                       | ✅                                                                   |
| Arithmetic `+ - * / % ^` between two vectors (`a / b`)                  | ✅ (one-to-one match on all labels but `__name__`; drops `__name__`) |
| Comparison `== != > < >= <=` between two vectors (`a > b`, with `bool`) | ✅ (one-to-one match; filters `left` or maps to 1/0)                 |
| Logical/set `and`, `or`, `unless`                                       | ✅ (many-to-many allowed)                                            |
| `on` / `ignoring` / `group_left` / `group_right` matching               | ✅ (many-to-many without a group side is a `400`)                    |

## Math & label functions

| Function                                                                                                 | Status                                                                       |
| -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `abs`, `ceil`, `floor`, `round`, `clamp`, `clamp_min`, `clamp_max`                                       | ✅ (drop `__name__`, as in Prometheus)                                       |
| `exp`, `ln`, `log2`, `log10`, `sqrt`, `sgn`                                                              | ✅ (drop `__name__`)                                                         |
| `sort`, `sort_desc`                                                                                      | ✅ (order the output by value)                                               |
| `label_replace`, `label_join`                                                                            | ✅                                                                           |
| `absent`                                                                                                 | ✅ (1 when the selector matches nothing; carries its `=` matchers as labels) |
| `vector`, `scalar`                                                                                       | ✅ (`vector(s)` constant series; `scalar(v)` single-series value)            |
| `timestamp`                                                                                              | ❌ (`400`, see below)                                                        |
| `time`, `day_of_week`, `day_of_month`, `day_of_year`, `days_in_month`, `hour`, `minute`, `month`, `year` | ✅                                                                           |

## Constructs that return 400

These parse as PromQL but have no Query IR equivalent yet. Both
`/api/v1/query` and `/api/v1/query_range` answer `400` with `errorType`
`bad_data` and an `error` naming the construct:

- a range vector as the result (`metric[5m]` outside a range function)
- a string literal as a value
- `timestamp()`, and any function not listed on this page (e.g.
  `predict_linear`, `holt_winters`, `limitk`, trigonometric functions)
- a negative `offset`, and `offset`/`@` on a subquery
- a subquery under anything but an `_over_time` function, and `time()`,
  `vector()` or scalar arithmetic inside a subquery at its own resolution
- `absent_over_time()` over a subquery
- `histogram_quantile`/`histogram_fraction` over `sum without (…)`, over a
  rated subquery, with `offset`/`@`, or per series over a selector matching
  several metric names; `histogram_fraction` with `lower > upper`
- `topk`/`bottomk`/`quantile` with a non-literal parameter, and a quantile
  outside `[0, 1]`
- `fill()` vector-matching modifiers
- a non-finite number (`NaN`, `Inf`) as an operand
- a function over a scalar where it needs a series

Errors raised while executing (a type error such as `rate()` over a gauge, a
many-to-many vector match) are also `400`; a construct the query engine does
not implement yet is `501`.

## Notes

Tracked under epic #328 and #336. The PromQL endpoints and the Query IR share
one metric evaluator. Label names map as in
[Query metrics with PromQL](querying-promql.md#discover-labels-and-series):
`__name__` is the metric name, `job`/`service`/`service_name` the service
name, and any other label a resource or point attribute, so matchers, `by`/
`without`, `on`/`ignoring` and `label_replace`/`label_join` work on every
label.
