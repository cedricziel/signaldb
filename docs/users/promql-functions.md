---
audience: user
type: reference
status: living
sources:
  - src/querier/src/query/promql.rs
  - src/querier/src/query/metrics.rs
---

# PromQL function & operator support

What SignalDB's `/prometheus` query API supports today, and what it doesn't
yet. See [Query metrics with PromQL](querying-promql.md) for usage.

SignalDB lowers a parsed PromQL expression to a DataFusion plan over the
metrics Iceberg tables (`metrics_gauge`, `metrics_sum`, `metrics_histogram`).
Range aggregations bucket samples with `date_bin(step)` rather than a sliding
window — exact when the query `step` equals the range, an approximation
otherwise. Anything listed as unsupported returns a clear error rather than a
wrong result.

## Selectors

| Feature | Status |
|---------|--------|
| Instant vector selector `metric{…}` | ✅ |
| Label matchers `=`, `!=`, `=~`, `!~` | ✅ (regex on attribute labels: `=`/`!=` only) |
| `__name__` matcher | ✅ |
| Range vector selector `metric[5m]` (as a function argument) | ✅ |
| `offset` modifier (`metric offset 5m`) | ✅ |
| `@` modifier | ❌ |
| Subqueries `expr[5m:1m]` | ❌ |

## Aggregation operators

| Operator | Status |
|----------|--------|
| `sum`, `avg`, `min`, `max`, `count` (with/without `by (…)`) | ✅ |
| `topk(k, …)`, `bottomk(k, …)` (no `by`/`without`) | ✅ |
| `stddev`, `stdvar` (population), `group` (with/without `by (…)`) | ✅ |
| `without (…)` grouping | ✅ (over `job`/`service`; attribute labels aren't materialized) |
| `quantile(phi, …)` (parameterized, with/without `by (…)`) | ✅ |
| `count_values` | ❌ |

## Range (`[range]`) functions

| Function | Status |
|----------|--------|
| `rate` | ✅ (counter delta ÷ range seconds) |
| `increase` | ✅ (counter delta) |
| `delta` | ✅ (gauge delta: last − first, no reset correction) |
| `deriv` | ✅ (per-second slope via linear regression; needs ≥2 samples) |
| `irate`, `idelta` | ✅ (from the last two samples in the window) |
| `avg_over_time`, `sum_over_time`, `min_over_time`, `max_over_time` | ✅ |
| `count_over_time`, `last_over_time` | ✅ |
| `stddev_over_time`, `stdvar_over_time` | ✅ (population) |
| `<agg>_over_time` under an outer aggregation, e.g. `sum(avg_over_time(…))` | ❌ |
| `resets`, `changes` | ❌ |
| `present_over_time` | ✅ (1 per bucket with samples) |
| `absent_over_time`, `quantile_over_time` | ❌ |

## Histograms

| Function | Status |
|----------|--------|
| `histogram_quantile(phi, metric)` | ✅ (interpolated from OTLP buckets; the argument is the histogram metric name, not `le`-keyed `_bucket` series) |
| `histogram_quantile(phi, rate(metric[5m]))` | ❌ |
| `histogram_count`, `histogram_sum` | ✅ (sum the stored count/sum columns) |
| `histogram_fraction` | ❌ |

## Binary operators

| Operator | Status |
|----------|--------|
| Arithmetic `+ - * / % ^` with a scalar (`metric * 8`, `1024 / metric`) | ✅ |
| Comparison `== != > < >= <=` with a scalar (`metric > 5`, `5 < metric`) | ✅ (filters series; with `bool` maps to 1/0) |
| Arithmetic between two vectors | ❌ |
| Comparison between two vectors | ❌ |
| Logical/set `and`, `or`, `unless` | ❌ |
| `on` / `ignoring` / `group_left` / `group_right` matching | ❌ |

## Math & label functions

| Function | Status |
|----------|--------|
| `abs`, `ceil`, `floor`, `round`, `clamp`, `clamp_min`, `clamp_max` | ✅ |
| `exp`, `ln`, `log2`, `log10`, `sqrt`, `sgn` | ✅ |
| `sort`, `sort_desc` | ❌ |
| `label_replace`, `label_join` | ❌ |
| `vector`, `scalar`, `absent` | ❌ |
| `time`, `timestamp`, `day_of_week`, `hour`, … | ❌ |

## Roadmap

Tracked under epic #328 and #336. Unsupported items above are being added
incrementally. Full vector-to-vector binary matching
(`on`/`ignoring`/`group_left`) and subqueries are the larger remaining
efforts.
