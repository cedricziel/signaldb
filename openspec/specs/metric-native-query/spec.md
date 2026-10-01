# metric-native-query Specification

## Purpose

Defines the metric-native query sub-model over the one logical metric model:
distinct instant/range/scalar relation types, temporality- and histogram-aware
functions computed over OTLP structure, and vector-matching arithmetic — so
metrics join the native IR soundly instead of being forced through a generic
scalar-per-sample stage.

## Requirements

### Requirement: Metrics have distinct relation types

The query model SHALL distinguish instant vectors, range vectors, and scalars as
separate relation types for metrics, rather than collapsing them into a single
series shape. A stage that consumes or produces a metric relation SHALL be typed
by which of these it operates on.

#### Scenario: Instant, range, and scalar are not interchangeable

- **WHEN** a query composes a stage expecting a range vector with an input that
  is an instant vector or a scalar
- **THEN** the query is rejected as a type error rather than silently coerced

### Requirement: Rate and increase respect aggregation temporality

Rate/increase over a cumulative series SHALL be computed using the known reset
points (from the series `start_time`) and over a delta series SHALL be computed
from the delta values directly. The computation SHALL depend on OTLP
`aggregation_temporality`, not on a monotonicity-only heuristic. The semantics
SHALL be fixed, not implementation-dependent: samples are ordered by timestamp;
`increase` returns the total accumulated over the range (unnormalized), while
`rate` returns that total divided by the range's elapsed seconds
(per-second-normalized); a detected reset contributes the post-reset value (the
counter is not treated as decreasing); and gaps are spanned by the surrounding
samples within the range without extrapolation beyond it.

#### Scenario: Rate respects temporality

- **WHEN** a rate is computed over a cumulative sum series and over a delta sum
  series
- **THEN** each is computed according to its temporality, using known resets for
  the cumulative case, not a single monotonicity-only heuristic

#### Scenario: Cumulative reset is handled from start_time, not scrape inference

- **WHEN** a cumulative series resets (a new `start_time`)
- **THEN** the reset is recognized from the OTLP start-time boundary rather than
  inferred from a sample-value decrease

#### Scenario: Delta series are summed, not differenced

- **WHEN** `increase` runs over a delta-temporality sum whose points in the
  range are 3, 4 and 5
- **THEN** it returns 12

#### Scenario: A series that starts inside the range counts from zero

- **WHEN** a cumulative series' first point in the range has a `start_time`
  inside the range
- **THEN** that point's full value counts toward `increase`, because the
  counter began within the range

#### Scenario: Series are identified by series identity, not by service

- **WHEN** one service emits two series of one cumulative metric that differ
  only in a point attribute
- **THEN** rate/increase and rate-mode histogram quantiles difference each
  series against itself, never one series against the other

#### Scenario: Rate over a gauge is rejected

- **WHEN** `rate`, `increase` or `irate` reads gauge points or a non-monotonic
  sum
- **THEN** the query is rejected (HTTP 400) with an error naming `delta` or
  `deriv` as the alternative

#### Scenario: rate is per-second, increase is the total

- **WHEN** a counter accumulates 120 over a 60-second range with no reset
- **THEN** `increase` returns 120 and `rate` returns 2 (per second)

### Requirement: Histogram quantiles are computed over OTLP bucket structure

Quantiles over histogram and exponential-histogram metrics SHALL be computed
across the metric's OTLP bucket structure — explicit bounds and counts for
histograms, and scale plus positive/negative/zero buckets for exponential
histograms — not by treating the metric as a scalar aggregate and not by
assuming Prometheus `le`-bucket layout.

#### Scenario: Histogram quantile uses explicit buckets

- **WHEN** a quantile is requested over an explicit-bounds histogram metric
- **THEN** it is computed across the metric's explicit bounds and counts

#### Scenario: Exponential-histogram quantile uses scale and offset buckets

- **WHEN** a quantile is requested over an exponential-histogram metric
- **THEN** it is computed from the metric's scale, zero bucket, and
  positive/negative offset buckets, not from a linear `le`-bucket assumption

#### Scenario: Exponential-histogram merge follows the OTel rule

- **WHEN** exponential-histogram points of different scales or zero
  thresholds are merged for one quantile
- **THEN** buckets are downscaled to the smallest scale and the largest zero
  threshold is used, folding buckets inside it into the zero count, and the
  result is clamped to the recorded `min`/`max` when present

#### Scenario: A merged zero threshold that cuts a bucket absorbs it

- **WHEN** the largest zero threshold of the merged points falls inside a
  populated bucket
- **THEN** the threshold is raised to that bucket's upper boundary and the
  bucket's count is folded into the zero count

### Requirement: Metric series are evaluated at evaluation instants

A metric series SHALL be evaluated at instants `start + k·step` and labelled
with that instant. An instant value SHALL be a series' latest point within the
lookback window ending at the instant (default five minutes); a range operator
SHALL read the window ending at the instant.

#### Scenario: Instant value uses the lookback

- **WHEN** a gauge series has points at 10:00:00 and 10:02:30 and is evaluated
  at 10:04:00 with the default lookback
- **THEN** the value is the 10:02:30 point, labelled 10:04:00

### Requirement: Vector-matching arithmetic between metric series

Binary arithmetic between metric series SHALL support vector matching that aligns
series by a chosen label set (match-on / ignoring) and one-to-many grouping
(group-left / group-right), producing a well-defined output label set. The
matching semantics SHALL be defined independently of any single query dialect's
surface syntax.

#### Scenario: One-to-many match produces defined output labels

- **WHEN** two metric series are combined with a one-to-many vector match over a
  specified label set
- **THEN** the result aligns series by that label set and carries the defined
  output label set, or is rejected when the match is ambiguous

#### Scenario: Many-to-many match is rejected

- **WHEN** both sides of an arithmetic or comparison operation hold more than
  one series for the same match key and no group side is declared
- **THEN** the query is rejected (HTTP 400) rather than pairing series
  arbitrarily

#### Scenario: Set operators allow many-to-many

- **WHEN** `and`, `or` or `unless` combine sides that each hold several series
  per match key
- **THEN** the result keeps or drops series by whether a match exists on the
  other side, without a cardinality error

### Requirement: PromQL is a projection of the metric model

The PromQL dialect SHALL lower to the same metric model and operators as the
native query surface, so one PromQL expression and its equivalent native query
return the same result. There SHALL be no second metric evaluator.

#### Scenario: PromQL and native query agree

- **WHEN** a PromQL expression and the native query it lowers to run over the
  same data
- **THEN** both return the same series, labels and values

### Requirement: Scalar result envelope

A metric query that reduces to a scalar SHALL be returned under a scalar result
envelope distinct from the row/series envelopes, so a client can tell a scalar
result from a single-row series result.

#### Scenario: Scalar result is enveloped as scalar

- **WHEN** a metric query evaluates to a single scalar value
- **THEN** the result is delivered in the scalar envelope, not as a one-row
  series

### Requirement: Metric functions compute over the typed metric substrate

Metric functions SHALL compute over the typed metric substrate (see
`typed-metric-storage`) — typed temporality/monotonicity/start-time fields and
bucket-native histogram columns — not over a serialized `data_json` blob that would
require the read-time reconstruction this change exists to eliminate. These
functions are custom query-engine operators (windowed accumulators for
rate/increase, array operators for histogram quantiles, a label-set join with
cardinality validation for vector matching), not SQL expression lowering.

#### Scenario: Quantile computes over typed buckets, not a blob

- **WHEN** `histogram_quantile` runs over a histogram metric
- **THEN** it reads the typed bucket columns of the metric substrate rather than
  parsing a JSON blob at read time
