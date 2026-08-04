## Purpose

Defines the metric-native query sub-model over the one logical metric model:
distinct instant/range/scalar relation types, temporality- and histogram-aware
functions computed over OTLP structure, and vector-matching arithmetic — so
metrics join the native IR soundly instead of being forced through a generic
scalar-per-sample stage.

## ADDED Requirements

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
