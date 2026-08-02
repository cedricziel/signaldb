## Purpose

Defines the metric-native sub-model for the Query IR: metric relation types
(instant/range/scalar), temporality- and histogram-aware functions, and
vector-matching arithmetic — so metrics join the IR soundly rather than being
forced through a generic scalar-per-sample stage. Stub scope; requirements below
are the headline guarantees, to be expanded once the data model is designed.

## ADDED Requirements

### Requirement: Temporality- and histogram-aware metric queries

The IR SHALL model metrics with their own relation types distinguishing instant
vectors, range vectors, and scalars, and metric functions SHALL respect OTLP
`aggregation_temporality` (cumulative vs delta) and `metric_type` (including
histogram and exponential-histogram) rather than assuming a scalar sample per
series per timestamp. Rate/increase over cumulative series SHALL use the known
reset points; histogram quantiles SHALL be computed across buckets.

#### Scenario: Rate respects temporality

- **WHEN** a rate is computed over a cumulative sum series and over a delta sum
  series
- **THEN** each is computed according to its temporality, not a single
  monotonicity-only heuristic

#### Scenario: Histogram quantile uses buckets

- **WHEN** a quantile is requested over a histogram metric
- **THEN** it is computed across the metric's buckets, not as a scalar aggregate
