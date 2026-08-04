## Purpose

Defines a typed physical substrate for OTLP metrics so the metric-native query
model can compute over structure instead of reconstructing from a `data_json`
blob: bucket-native histograms, exposed exemplar correlation keys, and preserved
temporality/monotonicity across all OTLP metric types.

## ADDED Requirements

### Requirement: Metric points are stored typed, not as an opaque JSON blob

Metric data points SHALL be stored in typed columns preserving OTLP structure, not
as a serialized `data_json` string requiring read-time reconstruction. Value(s),
`aggregation_temporality`, monotonicity, and `start_time_unix_nano` SHALL be typed
fields queryable and computable without parsing a blob.

#### Scenario: Rate reads typed temporality and start time

- **WHEN** a rate is computed over a stored metric series
- **THEN** temporality, monotonicity, and `start_time_unix_nano` are read from
  typed fields, not parsed from a JSON string

### Requirement: Histogram buckets are stored bucket-native

Explicit-bounds histograms SHALL store explicit bounds and counts as typed
list/array columns; exponential histograms SHALL store scale, zero count/threshold,
and positive/negative offsets and bucket counts as typed columns — so quantiles
can be computed over the buckets without deserializing a string.

#### Scenario: Histogram quantile reads typed buckets

- **WHEN** a quantile is computed over an explicit-bounds or exponential histogram
- **THEN** the bounds/counts (or scale/offset/counts) are read from typed columns

### Requirement: Exemplar correlation keys are first-class

An exemplar's `trace_id`, `span_id`, filtered attributes, and timestamp SHALL be
stored as retrievable, joinable fields — not buried in a blob — so metric↔trace
correlation can join on them.

#### Scenario: Metric-to-trace join uses exemplar trace_id

- **WHEN** a query correlates a metric to traces via exemplars
- **THEN** the exemplar's `trace_id`/`span_id` are available as join keys without
  parsing a blob

### Requirement: Summary metrics are passthrough, not recomputable histograms

OTLP Summary metrics carry precomputed client-side quantiles and SHALL be stored
and returned as such. The system SHALL NOT treat a Summary as a bucketed histogram
or claim to recompute an arbitrary `histogram_quantile` from it.

#### Scenario: Summary quantiles are returned as stored, not recomputed

- **WHEN** a Summary metric is queried
- **THEN** its precomputed quantiles are returned as stored, and an arbitrary
  `histogram_quantile` over it is rejected or passed through rather than fabricated
