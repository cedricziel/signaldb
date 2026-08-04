## Purpose

Defines the single canonical OpenTelemetry-native logical schema that ingest,
storage, query, and every compatibility dialect bind to, and establishes the
logical/physical separation as the property that makes SignalDB "OTel-native".

## ADDED Requirements

### Requirement: Logical schema is the sole nativeness surface

The system SHALL expose one canonical logical schema — organized as resource →
scope → signal, with fields named as dotted OpenTelemetry-native attribute names
— as the only surface any client, query, or compatibility dialect binds to. No
query or dialect surface SHALL expose or accept a physical storage
concept (column name, table split, partition column, ID encoding).

#### Scenario: Physical shape is not observable through any query surface

- **WHEN** a client inspects or queries any signal through the native IR or a
  compatibility dialect (TraceQL/LogQL/PromQL)
- **THEN** it observes only logical dotted OTel-native field names and their
  canonical types, and never a physical column name, table-split, or partition
  artifact

#### Scenario: Compatibility dialects are projections onto the logical schema

- **WHEN** the same underlying data is queried via the native IR and via a
  compatibility dialect
- **THEN** both resolve their field references against the one logical schema,
  and a field's identity and type are the same across surfaces

### Requirement: Attributes are typed AnyValue in the logical schema

Every scalar attribute in the logical schema (resource, scope, and signal-level)
SHALL carry its OpenTelemetry `AnyValue` type and be observable as its typed value
(string, bool, int, double, bytes). An array or key-value-list attribute SHALL be
preserved without loss and **retrievable but not required to be filterable**
(served from the structured residue). Resource-, scope-, and record-level
attributes share one dotted namespace; when a key appears at more than one level,
record-level SHALL shadow scope-level SHALL shadow resource-level, and an explicit
level qualifier SHALL be available to address a shadowed level unambiguously.

#### Scenario: Typed scalar attribute is not stringified

- **WHEN** a record carries an integer attribute (e.g. `http.response.status_code = 200`)
- **THEN** the logical schema presents it as an integer, and a numeric predicate
  (`> 400`) evaluates as a numeric comparison, not a string comparison

#### Scenario: Structured attribute is retrievable, not necessarily filterable

- **WHEN** a record carries an array or key-value-list attribute
- **THEN** the value is retrievable through the logical schema without loss of
  structure; a filter predicate on it MAY be rejected as unsupported rather than
  silently matching nothing

### Requirement: OTLP record metadata is first-class

The logical schema SHALL expose the OTLP record metadata a native store must not
silently discard: `dropped_attributes_count` (and `dropped_events_count`/
`dropped_links_count` for spans), and for logs `severity_number`/`severity_text`,
`observed_timestamp` vs `timestamp`, `trace_flags`, and event name.

#### Scenario: Dropped-attribute count survives

- **WHEN** an SDK sends a record with a non-zero `dropped_attributes_count`
- **THEN** that count is retrievable through the logical schema rather than dropped

### Requirement: Log body is preserved as AnyValue

The logical schema SHALL model a log record's `body` as an OpenTelemetry
`AnyValue`, preserving structured (map/array) bodies rather than collapsing them
to a string.

#### Scenario: Structured log body round-trips

- **WHEN** a log record is ingested with a structured (map or array) `body`
- **THEN** the body is retrievable through the logical schema with its structure
  intact

### Requirement: One metric model in the logical schema

The logical schema SHALL present metrics as a single model carrying metric type,
aggregation temporality, monotonicity, the metric points, exemplars, and
attributes — not as separate per-type surfaces. Any physical split by metric
type SHALL NOT be observable through the logical schema.

#### Scenario: Metric type is a field, not a surface

- **WHEN** a client queries metrics through the logical schema
- **THEN** metric type (gauge/sum/histogram/exponential-histogram/summary) is a
  queryable property of one metric source, and the client does not select a
  per-type table or surface

### Requirement: Cross-signal join keys are first-class

The logical schema SHALL expose cross-signal correlation keys — `trace_id`,
`span_id`, and metric exemplars — as first-class logical concepts, independent of
how each is physically encoded per signal. It MAY additionally expose a
**SignalDB-defined resource identity** computed from an explicit, configurable
subset of resource attributes; this identity SHALL be documented as a
SignalDB construct, not an OTLP-native concept (OTLP defines no canonical resource
identity; that is the OpenTelemetry Entity effort's territory).

#### Scenario: Resource identity is an explicit, configured key set

- **WHEN** resource identity is used as a correlation key
- **THEN** it is derived from a configured resource-attribute subset and documented
  as SignalDB-defined, so two signals only match when they agree on that subset

#### Scenario: Correlation key is uniform across signals despite physical encoding

- **WHEN** the same `trace_id` is referenced for correlation across traces and
  logs, where the two are physically encoded differently
- **THEN** the logical schema presents one logical `trace_id` concept usable as a
  join key without the client handling the physical encoding difference

### Requirement: Physical schema is the declared realization of the logical schema

The physical storage schema SHALL be defined as the realization of the logical
schema, with computed columns, promoted attribute columns, and partition columns
marked as physical-only annotations that carry no logical meaning. The logical
schema and the physical schema SHALL evolve on independent version clocks.

#### Scenario: Physical-only columns carry no logical meaning

- **WHEN** the physical schema adds a computed, promoted, or partition column
- **THEN** the logical schema is unchanged and no client can reference that
  column

#### Scenario: Independent evolution clocks

- **WHEN** the physical schema migrates (e.g. an attribute is promoted or a
  partition scheme changes) without any semantic-convention change
- **THEN** the logical schema version does not change, and existing queries are
  unaffected
