## Purpose

Lets a client ask, in one query, what distinguishes a selected cohort of
records from the rest of the matched records: the IR's terminal `compare` stage
partitions records by a predicate and contrasts the two cohorts' value
distributions across every attribute, ranked so the most distinguishing fields
come first, so an operator or agent no longer has to guess which dimension to
group by.

## ADDED Requirements

### Requirement: Cohort partitioning by predicate

An IR v4 `compare` stage SHALL be terminal and SHALL carry a `selection`
predicate expressed in the shared predicate grammar over logical field names.
Every record surviving the preceding pipeline stages SHALL belong to exactly one
of two cohorts: the **selection** when the predicate evaluates to `true`, the
**baseline** otherwise (`false` or absent). The `where` stages preceding
`compare` SHALL scope both cohorts identically. The response SHALL report the
record count of each cohort.

#### Scenario: A heatmap box is a selection

- **WHEN** a traces query filters `service.name = checkout` and applies a
  `compare` whose selection is `duration between [800ms, 5s] and timestamp
between [t0, t1]`
- **THEN** the selection cohort holds exactly the checkout spans inside that
  duration and time box and the baseline holds every other checkout span in the
  query range
- **AND** the response reports both cohort counts

#### Scenario: A group is compared to the rest

- **WHEN** the selection predicate is `http.route = "/api/checkout"`
- **THEN** records whose route is that value form the selection and every other
  matched record — including records with no `http.route` — forms the baseline

#### Scenario: Absent evaluates to baseline

- **WHEN** the selection predicate compares a field that is absent from a
  record
- **THEN** that record is in the baseline cohort, consistent with the IR's rule
  that a `where` admits only `true`

#### Scenario: An empty cohort is reported, not an error

- **WHEN** the selection predicate matches no record, or matches every record
- **THEN** the query succeeds, reports the zero-count cohort, and returns
  fields with the empty cohort's shares reported as zero

### Requirement: Comparison across every field in one query

The `compare` stage SHALL accept a `fields` list of logical names or the
wildcard `"*"`. With `"*"` the comparison SHALL cover every logical field and
every attribute key of the source that the registry can enumerate for the
tenant/dataset, across all attribute scopes (resource, scope, record). Query
cost SHALL NOT scale with the number of fields compared beyond a single scan of
the matched records. Fields that are retrieval-only, unresolvable, or above the
server's field-set cap SHALL be omitted from the comparison and named in a
`skipped` list with a reason; they SHALL NOT fail the query.

#### Scenario: Wildcard covers promoted and unpromoted attributes alike

- **WHEN** a `compare` uses `fields: ["*"]` on a dataset where some attributes
  are promoted columns and others live only in attribute maps
- **THEN** both kinds appear in the result with identical semantics, and the
  result does not depend on which are promoted

#### Scenario: Explicit field list is honoured

- **WHEN** a `compare` names `["http.route", "db.system", "duration"]`
- **THEN** exactly those fields are compared, in the ranked order the server
  computes

#### Scenario: Retrieval-only field is skipped with a reason

- **WHEN** a logs `compare` includes `body`, or the wildcard would reach it
- **THEN** `body` is absent from `fields` and present in `skipped` with a
  reason of retrieval-only, and the rest of the comparison is returned

#### Scenario: Attribute-scope prefixes address one container

- **WHEN** a field is named with a scope prefix (`resource.deployment.environment`)
- **THEN** only that container's key is compared, while the unqualified name
  compares the merged view the predicate grammar would filter on

### Requirement: Dimensions report per-value cohort shares

A field whose values are categorical, boolean, or low-cardinality integer SHALL
be reported as a `dimension` carrying, per value, the share of each cohort's
participating records holding that value, the absolute counts, the risk ratio
(selection share ÷ baseline share), and the support (selection share). The
value list SHALL be capped at the request's `maxValues` (server default and
ceiling documented), and when trimmed SHALL keep the values most frequent in
_either_ cohort and state that it was trimmed. Values SHALL be ordered by
baseline frequency descending, so the same field renders stably across
comparisons.

#### Scenario: Shares are proportions, not counts

- **WHEN** the selection holds 100 records and the baseline 100,000, and every
  selection record has `http.route = /a` while 5% of baseline records do
- **THEN** `/a` reports selection share 1.0 and baseline share 0.05 with risk
  ratio 20, and is not dwarfed by the baseline's larger absolute count

#### Scenario: Trimming keeps values prominent in the selection

- **WHEN** a dimension holds more distinct values than `maxValues` and one value
  is common in the selection but rare in the baseline
- **THEN** that value survives the trim and the response marks the field's
  value list as truncated

#### Scenario: Nominal fields do not swamp the result

- **WHEN** a field is near-unique across records (`trace.id`, `span.id`)
- **THEN** it is reported with its per-cohort distinct-count and participation
  only, without a per-value list, and ranks below any field with a repeated
  value

### Requirement: Measures report per-bucket cohort shares

A field whose values are numeric, duration, or timestamp with cardinality above
the dimension threshold SHALL be reported as a `measure` carrying a bucket list
with lower/upper edges and each cohort's share and count per bucket. Bucket
edges SHALL be chosen by the server from the combined value range so both
cohorts share one axis, and the number of buckets SHALL be bounded. Per cohort
the field SHALL also report min, max, and median.

#### Scenario: Both cohorts share one bucket axis

- **WHEN** a numeric attribute is compared and the selection's values are all
  larger than the baseline's
- **THEN** the buckets span the union of both ranges and the selection's mass
  visibly sits in higher buckets than the baseline's

#### Scenario: Duration is a measure with duration semantics

- **WHEN** `duration` is compared on traces
- **THEN** its edges and summaries are integer nanoseconds, coerced and
  encoded like every other duration value in the IR

### Requirement: Participation is reported per field

Every compared field SHALL report participation per cohort: the fraction of that
cohort's records in which the field is present. Presence SHALL follow the IR's
`exists` semantics. Shares within a dimension or measure SHALL be computed over
participating records only, so absence and value distribution are separately
visible.

#### Scenario: Presence itself distinguishes the cohorts

- **WHEN** 98% of selection records carry `db.system` and 10% of baseline
  records do
- **THEN** the field reports participation 0.98 / 0.10 and ranks high even if
  the values among participants are similar

#### Scenario: Zero selection participation sinks, not disappears

- **WHEN** no selection record carries a field that baseline records do carry
- **THEN** the field is still returned, with selection participation 0, and is
  ordered after every field with non-zero selection participation

### Requirement: Fields are ranked by a documented divergence score

Each field SHALL carry a `score` in `[0, 1]`, and `fields` SHALL be ordered by
score descending. The score SHALL combine the Jensen–Shannon divergence between
the two cohorts' distributions over the field's values or buckets with the
divergence of the cohorts' participation, weighted so a field the selection
barely carries cannot outrank one it consistently carries. The statistic SHALL
be named in the response metadata and in the public reference so clients can
re-rank consistently. Ties SHALL break by field name to keep results
deterministic.

#### Scenario: A perfectly separating field ranks first

- **WHEN** one field takes value `A` in every selection record and never in
  the baseline while every other field is distributed alike in both cohorts
- **THEN** that field carries the highest score and appears first

#### Scenario: Identical distributions score zero

- **WHEN** a field's distribution and participation are the same in both
  cohorts
- **THEN** its score is 0 and it appears after every field with a positive
  score

#### Scenario: Ranking is deterministic

- **WHEN** the same comparison is executed twice over unchanged data
- **THEN** the field order and per-field payloads are identical

### Requirement: Bounded and tenant-isolated execution

The server SHALL enforce bounds on a `compare`: a maximum field-set size, a
`maxValues` ceiling, a maximum bucket count for measures, an optional
per-cohort `sample` reservoir the client may request, and the same
window/step guards other terminal stages apply. A document exceeding a bound
SHALL be rejected before execution, naming the bound. Sampling, when requested
or applied, SHALL be stated in the response together with the sampled counts.
The comparison SHALL see only records of the authenticated tenant and dataset.

#### Scenario: Oversized request is rejected pre-execution

- **WHEN** a client requests `maxValues` above the ceiling or a field list
  larger than the cap
- **THEN** the document is rejected during validation with an error naming the
  limit, and no scan runs

#### Scenario: Sampling is visible

- **WHEN** a client sets `sample` and a cohort exceeds it
- **THEN** shares are computed over the sample, and the response states the
  sample sizes alongside the full cohort counts

#### Scenario: Isolation holds

- **WHEN** two tenants submit identical `compare` documents
- **THEN** each response derives only from its own tenant's and dataset's
  records

### Requirement: The `comparison` envelope

A `compare` stage SHALL require the `comparison` result envelope, and the
envelope SHALL be valid only for a pipeline terminating in `compare`. The
payload SHALL carry the resolved window, cohort counts (and sample sizes when
sampled), the ranking statistic name, the ordered `fields` list, and the
`skipped` list. It SHALL be described in the OpenAPI schema so the generated
clients decode one contract, and `fields` (the document-level projection) SHALL
be rejected on a `comparison` result as it is on `series` and `heatmap`.

#### Scenario: Envelope mismatch is rejected

- **WHEN** a document ends in `compare` but declares `table`, or declares
  `comparison` without a terminal `compare`
- **THEN** it is rejected at validation with an envelope-mismatch error

#### Scenario: Stages after compare are rejected

- **WHEN** a document places `order`, `limit`, `topk`, or `aggregate` after
  `compare`
- **THEN** it is rejected at validation naming the offending stage

#### Scenario: Available on every scalar source

- **WHEN** a `compare` is submitted on `logs`, `traces`, `profiles`, or
  `metrics` under IR v4
- **THEN** it validates and executes with the same envelope shape; on `logs`,
  fields derived by a preceding `extract` are comparable like registry fields
