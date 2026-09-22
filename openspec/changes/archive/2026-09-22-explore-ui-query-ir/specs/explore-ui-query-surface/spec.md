# Explore UI Query Surface Delta Spec

## ADDED Requirements

### Requirement: The Explore UI reads only through the Query IR

Every request the Explore UI makes to read signal data or discovery metadata
(field keys, field values, metric names, profile types) SHALL go to the Query IR
(`POST /api/v1/query` or `GET /api/v1/query/sources`). The UI SHALL NOT call the
`/loki`, `/prometheus`, `/tempo` or `/pyroscope` prefixes.

#### Scenario: No compat call on any tab

- **WHEN** a user opens each of the logs, traces, metrics, profiles, errors and
  catalog tabs, filters, and opens a detail view
- **THEN** every read request the browser sends targets `/api/v1/query*`

#### Scenario: A guard stops regressions

- **WHEN** a hand-written UI request call site (anything outside the generated
  client `src/api/gen/**`, the proxy/service-worker list `lib/proxiedPaths.ts`,
  and the connection-info fixtures that tell users where to point Grafana)
  targets a compat path prefix or calls a generated compat SDK function
- **THEN** the UI test suite fails

### Requirement: Logs keep attribute scopes

The logs tab SHALL show each log's resource, scope and log attributes as
separate groups, as the IR returns them.

#### Scenario: Same key in two scopes

- **WHEN** a log carries `service.name` as a resource attribute and a log
  attribute named `service.name`
- **THEN** the detail view shows both, each under its own scope

### Requirement: Value pickers show approximate discovery honestly

Where `describe values` answers from a sketch or sample, the picker SHALL say
the list may be incomplete and SHALL still accept a typed value.

#### Scenario: Sampled values

- **WHEN** a value picker's `describe` result has a non-exact tier
- **THEN** the picker marks the list as partial and a typed value not in it can
  be applied as a filter

### Requirement: The IR computes counter rates

The IR SHALL compute the per-second rate and the increase of a monotonic counter
per series over each `step`, treating a drop in value as a counter reset, on the
`metrics` and `metrics_histogram` sources.

#### Scenario: Rate across a reset

- **WHEN** a counter series reads 10, 20, 5, 15 at 10s intervals and a rate over
  a 30s step is asked for
- **THEN** the increase is 25 (10 + 5 + 10) and the rate is 25/30 per second

#### Scenario: Matches PromQL on the same data

- **WHEN** the same counter data is queried with PromQL `rate(x[30s])` and the
  IR rate at a 30s step
- **THEN** the values agree within floating-point tolerance

### Requirement: The IR evaluates formulas across queries

One IR request SHALL be able to carry several named queries and formulas over
their `series` results (`+ - * /`, scalar constants, parentheses), joining
series on identical label sets and timestamps.

#### Scenario: Error ratio

- **WHEN** a request holds query `a` (error count by service) and `b` (total
  count by service) and formula `a / b`
- **THEN** the result has one series per service whose points are `a/b`, and a
  service missing from `a` yields no series rather than an error

#### Scenario: Division by zero

- **WHEN** a point of `b` is 0
- **THEN** that point is absent from the formula result, not an error

## REMOVED Requirements

### Requirement: Raw PromQL and LogQL editors in Explore

**Reason**: Explore reads only through the IR; the Query IR tab is the text
escape hatch.
**Migration**: Use the Query IR tab, or Grafana for PromQL/LogQL.
