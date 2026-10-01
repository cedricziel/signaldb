# explore-ui-query-surface Specification

## Purpose
Guarantees that the Explore UI reads signal data and discovery metadata only
through the Query IR, never through the Loki, Prometheus, Tempo or Pyroscope
compatibility APIs, so the UI is never limited by what a compat dialect can
express.
## Requirements
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
