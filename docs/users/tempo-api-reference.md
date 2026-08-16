---
audience: user
type: reference
status: living
sources:
  - src/router/src/endpoints/tempo.rs
---

# Tempo API reference

Endpoints of SignalDB's Grafana Tempo-compatible HTTP API, served by the
router at `http://<router-host>:3000/tempo`. Every endpoint requires the
authentication headers described in [Authentication](authentication.md)
(`Authorization: Bearer <key>` and `X-Tenant-ID`; `X-Dataset-ID` optional).

## Endpoints

| Method | Path (under `/tempo`)             | Status      | Notes                                                                                                                                                                                                                                                                                                |
| ------ | --------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| GET    | `/api/echo`                       | implemented | Returns `echo`; still requires auth headers                                                                                                                                                                                                                                                          |
| GET    | `/api/traces/{trace_id}`          | implemented | Trace by ID; optional `start`/`end` (unix seconds) prune the scanned time range — pass a window bracketing the whole trace                                                                                                                                                                           |
| GET    | `/api/v2/traces/{trace_id}`       | implemented | Same handler as v1                                                                                                                                                                                                                                                                                   |
| GET    | `/api/search`                     | implemented | Trace search, executed by the querier; `spss` caps spans per span set (`matched` still reports the full count; omitted = all spans)                                                                                                                                                                  |
| GET    | `/api/search/tags`                | implemented | Attribute keys actually observed in the tenant's traces within the window (resource keys, span keys, and the intrinsics), sorted. Honors `start`/`end` — see [Tag discovery time window](#tag-discovery-time-window)                                                                                 |
| GET    | `/api/v2/search/tags`             | implemented | Same names, grouped into `resource`/`span`/`intrinsic` scopes; `scope` narrows the response to one group                                                                                                                                                                                             |
| GET    | `/api/search/tag/{tag}/values`    | implemented | Distinct values observed in the window for any tag — dedicated columns, map-stored attributes, and the static `status`/`kind` enums alike. An unknown or unobserved tag is `200` with an empty list, never `501`. Honors `start`/`end` — see [Tag discovery time window](#tag-discovery-time-window) |
| GET    | `/api/v2/search/tag/{tag}/values` | implemented | Same behavior as v1 (including `start`/`end`), v2 response shape; scoped names (`resource.x`, `span.x`, `.x`) resolve to the same attribute as their unscoped form                                                                                                                                   |
| GET    | `/api/metrics/query`              | **501**     | TraceQL metrics not implemented                                                                                                                                                                                                                                                                      |
| GET    | `/api/metrics/query_range`        | **501**     | TraceQL metrics not implemented                                                                                                                                                                                                                                                                      |

## Tag discovery time window

The tag-name endpoints (`/api/search/tags`, `/api/v2/search/tags`) and the
tag-value endpoints (`/api/search/tag/{tag}/values` and the v2 variant)
scan only a bounded time window of the traces table — a sampled read, not
an index, so it stays interactive on large tenants (up to 1000 rows per
request):

- `start` and `end` query parameters are unix **seconds**, matching
  Tempo's API. Grafana sends them automatically for tag dropdowns.
- When `end` is absent it defaults to now; when `start` is absent it
  defaults to 1 hour before `end` — the same default lookback as the
  [LogQL](logql-reference.md) label endpoints. A request without either
  parameter therefore returns names/values seen in the last hour, never a
  scan of all stored data. A tenant with no traces in the window still
  lists the intrinsic fields (`name`, `status`, `kind`, `duration`,
  `rootServiceName`, `rootName`) rather than an empty list.
- Values that are too large to be unix seconds (typically milliseconds
  from a client that guessed the wrong unit) are rejected with **400**.

Names and values are deduplicated and sorted; values are capped at 1000
per tag. Because discovery samples rather than indexes, a key or value
that exists only outside the sampled rows can be missed — widen the
window or narrow it with `start`/`end` around when the data was ingested
if a key you know exists doesn't show up.

## Span fields beyond Tempo's

Spanset spans in trace and search responses carry extra optional fields in
addition to Tempo's `spanID`/`startTimeUnixNano`/`durationNanos`/
`attributes`: `name`, `parentSpanID`, `serviceName`, and `status`
(`ok`/`error`/`unset`). They are omitted when unknown, so Tempo-compatible
clients are unaffected; SignalDB's own explore UI uses them to rebuild the
span hierarchy for its waterfall view.

Single-trace responses (`GET /api/traces/{trace_id}`) also include per-span
`events`: an array of `{ name, timeUnixNano, attributes }` objects, omitted
when a span has none. Exceptions follow the OpenTelemetry convention — the
event named `exception`, carrying `exception.message`/`.type`/`.stacktrace`
in its attributes — so a failure recorded on a span is visible in the trace.

Responses additionally carry SignalDB's server-side trace context and stage
timings (`Server-Timing` with `traceparent`, `querier`/`convert`/`total`
`dur` entries, and a `traceresponse` header) — see
[Trace Context on HTTP Responses](response-trace-context.md).

## Error mapping

| HTTP status | Meaning                                                                                                                                |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| 400         | Invalid search parameters, including `start`/`end` values that are not unix seconds (missing/invalid headers also yield 400 from auth) |
| 401 / 403   | Authentication or authorization failure                                                                                                |
| 404         | Trace not found                                                                                                                        |
| 429         | Per-tenant query rate limit exceeded                                                                                                   |
| 501         | Feature not implemented (TraceQL metrics)                                                                                              |
| 503         | No querier service available                                                                                                           |
| 504         | Query deadline exceeded (server-side budget, or the caller's own deadline)                                                             |

A query that runs out of time is always a 504, never a 500 — whether the
querier's `query_timeout` fired or the router's Flight channel deadline did.
A 500 means a genuine server fault.

Error responses are never bodyless: every failure carries a JSON body in
the same shape the [LogQL](logql-reference.md) and PromQL endpoints use,
with the reason in `error` (Grafana's Tempo datasource surfaces it in the
error popup):

```json
{
  "status": "error",
  "errorType": "bad_data",
  "error": "start/end look like unix milliseconds; did you send milliseconds where unix seconds were expected?"
}
```

`errorType` is `bad_data` (400), `not_found` (404), `rate_limited` (429),
`timeout` (504), `unavailable` (503, no querier), `not_implemented` (501),
or `internal` (500). Note this is a JSON envelope where upstream Tempo
returns `text/plain` bodies; the message content is equivalent.

Decoding the querier's Flight response is dictionary-safe: it goes through
`common::flight::decode::flight_data_vec_to_batches` rather than
`arrow_flight::utils::flight_data_to_batches`, so a response containing
dictionary-encoded columns (none of SignalDB's own schemas use one yet)
decodes correctly instead of surfacing as an internal 500.

## Tempo gRPC querier protocol

A standalone querier also serves Tempo's internal `tempopb.Querier` gRPC
protocol on its Flight port (default `50054`), so a Tempo query-frontend
can use SignalDB as a querier backend:

- `FindTraceByID` and `SearchRecent` are fully implemented (including
  `spans_per_span_set`).
- The tenant is taken from Tempo's `X-Scope-OrgID` header (dataset
  `default`), or from the authenticated tenant context when
  `[auth].internal_service_key` is configured. Note that with an internal
  service key set, the port requires SignalDB's internal auth headers,
  which a stock Tempo query-frontend cannot send — run without the key on
  a trusted network for Tempo interop.
- `SearchBlock` returns `Unimplemented` (SignalDB stores data in Iceberg
  tables, not Tempo blocks). Tag endpoints still advertise the old static
  three-name set (`service.name`, `name`, `status`) rather than the
  window-scoped discovery the HTTP API now does — not yet upgraded; tag
  _value_ enumeration remains HTTP-only.

## Related

- [Grafana datasource options](grafana-datasource.md) — pointing Grafana's
  built-in Tempo datasource at this API.
- [Querying with SQL](querying-sql.md) — the full-capability query path.
