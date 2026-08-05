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

| Method | Path (under `/tempo`)             | Status      | Notes                                                                                                                                                                                                                                                      |
| ------ | --------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| GET    | `/api/echo`                       | implemented | Returns `echo`; still requires auth headers                                                                                                                                                                                                                |
| GET    | `/api/traces/{trace_id}`          | implemented | Trace by ID; optional `start`/`end` (unix seconds) prune the scanned time range — pass a window bracketing the whole trace                                                                                                                                 |
| GET    | `/api/v2/traces/{trace_id}`       | implemented | Same handler as v1                                                                                                                                                                                                                                         |
| GET    | `/api/search`                     | implemented | Trace search, executed by the querier; `spss` caps spans per span set (`matched` still reports the full count; omitted = all spans)                                                                                                                        |
| GET    | `/api/search/tags`                | implemented | Static list of searchable tags: `service.name`, `name`, `status`                                                                                                                                                                                           |
| GET    | `/api/v2/search/tags`             | implemented | Same tags, grouped into `resource` and `intrinsic` scopes                                                                                                                                                                                                  |
| GET    | `/api/search/tag/{tag}/values`    | partial     | Real distinct values for `service.name` and `name` (also `resource.`/`span.`-scoped forms); static `ok`/`error`/`unset` for `status`; **501** for any other tag. Honors `start`/`end` (unix seconds) — see [Tag value time window](#tag-value-time-window) |
| GET    | `/api/v2/search/tag/{tag}/values` | partial     | Same behavior as v1 (including `start`/`end`), v2 response shape                                                                                                                                                                                           |
| GET    | `/api/metrics/query`              | **501**     | TraceQL metrics not implemented                                                                                                                                                                                                                            |
| GET    | `/api/metrics/query_range`        | **501**     | TraceQL metrics not implemented                                                                                                                                                                                                                            |

## Tag value time window

Tag-value endpoints (`/api/search/tag/{tag}/values` and the v2 variant)
scan only a bounded time window of the traces table:

- `start` and `end` query parameters are unix **seconds**, matching
  Tempo's API. Grafana sends them automatically for tag dropdowns.
- When `end` is absent it defaults to now; when `start` is absent it
  defaults to 24 hours before `end`. A request without either parameter
  therefore returns values seen in the last 24 hours — never a scan of
  all stored data.
- Values that are too large to be unix seconds (typically milliseconds
  from a client that guessed the wrong unit) are rejected with **400**.

Values are deduplicated, sorted, and capped at 1000 per tag.

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
| 501         | Feature not implemented (TraceQL metrics, unindexed tag values)                                                                        |
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
  tables, not Tempo blocks). Tag endpoints advertise the same static tag
  set as the HTTP API; tag _value_ enumeration is HTTP-only.

## Related

- [Grafana datasource options](grafana-datasource.md) — pointing Grafana's
  built-in Tempo datasource at this API.
- [Querying with SQL](querying-sql.md) — the full-capability query path.
