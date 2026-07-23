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

| Method | Path (under `/tempo`) | Status | Notes |
|---|---|---|---|
| GET | `/api/echo` | implemented | Returns `echo`; still requires auth headers |
| GET | `/api/traces/{trace_id}` | implemented | Trace by ID; `start`/`end` params accepted but not applied |
| GET | `/api/v2/traces/{trace_id}` | implemented | Same handler as v1 |
| GET | `/api/search` | implemented | Trace search, executed by the querier |
| GET | `/api/search/tags` | implemented | Static list of searchable tags: `service.name`, `name`, `status` |
| GET | `/api/v2/search/tags` | implemented | Same tags, grouped into `resource` and `intrinsic` scopes |
| GET | `/api/search/tag/{tag}/values` | partial | Real distinct values for `service.name` and `name` (also `resource.`/`span.`-scoped forms); static `ok`/`error`/`unset` for `status`; **501** for any other tag |
| GET | `/api/v2/search/tag/{tag}/values` | partial | Same behavior as v1, v2 response shape |
| GET | `/api/metrics/query` | **501** | TraceQL metrics not implemented |
| GET | `/api/metrics/query_range` | **501** | TraceQL metrics not implemented |

## Error mapping

| HTTP status | Meaning |
|---|---|
| 400 | Invalid search parameters (missing/invalid headers also yield 400 from auth) |
| 401 / 403 | Authentication or authorization failure |
| 404 | Trace not found |
| 429 | Per-tenant query rate limit exceeded |
| 501 | Feature not implemented (TraceQL metrics, unindexed tag values) |
| 503 | No querier service available |
| 504 | Query deadline exceeded |

## Related

- [Grafana datasource options](grafana-datasource.md) — pointing Grafana's
  built-in Tempo datasource at this API.
- [Querying with SQL](querying-sql.md) — the full-capability query path.
