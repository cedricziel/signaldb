---
name: tempo-api
description: SignalDB Tempo API compatibility - implemented/stub endpoints, query flow, admin API, Grafana native plugin, and built-in Tempo datasource support. Use when working with HTTP API, Grafana integration, or query endpoints.
user-invocable: false
sources:
  - src/router/src/endpoints/tempo.rs
  - src/router/src/endpoints/admin.rs
  - src/router/src/endpoints/pyroscope.rs
  - src/querier/src/query/trace.rs
  - src/querier/src/flight.rs
  - src/grafana-plugin/src/**
  - src/grafana-plugin/backend/src/**
---

# SignalDB Tempo API Compatibility

> **Native query surface.** The Tempo/LogQL/Prometheus endpoints below are
> _compatibility dialects_ for Grafana and existing clients. SignalDB also
> exposes a first-party, structured **Query IR** at `POST /api/v1/query`
> (`src/router/src/endpoints/query.rs`) — a versioned JSON query document over
> `logs`/`traces` that the SignalDB UI and CLI build directly, without a dialect
> string. It routes to the querier's `query_ir:` Flight ticket. See
> `docs/users/querying-ir.md`. The dialects are unchanged and sit alongside it.

## Implemented Endpoints (Router :3000)

| Endpoint                                         | Status              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------ | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /tempo/api/echo`                            | Implemented         | Health check                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `GET /tempo/api/traces/{trace_id}`               | Implemented         | Single trace lookup -> routes to Querier; `start`/`end` time hints prune the scanned range                                                                                                                                                                                                                                                                                                                                                                     |
| `GET /tempo/api/v2/traces/{trace_id}`            | Implemented         | Same handler as v1 for now                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `GET /tempo/api/search`                          | Implemented         | Trace search with filters -> routes to Querier; `spss` caps spans per span set in the response. `q` is parsed by the standalone `traceql` crate (syntax only; lowering stays in `querier/query/search_filter.rs`). Rejection class decides the status: input that is not TraceQL (`notbraces`, `{ foo }`, `{ zzz = 1 }`) is **400**; valid TraceQL we do not lower (`\|\|`, `!=`, `=~`, `duration`) is **501**. Nothing outside the subset is silently dropped |
| `GET /tempo/api/search/tags`                     | Implemented         | Attribute keys observed in the window (resource + span, via the querier's `trace_tags` ticket) plus the fixed intrinsics (`name`, `status`, `kind`, `duration`, `rootServiceName`, `rootName`) — real discovery, not a hardcoded list (#1073)                                                                                                                                                                                                                  |
| `GET /tempo/api/search/tag/{tag_name}/values`    | Implemented         | Real data for any tag via the querier's `trace_tag_values` ticket — dedicated columns, map-stored attributes, and the static `status`/`kind` enums alike; an unknown/unobserved tag is `200` with an empty list, never 501. Honors `start`/`end` (unix seconds), defaults to a 1h lookback when absent (matches the Loki metadata endpoints), 400 for millisecond-scale values (#929)                                                                          |
| `GET /tempo/api/v2/search/tags`                  | Implemented         | Same discovery, scoped (`resource`/`span`/`intrinsic`); `scope` query param narrows to one group                                                                                                                                                                                                                                                                                                                                                               |
| `GET /tempo/api/v2/search/tag/{tag_name}/values` | Implemented         | Same backing as v1 tag values (including the `start`/`end` window semantics); scoped names (`resource.x`, `span.x`, `.x`) resolve to the same attribute                                                                                                                                                                                                                                                                                                        |
| `GET /tempo/api/metrics/query`                   | 501 Not Implemented | TraceQL metrics not implemented (returns 501 since #552, no fabricated series)                                                                                                                                                                                                                                                                                                                                                                                 |
| `GET /tempo/api/metrics/query_range`             | 501 Not Implemented | Same as above                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

### Pyroscope-Compatible Profile Query (Router :3000)

`endpoints/pyroscope.rs` serves the profiles signal in the Pyroscope wire
format, mounted at `/pyroscope` and `/api/profiles`: `GET render`,
`GET render-diff`, `GET label-names`, `GET label-values`,
`GET profile-types` (operation ids `pyroscope_render`,
`pyroscope_render_diff`, `pyroscope_label_names`, `pyroscope_label_values`,
`pyroscope_profile_types`), and `GET /api/profiles/trace/{trace_id}`
(`profiles_by_trace`). All six are in the OpenAPI document (tag `profiles`)
and reachable through `signaldb-sdk`, `signaldb-cli profiles
{types,labels,label-values,render,diff,by-trace}`, and the MCP server's
`discover_profile_types`/`search_profiles`/`compare_profiles`/
`profiles_for_trace` tools plus `discover_attributes(signal="profiles")` —
not only raw HTTP. See `docs/users/profiles.md`.

Spanset spans carry optional extras beyond Tempo's shape — `name`,
`parentSpanID`, `serviceName`, `status` (skipped when absent) — populated by
`internal_trace_to_tempo` in `endpoints/tempo.rs`; the explore UI's waterfall
depends on them.

Tag discovery lives in the querier (`TraceService::get_tags`/`get_tag_values`
in `query/trace.rs`), mirroring `LogsService::get_labels`/`get_label_values`:
a bounded sample (1000 rows) of the window's `resource_attributes`/
`span_attributes` documents, unioned with the dedicated-column tags
(`service.name`, `name`) and the fixed intrinsics. The router's handlers
(`endpoints/tempo.rs`) are thin — they build the `trace_tags:`/
`trace_tag_values:` Flight ticket (JSON params: `start`, `end`, `scope` for
tags; `start`, `end` for values) and shape the response; no SQL string
building happens in the router anymore.

## Query Flow

1. Client -> Router HTTP API (:3000)
2. Router validates auth (API key -> TenantContext)
3. Router discovers Querier via `QueryExecution` capability
4. Router sends Flight `do_get` ticket to Querier
5. Ticket format: `find_trace:{tenant_slug}:{dataset_slug}:{trace_id}[:{start}:{end}]` (unix-second time hints, appended only when present) or `search_traces:{tenant_slug}:{dataset_slug}:{params}`
6. Querier executes DataFusion SQL against Iceberg tables. Its session options come from `querier::session_config_from` (`[querier.datafusion]`: `split_file_groups_by_statistics`, `pushdown_filters`, `reorder_filters`, all defaulting to `true`). `split_file_groups_by_statistics` is what lets an ordered scan over attested files drop its sort — the options and optimizer rules in force decide whether a scan's declared ordering survives to the physical plan, so `session_config_from` is `pub` and `tests-integration/tests/querier/declared_order_correctness.rs` plans against it rather than a bare session
7. Results stream back as Arrow RecordBatches (trace not found -> Flight `not_found` status -> HTTP 404; `deadline_exceeded` or `cancelled` -> HTTP 504, never 500)
8. Router formats as Tempo JSON response; errors carry the shared JSON envelope `{"status":"error","errorType":...,"error":<tonic Status message>}` (`ApiError` in `src/router/src/endpoints/api_error.rs`), never an empty body (#921). A `429` (per-tenant query rate limit) additionally carries `retryAfterMs` and the `Retry-After`/`X-RateLimit-Limit`/`X-RateLimit-Burst` headers, via `ApiError::rate_limited`; the SDK, CLI, MCP, and UI clients retry it automatically per `docs/users/client-retry.md`

Responses carry the server span's trace context and stage timings
(`Server-Timing: traceparent;desc="..."` + `traceresponse`; trace lookup adds
`querier`/`convert` `dur` entries via the `ServerTimings` response extension) —
see `docs/users/response-trace-context.md`.

## Tempo gRPC Querier Protocol (standalone querier)

The standalone querier serves Tempo's internal `tempopb.Querier` gRPC
protocol on its Flight port (default :50054) so a Tempo query-frontend can
use SignalDB as a querier (`src/querier/src/services/tempo.rs`):

- `FindTraceByID` / `SearchRecent`: implemented, backed by `TraceService`
- Tenant: authenticated `TenantContext` extension wins, else `X-Scope-OrgID`
  header (dataset `default`), else `default`/`default`
- `SearchBlock`: `Unimplemented` (no Tempo block model in SignalDB)
- Tag endpoints: still the old static three-name set (`service.name`,
  `name`, `status`) and empty tag values — not yet upgraded to the
  querier-backed discovery the HTTP API uses (#1073 only touched the HTTP
  path); a follow-up could route `src/querier/src/services/tempo.rs`
  through the same `TraceService::get_tags`/`get_tag_values`

## Admin API Endpoints

Requires `admin_api_key` from config:

| Endpoint                                           | Method         | Description            |
| -------------------------------------------------- | -------------- | ---------------------- |
| `/api/v1/admin/tenants`                            | GET/POST       | List/create tenants    |
| `/api/v1/admin/tenants/{id}`                       | GET/PUT/DELETE | Manage tenant          |
| `/api/v1/admin/tenants/{id}/api-keys`              | GET/POST       | List/create API keys   |
| `/api/v1/admin/tenants/{id}/api-keys/{key_id}`     | DELETE/PATCH   | Revoke / update scopes |
| `/api/v1/admin/tenants/{id}/datasets`              | GET/POST       | List/create datasets   |
| `/api/v1/admin/tenants/{id}/datasets/{dataset_id}` | DELETE         | Delete dataset         |

Every row above is also in the OpenAPI document, and reachable through
`signaldb-sdk`, the `signaldb-cli admin` group, and the MCP server's
unprefixed platform-admin tools (`list_tenants`, `create_tenant`,
`revoke_api_key`, ...) — not only raw HTTP.

A separate tenant self-service API is mounted at `/api/v1`, and a
management API at `/api/v1/manage` for tenant admins and `tenant:manage` keys (see the
`multi-tenancy` skill for both, including which CLI/MCP surfaces reach
each).

## Grafana Integration

### Native Datasource Plugin (`src/grafana-plugin/`)

- **Frontend**: TypeScript React-based query/config editors (`@grafana/data`, `@grafana/ui`)
- **Backend**: Rust via `grafana-plugin-sdk`, connects to Router's Flight service (default `http://localhost:50053`); standalone cargo workspace (own lockfile/target), built via `npm run build:backend`
- **Auth passthrough**: API key, tenant ID, dataset ID from Grafana secure JSON -> Flight headers
- **Signal support**: Traces, metrics, logs query types
- **Arrow conversion**: Direct RecordBatch -> Grafana Frame

### Using Grafana's Built-in Tempo Datasource

The Router's Tempo-compatible endpoints at `/tempo/api/...` work directly with Grafana's Tempo datasource for trace lookup and basic search.

## Key Files

| File                                | Purpose                                                             |
| ----------------------------------- | ------------------------------------------------------------------- |
| `src/router/src/endpoints/tempo.rs` | Tempo API HTTP handlers                                             |
| `src/router/src/endpoints/admin.rs` | Admin API handlers                                                  |
| `src/tempo-api/`                    | Protobuf definitions and Tempo types                                |
| `src/querier/src/query/trace.rs`    | Trace search/lookup and tag discovery (`get_tags`/`get_tag_values`) |
| `src/querier/src/flight.rs`         | Query execution, ticket parsing                                     |
| `src/grafana-plugin/`               | Native Grafana plugin                                               |
