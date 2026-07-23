---
name: tempo-api
description: SignalDB Tempo API compatibility - implemented/stub endpoints, query flow, admin API, Grafana native plugin, and built-in Tempo datasource support. Use when working with HTTP API, Grafana integration, or query endpoints.
user-invocable: false
sources:
  - src/router/src/endpoints/tempo.rs
  - src/router/src/endpoints/admin.rs
  - src/grafana-plugin/**
---

# SignalDB Tempo API Compatibility

## Implemented Endpoints (Router :3000)

| Endpoint | Status | Description |
|----------|--------|-------------|
| `GET /tempo/api/echo` | Implemented | Health check |
| `GET /tempo/api/traces/{trace_id}` | Implemented | Single trace lookup -> routes to Querier; `start`/`end` time hints prune the scanned range |
| `GET /tempo/api/v2/traces/{trace_id}` | Implemented | Same handler as v1 for now |
| `GET /tempo/api/search` | Implemented | Trace search with filters -> routes to Querier; `spss` caps spans per span set in the response |
| `GET /tempo/api/search/tags` | Implemented | Static tag set of actually-queryable tags (`service.name`, `name`, `status`) |
| `GET /tempo/api/search/tag/{tag_name}/values` | Implemented | Real data: distinct column values via Flight SQL for supported tags; static values for `status`; 501 for unindexed attribute tags |
| `GET /tempo/api/v2/search/tags` | Implemented | Same tag set, scoped (resource/intrinsic) |
| `GET /tempo/api/v2/search/tag/{tag_name}/values` | Implemented | Same backing as v1 tag values |
| `GET /tempo/api/metrics/query` | 501 Not Implemented | TraceQL metrics not implemented (returns 501 since #552, no fabricated series) |
| `GET /tempo/api/metrics/query_range` | 501 Not Implemented | Same as above |

## Query Flow

1. Client -> Router HTTP API (:3000)
2. Router validates auth (API key -> TenantContext)
3. Router discovers Querier via `QueryExecution` capability
4. Router sends Flight `do_get` ticket to Querier
5. Ticket format: `find_trace:{tenant_slug}:{dataset_slug}:{trace_id}[:{start}:{end}]` (unix-second time hints, appended only when present) or `search_traces:{tenant_slug}:{dataset_slug}:{params}`
6. Querier executes DataFusion SQL against Iceberg tables
7. Results stream back as Arrow RecordBatches (trace not found -> Flight `not_found` status -> HTTP 404)
8. Router formats as Tempo JSON response

## Tempo gRPC Querier Protocol (standalone querier)

The standalone querier serves Tempo's internal `tempopb.Querier` gRPC
protocol on its Flight port (default :50054) so a Tempo query-frontend can
use SignalDB as a querier (`src/querier/src/services/tempo.rs`):

- `FindTraceByID` / `SearchRecent`: implemented, backed by `TraceService`
- Tenant: authenticated `TenantContext` extension wins, else `X-Scope-OrgID`
  header (dataset `default`), else `default`/`default`
- `SearchBlock`: `Unimplemented` (no Tempo block model in SignalDB)
- Tag endpoints: static queryable tag set; tag values empty (HTTP API serves
  real values via Flight SQL)

## Admin API Endpoints

Requires `admin_api_key` from config:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/tenants` | GET/POST | List/create tenants |
| `/api/v1/admin/tenants/{id}` | GET/PUT/DELETE | Manage tenant |
| `/api/v1/admin/tenants/{id}/api-keys` | GET/POST | List/create API keys |
| `/api/v1/admin/tenants/{id}/api-keys/{key_id}` | DELETE | Revoke API key |
| `/api/v1/admin/tenants/{id}/datasets` | GET/POST | List/create datasets |
| `/api/v1/admin/tenants/{id}/datasets/{dataset_id}` | DELETE | Delete dataset |

A separate tenant self-service API is mounted at `/api/v1` (see the
`multi-tenancy` skill).

## Grafana Integration

### Native Datasource Plugin (`src/grafana-plugin/`)

- **Frontend**: TypeScript React-based query/config editors (`@grafana/data`, `@grafana/ui`)
- **Backend**: Rust via `grafana-plugin-sdk`, connects to Router's Flight service (default `http://localhost:50053`)
- **Auth passthrough**: API key, tenant ID, dataset ID from Grafana secure JSON -> Flight headers
- **Signal support**: Traces, metrics, logs query types
- **Arrow conversion**: Direct RecordBatch -> Grafana Frame

### Using Grafana's Built-in Tempo Datasource

The Router's Tempo-compatible endpoints at `/tempo/api/...` work directly with Grafana's Tempo datasource for trace lookup and basic search.

## Key Files

| File | Purpose |
|------|---------|
| `src/router/src/endpoints/tempo.rs` | Tempo API HTTP handlers |
| `src/router/src/endpoints/admin.rs` | Admin API handlers |
| `src/tempo-api/` | Protobuf definitions and Tempo types |
| `src/querier/src/flight.rs` | Query execution, ticket parsing |
| `src/grafana-plugin/` | Native Grafana plugin |
