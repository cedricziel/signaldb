---
name: tempo-api
description: SignalDB Tempo API compatibility - implemented/stub endpoints, query flow, admin API, Grafana native plugin, and built-in Tempo datasource support. Use when working with HTTP API, Grafana integration, or query endpoints.
user-invocable: false
---

# SignalDB Tempo API Compatibility

## Implemented Endpoints (Router :3000)

| Endpoint | Status | Description |
|----------|--------|-------------|
| `GET /tempo/api/echo` | Implemented | Health check |
| `GET /tempo/api/traces/{trace_id}` | Implemented | Single trace lookup -> routes to Querier |
| `GET /tempo/api/search` | Implemented | Trace search with filters -> routes to Querier |
| `GET /tempo/api/search/tags` | Stub (empty) | |
| `GET /tempo/api/search/tag/{tag_name}/values` | Stub (empty) | |
| `GET /tempo/api/v2/search/tags` | Stub (empty) | |
| `GET /tempo/api/v2/search/tag/{tag_name}/values` | Stub (empty) | |
| `GET /tempo/api/metrics/query` | Stub (hardcoded) | |
| `GET /tempo/api/metrics/query_range` | Stub (hardcoded) | |

## Query Flow

1. Client -> Router HTTP API (:3000)
2. Router validates auth (API key -> TenantContext)
3. Router discovers Querier via `QueryExecution` capability
4. Router sends Flight `do_get` ticket to Querier
5. Ticket format: `find_trace:{tenant_slug}:{dataset_slug}:{trace_id}` or `search_traces:{tenant_slug}:{dataset_slug}:{params}`
6. Querier executes DataFusion SQL against Iceberg tables
7. Results stream back as Arrow RecordBatches
8. Router formats as Tempo JSON response

## Admin API Endpoints

Requires `admin_api_key` from config:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/admin/tenants` | GET/POST | List/create tenants |
| `/api/v1/admin/tenants/{id}` | GET/PUT/DELETE | Manage tenant |
| `/api/v1/admin/tenants/{id}/api-keys` | GET/POST | List/create API keys |
| `/api/v1/admin/tenants/{id}/datasets` | GET/POST | List/create datasets |

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
| `src/router/src/tempo.rs` | Tempo API HTTP handlers |
| `src/router/src/admin.rs` | Admin API handlers |
| `src/tempo-api/` | Protobuf definitions and Tempo types |
| `src/querier/src/flight.rs` | Query execution, ticket parsing |
| `src/grafana-plugin/` | Native Grafana plugin |
