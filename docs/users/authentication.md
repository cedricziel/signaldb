---
audience: user
type: reference
status: living
sources:
  - src/acceptor/src/middleware/grpc_auth.rs
  - src/acceptor/src/middleware/auth.rs
  - src/router/src/endpoints/tenant.rs
  - src/router/src/endpoints/session.rs
  - src/common/src/auth/session.rs
  - src/common/src/bootstrap.rs
---

# Authentication reference

How clients authenticate to SignalDB: the request headers, the tenant and
dataset concepts behind them, and where API keys come from.

## The three headers

Every authenticated surface uses the same three values. HTTP APIs read
them as headers; gRPC/Flight surfaces read them as request metadata
(lowercase keys).

| HTTP header     | gRPC metadata key | Required | Value                                                                                                     |
| --------------- | ----------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `Authorization` | `authorization`   | yes      | `Bearer <api-key>` (HTTP accepts any casing of the scheme; gRPC/Flight accepts only `Bearer` or `bearer`) |
| `X-Tenant-ID`   | `x-tenant-id`     | yes      | tenant the request acts on                                                                                |
| `X-Dataset-ID`  | `x-dataset-id`    | no       | dataset within the tenant; omitted → the tenant's default dataset                                         |

Tenant and dataset IDs are validated: restricted character set, length
cap, and path-traversal patterns rejected (they become WAL paths and
Iceberg namespaces). Invalid IDs fail with 400 / `INVALID_ARGUMENT`.

## Where the headers are required

| Surface                                   | Port  | Doc                                                                                      |
| ----------------------------------------- | ----- | ---------------------------------------------------------------------------------------- |
| OTLP gRPC ingestion                       | 4317  | [Sending OTLP data](sending-otlp.md)                                                     |
| Prometheus remote_write (`/api/v1/write`) | 4318  | [Prometheus remote_write](prometheus-remote-write.md)                                    |
| Tempo HTTP API (`/tempo/*`)               | 3000  | [Tempo API reference](tempo-api-reference.md)                                            |
| Loki HTTP API (`/loki/*`)                 | 3000  | [LogQL reference](logql-reference.md)                                                    |
| Flight SQL queries                        | 50053 | [Querying with SQL](querying-sql.md); enforced when the operator has enabled Flight auth |
| Tenant self-service API (`/api/v1/*`)     | 3000  | see below                                                                                |

## Browser sessions (embedded UI)

The router's HTTP APIs additionally accept a session cookie in place of
the headers, for browsers using the [embedded explore UI](explore-ui.md):

- `POST /ui/session` (public) takes
  `{"email", "password", "tenant"?, "dataset"?}` as JSON. It verifies the
  password, creates a 12-hour server-side session, and sets an `HttpOnly`,
  `Secure`, `SameSite=Strict` cookie containing only an opaque random
  token. The response lists the user's tenant memberships (with display
  names and roles). `tenant` is optional: a sole membership is
  auto-selected; with several, the response's `tenant` is null and the
  client picks one afterwards (the UI shows a selector). An explicitly
  requested tenant is still validated against membership, and a user with
  no memberships is rejected with 403.
- On requests without an `Authorization` header, the router validates the
  opaque session and resolves `X-Tenant-ID` through the user's memberships.
  The optional `X-Dataset-ID` selects a dataset in that tenant.
- `DELETE /ui/session` revokes the server-side session before clearing the
  cookie. Disabling a user immediately invalidates all of their sessions.
- `GET /api/v1/whoami` returns the human identity, all memberships, and the
  selected tenant's datasets. API-key requests remain supported and omit
  the human identity.

## Error codes

| HTTP | gRPC                | Meaning                                                                                       |
| ---- | ------------------- | --------------------------------------------------------------------------------------------- |
| 400  | `INVALID_ARGUMENT`  | Header malformed (wrong scheme, invalid tenant/dataset ID)                                    |
| 401  | `UNAUTHENTICATED`   | Credentials missing/invalid, or the API key/session is unknown, revoked, expired, or disabled |
| 403  | `PERMISSION_DENIED` | Principal is not authorized for the named tenant or dataset                                   |

## Tenants and datasets

- A **tenant** is the isolation boundary. API keys belong to a tenant;
  storage (WAL directories, Iceberg namespaces) is separated per tenant,
  and queries are pinned to the authenticated tenant.
- A **dataset** partitions data within a tenant (for example
  `production` vs `staging`). Each tenant has a default dataset used when
  `X-Dataset-ID` is omitted.
- In SQL, the tenant slug is the catalog and the dataset slug is the
  schema — see [Querying with SQL](querying-sql.md).

## Getting an API key

**First boot:** when the monolithic `signaldb` binary starts with no
tenants at all (none in `signaldb.toml`, none in the catalog), it
auto-provisions a `default` tenant with a `default` dataset and prints a
fresh API key once in the startup logs. The key is stored hashed and never
shown again; if any tenant already exists, nothing is generated.

Beyond that, tenants, datasets, and API keys are managed by your SignalDB
operator via one of:

| Method        | Where                                                                                                                                                                                                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Static config | `[[auth.tenants]]` blocks in `signaldb.toml`                                                                                                                                                                                                                                         |
| Admin API     | `/api/v1/admin/*` on the router (port 3000), authenticated with `Authorization: Bearer <admin-api-key>`                                                                                                                                                                              |
| CLI           | `signaldb-cli admin tenant\|api-key\|dataset ...` — a client for the admin API (`--url`, default `http://localhost:3000`; `--admin-key` or `SIGNALDB_ADMIN_KEY`; `--no-retry` / `SIGNALDB_NO_RETRY=1` to fail fast on throttling, exit code 4 — see [client retry](client-retry.md)) |

Example (operator-side):

```bash
signaldb-cli --admin-key <admin-key> admin api-key create acme \
  --name "Production Key" --scope traces:write --scope logs:write
```

### API-key scopes

Every API key carries an explicit, non-empty list of scopes chosen at
creation time; a request that names no scopes or an unknown scope is
rejected on every surface (UI, admin/management HTTP API, SDK, CLI, MCP).
The vocabulary is shared:

| Scope                                                           | Grants                                                                    |
| --------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `metrics:write`, `logs:write`, `traces:write`, `profiles:write` | OTLP ingestion of that signal                                             |
| `traces:read`, `logs:read`, `metrics:read`, `profiles:read`     | Query access to that signal (Tempo/Loki/Prometheus/Pyroscope APIs, MCP)   |
| `schema:read`                                                   | Reading the schema registry (registries, attribute/entity/metric lookups) |
| `schema:write`                                                  | Creating, replacing, validating, and deleting custom schema registries    |

Keys may additionally be restricted to one dataset (`--dataset` / `dataset_id`).
Keys defined in `signaldb.toml` (and keys that predate scopes) carry no scope
list and remain unrestricted. Human sessions read the schema with any tenant
role and write it as tenant admin or instance admin.

The scopes and dataset restriction of a live key can be changed without
rotating its secret; the change applies to the key's next request:

```bash
signaldb-cli --admin-key <admin-key> admin api-key update acme <key-id> \
  --scope traces:write --scope schema:read --scope schema:write
```

Over HTTP this is `PATCH /api/v1/admin/tenants/{id}/api-keys/{key_id}` (or
`/api/v1/manage/tenants/{id}/api-keys/{key_id}` for a tenant-admin session)
with a body of `{"scopes": [...], "dataset_id": "..."}`; absent fields are
left untouched, and revoked keys cannot be updated. Listing keys on any
surface shows each key's scopes.

Tenants and datasets created through the Admin API or CLI are usable for
both ingest and query the moment they are created — no service restart and
no matching `[[auth.tenants]]` block in `signaldb.toml` are required. The
querier resolves a tenant's catalog on demand from the tenant registry, so
the first query after creation succeeds. (Config-file tenants remain the
bootstrap seed and are equally first-class.)

Creating a tenant with a `default_dataset` creates that dataset too, so no
separate dataset call is needed to start sending data. Changing a tenant's
`default_dataset` likewise creates the new one; the previous default is
left in place, since it may still hold data.

Bootstrap the first human user directly into the service catalog using the
same configuration file as SignalDB:

```bash
SIGNALDB_USER_PASSWORD='a-long-bootstrap-password' \
  signaldb-cli --config signaldb.toml user create admin@example.com \
  --tenant acme --role admin --instance-admin
```

The `signaldb-sdk` Rust crate is the single client for the whole HTTP API —
admin, management, tenant self-service, and the PromQL/LogQL/TraceQL/Query-IR
query-compat endpoints (SQL is separate, served over Arrow Flight). The CLI
and MCP server are both built on it and expose no capability it doesn't.

## Tenant self-service API

With a regular tenant API key (the three headers above), the router
exposes tenant-scoped endpoints under `/api/v1` (read-only, plus one
table-creation endpoint). Every row below is in the OpenAPI document, so
each is reachable through `signaldb-sdk`, not only raw HTTP:

| Method | Path                                        | Returns                                                                          | SDK operation            | CLI / MCP                                                                      |
| ------ | ------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------ | ------------------------------------------------------------------------------ |
| GET    | `/api/v1/whoami`                            | The authenticated tenant (id, slug, name), its datasets, and the default dataset | `whoami`                 | `signaldb-cli whoami` / `server_info`                                          |
| GET    | `/api/v1/tenants`                           | All configured tenants, filtered to the caller's own                             | `list_tenants_self`      | none — redundant with `whoami`/`server_info`; not exposed separately           |
| GET    | `/api/v1/tenants/{tenant_id}`               | Tenant details                                                                   | `get_tenant_self`        | none — same rationale                                                          |
| GET    | `/api/v1/tenants/{tenant_id}/tables`        | The tenant's provisioned tables, grouped by dataset                              | `list_tenant_tables`     | `signaldb-cli tenant table list` / `tenant_list_tables`                        |
| POST   | `/api/v1/tenants/{tenant_id}/tables/create` | Creates the tenant's signal tables (see below)                                   | `create_tenant_tables`   | `signaldb-cli tenant table provision` / `tenant_create_tables`                 |
| GET    | `/api/v1/tenants/{tenant_id}/schemas`       | The tenant's configured table schema types                                       | `list_tenant_schemas`    | `signaldb-cli tenant table schemas` / `tenant_list_table_schemas`              |
| GET    | `/api/v1/schemas/available`                 | Every table schema type SignalDB can provision                                   | `list_available_schemas` | `signaldb-cli tenant table available-schemas` / `list_available_table_schemas` |

`GET /tenants` and `GET /tenants/{tenant_id}` return only the caller's own
tenant — a single-entry view — so they duplicate what `whoami` already tells
you; they are not given their own CLI command or MCP tool.

This is distinct from the **management API** (`/api/v1/manage/...`,
`manage_*` operations), which requires a human-authenticated session — a
browser session cookie or an OAuth access token with a real per-tenant
role — not a plain API key; see [MCP tenant self-management](mcp.md#tenant-self-management)
for the exact boundary and the two credential types it maps to.

### Creating a tenant's signal tables

`POST /api/v1/tenants/{tenant_id}/tables/create` provisions an Iceberg table
for every signal type enabled for the tenant, across all of its datasets,
before returning `201`. It requires tenant-administrator privileges — in
practice any valid tenant API key, since `can_manage_tenant()` treats API-key
possession as sufficient trust for this endpoint (unlike the management
API above) — and returns `500` if any table could not be created.

You rarely need it: SignalDB provisions those tables on its own, shortly after
a tenant or dataset is created, and a query against a dataset with no tables
yet returns an empty result rather than an error. Use the endpoint (or
`signaldb-cli tenant table provision` / the `tenant_create_tables` MCP tool,
or the web UI's management area) when you want the tables to exist _now_ —
see [Signal table provisioning](../operations/table-provisioning.md).
