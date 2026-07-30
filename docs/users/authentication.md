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

Tenants, datasets, and API keys are managed by your SignalDB operator via
one of:

| Method        | Where                                                                                                                                                      |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Static config | `[[auth.tenants]]` blocks in `signaldb.toml`                                                                                                               |
| Admin API     | `/api/v1/admin/*` on the router (port 3000), authenticated with `Authorization: Bearer <admin-api-key>`                                                    |
| CLI           | `signaldb-cli tenant\|api-key\|dataset ...` — a client for the admin API (`--url`, default `http://localhost:3000`; `--admin-key` or `SIGNALDB_ADMIN_KEY`) |

Example (operator-side):

```bash
signaldb-cli --admin-key <admin-key> api-key create acme --name "Production Key"
```

Bootstrap the first human user directly into the service catalog using the
same configuration file as SignalDB:

```bash
SIGNALDB_USER_PASSWORD='a-long-bootstrap-password' \
  signaldb-cli --config signaldb.toml user create admin@example.com \
  --tenant acme --role admin --instance-admin
```

The `signaldb-sdk` Rust crate is a client for this admin API only; it is
not an ingestion or query SDK.

## Tenant self-service API

With a regular tenant API key (the three headers above), the router
exposes tenant-scoped endpoints under `/api/v1` (read-only, plus one
table-creation endpoint):

| Method | Path                                        | Returns                                                                          |
| ------ | ------------------------------------------- | -------------------------------------------------------------------------------- |
| GET    | `/api/v1/whoami`                            | The authenticated tenant (id, slug, name), its datasets, and the default dataset |
| GET    | `/api/v1/tenants`                           | All configured tenants                                                           |
| GET    | `/api/v1/tenants/{tenant_id}`               | Tenant details                                                                   |
| GET    | `/api/v1/tenants/{tenant_id}/tables`        | The tenant's tables                                                              |
| POST   | `/api/v1/tenants/{tenant_id}/tables/create` | Creates the tenant's signal tables                                               |
| GET    | `/api/v1/tenants/{tenant_id}/schemas`       | The tenant's schema configuration                                                |
| GET    | `/api/v1/schemas/available`                 | Available schema definitions                                                     |
