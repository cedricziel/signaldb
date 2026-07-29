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

`POST /ui/session` (public) accepts either credential shape as JSON and,
on success, sets an `HttpOnly`, `SameSite=Strict` cookie
(`signaldb_session`). It returns 204 on success, an error status with a
JSON `error` message otherwise.

| Body                                   | Credential           | Cookie contents                                                     |
| -------------------------------------- | -------------------- | ------------------------------------------------------------------- |
| `{"api_key", "tenant", "dataset"?}`    | tenant API key       | the same three values, validated exactly like the headers           |
| `{"email", "password"}`                | user account         | an opaque server-issued session token (prefix `sdbs_`), valid 24h   |

- On requests without an `Authorization` header, the router falls back to
  the cookie.
  - **API-key cookie**: supplies the API key and any tenant/dataset value
    not given as a header. Explicit headers always win over cookie
    values.
  - **User-session cookie** (`sdbs_…`): the token is validated
    server-side; the tenant comes from the user's tenant memberships. If
    the user belongs to exactly one tenant it is used implicitly;
    otherwise `X-Tenant-ID` is required (400) and must name a tenant the
    user is a member of (403 otherwise). Dataset resolution is unchanged
    (`X-Dataset-ID`, else the tenant's default).
- Email/password login answers with a single, uniform 401 (`Invalid email
  or password`) for an unknown email, a wrong password, and a disabled
  account alike.
- `DELETE /ui/session` clears the cookie; for a user-session cookie it
  also revokes the session server-side, so the token stops working
  immediately even if it was copied elsewhere.
- Sessions also end when they expire (24 hours after login) or when the
  user account is disabled.

## Error codes

| HTTP | gRPC                | Meaning                                                                                    |
| ---- | ------------------- | ------------------------------------------------------------------------------------------ |
| 400  | `INVALID_ARGUMENT`  | Header malformed (wrong scheme, invalid tenant/dataset ID)                                 |
| 401  | `UNAUTHENTICATED`   | Credentials missing (no auth header/cookie or no tenant ID), or API key unknown or revoked |
| 403  | `PERMISSION_DENIED` | Key is valid but not authorized for the named tenant or dataset                            |

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

The `signaldb-sdk` Rust crate is a client for this admin API only; it is
not an ingestion or query SDK.

## Tenant self-service API

With a regular tenant API key (the three headers above), the router
exposes tenant-scoped endpoints under `/api/v1` (read-only, plus one
table-creation endpoint):

| Method | Path                                        | Returns                                                                          |
| ------ | ------------------------------------------- | -------------------------------------------------------------------------------- |
| GET    | `/api/v1/whoami`                            | The authenticated tenant (id, slug, name), its datasets, and the default dataset |

For a request authenticated with a **user session**, `/api/v1/whoami` adds
two fields alongside the unchanged tenant/dataset ones: `user`
(`id`, `email`, `display_name`) and `memberships` (one
`{tenant_id, role}` entry per tenant the user belongs to; roles are
`admin`, `member`, or `viewer`). API-key responses do not carry them.
Roles are reported but not yet enforced.
| GET    | `/api/v1/tenants`                           | All configured tenants                                                           |
| GET    | `/api/v1/tenants/{tenant_id}`               | Tenant details                                                                   |
| GET    | `/api/v1/tenants/{tenant_id}/tables`        | The tenant's tables                                                              |
| POST   | `/api/v1/tenants/{tenant_id}/tables/create` | Creates the tenant's signal tables                                               |
| GET    | `/api/v1/tenants/{tenant_id}/schemas`       | The tenant's schema configuration                                                |
| GET    | `/api/v1/schemas/available`                 | Available schema definitions                                                     |
