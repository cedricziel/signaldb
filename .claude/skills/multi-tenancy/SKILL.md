---
name: multi-tenancy
description: SignalDB multi-tenancy and authentication - tenant model, auth flow, isolation layers, slug-based naming, API keys, admin API, and CLI. Use when working with tenant isolation, authentication, API keys, or dataset management.
user-invocable: false
sources:
  - src/common/src/auth/**
  - src/common/src/config/mod.rs
  - src/common/src/ratelimit.rs
  - src/router/src/endpoints/admin.rs
  - src/router/src/endpoints/management.rs
  - src/router/src/endpoints/tenant.rs
  - src/router/src/endpoints/session.rs
  - src/router/src/endpoints/oauth.rs
  - src/router/src/read_scope.rs
  - src/signaldb-cli/src/commands/tenant_self.rs
  - src/mcp-server/src/server.rs
---

# SignalDB Multi-Tenancy & Authentication

## Tenant Model

```
Tenant (e.g., "acme", slug: "acme")
  +-- API Keys (SHA-256 hashed, revocation support)
  +-- Datasets
  |   +-- "production" (slug: "prod", default)
  |   +-- "staging" (slug: "staging")
  +-- Schema Config (optional per-tenant overrides)
```

## Authentication Flow

1. Client sends `Authorization: Bearer <api-key>` + optional `X-Tenant-ID` / `X-Dataset-ID`
2. `Authenticator` hashes key (SHA-256) and checks:
   - Config-based keys first (from `signaldb.toml`)
   - Database-backed keys second (from service catalog)
3. Validates tenant_id matches key's tenant (403 on mismatch)
4. Resolves dataset: explicit header -> tenant default_dataset -> first `is_default` -> 400 error
5. For a database tenant, the resolved dataset must have a `datasets` row — `resolve_database_tenant` fails closed with `403 Dataset '<name>' not found for tenant '<id>'`. Tenant creation and update therefore **materialize the `default_dataset` as a real row**, via `Catalog::upsert_tenant_with_default_dataset` — one transaction, because a tenant row that commits without its dataset row cannot be repaired by a retry (creation 409s on an existing id). Config sync uses the idempotent `Catalog::ensure_dataset`, and `Catalog::backfill_default_datasets` converges tenants created before this at router/monolith boot (#1066). Never use `create_dataset` on a path that may run twice — it is a bare INSERT and errors on a duplicate
6. For a **config**-defined tenant, an explicit `X-Dataset-ID` is first checked against `tenant_config.datasets` (the TOML list). A dataset created at runtime (Admin API/CLI/UI) has no entry there, so on a miss `resolve_config_dataset` falls back to the catalog's `datasets` rows before returning the 403 — a config API key and a database-minted key both resolve a UI-provisioned dataset on a config tenant (`src/common/src/auth/authenticator.rs`)
7. Returns `TenantContext { tenant_id, dataset_id, tenant_slug, dataset_slug }`

### Session-Cookie Fallback (Embedded UI)

When a router HTTP request has no `Authorization` header, `auth_middleware`
falls back to the `signaldb_session` cookie (an opaque random token whose
hash indexes a server-side session; helpers in
`src/common/src/auth/session.rs`). The session identifies the user; the
request's `X-Tenant-ID`/`X-Dataset-ID` headers pick the tenant, validated
against the user's memberships on every request. The cookie is set by
`POST /ui/session` and cleared by `DELETE /ui/session`
(`src/router/src/endpoints/session.rs`), both public routes on the router.
Login `tenant` is optional: the response always lists the user's
memberships (`SessionMembership { tenant_id, name, role }`); a sole
membership is auto-selected, several leave `tenant` null so the UI shows a
picker, none is a 403.

### OAuth 2.1 access tokens (MCP connectors)

A third credential type, for Claude.ai / ChatGPT connectors (change:
mcp-oauth-dcr; router serves the authorization server, see `docs/users/mcp.md`).
An `Authorization: Bearer` whose value starts with `sdb_at_` is an **opaque
OAuth access token**: `auth_middleware` routes it to
`Authenticator::authenticate_oauth_token`, which looks the token up in the
catalog and resolves `(user, tenant, scopes)` **from the token record** — not
from `X-Tenant-ID`, which is ignored for this credential (an OAuth session
cannot be pointed at a tenant it wasn't granted). Tokens are audience-bound to
the configured `mcp.oauth.resource_url` (a token for another resource is
rejected). Tenant is fixed at consent time; one connector per tenant.

**Read scopes.** OAuth scopes populate `TenantContext.api_key_scopes` and are
enforced like API-key write scopes. `can_read(<signal>)` requires the matching
`traces:read` / `logs:read` / `metrics:read` scope; a legacy unscoped key or a
human session is unrestricted. A per-signal guard on the Tempo / Loki /
Prometheus query routers rejects a scoped caller lacking the scope with `403`.
The mirror of the acceptor's `<signal>:write` ingest scopes.

**Schema scopes** (change `schema-registry`). `schema:read` (in `READ_SCOPES`, so
part of the OAuth default grant) gates `/api/v1/schema/*` reads via
`TenantContext::can_read_schema()`; `schema:write` gates custom-registry
create/replace/validate/delete via `can_write_schema()` (sessions additionally
need tenant Admin / instance admin; not OAuth-grantable). `API_KEY_SCOPES` is
the single vocabulary (`validate_scopes()`), used by key creation on every
surface. Bundled registries answer `409` on mutation regardless of scope.

**Management scope** (change `management-api-key-scope`). `tenant:manage`
(`TENANT_MANAGE_SCOPE`, in `API_KEY_SCOPES`, never in `READ_SCOPES` so never
OAuth-grantable) lets an API key call the management API for its own tenant.
`TenantContext::can_manage_via_key()` = API-key principal (`user_id.is_none()`)
whose **explicit** scopes contain it — deliberately not
`has_scope_or_unrestricted`: this is the one scope where a legacy unscoped key
is NOT unrestricted (management is opt-in; widening pre-scope keys silently
would be a security surprise). Human sessions never satisfy it; they go
through membership roles.

**Dataset restriction** (change `multi-dataset-key-restriction`). An API key
or OAuth token may additionally be restricted to a *set* of datasets within
its tenant: `TenantContext.api_key_dataset_ids: Option<Vec<String>>`
(renamed from the single-dataset `api_key_dataset_id`), checked by the shared
`dataset_allowed`/resolution helper in `common::auth` from both
`Authenticator::authenticate_from_database` (API keys) and
`authenticate_oauth_token` (OAuth). `None` = unrestricted (every dataset in
the tenant, unchanged from before this feature); a request naming no dataset
resolves to the restriction's sole element when it has exactly one, or is
rejected (never silently falls through to the tenant default) when it has
two or more. `api_keys.dataset_ids`/`oauth_*.dataset_ids` are JSON-array-in-
TEXT columns (same pattern as `scopes`); `api_keys` additionally dual-writes
the legacy single-value `dataset_id` column so old code keeps working during
a rolling upgrade — OAuth has no such legacy column, so any non-empty OAuth
restriction (not just multi-element) is unsafe until every node runs the new
code. `[auth].dataset_restriction_rollout_complete` (default `false`) gates
the mixed-version-unsafe cases at the request boundary. A dataset-restricted
credential is refused entirely by the management API (`can_manage`/
`authorize_tenant`), regardless of `tenant:manage` or role, and
`discover_datasets`/`tenant_list_tables`/`whoami` filter their dataset
listing to the restriction so an unlisted dataset is never named.

### Error Codes

- **400**: Malformed auth headers (wrong scheme, invalid tenant/dataset ID)
- **401**: Missing credentials (no auth header/cookie or tenant ID), invalid API key, or expired/revoked/wrong-audience OAuth token
- **403**: Key valid but wrong tenant/dataset, or a scoped credential lacking the required `<signal>:read`/`:write` scope, or a management endpoint called without the tenant-admin role / `tenant:manage` scope

## Isolation Layers

| Layer                 | Mechanism                                           |
| --------------------- | --------------------------------------------------- |
| **WAL**               | `{wal_dir}/{tenant_id}/{dataset_id}/{signal_type}/` |
| **Iceberg Namespace** | `[tenant_slug, dataset_slug]`                       |
| **Object Store**      | `{base}/{tenant_slug}/{dataset_slug}/{table}/`      |
| **DataFusion**        | Per-tenant catalog in SessionContext                |
| **Storage Backend**   | Per-dataset storage override                        |

Per-tenant WAL instances are cached and reopened on demand, but the cache is soft-capped (`[wal].max_instances`, default 256); see `docs/operations/wal-persistence.md#instance-cap`.

## Slug-Based Naming

All storage paths and Iceberg identifiers use **slugs** (URL-friendly), not raw IDs.

- `CatalogManager::get_tenant_slug(tenant_id)` -> slug from config, or tenant_id if not found
- `CatalogManager::get_dataset_slug(tenant_id, dataset_id)` -> slug from config, or dataset_id if not found

**Security**: Slugs validated against alphanumeric, hyphen, and underscore pattern. Path traversal (`../`) is checked.

## Configuration

Tenant auth is always enforced on the tenant-facing APIs; there is no
`enabled` flag (removed in #601).

```toml
[auth]
admin_api_key = "sk-admin-key"

[[auth.tenants]]
id = "acme"
slug = "acme"
name = "Acme Corporation"
default_dataset = "production"

[[auth.tenants.datasets]]
id = "production"
slug = "prod"
is_default = true

[[auth.tenants.datasets]]
id = "archive"
slug = "archive"
[auth.tenants.datasets.storage]
dsn = "s3://acme-archive/signals"    # Per-dataset override

[[auth.tenants.api_keys]]
key = "sk-acme-prod-key-123"
name = "Production Key"
```

## Rate Limits & Quotas

`TenantLimits` (`[auth.default_limits]`, overridable per tenant via
`[[auth.tenants]].limits`; resolved by `AuthConfig::limits_for`). Unset
fields mean unlimited; DB-provisioned tenants get the defaults.

| Limit                                                      | Enforced at                                                           | On exceed                                 |
| ---------------------------------------------------------- | --------------------------------------------------------------------- | ----------------------------------------- |
| `max_ingest_requests_per_sec` / `max_ingest_bytes_per_sec` | Acceptor (OTLP gRPC incl. profiles, OTLP/HTTP profiles, remote_write) | 429 / RESOURCE_EXHAUSTED                  |
| `max_query_requests_per_sec`                               | Router HTTP query API (`/tempo`, `/api/v1`)                           | 429                                       |
| `max_api_keys` (active keys only)                          | Admin API key creation                                                | 429 `quota_exceeded`                      |
| `max_datasets`                                             | Admin API dataset creation                                            | 429 `quota_exceeded`                      |
| `max_storage_bytes`                                        | Acceptor (OTLP gRPC incl. profiles, OTLP/HTTP profiles, remote_write) | 429 / RESOURCE_EXHAUSTED `quota_exceeded` |
| `[querier] max_concurrent_queries_per_tenant`              | Querier                                                               | query rejected                            |

Token buckets per dimension (`common::ratelimit::TenantRateLimiter`);
ingest and query budgets are independent. `burst_seconds` (default **10.0**,
minimum 1.0) sets how many seconds of budget a tenant may consume in a
burst — generous by default so an interactive fan-out (an Explore page
load, an MCP investigation running several tools back-to-back) doesn't trip
a freshly configured deployment. Storage quotas
(`common::storage_usage::StorageUsageTracker`) compare cached per-tenant
usage — refreshed from Iceberg manifests every
`[auth].storage_usage_refresh_interval` (default 60s) — against
`max_storage_bytes`; enforcement is eventually consistent by design and
usage is exported as the `signaldb.tenant.storage_usage` gauge.

### The 429 retry contract

Every rate-limit rejection over HTTP — router query budget, admin quotas,
acceptor OTLP/HTTP and Prometheus `remote_write` — carries the same three
headers, computed from the rejected bucket's actual state
(`common::ratelimit::retry_headers`):

- `Retry-After`: whole seconds until the request would be admitted, rounded
  up, never below 1
- `X-RateLimit-Limit`: the per-second budget of the rejected dimension
- `X-RateLimit-Burst`: the burst allowance of the rejected dimension (admin
  count quotas omit this — there is no token bucket to report a burst for)

The router's query 429 body is the standard `ApiError` JSON envelope
(`endpoints::api_error::ApiError::rate_limited`):
`{"status":"error","errorType":"rate_limited","error":"...","retryAfterMs":N}`
— `retryAfterMs` is the same wait as `Retry-After`, in milliseconds.
Rejections increment `signaldb_rate_limit_rejections_total{surface,kind}`
(`surface` ∈ `query | admin | otlp_http | otlp_grpc | prometheus`; `kind` ∈
`query_requests | requests | bytes | quota`) and log one `warn` with
`retry_after_ms`. OTLP/gRPC keeps `RESOURCE_EXHAUSTED` unchanged (no
`retry-after` gRPC trailer — not part of the OTLP contract).

Client side, every SignalDB client honours that contract the same way
(`docs/users/client-retry.md`, fixture `api/retry-cases.json`): the Rust SDK
(`signaldb_sdk::retry`, an `impl ClientHooks for Client` overriding
progenitor's `exec`, the policy carried as the generated client's inner
value; consumers construct via `signaldb_sdk::ClientBuilder`) and the UI's `retryingFetch`
(`src/ui/src/api/http.ts`) retry `429` on any method and `502/503/504`,
connection failures, and timeouts on idempotent methods, waiting
`Retry-After` (else jittered backoff), capped at 10 s per attempt / 30 s per
call / 4 attempts, failing fast when the server asks for more — a `Retry-After`
that carries no header (e.g. a 429 from an upstream `ResourceExhausted`,
mapped by `ApiError::new` rather than `ApiError::rate_limited`) falls back to
jittered backoff, not the minimum-wait guarantee. Exhaustion: CLI prints `rate
limited; server asked to retry in Ns` and exits `4` (`--no-retry` /
`SIGNALDB_NO_RETRY=1` for fail-fast — `main` applies the env var via
`init_no_retry_from_env()` before dynamic shell completion runs, since
completion builds its own client before `Cli::run` gets a chance to);
MCP returns a `throttled:`-prefixed error with `data.retryAfterMs`. Per-call,
MCP additionally bounds the whole retry loop with a total deadline
(`router_timeout + 30s`, distinct from `router_timeout` itself, which only
bounds one attempt) — a call still running past it is `outcome=error`,
`error.type=deadline` (`docs/users/mcp.md`). UI panels show `Rate limited —
server asked to retry in N s` and a shell banner while retries are pending.

## Admin API (Router)

Mounted at `/api/v1/admin`, requires `admin_api_key` (`src/router/src/lib.rs`):

| Endpoint                                           | Methods          | Description                                                                          |
| -------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------ |
| `/api/v1/admin/tenants`                            | GET, POST        | List/create tenants                                                                  |
| `/api/v1/admin/tenants/{id}`                       | GET, PUT, DELETE | Manage a tenant                                                                      |
| `/api/v1/admin/tenants/{id}/api-keys`              | GET, POST        | List/create API keys                                                                 |
| `/api/v1/admin/tenants/{id}/api-keys/{key_id}`     | DELETE, PATCH    | Revoke API key / update its scopes and dataset restriction                           |
| `/api/v1/admin/tenants/{id}/datasets`              | GET, POST        | List/create datasets                                                                 |
| `/api/v1/admin/tenants/{id}/datasets/{dataset_id}` | DELETE           | Delete dataset                                                                       |
| `/api/v1/admin/users`                              | POST             | Create a human user + initial tenant membership (used by `signaldb-cli user create`) |

## Tenant Self-Service API (Router)

Mounted at `/api/v1` with tenant auth — a plain API key is enough
(`src/router/src/endpoints/tenant.rs`, `can_manage_tenant()`-gated for the
mutating one, which treats API-key possession as sufficient trust):

| Endpoint                             | Methods | Description                                                                                                                                                   | SDK operation            |
| ------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `/api/v1/whoami`                     | GET     | Authenticated tenant (id, slug, name) + datasets + default dataset (`endpoints/session.rs`)                                                                   | `whoami`                 |
| `/api/v1/connection`                 | GET     | Public ingest/query endpoints (`[public]` config), headers with the caller's tenant/dataset filled in, required scopes, OTel env vars (`endpoints/session.rs`); MCP `connection_info` | `connection_info`        |
| `/api/v1/tenants`                    | GET     | List tenants visible to the caller — single-entry view of the caller's own tenant; `tenant show` / `tenant_info`                                              | `list_tenants_self`      |
| `/api/v1/tenants/{id}`               | GET     | Tenant details — `tenant show` / `tenant_info`                                                                                                                | `get_tenant_self`        |
| `/api/v1/tenants/{id}/tables`        | GET     | List tenant tables from the Iceberg catalog, grouped by dataset (`dataset` on each `TableInfo`, plus a `datasets` grouping alongside the flat `tables` list)  | `list_tenant_tables`     |
| `/api/v1/tenants/{id}/tables/create` | POST    | Provision the tenant's enabled signal tables across its datasets, before returning `201`. Manual trigger for what the writer's reconciler does on an interval | `create_tenant_tables`   |
| `/api/v1/tenants/{id}/schemas`       | GET     | List the tenant's configured table schema types                                                                                                               | `list_tenant_schemas`    |
| `/api/v1/schemas/available`          | GET     | List every table schema type SignalDB can provision                                                                                                           | `list_available_schemas` |

CLI: `signaldb-cli tenant show`, `signaldb-cli tenant table {list,provision,schemas,available-schemas}`.
MCP: `tenant_info`, `tenant_list_tables`, `tenant_create_tables`,
`tenant_list_table_schemas`, `list_available_table_schemas`.

## Management API (Router) — tenant admin or `tenant:manage` key

Mounted at `/api/v1/manage`, self-service for the caller's own tenant but
**gated differently from the endpoints above**: every handler except
`create_tenant` goes through `can_manage(ctx)` (`authorize_tenant` adds the
path-tenant match): a human principal (`user_id.is_some()` — browser session
or OAuth token) with `can_manage_tenant()` (tenant Admin role or instance
admin), **or** an API key with `can_manage_via_key()` (explicit
`tenant:manage` scope). Ingest-only keys and legacy unscoped keys get `403`
`Tenant administrator role or tenant:manage scope required` (tested:
`ingestion_api_key_cannot_use_human_management_endpoints` in
`endpoints/session.rs` plus `key_scope_authorization_tests` in
`endpoints/management.rs` — positive, legacy-unscoped, cross-tenant, and
OAuth-consent cases). `get_schema` uses the same rule (it used to require
`is_instance_admin`). `create_tenant` stays `is_instance_admin`-only; keys
create tenants through the admin API.

| Endpoint                                        | Methods       | Description                                   | SDK operation                                         |
| ----------------------------------------------- | ------------- | --------------------------------------------- | ----------------------------------------------------- |
| `/api/v1/manage/tenants`                        | POST          | Create a tenant (instance-admin session only) | `manage_create_tenant`                                |
| `/api/v1/manage/tenants/{id}/datasets`          | GET, POST     | List/create datasets                          | `manage_list_datasets`, `manage_create_dataset`       |
| `/api/v1/manage/tenants/{id}/datasets/{name}`   | DELETE        | Delete a dataset by name                      | `manage_delete_dataset`                               |
| `/api/v1/manage/tenants/{id}/api-keys`          | GET, POST     | List/create API keys                          | `manage_list_api_keys`, `manage_create_api_key`       |
| `/api/v1/manage/tenants/{id}/api-keys/{key_id}` | DELETE, PATCH | Revoke / update an API key                    | `manage_revoke_api_key`, `manage_update_api_key`      |
| `/api/v1/manage/tenants/{id}/memberships`       | GET, PUT      | List / upsert a member's role                 | `manage_list_memberships`, `manage_upsert_membership` |
| `/api/v1/manage/tenants/{id}/memberships/{uid}` | DELETE        | Remove a member                               | `manage_remove_membership`                            |
| `/api/v1/manage/schema`                         | GET           | Logical + physical schema                     | `manage_get_schema`                                   |

CLI (`signaldb_cli::commands::tenant_self`, API key with `tenant:manage`):
`tenant dataset {list,create,delete}`, `tenant api-key {list,create,update,revoke}`,
`tenant membership {list,set,remove}`, `tenant schema get`; destructive verbs
take `--yes` or confirm on a TTY. MCP: `tenant_list_datasets`,
`tenant_create_dataset`, `tenant_delete_dataset`, `tenant_list_api_keys`,
`tenant_create_api_key`, `tenant_update_api_key`, `tenant_revoke_api_key`,
`tenant_list_memberships`, `tenant_upsert_membership`,
`tenant_remove_membership`, `tenant_get_schema` (a 403 surfaces the router's
reason via `map_manage_err`). The whole-SDK parity check
(`tests-integration/tests/query_parity.rs`) maps all of them; only the two
OAuth consent endpoints and `manage_create_tenant` stay excluded. E2E:
`tests-integration/tests/tenant_manage_clients.rs`. See `docs/users/mcp.md`
and `docs/users/authentication.md#tenant-management-api`.

## CLI Tool

Subcommands: `query` (one required language flag —
`--sql`/`--promql`/`--logql`/`--traceql`/`--ir`, plus `--trace-id` for a
single trace by ID, and `--start`/`--end`/`--step` on `--promql`/`--logql`
for a range query), `whoami`, `connection` (this deployment's public
ingest/query/mcp endpoints, headers, scopes, and OTel env vars —
`GET /api/v1/connection` / MCP `connection_info`), `discover`, `schema`
(`registry`/`attribute`/`entity`/`metric` lookup with a tenant key holding
`schema:read`), `admin` (`tenant`/`api-key`/`dataset`, plus `schema`
create/replace/delete/validate with a tenant key holding `schema:write`),
`tenant` (`show`, `table`, and — with a `tenant:manage` key — `dataset`,
`api-key`, `membership`, `schema`), `user`, `tui`,
`completions` (static shell scripts; dynamic tenant-ID completion for
tenant-taking args via `COMPLETE=<shell> signaldb-cli` — queries the admin
API like `admin tenant list`, silently empty when the backend is
unreachable).

```bash
signaldb-cli admin tenant list
signaldb-cli admin tenant create acme --name "Acme Corp" [--default-dataset production]
signaldb-cli admin api-key create acme --name "Production Key" --scope traces:write --scope schema:read
signaldb-cli admin api-key update acme <key-id> --scope traces:write --scope schema:write
signaldb-cli admin dataset create acme --name production
signaldb-cli admin schema create --file conventions.yaml --api-key <schema:write key> --tenant-id acme
signaldb-cli schema attribute get k8s.pod.uid --api-key <schema:read key> --tenant-id acme
signaldb-cli tenant table provision --api-key <any tenant key> --tenant-id acme
signaldb-cli whoami --api-key <tenant key> --tenant-id acme
signaldb-cli connection --api-key <tenant key> --tenant-id acme
signaldb-cli query --sql "SELECT ..."   # also --promql/--logql/--traceql/--ir/--trace-id
signaldb-cli tui                         # Interactive terminal UI
```

### User credential primitives (groundwork)

`src/common/src/auth/password.rs` provides the hashing primitives for the
planned user/tenant-membership model (users-tenant-membership ADR):
Argon2id `hash_password`/`verify_password` (PHC strings) for low-entropy
user passwords, and `generate_session_token` (`sdbs_` prefix, 32 OS-random
bytes) + SHA-256 `hash_session_token` for opaque browser-session tokens.
API keys keep the existing fast SHA-256 path — the split is entropy-based.

## Key Implementation Files

| File                                           | Purpose                                                                            |
| ---------------------------------------------- | ---------------------------------------------------------------------------------- |
| `src/common/src/config/mod.rs`                 | Tenant/dataset config structs                                                      |
| `src/common/src/auth/`                         | Authenticator, TenantContext, middleware, validation                               |
| `src/common/src/auth/password.rs`              | Argon2id password hashing + opaque session tokens                                  |
| `src/common/src/catalog_manager.rs`            | Slug resolution                                                                    |
| `src/router/src/endpoints/admin.rs`            | Admin API endpoints (incl. quota checks)                                           |
| `src/router/src/endpoints/management.rs`       | Management API endpoints (tenant admin or `tenant:manage` key; `authorize_tenant`) |
| `src/router/src/endpoints/tenant.rs`           | Tenant self-service API endpoints (API-key-friendly)                               |
| `src/router/src/endpoints/session.rs`          | UI session login/logout + whoami endpoints                                         |
| `src/common/src/auth/session.rs`               | Session cookie codec (`signaldb_session`)                                          |
| `src/common/src/ratelimit.rs`                  | Per-tenant token-bucket rate limiter                                               |
| `src/signaldb-cli/`                            | CLI for tenant management                                                          |
| `src/signaldb-cli/src/commands/tenant_self.rs` | `tenant table` group (only the API-key-friendly surface)                           |
| `src/mcp-server/src/server.rs`                 | MCP tools, incl. platform-admin and `tenant_*` families                            |

Under `[compactor.attr_promotion]` (auto-promotion decision pass), a tenant's resolved materialized-label allowlist is the _pinned_ set: those keys are never demotion candidates.
