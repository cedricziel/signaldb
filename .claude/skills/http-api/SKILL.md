---
name: http-api
description: SignalDB HTTP API design rules - versioned /api/v{N} root, resource-oriented paths, per-endpoint privilege checks (never prefix-based access control), hypermedia links for discoverability, error/timestamp/pagination conventions, and OpenAPI as the contract. Use when adding, changing, or reviewing any router HTTP endpoint or its clients.
user-invocable: false
sources:
  - src/router/src/lib.rs
  - src/router/src/openapi.rs
  - src/router/src/endpoints/api_error.rs
  - src/router/src/read_scope.rs
  - src/common/src/auth/mod.rs
---

# HTTP API design

These rules cover SignalDB's **first-party** HTTP API: the endpoints SignalDB
itself defines. The compat APIs (`/tempo`, `/loki`, `/prometheus`,
`/pyroscope`) follow their upstream wire formats and are out of scope,
except for rule 2 (privileges), which applies everywhere.

## 1. One versioned root

Every first-party endpoint lives under `/api/v{N}/`. The only paths allowed
outside that root:

- compat APIs whose path upstream dictates (`/tempo`, `/loki`, `/prometheus`,
  `/pyroscope`)
- paths a spec fixes (`/.well-known/*` and the OAuth 2.1 authorization-server
  endpoints)
- `/health` for probes
- the UI's static assets and SPA fallback

A new first-party route outside `/api/v{N}` is a bug, even if it's public.

## 2. Access control is per endpoint, never per prefix

A path prefix only organizes URLs. It never grants or denies access. Every
handler states the privilege it needs and checks it itself:

- Take `TenantContext` (via `TenantContextExtractor`) and call an explicit
  check at the top of the handler, or take a typed extractor that performs
  it. The existing checks are `ctx.can_manage_tenant()` and the
  `require_read` / `require_write` helpers in `endpoints/schema.rs` and
  `endpoints/processors.rs`. Add a shared helper rather than copying one.
- Instance-admin operations check instance-admin privilege in the handler
  too. Living under `/manage/admin` doesn't make an endpoint admin-only; its
  check does.
- Authentication (who is calling) may stay a router layer, because that's
  identity, not authorization. Authorization (what they may do) belongs in
  the handler.
- Tenant-scoped handlers take the tenant from the auth context. When the
  path also names a tenant, the handler checks that it matches the caller's
  tenant, or that the caller holds the cross-tenant privilege.
- Every endpoint has a test proving a caller without the privilege gets
  `403`, plus one proving an allowed caller succeeds.

Why: prefix layers fail open. A route added under the wrong nest, or merged
into a router whose layer order is different, silently loses its check. A
check written in the handler travels with it.

## 3. Resources, not verbs

- Paths name plural resources and their ids:
  `/api/v1/manage/tenants/{tenant_id}/api-keys/{key_id}`.
- The method carries the verb:
  - `GET` reads
  - `POST` creates, returning `201` and a `Location` header
  - `PUT` replaces
  - `PATCH` partially updates
  - `DELETE` removes, returning `204`
- A verb segment (`POST .../tables/create`) is allowed only for operations
  that aren't CRUD. Prefer modelling the result as a resource.
- Scope shows in the path so a reader can tell it at a glance:
  - `/api/v1/manage/...` acts on the caller's tenant
  - `/api/v1/manage/admin/...` acts across tenants
  - `/api/v1/ops/...` is operational control

  The path is still only descriptive; enforcement is rule 2.

## 4. Self-discoverable (hypermedia)

A client should be able to go from the API root to any resource by following
links, without building URLs from documentation.

- `GET /api/v{N}` returns an index: `_links` to the top-level collections the
  caller can reach (query, schema, manage, whoami, the OpenAPI document).
- Every resource representation carries `_links`:
  - `self` always
  - related resources: a tenant links to its `datasets`, `api-keys`,
    `memberships` and `tables`
  - actions the caller is allowed to take on this resource right now:
    `update`, `delete`, `revoke`, and so on
- Links are objects: `{"href": "...", "method": "DELETE"}`. `method` is
  omitted for `GET`.
- Links reflect privileges. Leave out actions the caller isn't allowed to
  take. This is a UX hint only; the handler still enforces rule 2.
- Collections return `{"items": [...], "_links": {"self": ..., "next": ...}}`.
  `next` is present only when there's another page. Pagination is cursor-based
  (`?cursor=...&limit=...`); clients follow `next` and never build cursors.
- Build `href`s from the same path constants the router mounts. Never write
  them as string literals scattered through handlers.

## 5. Consistent payloads

- **Errors:** always the shared `ApiError` JSON envelope
  (`{"status":"error","errorType":...,"error":...}`), with the status code
  matching the failure. Never an empty body.
- **Timestamps:** `chrono::DateTime<Utc>` in response types, which serialize
  as RFC 3339 with a `Z` suffix and appear in OpenAPI as `format: date-time`.
  Never pre-formatted strings.
- **IDs:** opaque strings. Clients don't parse them.
- **Names:** JSON fields are `snake_case`, paths are `kebab-case`, and query
  parameters are `snake_case`.

## 6. OpenAPI is the contract

- Every first-party route is annotated with `#[utoipa::path]` and registered
  in `openapi.rs`. That includes its security requirement and every status
  code it can return, 403 among them.
- The SDK (`signaldb-sdk`), the CLI and the UI client are generated from the
  spec (`cargo xtask generate`). First-party clients don't hand-write calls
  to first-party endpoints.
- `openapi_spec_is_up_to_date` must pass. Regenerate; never hand-edit the
  generated files.

## 7. Versioning and breaking changes

- Before 1.0, breaking changes inside `v1` are allowed. They must be marked
  in the commit (`feat(router)!:` plus a `BREAKING CHANGE:` footer) so
  release-please surfaces them. All first-party clients are migrated in the
  same PR.
- Once there are external API users, a breaking change means a new
  `/api/v{N+1}` root, with the old version kept for a stated deprecation
  window.
- A wire-format change that looks cosmetic is still breaking, for example
  `+00:00` becoming `Z`. Call it out.

## Checklist for a new or changed endpoint

- [ ] It lives under `/api/v{N}`, or under one of rule 1's exceptions.
- [ ] The handler checks its privilege itself, with a 403 test and a success
      test.
- [ ] The path is resource-oriented and the method matches the semantics.
- [ ] The response carries `_links` (`self`, related, allowed actions).
      Collections return `items` plus `next`.
- [ ] Errors use `ApiError`, and timestamps are `DateTime<Utc>`.
- [ ] It's registered in OpenAPI and the clients are regenerated.
- [ ] Breaking changes are marked and every client is migrated.

## Known gaps (as of this skill's creation)

Treat these as debt to pay down. Don't copy them.

- `read_scope::require_read_scope` enforces read scopes as a router layer on
  the compat prefixes (`/tempo`, `/loki`, `/prometheus`, `/pyroscope`,
  `/api/profiles`). This is prefix-based authorization, which rule 2
  forbids.
- `/api/profiles` has no version.
- The session, OIDC and GitHub-callback routes are mounted at the root, not
  under `/api/v1`. The OIDC and GitHub callbacks are URLs registered with
  external providers, so moving them means updating those registrations too.
- No endpoint emits `_links` yet, and no `/api/v1` index exists.
- List endpoints return bare arrays or ad-hoc wrappers, not
  `{items, _links}`.
