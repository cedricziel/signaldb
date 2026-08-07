## Why

The router already implements the Loki/Prometheus metadata endpoints, the full
Pyroscope surface, and the UI's session/whoami endpoints — but only a subset
carry `#[utoipa::path]` annotations and are registered in `openapi.rs`, so
they're invisible to the published OpenAPI document and to the generated
clients derived from it. The document also declares `bearerAuth` as the only
security scheme, even though the auth middleware has always accepted the UI's
session cookie as an equally valid alternative for every route — a
pre-existing inaccuracy this change surfaces and fixes rather than causes.
Two of the already-annotated operations (`logql::query`, `logql::query_range`)
also declare their response body as untyped `serde_json::Value`, which defeats
type fidelity for the two most-used Loki operations even though they're
nominally "in the spec." Closing these gaps is a prerequisite for
`ui-migrate-to-generated-sdk`, which needs real generated operations and types
to migrate the UI's remaining hand-written fetch clients onto.

## What Changes

- Add `#[utoipa::path]` (with real typed response bodies, not
  `serde_json::Value`) and register in `openapi.rs`:
  - `logql::labels`, `logql::label_values`, `logql::series`,
    `logql::detected_fields`
  - `promql::labels`, `promql::label_values`, `promql::label_stats`
  - `pyroscope::render`, `render_diff`, `label_names`, `label_values`,
    `profile_types`, `profiles_by_trace`
  - `session::create_session`, `session::delete_session`, `session::whoami`
- Retrofit `logql::query`, `logql::query_range`, `promql::query`, and
  `promql::query_range` — confirmed to all share the same
  `body = serde_json::Value` gap — to reference their real response DTOs
  instead.
- Add `ToSchema` derives to the DTOs these operations use, in `loki-api`,
  `prometheus-api`, `pyroscope-api`, `tempo-api` (`ProfileSummary`, used by
  `profiles_by_trace`), and the router's local session types.
- Add a `cookieAuth` (`ApiKey::Cookie`) security scheme and change the
  document's default security requirement from `[bearerAuth]` to
  `[bearerAuth] OR [cookieAuth]`, matching what the middleware has always
  accepted. `POST /ui/session` (login) is documented with no security
  requirement (credential exchange); `DELETE /ui/session` (logout) is
  documented as accepting an optional cookie (`[cookieAuth] OR []`, not a
  bare empty requirement, so the spec still advertises the cookie).
- Fix `oauth::authorize_decision` and `oauth::consent_context` — a
  pre-existing inaccuracy independent of the global-default gap above: both
  handlers require the session cookie exclusively (`session_token_from_headers`,
  no bearer fallback) but currently declare `security(())`, documenting them
  as public. Change both to `security(("cookieAuth" = []))`.
- Regenerate `api/signaldb-api.json` (the golden test in `router::openapi`),
  the Rust SDK (`signaldb-sdk`), and the TypeScript client
  (`src/ui/src/api/gen`) — the UI itself keeps using its hand-written fetch
  clients in this change; only `ui-migrate-to-generated-sdk` switches
  consumption over.

Not breaking: no request/response wire behavior changes — this only makes the
spec describe behavior that already exists.

## Capabilities

### New Capabilities

- `query-compat-api-contract`: the OpenAPI document fully and faithfully
  describes the Loki-, Prometheus-, and Pyroscope-compatible query and
  metadata endpoints the router serves, matching implementation with no drift.
- `ui-session-auth-contract`: the OpenAPI document describes the UI's session
  lifecycle (`POST`/`DELETE /ui/session`) and `GET /api/v1/whoami`, and
  documents the session cookie as a security scheme every authenticated
  operation accepts as an alternative to the bearer token.

### Modified Capabilities

- `admin-management-api-contract`: the requirement that every admin/management
  operation "declares that authentication is required (a bearer security
  scheme)" is updated — those operations inherit the document's global
  security default, which now includes the session cookie as an alternative,
  so the contract must describe both rather than bearer-only.

## Impact

- **router**: `src/router/src/openapi.rs` (security scheme, `paths()`
  registration), `src/router/src/endpoints/logql.rs`,
  `src/router/src/endpoints/promql.rs`, `src/router/src/endpoints/pyroscope.rs`,
  `src/router/src/endpoints/session.rs`, `src/router/src/endpoints/oauth.rs`
  (security annotation fix only — no behavior change).
- **loki-api**, **prometheus-api**, **pyroscope-api**, **tempo-api**:
  `ToSchema` derives on response/request DTOs.
- **signaldb-sdk**: regenerated from the updated spec (progenitor).
- **src/ui**: `src/api/gen/*` regenerated (not yet consumed — see
  `ui-migrate-to-generated-sdk`).
- **api/signaldb-api.json**: regenerated golden file.
