## Context

`api/signaldb-api.json` is assembled in `src/router/src/openapi.rs` from a
manually curated `paths()` list of `#[utoipa::path]`-annotated handlers, a
`components(schemas(...))` list of `ToSchema`-deriving DTOs, and a
`SecurityAddon` that currently registers only a `bearerAuth` HTTP-bearer
scheme and applies it as the sole global security requirement. Annotation and
registration are both required — a handler with `#[utoipa::path]` but missing
from `paths()` still won't appear.

Query-compat DTOs live in dedicated per-protocol crates (`loki-api`,
`prometheus-api`, `pyroscope-api`, `tempo-api`), none of which currently
derive `ToSchema`. Tempo's operations already reference their real DTOs
(`tempo_api::Trace`, `tempo_api::SearchResult`, etc.) as response bodies —
that crate needs no schema-fidelity fix, only its DTOs gaining `ToSchema`.
Loki's `query`/`query_range` operations are already annotated and registered,
but declare `body = serde_json::Value` rather than referencing
`loki_api::QueryResponse`, so real type information is lost even though the
operations are nominally "in the spec." Confirmed the same gap exists on
`promql::query`/`query_range` — both declare `body = serde_json::Value` too,
so the fix is unconditional across both protocols, not a Loki-only concern
that might extend to Prometheus.

The auth middleware (`common::auth::middleware`) resolves a `TenantContext`
from either an `Authorization: Bearer <key>` header or the `signaldb_session`
cookie before any handler runs, via `TenantContextExtractor` — this is
already uniform across every route, proven by the session-cookie integration
tests hitting `/tempo/api/echo` and `/api/v1/manage/...` with only a cookie.
The document's bearer-only default has therefore been inaccurate since before
this change; extending it for `/ui/session` is what surfaces the gap, not
what creates it.

A second, independent instance of the same class of inaccuracy: `oauth.rs`'s
`authorize_decision` and `consent_context` handlers already declare an
explicit `security(())` override (not just inheriting the bearer-only
default), yet both call `session_token_from_headers` directly and 401 without
a valid session cookie — they're cookie-authenticated, not public, and the
document currently says otherwise. Unlike every other operation, they don't
accept bearer at all (no call into the dual-credential extractor), so once
`cookieAuth` exists, their correct annotation is `cookieAuth` alone, not
`bearerAuth` OR `cookieAuth`.

See `proposal.md` for the motivating gap (UI can't migrate off hand-written
fetch until the operations and types exist to migrate onto).

## Goals / Non-Goals

**Goals:**

- Every existing router handler the UI's five hand-written clients call is
  `#[utoipa::path]`-annotated, registered, and returns a real typed schema.
- The document's security model matches actual middleware behavior: bearer
  and cookie are documented as equally valid alternatives.
- `logql::query`/`query_range` and `promql::query`/`query_range`'s
  response-type regression (`serde_json::Value`) is fixed alongside the
  newly-annotated operations, since leaving it in place would still block
  loki.ts/prom.ts from getting typed generated calls.
- The two pre-existing `security(())` overrides on `oauth.rs`'s consent-flow
  operations are corrected to `cookieAuth`, since they're the same class of
  documentation-accuracy defect this change exists to close.

**Non-Goals:**

- Migrating the UI's fetch clients onto the generated SDK — that's
  `ui-migrate-to-generated-sdk`.
- Adding enforcement (lint) against future raw-fetch regressions — that's
  `ui-enforce-sdk-only-http`.
- Any new query-compat capability the router doesn't already implement (e.g.
  Loki `series` filtering beyond what exists today).
- Auditing every other already-annotated operation's security scenario
  wording beyond `admin-management-api-contract` and the new capabilities —
  scoped to what this change's security-default flip actually touches.

## Decisions

**Annotate `profiles_by_trace` and `render_diff` even though the UI doesn't
call them yet.** Both are real, already-implemented router handlers
(`/api/profiles/trace/{trace_id}`, `/pyroscope/render-diff`). Leaving them
unannotated while their siblings get covered would recreate the exact kind of
partial-coverage drift this change exists to close, and `client-surface-parity`-
style checks elsewhere in the project treat "implemented but undocumented" as
a defect. `ui-migrate-to-generated-sdk` is not obligated to consume operations
this change adds if the UI has no current use for them.

**Fix `logql::query`/`query_range` and `promql::query`/`query_range`'s
response types, not just add new operations.** "Keep shapes aligned" (the
guiding principle for the UI migration) only holds if the operations the UI
already could migrate onto (query/query_range, both protocols) carry real
types too. Retrofitting is a small, contained change per protocol (swap
`body = serde_json::Value` for the real DTO plus a `ToSchema` derive) and
avoids a second change having to revisit `openapi.rs` for the same reason.

**Global security default becomes `[bearerAuth] OR [cookieAuth]` rather than
per-operation overrides everywhere.** utoipa/OpenAPI 3 represents alternative
security requirements as an array of requirement objects at the `security`
level (each array element is an alternative; `bearerAuth: [], cookieAuth: []`
inside one object would mean both required together, which is wrong here).
Changing the `SecurityAddon`'s default once is lower-diff than adding a
`security(...)` override to every existing `#[utoipa::path]`, and is accurate
for every operation _except_ the two that must opt out entirely.

**`POST /ui/session` opts out of the global default entirely
(`security(())`); `DELETE /ui/session` documents the cookie as optional
(`security(("cookieAuth" = []), ())`), not absent.** Login is inherently
pre-auth (that's the point of the endpoint) — neither `bearerAuth` nor
`cookieAuth` fits. Logout is different: it _does_ read the cookie when
present (to revoke that specific session) and only treats a missing/invalid
one as a no-op rather than rejecting the request. An empty `security(())`
would under-document this — OpenAPI's convention for "this credential is
accepted but not required" is an alternatives list that includes both the
scheme and the empty requirement, which is what advertises the cookie at all
rather than silently omitting it.

**`oauth::authorize_decision` and `oauth::consent_context` get
`security(("cookieAuth" = []))`, not the global default.** They're the one
case that's cookie-only with no bearer fallback (see Context) — using the
global `bearerAuth OR cookieAuth` default here would incorrectly document
bearer tokens as accepted. Fixing their existing `security(())` is bundled
into this change rather than deferred, since it's the same
documentation-accuracy defect this change already exists to close, discovered
while auditing every operation that currently opts out of the default.

**DTOs gain `ToSchema` in their home crates, not via wrapper types in
`router`.** Mirrors how `signaldb_api::*` DTOs are already schema'd in place
for the admin/management surface; keeps the schema next to the type it
describes instead of introducing a parallel router-local shadow type per DTO.

## Risks / Trade-offs

- **Widening the documented security requirement for every existing
  operation is a larger diff than it looks.** Every `#[utoipa::path]` that
  inherits the global default will show `cookieAuth` as a newly-valid
  alternative in the regenerated `api/signaldb-api.json`, even though no
  Rust code for those operations changes. Mitigation: this is exactly the
  golden-file diff the `admin-management-api-contract` delta calls for: it's
  documentation catching up to reality, not a behavior change, and the
  golden test will show the diff precisely so reviewers can confirm nothing
  besides security metadata moved.
- **`serde_json::Value` → typed body could be a breaking schema-shape
  surprise if the handler's actual serialization doesn't match the DTO's
  derived shape exactly (e.g. an untagged enum variant).** Mitigation: add
  the `ToSchema` derive first and diff the generated schema against a sample
  real response before wiring it into `responses(...)`; existing handler
  tests (`logql.rs`'s `#[cfg(test)]` module) already assert response JSON
  shape and will catch a mismatch.
- **Pyroscope's `Flamebearer`/`RenderResponse` types use delta-encoded,
  loosely-typed arrays (`Vec<Vec<i64>>` levels) that don't self-document well
  as an OpenAPI schema.** Mitigation: accept a weakly-typed but present
  schema (e.g. `levels: array of array of integer`) with a `description`
  explaining the delta-encoding — still strictly better than being absent
  from the document, and the UI's hand-written `pyroscopeRender` decoding
  logic (kept per `ui-migrate-to-generated-sdk`'s adapter principle) is what
  actually gives it meaning downstream.

## Migration Plan

1. Add `ToSchema` derives crate-by-crate (`loki-api`, `prometheus-api`,
   `pyroscope-api`, `tempo-api`, router-local session types), verified by
   `cargo build -p router`.
2. Annotate and register handlers one protocol at a time (loki, prometheus,
   pyroscope, session), running `UPDATE_OPENAPI=1 cargo test -p router`
   after each to regenerate `api/signaldb-api.json` incrementally and keep
   diffs reviewable.
3. Add the `cookieAuth` scheme and flip the global default; in the same
   commit, fix `oauth::authorize_decision`/`consent_context`'s
   `security(())` to `security(("cookieAuth" = []))` — both changes touch
   the same security metadata and are easiest to review together.
4. Run `cargo xtask generate` to regenerate `signaldb-sdk` and
   `src/ui/src/api/gen`; commit the regenerated output even though nothing
   consumes it yet, so `ui-migrate-to-generated-sdk` starts from real
   generated code.

No runtime migration or rollback concerns — this only changes documentation
and generated-but-unconsumed client code; the router's actual request
handling is untouched.
