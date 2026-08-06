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
operations are nominally "in the spec."

The auth middleware (`common::auth::middleware`) resolves a `TenantContext`
from either an `Authorization: Bearer <key>` header or the `signaldb_session`
cookie before any handler runs, via `TenantContextExtractor` — this is
already uniform across every route, proven by the session-cookie integration
tests hitting `/tempo/api/echo` and `/api/v1/manage/...` with only a cookie.
The document's bearer-only default has therefore been inaccurate since before
this change; extending it for `/ui/session` is what surfaces the gap, not
what creates it.

See `proposal.md` for the motivating gap (UI can't migrate off hand-written
fetch until the operations and types exist to migrate onto).

## Goals / Non-Goals

**Goals:**

- Every existing router handler the UI's five hand-written clients call is
  `#[utoipa::path]`-annotated, registered, and returns a real typed schema.
- The document's security model matches actual middleware behavior: bearer
  and cookie are documented as equally valid alternatives.
- `logql::query`/`query_range`'s response-type regression (`serde_json::Value`)
  is fixed alongside the newly-annotated operations, since leaving it in
  place would still block loki.ts from getting typed generated calls.

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

**Fix `logql::query`/`query_range`'s response type, not just add new
operations.** "Keep shapes aligned" (the guiding principle for the UI
migration) only holds if the operations the UI already could migrate onto
(query/query_range) carry real types too. Retrofitting is a small, contained
change (swap `body = serde_json::Value` for `body = loki_api::QueryResponse`
plus a `ToSchema` derive) and avoids a second change having to revisit
`openapi.rs` for the same reason.

**Global security default becomes `[bearerAuth] OR [cookieAuth]` rather than
per-operation overrides everywhere.** utoipa/OpenAPI 3 represents alternative
security requirements as an array of requirement objects at the `security`
level (each array element is an alternative; `bearerAuth: [], cookieAuth: []`
inside one object would mean both required together, which is wrong here).
Changing the `SecurityAddon`'s default once is lower-diff than adding a
`security(...)` override to every existing `#[utoipa::path]`, and is accurate
for every operation _except_ the two that must opt out entirely.

**`POST /ui/session` and `DELETE /ui/session` opt out of the global default
with an explicit empty `security(())`.** Login is inherently pre-auth
(that's the point of the endpoint), and logout treats a missing/invalid
cookie as a no-op rather than rejecting the request — neither fits "requires
bearer or cookie."

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
   `pyroscope-api`, router-local session types), verified by `cargo build -p
router`.
2. Annotate and register handlers one protocol at a time (loki, prometheus,
   pyroscope, session), running `UPDATE_OPENAPI=1 cargo test -p router`
   after each to regenerate `api/signaldb-api.json` incrementally and keep
   diffs reviewable.
3. Add the `cookieAuth` scheme and flip the global default last, so its diff
   (touching every existing operation's security metadata) is isolated to
   its own commit.
4. Run `cargo xtask generate` to regenerate `signaldb-sdk` and
   `src/ui/src/api/gen`; commit the regenerated output even though nothing
   consumes it yet, so `ui-migrate-to-generated-sdk` starts from real
   generated code.

No runtime migration or rollback concerns — this only changes documentation
and generated-but-unconsumed client code; the router's actual request
handling is untouched.
