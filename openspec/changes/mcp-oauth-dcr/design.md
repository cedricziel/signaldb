## Context

See proposal.md — Why. The load-bearing facts that shape the approach:

- **The MCP sidecar is a stateless credential forwarder** (`src/mcp-server`): it requires `Bearer` + `X-Tenant-ID`, pins session identity, and forwards verbatim to the router; the router is the sole auth authority. Preserving that "forwarder soul" is a design constraint, not just a preference.
- **The router already has the human-identity substrate** an Authorization Server needs: users, password login, server-side sessions (`signaldb_session` cookie), `MembershipRole {Admin, Member, Viewer}`, instance admins, `Authenticator::authenticate` / `authenticate_session`, and the catalog.
- **The scope model is half-built.** `TenantContext.api_key_scopes: Option<Vec<String>>` (`None` = legacy-unrestricted, `Some` = always enforced) already drives `can_ingest(signal)` = role gate + `{signal}:write` scope. Read scopes and a `can_read` twin are the only missing halves.
- **Both target clients follow the MCP authorization spec (2025-06-18):** RFC 9728 protected-resource metadata, RFC 8414 AS metadata, RFC 7591 DCR, OAuth 2.1 authorization-code + PKCE (`S256`), RFC 8707 resource-indicator audience binding.
- FDAP version-alignment (Arrow/Parquet via DataFusion re-exports) and Flight v1-wire/v2-storage transforms are **not touched** by this change — it is auth surface only, with no data-plane, WAL, or Iceberg impact.

## Goals / Non-Goals

**Goals:**

- One-click connector registration from Claude.ai and OpenAI/ChatGPT with zero human pre-registration.
- Tenant isolation enforced by the credential itself — the agent has no channel to name a tenant.
- Reuse the existing identity, session, catalog, and scope machinery; add no second identity system.
- Keep the sidecar a forwarder; concentrate all OAuth weight in the router.

**Non-Goals:**

- Delegating to an external IdP (see Decision 1 — rejected).
- Signed/JWT tokens and a JWKS endpoint (see Decision 4 — opaque chosen).
- Per-dataset consent binding, write/admin scopes over MCP, and a `viewer` read-only role split — deferred; this change is read-only-agent-facing.
- Changing the existing `mcp-server` toolset, transport, or the API-key path.

## Decisions

### Decision 1 — The Authorization Server lives in the router

The router hosts `/.well-known/oauth-authorization-server`, `/register`, `/authorize` (+ consent), and `/token`. It already owns the user DB, login, sessions, memberships, and catalog — everything an AS consumes.

- **Alternative: AS in the sidecar.** Rejected — the sidecar is deliberately dependency-isolated from SignalDB internals; giving it signing/consent/user-DB access destroys the property that makes it a thin, independently-deployable forwarder.
- **Alternative: external IdP (Auth0/Keycloak/Google) with DCR.** Rejected — requires operators to run and configure an IdP, which is antithetical to SignalDB's homelab "just works" positioning. Self-hosting the AS keeps the connector working out of the box.

Consequence: the resource server responsibilities that must live on the _resource's own origin_ — the RFC 9728 metadata document and the `401 WWW-Authenticate` challenge — stay on the **sidecar** (that is the origin clients call). Everything else is the router's. The sidecar still forwards the bearer verbatim; the router validates it. The sidecar learns nothing about OAuth beyond "serve one static document and one 401."

```text
 Claude/OpenAI ──Bearer token──▶  Sidecar (Resource origin)  ──forward verbatim──▶  Router (AS + validator)
                                  • /.well-known/oauth-protected-resource            • /.well-known/oauth-authorization-server
                                  • 401 + WWW-Authenticate                            • /register /authorize /token + consent
                                  • forwards, still stateless                         • Authenticator validates opaque token
```

### Decision 2 — OAuth is a third credential type on `Authenticator`; the sidecar path is nearly unchanged

`Authenticator` today resolves two credential kinds into a `TenantContext` (API key, session cookie). OAuth adds a third: an opaque access token looked up in the catalog, yielding `(user, tenant, scopes, audience)`. Because the router validates the forwarded bearer, the sidecar keeps forwarding `Authorization` verbatim and needs only to (a) serve the PRM document and (b) relax its hard `X-Tenant-ID` requirement for OAuth sessions — tenant now travels inside the token. API-key sessions keep requiring `X-Tenant-ID`.

- **Alternative: sidecar exchanges the OAuth token for an API key.** Rejected — makes the sidecar stateful and reintroduces credential handling it was designed to avoid.

### Decision 3 — Tenant bound at consent; absent from the entire agent surface

Tenant binding and "tenant off the tool surface" are one decision from two sides. The token carries exactly one tenant, chosen by the human at `/authorize`. The router resolves tenant from the token — never from `X-Tenant-ID`, never from a tool argument. An `X-Tenant-ID` header on an OAuth request is ignored, not honored. Reaching a second tenant means authorizing a second connector.

- **Alternative: token grants "all the user's tenants," agent passes `X-Tenant-ID` per call.** Rejected — drags tenant back onto every tool, forces the agent to choose, and reopens the cross-tenant/confused-deputy surface. The cost of the chosen model (one connector per tenant) is acceptable for the homelab/small-team target and is arguably more correct (explicit per-tenant grants).

Dataset is **not** an isolation boundary of the same kind: a member sees all datasets in their tenant. So the token binds only the tenant; `dataset` stays an optional per-call tool argument defaulting to the tenant's default dataset. (Single-dataset consent binding via the existing `api_key_dataset_id` is a future option, out of scope.)

### Decision 4 — Opaque, catalog-backed tokens (no JWT)

Access tokens, refresh tokens, and authorization codes are opaque high-entropy strings; the catalog stores their hash plus `(subject, tenant, scopes, audience, expiry)`, mirroring the existing `signaldb_session` token pattern (`hash_session_token`, `generate_session_token`). Validation is a catalog lookup — the same path and cost the router already pays for API-key auth. Revocation is a row delete. The router is issuer _and_ validator, so there is no RFC 7662 introspection endpoint and no JWKS.

- **Alternative: signed JWT + `/.well-known/jwks.json`.** Rejected for v1 — adds key generation/rotation and a JWKS surface, and buys stateless validation that only pays off if the querier/writer validate these tokens _directly_ (they don't; the router is the single validator). Revisit only if that changes.

### Decision 5 — Read scopes complete the existing scope model

Add `traces:read` / `logs:read` / `metrics:read` and a `can_read(signal)` mirroring `can_ingest`: a read tool requires the matching `<signal>:read` scope; `None` scopes remain legacy-unrestricted. OAuth-granted `scope` is written straight into `TenantContext.api_key_scopes`, so existing enforcement carries it with no new mechanism. The consent screen surfaces the requested scopes so the grant is deliberate.

### Decision 6 — Consent UI reuses the explore-UI login, served at root

`/authorize` reuses the existing login/session; on top of it a consent screen (a route in the React explore-UI) lists the client, requested scopes, and the user's grantable tenants (their memberships) for selection. No new auth UI stack. The consent decision posts to a router consent API consumed via the generated TypeScript client (never raw fetch), which means the consent API is annotated into the OpenAPI document and the TS client is regenerated.

Consequence: consent works only where the UI bundle is served, so the explore-UI is now mounted at **root (`/`)** rather than `/ui`, and `/authorize` redirects to `/oauth/consent` at root. Mounting at `/` makes the SPA a **fallback service** so every existing API/query route still takes precedence, and the bundle's base path moves `/ui` → `/`. This is a deliberate sub-task (SPA-fallback ordering, asset base path, updating existing `/ui` references and docs) sequenced with the consent work, not a silent reroute.

- **Alternative: server-rendered minimal consent page in the router.** Robust (no UI-bundle dependency, works headless) but plainer and a second UI surface; rejected in favor of a single, consistent explore-UI experience.

## Risks / Trade-offs

- **Two auth paths diverge on tenant resolution** (OAuth = token; API-key = `X-Tenant-ID` header) → keep the split explicit in `Authenticator` with a single resolved `TenantContext` output, and cover both paths with tests so neither leaks the other's assumption.
- **Client interop drift** — Claude and OpenAI implement the MCP auth spec with subtle differences (metadata path suffixing, `resource` parameter handling, redirect-URI matching strictness) → validate against both real clients before shipping; keep discovery documents spec-literal.
- **Opaque token = lookup per request** → acceptable (same as API-key auth today); add an in-router cache only if profiling shows it matters.
- **One-connector-per-tenant friction** for multi-tenant power users → documented behavior, not a bug; a single-token multi-tenant model was explicitly rejected (Decision 3).
- **AS must be TLS-reachable by external clients** — the connector redirects a live browser and carries bearer tokens → operator docs must require TLS on the resource origin and the AS issuer for any non-loopback deployment (the sidecar already binds loopback by default).
- **DCR is unauthenticated by spec** → registered-but-unused clients accumulate; codes/tokens/clients need expiry and cleanup. Consent (human approval) — not registration — is the actual trust gate.
- **Audience binding must be enforced on read, not just minted** → the resource server rejects a token whose recorded audience ≠ the resource called, closing token-replay across resources.

## Migration Plan

Purely additive; no data migration. New catalog tables (registered clients, authorization codes, access/refresh tokens — all hashed, with expiry) are created forward-only. Deploy order: router (AS + validation + catalog schema) before the sidecar advertises the AS, so discovery never points at endpoints that don't exist yet. Rollback: disable the OAuth endpoints and the sidecar's PRM/401 advertisement; the API-key path is untouched throughout, so existing callers are never affected.

## Deferred follow-ups (from CodeRabbit review)

Tracked, intentionally out of this change's scope:

- **Refresh-token grant-family revocation.** Refresh tokens now rotate (single-use;
  the presented token is revoked on use). Detecting *replay* of an already-consumed
  refresh token to revoke the whole grant family (OAuth 2.1 §4.3.1) needs a
  grant-family identifier on the token rows — a follow-up.
- **Per-IP rate limiting on Dynamic Client Registration.** DCR is unauthenticated;
  this change adds count/length caps (redirect_uris, client_name, URI length). A
  per-IP rate limit (RFC 7591 §5) needs pre-tenant rate-limit infrastructure the
  router does not yet have — a follow-up.
