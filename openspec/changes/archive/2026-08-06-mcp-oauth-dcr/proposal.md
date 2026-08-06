## Why

Claude.ai and OpenAI/ChatGPT register remote MCP servers by driving the MCP authorization flow — OAuth 2.1 with Dynamic Client Registration — with **zero human pre-registration**. SignalDB's MCP surface today accepts only a raw `Authorization: Bearer <api-key>` + `X-Tenant-ID`, which those clients cannot produce. To be a one-click connector, SignalDB must become an OAuth 2.1 Authorization Server + Resource Server that these clients can discover, self-register with, and obtain audience-bound tokens from.

## What Changes

- **The router becomes SignalDB's OAuth 2.1 Authorization Server.** It gains the standard endpoints — authorization-server metadata (RFC 8414), Dynamic Client Registration (RFC 7591), `/authorize` with a human login + consent step, and `/token` (authorization-code + PKCE `S256`). It reuses the existing user/password/session model and tenant memberships; no new identity system.
- **Opaque, catalog-backed tokens.** Access, refresh, and authorization-code values are opaque high-entropy strings whose hashes and metadata live in the catalog (mirroring the existing session-token pattern). Validation is a catalog lookup on the router — the same path and cost as API-key auth. Revocation is a row delete. The router is both issuer and validator, so there is no token-introspection endpoint.
- **Tokens are tenant-bound at consent time and the tenant leaves the agent-facing surface entirely.** During `/authorize` the human picks which of their tenants the client may access; the minted token carries `(user, tenant, granted scopes, audience)`. The router resolves the tenant from the token — not from `X-Tenant-ID`, not from a tool argument. An agent has no channel to name a tenant it was not granted. A user who wants a client to reach two tenants authorizes two connectors.
- **Read scopes complete the scope model.** New `traces:read` / `logs:read` / `metrics:read` scopes and a `can_read(signal)` check mirror the existing `{signal}:write` / `can_ingest` pattern. OAuth-granted `scope` maps directly onto `TenantContext.api_key_scopes`, so existing enforcement plumbing carries it. The consent screen shows the scopes the client requested.
- **The MCP resource advertises its authorization server.** The `signaldb-mcp` sidecar serves the RFC 9728 Protected Resource Metadata document at `/.well-known/oauth-protected-resource` on its own origin (pointing at the router AS) and answers an unauthenticated request with `401 WWW-Authenticate: Bearer resource_metadata="…"`. The sidecar stays a credential forwarder — it gains only these two resource-origin responsibilities and forwards the bearer verbatim; the router validates it.
- **Dataset stays a per-call tool argument.** The token binds only the tenant (the hard isolation boundary). Datasets remain query-scoping within the granted tenant, defaulting to the tenant's default dataset — matching the membership model where a member sees all datasets in their tenant. (A future option, not in this change: offer single-dataset binding at consent via the existing `api_key_dataset_id`.)
- **Backward compatible.** The existing `Bearer api-key + X-Tenant-ID` path to the MCP sidecar and to the router is unchanged; OAuth is an added credential path, not a replacement. Non-OAuth API-key callers still send `X-Tenant-ID`.
- **Audience binding.** Tokens are audience-bound to the MCP resource URL (RFC 8707 resource indicators) so a SignalDB token cannot be replayed at another resource, nor a foreign token accepted here.

Not BREAKING: no change to OTLP ingest, Tempo/LogQL/PromQL result behavior, Flight wire schemas, or on-disk Iceberg/WAL layout. This is additive authorization surface.

## Capabilities

### New Capabilities

- `mcp-oauth`: SignalDB's OAuth 2.1 Authorization Server + MCP Resource Server surface — protected-resource and authorization-server discovery, Dynamic Client Registration, the authorization-code + PKCE flow with human login and consent-time tenant selection, opaque catalog-backed token issuance/validation/revocation, audience binding, and the read-scope model (`{signal}:read` / `can_read`) that OAuth grants populate.

### Modified Capabilities

<!-- None in main specs. The `mcp-server` capability (transport, credential forwarding, toolset) is still an in-progress change not yet synced to openspec/specs/, so its interaction — the sidecar's new PRM/401 responsibilities and accepting OAuth tokens with tenant-from-token — is specified as requirements within the new `mcp-oauth` capability rather than as a cross-change delta. `ingest-auth-tenancy` is the acceptor ingest path and is unaffected; the new read scopes are a router/query-side concern owned by `mcp-oauth`. -->

## Impact

- **`router`**: new OAuth AS endpoints (`/.well-known/oauth-authorization-server`, `/register`, `/authorize`, `/token`) and the consent handler; `Authenticator` gains an opaque-OAuth-token credential path resolving `(user, tenant, scopes)` from the catalog; `can_read(signal)` scope gate on the query surface.
- **`common`**: `can_read` on `TenantContext`; catalog schema for dynamically registered clients, access/refresh tokens, and authorization codes (hashed, with expiry, audience, tenant, scopes); `[mcp]`/auth config for the AS issuer URL and token lifetimes.
- **`mcp-server`** (sidecar): serve the Protected Resource Metadata document + emit the `401` challenge on its own origin; relax the hard `X-Tenant-ID` requirement for OAuth-authenticated sessions (tenant now travels in the token) while keeping it for legacy API-key sessions; continue forwarding the bearer verbatim.
- **`signaldb-sdk` / OpenAPI**: the AS/discovery endpoints are documented in the code-first OpenAPI doc if surfaced there (golden test `openapi_spec_is_up_to_date`); no query-endpoint behavior changes.
- **UI (`src/ui`)**: a consent screen ("Authorize <client> to access tenant X with scopes …") reusing the explore-UI login; tenant selection among the user's memberships.
- **Docs & skills**: `multi-tenancy` and the MCP/`tempo-api` docs/skills gain the OAuth connector flow and operator setup (issuer URL, TLS requirement for a non-loopback resource origin).
- **Ops**: the AS issuer must be reachable over TLS by external clients (Claude/OpenAI); homelab guidance for exposing the MCP resource origin + router AS.
