## Context

See proposal.md — Why. Two facts about the current codebase shape this design:

1. `signaldb-sdk` is a progenitor client generated from the router's code-first OpenAPI document (introduced in #856). That document covers only the admin/management surface (`/api/v1/admin/*`, `/api/v1/manage/*`); the Tempo/Loki/Prometheus query handlers are unannotated, so the SDK exposes no query operations.
2. `common::auth::Authenticator::authenticate(api_key, tenant_id, dataset_id) -> Result<TenantContext, _>` already turns a bearer + tenant header into a resolved `TenantContext`, independent of any HTTP middleware — the exact primitive an out-of-band MCP server needs.

FDAP version alignment is not a concern here: `rmcp` and `utoipa` are outside the Arrow/Parquet/DataFusion stack, and this change touches no Flight wire schema, WAL, or Iceberg layout.

## Goals / Non-Goals

**Goals:**

- A standalone `signaldb-mcp` service that is a pure credential-forwarding client — zero privileged state.
- Reuse the generated SDK as the single downstream client; extend it to query rather than hand-roll HTTP.
- Keep tenant isolation provably in the router: the MCP server only forwards tokens.

**Non-Goals:**

- Role-based tool visibility, `viewer` enforcement, and admin/mutating toolsets (deferred phase).
- MCP prompts (follow-up once tools land).
- Raw SQL tool (gated on read-only statement enforcement in the querier).
- In-process coupling to `RouterState` — deliberately avoided to keep the trust boundary clean.

## Decisions

### D1: Standalone service over SDK, not in-process on the router

The MCP server runs as its own binary/service and talks to the router over HTTP via `signaldb-sdk`. Alternative considered: mount `/mcp` inside the router and call handler functions in-process on shared `RouterState`. Rejected because in-process access hands the MCP layer the catalog/authenticator directly, dissolving the "thin client with no privilege" property that makes a compromised MCP server harmless. Forwarding tokens through the existing HTTP API keeps isolation and quotas enforced in one place. The service is still embeddable in monolithic mode like every other SignalDB service.

### D2: Extend the code-first OpenAPI to query, regenerate the SDK (Phase A)

Rather than hand-code query HTTP in the MCP server, annotate the query handlers so the generated SDK gains typed methods. This keeps one client contract, rides the #856 machinery, and as a side effect documents the query API and yields a TS client. Alternative considered: SDK-for-admin + raw reqwest-for-query in the MCP server — rejected as it re-encodes query contracts by hand and splits the client story. Trade-off: front-loads annotation work before the first MCP tool exists. Prioritize the cleanly-typed Tempo endpoints (`search`, `query_single_trace`, tags); Loki/Prom responses may use a faithful JSON-envelope schema where full typing is disproportionate.

### D3: Per-session SDK client carries the caller's credential

Progenitor clients wrap a `reqwest::Client`. At session initialize, after `Authenticator::authenticate` succeeds, build a `reqwest::Client` whose default headers include the caller's `Authorization: Bearer` and `X-Tenant-ID` (and `X-Dataset-ID` when supplied), wrap it with `Client::new_with_client(base_url, ...)`, and hold it for the session's lifetime. Every tool call thus executes as the caller. The MCP server never stores or injects a key of its own.

### D4: Authenticate at initialize, fail closed

The bearer is validated once at session initialize via the shared `Authenticator`. Failure yields an MCP auth error and no session — no tool or resource is ever exposed to an unauthenticated caller. The resolved `TenantContext` (tenant/dataset/slugs) is kept in session state for constructing the SDK client and for tenant-scoped resource URIs.

### D5: Error mapping

Query API responses map to MCP outcomes: 2xx → tool result (JSON, size-capped); 404 → MCP "not found" tool error; 400/422 → actionable "bad query" error; 429 → retryable "throttled" error; 5xx/transport → generic tool error. This makes agent-visible failures actionable instead of opaque.

## Risks / Trade-offs

- **rmcp Streamable-HTTP wiring is the one unproven detail** → spike the transport (header access, session lifecycle, SSE) in Phase B behind a `server_info`/`ping` tool before wiring any domain tool; architecture above does not depend on how headers surface because auth is done at the MCP boundary and the SDK client is built there.
- **Loki/Prometheus responses are loosely typed** → accept JSON-envelope schemas for those two; do not block trace tooling on perfect metric/log typing.
- **OpenAPI golden test drift** → `openapi_spec_is_up_to_date` will fail until `api/signaldb-api.json` is regenerated; Phase A includes the regeneration as a task.
- **New port/service surface** → gate with `[mcp].enabled`; document the port and monolithic embedding.

## Migration Plan

Purely additive, shipped as three stacked PRs (A → B → C):

- **A** — OpenAPI/SDK query extension. Deployable on its own; changes only generated artifacts and annotations. Rollback: revert the annotations + regenerate.
- **B** — `signaldb-mcp` scaffold (transport, auth, per-session SDK client, `server_info`, `[mcp]` config, wiring). Behind `[mcp].enabled`; default off until C lands if desired. Rollback: disable the service.
- **C** — read tools + schema resources. Rollback: revert the tool/resource registration; the service falls back to `server_info` only.

No data migration; no rollback of on-disk or wire formats because none change.

## Open Questions

- Whether the query operations should also be surfaced in the generated TypeScript client now or when the UI consumes them — does not affect the Rust SDK, the MCP design, or the task breakdown, so deferrable.
