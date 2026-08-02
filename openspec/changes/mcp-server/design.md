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

### D4: Authenticate every request, bind the session to its credential

The bearer is validated on every Streamable HTTP request via the shared `Authenticator` (an axum layer ahead of the transport), not only at `initialize` — the transport is stateful and re-authenticating each request is cheap and closes the "auth once, then drift" gap. Failure (missing/invalid bearer, or `Authenticator` rejection) yields a 401 before the transport sees the request, so no tool or resource is ever exposed to an unauthenticated caller.

**Session binding.** The first authenticated request on a session (keyed by the `mcp-session-id` the transport assigns) pins the resolved `(tenant_id, credential hash)`. Any later request on that session whose bearer resolves to a different tenant, or whose credential hash differs, is rejected — a session cannot be smuggled from one identity to another mid-stream. Binding lives for the session's lifetime; when the credential is revoked, the next request's `Authenticator::authenticate` fails and the session is denied (revocation takes effect within one request). Sessions carry no independent expiry beyond the transport's own idle timeout.

**Stdio.** The stdio transport (dev only) has no per-request headers, so it cannot carry a bearer; it therefore runs unauthenticated and is documented as development-only. Production deployments use Streamable HTTP.

### D5: Error mapping

Query API responses map to MCP outcomes: 2xx → tool result (JSON, size-capped per D6); 400/422 → actionable "bad query" error; 404 → MCP "not found" tool error; **401 → session-authentication failure (the forwarded credential expired or was revoked; the client must re-authenticate / re-establish the session); 403 → access-denied tool error (the credential is valid but lacks access to the requested tenant/dataset/resource)**; 429 → retryable "throttled" error; 5xx/transport → generic tool error. Both 401 and 403 paths are covered by tests. This makes agent-visible failures actionable instead of opaque.

### D6: Bounded tool payloads

Each query tool caps its serialized result at a fixed byte budget (default 256 KiB). When a downstream response exceeds the cap, the tool does not stream an unbounded blob: it returns valid structured JSON truncated at a record boundary with a `truncated: true` flag and a `hint` telling the agent to narrow the query (tighter time range or lower `limit`). Callers detect truncation from the flag. This keeps a single tool call from blowing an agent's context window.

### D7: Loki/Prometheus query in the SDK — type the envelope, keep the result payload permissive

`search_logs` (LogQL) and `query_metrics` (PromQL) follow the same rule as every other tool: **wrap the generated `signaldb-sdk` method — no hand-rolled HTTP.** That requires the Loki (`/loki/api/v1/query`, `/query_range`) and Prometheus (`/prometheus/api/v1/query`, `/query_range`) handlers to join the code-first OpenAPI document so the SDK regenerates with typed methods.

The obstacle is shape: these responses model Prometheus/Loki result payloads with custom `serde` — `resultType`-tagged `QueryResult` enums (`streams`/`matrix`/`vector`) and heterogeneous sample tuples (`Sample`, `LogEntry` serialize as `[timestamp, value]` arrays via `SerializeSeq`). utoipa cannot derive `ToSchema` for those, and forcing a full tuple/`oneOf` model is disproportionate — an agent just needs the JSON.

**Decision:** type the _envelope_ precisely (`status`, error fields, `resultType` discriminator) and represent the polymorphic result payload as a permissive schema (open object / `serde_json::Value`), via a small manual `ToSchema` on `QueryResult` (and `Sample`/`LogEntry` if referenced) or a `#[schema(value_type = …)]` override on the dynamic field. Progenitor then generates a typed `QueryResponse` whose result is a `serde_json::Value` — exactly the treatment Tempo's polymorphic `Attribute.value` already gets (#861). This satisfies "only SDK/OpenAPI, enhance where necessary": the enhancement is the annotations + envelope typing, nothing is hand-modeled beyond what adds value, and no custom client call is introduced.

Alternatives rejected: (a) hand-rolled reqwest for Loki/Prom — violates the no-custom-calls constraint and splits the client story; (b) fully typed matrix/vector/sample schemas — high effort, low agent value, brittle against Prometheus's integer-vs-float timestamp quirk.

### D8: Credential source per transport — HTTP headers vs. a configured stdio credential

The query tools forward the caller's credential by reading it from the HTTP request `Parts`. Streamable HTTP carries per-request `Authorization` + `X-Tenant-ID`, so each caller acts as itself and the router enforces isolation. **Stdio has no per-request headers**, so a tool invoked over stdio has no credential to forward — the current server_info tool degrades to "unauthenticated," but a _query_ tool cannot reach the router at all.

**Decision:** the standalone `signaldb-mcp` binary supports both transports with distinct credential sources:

- **HTTP** (production, multi-tenant): unchanged — the per-request bearer + tenant headers are forwarded; no server-held credential.
- **Stdio** (single-user dev): the binary accepts a fixed credential via CLI flags / env / config (`--token`/`--tenant`/`--dataset`, or `SIGNALDB_MCP_TOKEN`/`_TENANT`/`_DATASET`). In stdio mode the handler holds this static credential and builds the per-call SDK client from it when no HTTP `Parts` are present. If stdio is started without a configured credential, query tools return a clear "stdio requires a configured credential" error rather than a confusing auth failure at the router.

This makes `signaldb-mcp --stdio` genuinely usable for a developer pointing an MCP client at a running dev router, while keeping the HTTP path credential-free on the server. The SDK-client builder gains one branch (prefer `Parts` headers; else the configured stdio credential; else error); the forwarding principle is unchanged.

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
