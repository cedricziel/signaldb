## Why

AI agents (Claude Code, Claude.ai, IDE assistants, on-call bots) are becoming primary consumers of observability data — "why is checkout slow?", "find the trace for this error" — but reaching SignalDB today means hand-rolling HTTP against the Tempo/Loki/Prometheus APIs. A Model Context Protocol server makes SignalDB natively agent-accessible while keeping tenant isolation exactly where it already lives (the router/querier).

## What Changes

- **New standalone service `signaldb-mcp`** that speaks MCP over Streamable HTTP at `/mcp` on its own port, plus stdio for local development. It depends on **only `signaldb-sdk`** — no SignalDB internal crate — so it is always a separate service (a sidecar), never an in-process `/mcp` route on the router or monolith.
- **Credential forwarding, no privileged state, no local validation.** The server holds no key of its own and does not validate credentials — the router is the sole authority. It requires a bearer + `X-Tenant-ID` to be present (rejecting requests that carry neither), pins each session to that identity, and builds a per-session `signaldb-sdk` client that forwards the caller's `Authorization` bearer and `X-Tenant-ID` (optional `X-Dataset-ID`) on every downstream call. An invalid or revoked credential is rejected by the router and surfaces as a clean MCP error. Tenant isolation and quotas remain enforced by the router.
- **Read/query tools** (available to every authenticated tenant session, no role gating in v1): `search_traces`, `get_trace`, `search_logs`, `query_metrics`, `discover_attributes`, plus discovery tools `list_datasets` / `list_schemas` / `list_tables`.
- **MCP resources**: table schemas (traces/logs/metrics column definitions) exposed as readable resources with stable URIs so agents can ground queries without spending tool calls.
- **Enabling change (Phase A): extend the code-first OpenAPI document + `signaldb-sdk` to cover the query endpoints.** Today the OpenAPI doc — and thus the generated SDK — covers only the admin/management surface; the Tempo/Loki/Prometheus query handlers are unannotated. They gain `#[utoipa::path]` annotations and `ToSchema`/`IntoParams` DTOs so the SDK regenerates with typed query methods. This is annotation/tooling only — **no change to endpoint behavior or responses**. Phase A landed the Tempo (trace) slice; **Phase E** extends the same treatment to Loki (LogQL) and Prometheus (PromQL) so `search_logs`/`query_metrics` wrap generated SDK methods too — no hand-rolled HTTP. Their `resultType`-tagged, tuple-sample payloads are typed at the envelope and represented as `serde_json::Value` at the result (see design D7).
- **Both transports usable from the standalone binary (design D8):** HTTP forwards each caller's bearer/tenant per request; stdio (single-user dev) takes one fixed credential from CLI/env/config so its query tools can reach the router, erroring clearly when none is configured.
- **New `[mcp]` config section** (enabled flag, bind address/port, router URL) following existing config precedence; wiring into `signaldb-bin`, `run-dev.sh`, and docker compose.
- **Secure-by-default deployment** (established before Phase B):
  - The MCP listener is **off unless `[mcp].enabled = true`**.
  - The standalone service **binds loopback (`127.0.0.1`) by default**. Because the server forwards live bearer credentials, any non-loopback bind must sit behind TLS — direct HTTPS or a documented trusted TLS terminator — so credentials are never carried in plaintext over a network.
  - **No duplicate listeners:** the MCP surface is served by exactly one process. When SignalDB runs monolithically, the embedded MCP listener and a separately-run `signaldb-mcp` must not both bind the same address; the embedded listener defers to an explicitly configured standalone one.
- **Explicitly deferred to a later phase:** per-API-key role model, `viewer` read-only enforcement, and admin toolsets (tenant/key/dataset CRUD). v1 is read-only-agent-facing and gated by the caller's existing tenant credential.

Not BREAKING: no change to OTLP ingest, the Tempo/LogQL/PromQL result behavior, Flight wire schemas, or on-disk layout. The OpenAPI extension is additive.

## Capabilities

### New Capabilities

- `mcp-server`: The MCP surface SignalDB exposes to agents — transport, per-session bearer authentication and credential forwarding, the read/exploration toolset and its tenant scoping, schema resources, and error mapping.

### Modified Capabilities

<!-- None. The Phase A OpenAPI/SDK extension documents existing query endpoints without changing their observable behavior, so no existing spec's requirements change. -->

## Impact

- **New crate:** `src/mcp-server` (binary `signaldb-mcp`), added to the workspace and `default-members`.
- **New dependency:** `rmcp` (official Rust MCP SDK) in `mcp-server`; `utoipa` added to `tempo-api`, `loki-api`, `prometheus-api`.
- **Modified crates:** `router` (annotate query handlers, register them in `openapi.rs`); `tempo-api` / `loki-api` / `prometheus-api` (derive `ToSchema`/`IntoParams`); `signaldb-sdk` (regenerated); `signaldb-bin` (embed the service); `common` (reuse `Authenticator`; `[mcp]` config in `common::config`).
- **Regenerated artifacts:** `api/signaldb-api.json` (golden test `openapi_spec_is_up_to_date`) and the TypeScript client, if the query operations are surfaced there.
- **Ops:** new port + `[mcp]` config; `run-dev.sh` and docker compose gain the service; `signaldb.dist.toml` gains a commented `[mcp]` section.
- **Tracks epic #620** (Phases A–C); supersedes the standalone-SDK framing details of #624 and defers the role stories #621/#622/#627/#628.
