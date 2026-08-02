## Why

AI agents (Claude Code, Claude.ai, IDE assistants, on-call bots) are becoming primary consumers of observability data — "why is checkout slow?", "find the trace for this error" — but reaching SignalDB today means hand-rolling HTTP against the Tempo/Loki/Prometheus APIs. A Model Context Protocol server makes SignalDB natively agent-accessible while keeping tenant isolation exactly where it already lives (the router/querier).

## What Changes

- **New standalone service `signaldb-mcp`** that speaks MCP over Streamable HTTP at `/mcp` (its own port; also embeddable in monolithic mode) plus stdio for local development.
- **Credential forwarding, no privileged state.** The server holds no key of its own. It authenticates each session's bearer via the shared `Authenticator`, then builds a per-session `signaldb-sdk` client that forwards the caller's `Authorization` bearer and `X-Tenant-ID` (optional `X-Dataset-ID`) on every downstream call. Tenant isolation and quotas remain enforced by the router.
- **Read/query tools** (available to every authenticated tenant session, no role gating in v1): `search_traces`, `get_trace`, `search_logs`, `query_metrics`, `discover_attributes`, plus discovery tools `list_datasets` / `list_schemas` / `list_tables`.
- **MCP resources**: table schemas (traces/logs/metrics column definitions) exposed as readable resources with stable URIs so agents can ground queries without spending tool calls.
- **Enabling change (Phase A): extend the code-first OpenAPI document + `signaldb-sdk` to cover the query endpoints.** Today the OpenAPI doc — and thus the generated SDK — covers only the admin/management surface; the Tempo/Loki/Prometheus query handlers are unannotated. They gain `#[utoipa::path]` annotations and `ToSchema`/`IntoParams` DTOs so the SDK regenerates with typed query methods. This is annotation/tooling only — **no change to endpoint behavior or responses**.
- **New `[mcp]` config section** (enabled flag, bind address/port) following existing config precedence; wiring into `signaldb-bin`, `run-dev.sh`, and docker compose.
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
