# Tasks

Shipped as three stacked PRs: A → B → C. Each phase is independently mergeable.

## Phase A — Extend OpenAPI + `signaldb-sdk` to query (PR 1, issue #859)

- [ ] A1. Add `utoipa` to `tempo-api`, `loki-api`, `prometheus-api` (workspace dep); derive `ToSchema` on the trace DTOs (`SearchResult`, `Trace`, `SpanSet`, `Span`, `SpanEvent`, `TagSearchResponse`, `TagValuesResponse`) and `IntoParams` on `SearchQueryParams` / `TraceQueryParams`.
- [ ] A2. Derive `ToSchema`/`IntoParams` on the Loki and Prometheus query param + response types (JSON-envelope schema acceptable where full typing is disproportionate).
- [ ] A3. Add `#[utoipa::path]` to the trace handlers (`tempo::search`, `query_single_trace`, `search_tags`, `search_tag_values`) and the Loki/Prom query handlers (`logql::query`/`query_range`, `promql::query`/`query_range`); register paths + schemas in `router/src/openapi.rs`; update the OpenAPI `info` description to cover query, not just admin.
- [ ] A4. Regenerate `api/signaldb-api.json`; update the `router` golden test `openapi_spec_is_up_to_date` so it passes (`cargo test -p router openapi_spec_is_up_to_date`).
- [ ] A5. Regenerate `signaldb-sdk`; add a `signaldb-sdk` test asserting the new query client methods exist and that a client built with `new_with_client` forwards `Authorization` + `X-Tenant-ID` default headers.
- [ ] A6. Regression: existing `router` endpoint tests still pass unchanged (`cargo test -p router`) — annotation must not alter responses.
- [ ] A7. `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`.

## Phase B — Scaffold `signaldb-mcp` (PR 2, issue #624)

- [ ] B1. Streamable HTTP integration tests for `/mcp` (`cargo test -p mcp-server`): unauthenticated request → 401; valid bearer clears the auth layer and reaches the transport; `initialize` then a follow-up request on the same session; bearer + `X-Tenant-ID` extraction and downstream header propagation; session bound to its first identity — a later request resolving to a different tenant is rejected. Keep a separate test for the unauthenticated stdio credential path.
- [ ] B2. Create `src/mcp-server` workspace member + binary `signaldb-mcp`; add `rmcp`; add to workspace `members`/`default-members`.
- [ ] B3. Implement Streamable HTTP transport at `/mcp` and stdio transport; complete the `initialize` handshake advertising `tools` + `resources`.
- [ ] B4. Authenticate every request's bearer via `common::auth::Authenticator::authenticate`; fail closed (401, no session) on missing/invalid token; attach resolved `TenantContext` to the request; bind the session to its first identity and reject later identity changes.
- [ ] B5. Build the per-session `signaldb-sdk` client from the caller's bearer + `X-Tenant-ID`/`X-Dataset-ID` default headers; implement `server_info` proving the pipeline. Make B1 pass.
- [ ] B6. Add `[mcp]` config (enabled flag, bind address, router URL) to `common::config` with precedence tests; **default the bind address to loopback**; register the service via `ServiceBootstrap`.
- [ ] B7. Embed in monolithic mode (`signaldb-bin`) without port conflicts (no duplicate MCP listener); add to `scripts/run-dev.sh` and docker compose; add commented `[mcp]` to `signaldb.dist.toml`.
- [ ] B8. `cargo fmt`, clippy, machete.

## Phase C — Read tools + schema resources (PR 3, issues #625 + #626)

- [ ] C1. Failing tests (per tool): `search_traces`, `get_trace` (incl. not-found → MCP error), `search_logs`, `query_metrics`, `discover_attributes` return structured results via the SDK; malformed query → actionable error; 429 → retryable error; oversized result → truncated JSON with `truncated: true`. (`cargo test -p mcp-server`)
- [ ] C2. Implement the query tools as thin wrappers over the extended SDK, forwarding the caller's credential; optional `dataset` argument mapped to `X-Dataset-ID` and validated against the tenant context (missing → default, matching → forwarded, mismatched → access-denied); byte-budget payload caps with `truncated` flag + narrowing hint; agent-oriented tool descriptions. Make C1 pass.
- [ ] C3. Implement error mapping (D5): 400/422→bad-query, 401→session-auth failure, 403→access-denied, 404→not-found, 429→throttled, else→tool error.
- [ ] C4. Implement discovery tools (`list_datasets`/`list_schemas`/`list_tables`) and expose table schemas as MCP resources with the stable `signaldb://schema/{dataset}/{table}` URI grammar (tenant from session, not URI); `resources/read` rejects unknown/foreign URIs with a not-found error revealing no data.
- [ ] C5. Integration tests through the MCP path in `tests-integration`: tenant isolation (tenant A key, tenant B data → denied); `tools/list` availability snapshot (query + discovery tools present); discovery tools return structured tenant-scoped results; `resources/list`/`resources/read` round trip; a tenant-A resource URI read by a tenant-B session returns not-found; payload-cap truncation.
- [ ] C6. Docs (route via the docs skill): `docs/users/mcp.md` — connecting Claude Code/Claude.ai/generic clients, bearer setup, example flows; `[mcp]` config reference; add the MCP server to the README architecture/service list.
- [ ] C7. `cargo fmt`, clippy, machete.
