# Tasks

Shipped as three stacked PRs: A → B → C. Each phase is independently mergeable.

> **2026-08-07 sync:** Phases A, B, C's query tools, and E were already merged
> (#864, #863, #873, #892, #881, #899, #1016 and others) — this file had never
> been checked off. Checkboxes below now reflect the verified repo state, not
> just this session's work. Two gaps surfaced during the sync, both pre-existing
> and independent of Phase F: **C4/C5** (`list_datasets`/`list_schemas`/
> `list_tables` + schema resources) were never built — the live server
> explicitly comments "this server exposes no data resources" — left unchecked
> below. **EB3/EB4** (configurable stdio credential) were also never built;
> stdio shipped deliberately unauthenticated-only instead (see `main.rs`'s doc
> comment and `docs/users/mcp.md`) — left unchecked as a design deviation, not
> a bug.

## Phase A — Extend OpenAPI + `signaldb-sdk` to query (PR 1, issue #859)

- [x] A1. Add `utoipa` to `tempo-api`, `loki-api`, `prometheus-api` (workspace dep); derive `ToSchema` on the trace DTOs (`SearchResult`, `Trace`, `SpanSet`, `Span`, `SpanEvent`, `TagSearchResponse`, `TagValuesResponse`) and `IntoParams` on `SearchQueryParams` / `TraceQueryParams`.
- [x] A2. Loki and Prometheus query param + response types: shipped as a permissive `body = serde_json::Value` on the `#[utoipa::path]` responses with query params listed inline in the macro, rather than derived `ToSchema`/`IntoParams` on the DTOs — same outcome (JSON-envelope schema, no custom HTTP), simpler mechanism.
- [x] A3. Add `#[utoipa::path]` to the trace handlers (`tempo::search`, `query_single_trace`, `search_tags`, `search_tag_values`) and the Loki/Prom query handlers (`logql::query`/`query_range`, `promql::query`/`query_range`); register paths + schemas in `router/src/openapi.rs`; update the OpenAPI `info` description to cover query, not just admin.
- [x] A4. Regenerate `api/signaldb-api.json`; update the `router` golden test `openapi_spec_is_up_to_date` so it passes (`cargo test -p router openapi_spec_is_up_to_date`).
- [x] A5. Regenerate `signaldb-sdk`; add a `signaldb-sdk` test asserting the new query client methods exist and that a client built with `new_with_client` forwards `Authorization` + `X-Tenant-ID` default headers.
- [x] A6. Regression: existing `router` endpoint tests still pass unchanged (`cargo test -p router`) — annotation must not alter responses.
- [x] A7. `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`.

## Phase B — Scaffold `signaldb-mcp` (PR 2, issue #624)

- [x] B1. Streamable HTTP integration tests for `/mcp` (`cargo test -p mcp-server`): unauthenticated request → 401; valid bearer clears the auth layer and reaches the transport; `initialize` then a follow-up request on the same session; bearer + `X-Tenant-ID` extraction and downstream header propagation; session bound to its first identity — a later request resolving to a different tenant is rejected. Keep a separate test for the unauthenticated stdio credential path.
- [x] B2. Create `src/mcp-server` workspace member + binary `signaldb-mcp`; add `rmcp`; add to workspace `members`/`default-members`.
- [x] B3. Implement Streamable HTTP transport at `/mcp` and stdio transport; complete the `initialize` handshake advertising `tools` + `resources`.
- [x] B4. Forward-only auth (SDK-only, no `common`): require a bearer + `X-Tenant-ID` to be present (401 if absent); forward them to the router, which is the sole validator; bind the session to its first identity (tenant + credential hash) and reject later identity changes (403). Do not validate credentials locally.
- [x] B5. Build the per-session `signaldb-sdk` client from the caller's bearer + `X-Tenant-ID`/`X-Dataset-ID` default headers; implement `server_info` proving the pipeline. Make B1 pass.
- [x] B6. Add `[mcp]` config (enabled flag, bind address, router URL) to `common::config` with precedence tests; **default the bind address to loopback**; register the service via `ServiceBootstrap`.
- [x] B7. Deploy as a **sidecar** (separate process/container pointing at a router) — never an in-process route on the router or monolith. Ship `signaldb-mcp` in the monolithic image so it can run as a sidecar via an entrypoint override; add to `scripts/run-dev.sh` and docker compose.
- [x] B8. `cargo fmt`, clippy, machete.

## Phase C — Read tools + schema resources (PR 3, issues #625 + #626)

- [x] C1. Failing tests (per tool): `search_traces`, `get_trace` (incl. not-found → MCP error), `search_logs`, `query_metrics`, `discover_attributes` return structured results via the SDK; malformed query → actionable error; 429 → retryable error; oversized result → truncated JSON with `truncated: true`. (`cargo test -p mcp-server`)
- [x] C2. Implement the query tools as thin wrappers over the extended SDK, forwarding the caller's credential; optional `dataset` argument mapped to `X-Dataset-ID` and validated against the tenant context (missing → default, matching → forwarded, mismatched → access-denied); byte-budget payload caps with `truncated` flag + narrowing hint; agent-oriented tool descriptions. Make C1 pass.
- [x] C3. Implement error mapping (D5): 400/422→bad-query, 401→session-auth failure, 403→access-denied, 404→not-found, 429→throttled, else→tool error.
- [x] C4. **Scoped out at archive (2026-08-16); delta spec pruned accordingly, tracked in issue #626.** Discovery tools (`list_datasets`/`list_schemas`/`list_tables`) and table schemas as MCP resources with the `signaldb://schema/{dataset}/{table}` URI grammar were never built; the live server's `ServerHandler::get_info` explicitly notes it "exposes no data resources" (only the MCP Apps UI documents). Needs its own scoping pass — issue #626 is still open for it.
- [x] C5. Scoped out with C4 (issue #626).
- [x] C6. Docs (route via the docs skill): `docs/users/mcp.md` — connecting Claude Code/Claude.ai/generic clients, bearer setup, example flows; `[mcp]` config reference; add the MCP server to the README architecture/service list.
- [x] C7. `cargo fmt`, clippy, machete.

## Phase E — Loki/Prometheus query in the SDK + stdio credentials (follow-up)

Delivered as two stacked PRs, mirroring the Tempo slice (A → C).

### E-A. Extend OpenAPI + `signaldb-sdk` to Loki/Prom query (PR 1)

- [x] EA1. Loki/Prometheus query envelope + param types joined the OpenAPI document (permissive `body = serde_json::Value` pattern, per A2 above — no dedicated `ToSchema`/`IntoParams` derive was needed).
- [x] EA2. Represent the polymorphic result payload permissively (D7): `body = serde_json::Value` on the `#[utoipa::path]` response, so progenitor generates `serde_json::Value` for the result while the envelope stays typed by the handler's return type. No custom HTTP.
- [x] EA3. Annotate `logql::query`/`query_range` and `promql::query`/`query_range` with `#[utoipa::path]`; register paths + schemas in `router/src/openapi.rs`; update the `info` description.
- [x] EA4. Regenerate `api/signaldb-api.json` (golden test) and the SDK/TS client; assert the new client methods (`logql_query_range`, `promql_query`, …) exist and forward `X-Tenant-ID`.
- [x] EA5. Regression: existing `router` tests unchanged (annotation only). `cargo fmt`, clippy, machete.

### E-B. `search_logs` / `query_metrics` tools + stdio credential (PR 2)

- [x] EB1. Failing tests: `search_logs` (LogQL) and `query_metrics` (PromQL) return structured results via the SDK; malformed query → bad-query; 429 → retryable; oversized → truncated; optional `dataset` argument honored.
- [x] EB2. Implement both tools as thin wrappers over the extended SDK (no custom calls), reusing the shared credential-forwarding client, dataset selection, payload cap, and error mapping. Make EB1 pass.
- [x] EB3. **Scoped out at archive (2026-08-16); the delta spec now specifies unauthenticated dev-only stdio.** No `--token`/`--tenant`/`--dataset` (or `SIGNALDB_MCP_TOKEN`/`_TENANT`/`_DATASET`) credential was added to the standalone binary. Stdio shipped simpler: always unauthenticated, dev-only (`main.rs`: "Stdio has no per-request credential, so downstream calls carry none — dev only"). Revisit only if a real dev workflow needs it.
- [x] EB4. Scoped out with EB3.
- [x] EB5. Docs: `docs/users/mcp.md` documents stdio as unauthenticated dev-only (matching what actually shipped, not the originally-planned credentialed stdio).

## Phase F — Metric/label discovery tools

Delivered 2026-08-07, as a single pass covering the SDK extension, the MCP
tools, and — per the `client-surface-parity` capability, once the CLI's total
absence of any attribute/label discovery command (for _any_ signal, not just
the two added here) was flagged mid-implementation — a new CLI `discover`
command reaching the same capability. UI and HTTP-API parity already existed
before this change: `src/ui/src/api/prom.ts`/`loki.ts` have hand-rolled
`fetch()`-based label/metric-name discovery predating this work (outside the
generated TS client, which now also covers these operations), and the
`labels`/`label_values` HTTP endpoints themselves predate this change too —
only their OpenAPI/SDK/MCP/CLI exposure was missing.

### F-A. Extend OpenAPI + `signaldb-sdk` to Loki/Prometheus label discovery

- [x] FA1. Loki/Prometheus label-name/label-value handlers joined the OpenAPI document via the same permissive `body = serde_json::Value` + inline `params(...)` pattern as A2/EA1 — no `ToSchema`/`IntoParams` derives needed (matches the established codebase convention for `promql`/`logql` query handlers).
- [x] FA2. Annotate `logql::labels`/`logql::label_values` and `promql::labels`/`promql::label_values` with `#[utoipa::path]`; register paths + schemas in `router/src/openapi.rs`. (`/series` and `label_stats` explicitly deferred per D9 — different shape, not requested.)
- [x] FA3. Regenerate `api/signaldb-api.json` (golden test) and the SDK (`cargo xtask generate`, Rust + TS clients); `signaldb-sdk` test `client_exposes_label_discovery_builders` asserts the new client methods exist.
- [x] FA4. Regression: `cargo test -p router` (147 tests) unchanged. `cargo fmt`, clippy, machete clean across `router`/`signaldb-sdk`/`mcp-server`/`signaldb-cli`.

### F-B. Signal-aware `discover_attributes` + `discover_metrics`

- [x] FB1. Coverage via: `signaldb-sdk`'s `client_exposes_label_discovery_builders` (compile-time method-existence, mirroring the existing pattern for `discover_attributes`'s original trace-only test); `signaldb-cli`'s `discover_attributes_dispatches_per_signal_via_sdk` and `discover_metrics_queries_prometheus_name_label` (mockito-backed, exercising the _identical_ SDK call chain the MCP tool uses); `mcp-server`'s `read_tools_are_registered` (now includes `discover_metrics`). No dedicated mock-HTTP test was added inside `mcp-server` itself — that crate had none for the original `discover_attributes` either; the CLI tests exercise the same code path.
- [x] FB2. Implemented the `signal` argument on `discover_attributes` (`Signal::Traces|Logs|Metrics`, default `Traces`), dispatching to Tempo/Loki/Prometheus SDK methods per D9; implemented `discover_metrics` as a thin wrapper over `promql_label_values().name("__name__")`; reuses the shared dataset-scoping, payload cap (`json_result`), and error mapping (`map_sdk_err`).
- [x] FB3. `tests-integration`'s `query_parity.rs` re-run clean (unaffected — its manifest is query languages, not discovery tools, by spec design). `mcp-server`'s `read_tools_are_registered` asserts `discover_metrics` is present.
- [x] FB4. Docs: `docs/users/mcp.md` — updated tool table, new "signal" explanation, and a "From the CLI" section pointing at the new `signaldb-cli discover` command.
- [x] FB5. **CLI parity (added mid-implementation, not in the original plan):** `signaldb-cli discover attributes --signal traces|logs|metrics [--tag NAME]` and `signaldb-cli discover metrics`, in `src/signaldb-cli/src/commands/discover.rs`, wired through `signaldb-sdk` (reuses `query::build_http_client`). Tests: clap parsing (default signal, signal+tag, metrics subcommand, unknown signal rejected), an unreachable-endpoint error case, and two mockito-backed tests proving the dispatch reaches the right endpoint (`/tempo/api/search/tag/{name}/values`, `/prometheus/api/v1/label/__name__/values`).
