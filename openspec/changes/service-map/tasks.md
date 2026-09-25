## 1. Graph envelope (query-ir, querier)

- [x] 1.1 Write failing `query-ir` tests: `"result": "graph"` accepted only for `traces` at IR version 8; `focus`/`depth`/`trace_id` validation (depth 1-3, `focus` and `trace_id` mutually exclusive). Verify `cargo test -p query-ir` fails.
- [x] 1.2 Add the `graph` envelope and scoping options. Verify `cargo test -p query-ir` passes.
- [x] 1.3 Write failing `querier` tests over an in-memory traces table: service edges and metrics, error rate on an edge, external nodes named and typed per the attribute order, instrumented callee yields no external node, one-hop and depth-2 focus, single-trace scope, empty graph for unknown service, node cap with warning. Verify `cargo test -p querier` fails.
- [x] 1.4 Implement graph assembly (edge, node, external-edge queries; merge; depth walk; `[querier].graph_max_nodes`). Verify `cargo test -p querier` passes.
- [x] 1.5 Add a `tests-integration` test that ingests OTLP traces for three services plus a database client span across two tenants and checks the graph and tenant isolation via `POST /api/v1/query`. Verify it passes.

## 2. API contract and clients

- [x] 2.1 Add the `graph` envelope and scoping fields to the OpenAPI document; refresh with `UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date` and verify the test passes without the env var.
- [x] 2.2 Regenerate the Rust SDK and TypeScript client with `cargo xtask generate`; verify `cargo xtask check` and `pnpm --filter signaldb-ui typecheck` pass.

## 3. UI

- [x] 3.1 Write failing tests for a client-side trace-to-graph derivation (nodes, edges, time per service, failed calls). Verify with the UI test suite on Node 24.
- [x] 3.2 Implement the derivation and a shared `ServiceGraph` component in `src/ui/src/components/` (the directory /design-sync reads), with a layered layout and VizTooltip on hover. Add `ServiceGraph.stories.tsx` covering: whole-system graph, one-hop neighbourhood, single trace, selected node, error edges at each threshold, external nodes shown and hidden, empty graph, node-cap warning, loading and error. Verify tests pass and `pnpm --filter signaldb-ui build-storybook` succeeds.
- [x] 3.3 Trace detail: Waterfall | Map | Both switch and click-to-filter, with tests for the filter and failed-call marking, and a `TracesView` story for the trace map (Both mode, failed call). Verify tests and Storybook build pass.
- [x] 3.4 Service page: neighbourhood map using the `graph` envelope with `focus` via the generated client, Map | Table switch, empty-callers state, with tests and a `CatalogView` story for the service page with the map (plus the no-callers state). Verify tests and Storybook build pass.
- [x] 3.5 Catalog: List | Map switch kept in the URL, side panel, hide-external toggle, node-cap warning, with tests and `CatalogView` stories for Map view (default, node selected, node-cap warning). Verify tests and Storybook build pass.
- [ ] 3.6 Run /design-sync to push `ServiceGraph` and the updated view stories to the "SignalDB UI" Claude Design project (`.design-sync/config.json`; add a `titleMap` entry if the story title differs from the export name). Verify the new cards render there and compare them against the mockups in the "SignalDB Design System" project, `ui_kits/console/service-map/`. **Pending**: runs separately; the change stays open until it is done.

## 4. MCP

- [x] 4.1 Write failing tests for `get_service_map` (graph plus summary, `ui://` metadata only for capable clients, web UI link) and for the services summary in `get_trace`. Verify `cargo test -p mcp-server` fails.
- [x] 4.2 Implement the tool, register `ui://signaldb/service-map` in `apps.rs`, add `service_map_url` to `ui_links.rs`, add the summary to `get_trace`, and add the "investigate a failing dependency" prompt in `prompts.rs`. Verify `cargo test -p mcp-server` passes.

## 5. CLI

- [x] 5.1 Write failing tests for `signaldb-cli services map` in each format (`table`, `json`, `dot`, `mermaid`) and the empty-graph exit path. Verify `cargo test -p signaldb-cli` fails.
- [x] 5.2 Implement the `services` command group with `map` through the SDK. Verify `cargo test -p signaldb-cli` passes and `dot -Tsvg` renders the `dot` output.

## 6. Docs and skills

- [x] 6.1 Via the docs skill, add a "Service graphs (`graph` envelope)" section to `docs/users/querying-ir.md` and a user guide page for the service map covering the UI, MCP and CLI, including the window-start caveat. Verify the docs frontmatter check passes. The graph section landed with the querier work; the guide is `docs/users/service-map.md`.
- [x] 6.2 Update the MCP `query-ir` skill and the `http-api`/`crate-map` skills if their described behavior changed. Verify `openspec validate --all` passes. The MCP `query-ir` skill is `docs/users/querying-ir.md` itself, and the `architecture` skill already describes the graph path; `crate-map` gained `query/graph.rs`; `http-api` holds design rules only and needed no change.
