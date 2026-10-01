## Why

SignalDB has no view of how services connect. The service page shows where a service's outgoing time goes, but not who calls it, and nothing shows the whole system. Operators need that picture to find the failing hop in an incident and to understand a system they did not build. Once `query-ir-span-join` gives the IR real caller/callee edges, every first-party surface can show them.

## What Changes

- **Query IR**: a new `graph` result envelope (IR version 8, after `query-ir-span-join`) that returns services as nodes and calls as edges, each with request rate, error rate and p95 latency. Uninstrumented dependencies (databases, message brokers, external HTTP hosts) appear as `external` nodes derived from client-span attributes. The graph can be scoped to one service and a hop depth, or to a single trace.
- **UI**:
  - Catalog gets a List | Map switch; Map shows the whole system for the selected time range.
  - The service page gets a one-hop map (callers, service, dependencies) next to "Time by dependency".
  - Trace detail gets a Waterfall | Map | Both switch; Map shows the services in that trace.
- **MCP**: a `get_service_map` tool with an interactive `ui://signaldb/service-map` view, a services summary in `get_trace` output, and an "investigate a failing dependency" prompt.
- **CLI**: `signaldb-cli services map` with `table`, `json`, `dot` and `mermaid` output.
- **HTTP API**: no new endpoint; the envelope is served by `POST /api/v1/query` and described in the OpenAPI document.

Mockups: Claude Design project "SignalDB Design System", `ui_kits/console/service-map/`.

## Capabilities

### New Capabilities

- `query-ir-service-graph`: the `graph` result envelope — node and edge shape, metrics, external nodes, scoping by service/depth/trace, and limits.

### Modified Capabilities

- `explore-ui-catalog`: adds the Catalog Map view, the service-page neighbourhood map, and the trace map.
- `mcp-tool-surface`: adds `get_service_map`, its interactive view, and the services summary in `get_trace`.
- `cli-command-surface`: adds the `services map` command.

## Impact

- **query-ir**: `graph` envelope, its scoping parameters, validation.
- **querier**: graph assembly on top of the span `correlate` join plus client-span attribute grouping for external nodes.
- **router**: OpenAPI schema for the envelope; regenerated Rust SDK and TypeScript client.
- **ui**: graph component, Catalog Map view, service neighbourhood panel, trace map.
- **mcp-server**: tool, `ui://` app, `ui_links` URL, `get_trace` summary, prompt.
- **signaldb-cli**: `services` command group.
- **docs**: `querying-ir.md` graph section, a user guide page for the service map, MCP skill.
- Depends on `query-ir-span-join`. No ingest, compatibility-API, Flight or storage change. Not breaking.
