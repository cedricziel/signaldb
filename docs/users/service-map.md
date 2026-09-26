---
audience: user
type: how-to
status: living
sources:
  - src/querier/src/query/graph.rs
  - src/common/src/service_graph.rs
  - src/ui/src/components/ServiceGraph.tsx
  - src/ui/src/features/catalog/CatalogServiceMap.tsx
  - src/ui/src/features/catalog/ServiceNeighborhood.tsx
  - src/signaldb-cli/src/commands/services.rs
---

# Service map

The service map shows which services call which, and which databases, brokers
and other dependencies they call, built from the traces you already send. You
can open it in the UI, ask for it over the API, get it from an agent through
MCP, or print it with the CLI. All four run the same server-side query, the
Query IR [`graph` envelope](querying-ir.md#graph-envelope-traces-only-ir-v8).

## How the map is built

- **Service edges.** An edge `A → B` counts the server and consumer spans of
  `B` whose parent span belongs to `A`.
- **External nodes.** A client or producer span with no server or consumer
  child becomes an edge to an external node: a dependency that sends no spans
  of its own. The node takes its name from the first attribute present out
  of `db.namespace`, `messaging.destination.name`, `rpc.service`,
  `server.address` and `peer.service`. If none is set, the node is named
  `unnamed <kind>` and belongs to its caller, so two services with unnamed
  HTTP calls get two nodes, not one.

The [graph envelope reference](querying-ir.md#response) has the node ids,
dependency kinds and every response field.

## Reading the map

- Rates are calls per second over the selected window.
- Edge thickness follows call rate.
- Nodes and edges are coloured by error rate: neutral below 0.5%, warning
  from 0.5%, critical from 2%.
- External nodes have a dashed border.

## Open the map

### In the UI

- **At a glance:** the [Overview](explore-ui.md#the-overview) (`/overview`,
  the UI's landing page) shows the whole-system map scoped to the selected
  environment, with zoom buttons, ⌘/Ctrl + scroll to zoom and drag to pan.
  Click a node to open that service's catalog entry.
- **Whole system:** Catalog → Services → **Map**. The URL keeps
  `?cview=map`, so the link reopens the map. Click a node for its side panel;
  use **Hide external** to show only instrumented services.
- **One service:** each service page has a **Service map** panel next to
  **Time by dependency**, showing its direct callers and callees, with a
  **Map | Table** switch.
- **One trace:** trace detail has a **Waterfall | Map | Both** switch. Click a
  service node to filter the waterfall to its spans.

[Explore UI](explore-ui.md) describes each view in full.

### Over the API

Send a `traces` document with `"result": "graph"` at IR version 8 to
`POST /api/v1/query`. Scope it with `focus` and `depth`, or with `trace_id`:

```json
{
  "irVersion": 8,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "graph",
  "focus": "checkout",
  "depth": 2,
  "pipeline": []
}
```

See [Graph envelope](querying-ir.md#graph-envelope-traces-only-ir-v8) for the
rules and the response.

### From an agent (MCP)

Call the `get_service_map` tool, optionally with `service` and `depth`.
Clients that support MCP Apps render it as an interactive map. `get_trace`
also returns a summary of the services in the trace, and the
`investigate_failing_dependency` prompt starts from the map. See
[MCP server](mcp.md).

### From the CLI

```bash
signaldb-cli services map --service checkout --depth 2
```

`--format` takes `table` (default), `json`, `dot` or `mermaid`; `--trace-id`
maps one trace. See the [CLI section](querying-ir.md#cli) for every flag.

## Limits

- **Node cap.** A map holds at most `[querier].graph_max_nodes` nodes
  (default 200). Past that it keeps the focused service, then the busiest
  nodes. The response carries a `graph_node_limit` warning, and the UI shows a
  notice above the map.
- **Row cap.** The joins behind the map stop at
  `[querier].correlate_max_rows` rows (default 5,000,000). The map is then
  built from partial data and the response carries a `correlate_row_limit`
  warning. Some calls into instrumented services can show up as external
  edges.
- **Window start.** A call whose caller span started before the window has no
  parent to join, so it is missing from its edge.
- **Window end.** A call still in flight when the window ends shows up as an
  edge to an external node, because its callee span starts after the window.

A wider window reduces both window effects.
