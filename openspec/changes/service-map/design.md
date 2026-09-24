## Context

`query-ir-span-join` adds a `correlate` stage that joins each span to its parent. The UI already derives outgoing dependencies from client-span attributes (`src/ui/src/api/dependencyTargets.ts`) and has a flamegraph-style precedent for a non-tabular envelope (`ResultEnvelope::Flamegraph`, rendered by an MCP App at `ui://signaldb/profile`). Mockups live in the Claude Design project "SignalDB Design System", `ui_kits/console/service-map/`. See proposal.md for motivation and specs for behavior.

## Goals / Non-Goals

**Goals:**
- One server-side graph definition that the UI, MCP and CLI all consume, so the three surfaces never disagree.
- A trace map that works without any backend change.

**Non-Goals:**
- Precomputed edge tables or ingest-time aggregation (revisit if graph queries are too slow at scale).
- A Grafana node-graph or Prometheus `traces_service_graph_*` compatibility feed.
- Graphs for entity types other than services, historical diffing of graphs, and a TUI graph view.

## Decisions

**A `graph` envelope, not a new endpoint.** Follows the rule that first-party reads go through the Query IR. The envelope is declared with scoping options (`focus`, `depth`, `trace_id`) on the document, and the querier builds it with fixed internal pipelines: an edge query (correlate inner, where parent service differs or span kind is server/consumer, aggregate by `parent.service_name, service_name`), a node query (server/consumer spans aggregated by `service_name`), and an external-edge query (client/producer spans left-joined to children, kept where the child is null, grouped by the naming attribute). Rejected: letting clients compose the three queries themselves. Every surface would reimplement the merge, which is what the UI's dependency table already does client-side.

**External node naming order.** `db.namespace`, `messaging.destination.name`, `rpc.service`, `server.address`, `peer.service`, matching the order the UI dependency table already uses, so today's table and the new map agree. `peer.service` is last because it is deprecated in current semantic conventions.

**Depth scoping in the querier.** For `focus` with `depth > 1`, the querier computes the full edge set once and walks it in memory; edge sets are small after aggregation. Capped at depth 3 to keep the UI readable.

**Node cap.** `[querier].graph_max_nodes`, default 200, keeping the busiest nodes by request rate. The UI shows the warning above the map.

**Trace map is client-side.** The trace detail already holds all spans. A small function derives nodes and edges from parent links and runs in the browser; the MCP `get_trace` summary uses the same derivation in Rust in the MCP server. The server `graph` envelope with `trace_id` exists for CLI and API parity but the UI does not need it.

**One graph component in the UI.** Layered left-to-right layout (callers to callees), sized nodes, edge width by call rate. Use a small layered-layout library (dagre-style) rather than a force layout: call graphs are mostly acyclic and a stable left-to-right order is easier to read. All hover content goes through the shared `VizTooltip`.

**MCP App.** `ui://signaldb/service-map` bundles the same graph component, following how the trace and profile apps are built.

**No storage or wire change.** Query-time only; Flight v1/v2 schemas and WAL/Iceberg layout are untouched. Arrow types come from DataFusion's re-exports.

## Risks / Trade-offs

- [Whole-system graph over a long window is slow] → Default the Map view to the last hour, show the node cap warning, and reuse the correlate row bound. If still too slow, ingest-time edge aggregation is the follow-up.
- [Edges lost at the window start when the parent is outside it] → Documented in the user guide; inherent to window-bounded joins.
- [External node names differ per client library] → Naming follows semantic conventions; a tenant's schema registry can be used to add aliases later.
- [Layout library adds a UI dependency] → Pick a small, permissively licensed library with few transitive dependencies.

## Open Questions

- Exact default depth for the service page if a service has more than about 20 neighbours (collapse into "+N more" vs. scroll). UI-only, can be settled during implementation.
