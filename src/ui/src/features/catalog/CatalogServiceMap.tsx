// The Catalog Map view: the tenant's whole service graph for the current
// window and filters, a hide-external toggle, node-cap/correlate-truncation
// warnings above the map, and a side panel on node select showing its RED
// figures, callers and dependencies with links to the service page, its
// traces and its errors.
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchServiceGraph } from "../../api/serviceGraph";
import type { GraphEdge, GraphNode, QueryWarning } from "../../api/gen";
import { ServiceGraph } from "../../components/ServiceGraph";
import { QueryError } from "../../components/QueryError";
import { SkeletonLines } from "../explore/Skeleton";
import { formatRatePerSec, compositeKey } from "../../lib/traceGroups";
import { formatDurationMs } from "../../lib/waterfall";
import { errorRatePercent } from "./entityKpiFormat";
import { neighbourRows, toGraphView } from "./graphView";
import { NeighbourList } from "./ServiceNeighborhood";
import type { ResolvedRange } from "../../lib/time";
import type { UpdateFn } from "../../lib/urlState";
import "./catalog.css";
import "../traces/traces.css";

interface Props {
  range: ResolvedRange;
  rangeKey: string;
  update: UpdateFn;
}

/** Diagnostics the Catalog Map surfaces above the graph — the node cap
 * (also reflected in `ServiceGraph`'s own "showing busiest N of M" note)
 * and the span-join row cap the `graph` envelope's `correlate` stage hits
 * on a large window. */
const SURFACED_WARNING_CODES = new Set([
  "graph_node_limit",
  "correlate_row_limit",
]);

function relevantWarnings(warnings: QueryWarning[]): QueryWarning[] {
  return warnings.filter((w) => SURFACED_WARNING_CODES.has(w.code));
}

export function CatalogServiceMap({ range, rangeKey, update }: Props) {
  const [hideExternal, setHideExternal] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const query = useQuery({
    queryKey: ["catalog-service-map", rangeKey],
    queryFn: () => fetchServiceGraph(range, {}),
  });

  const openService = (name: string) =>
    update(
      { catalogPrimary: compositeKey([name]), catalogSecondary: "" },
      { push: true },
    );
  const openTraces = (name: string) =>
    update(
      {
        signal: "traces",
        traceFilters: [{ field: "service.name", value: name }],
      },
      { push: true },
    );
  const openErrors = (name: string) =>
    update(
      {
        signal: "errors",
        filters: [{ label: "service.name", op: "=", value: name }],
      },
      { push: true },
    );

  return (
    <div className="catalog-main">
      <div className="catalog-headline">
        <span className="catalog-title">Services</span>
        <label className="catalog-map-hide-external">
          <input
            type="checkbox"
            checked={hideExternal}
            onChange={(e) => setHideExternal(e.target.checked)}
          />
          Hide external
        </label>
      </div>
      {query.isPending ? (
        <SkeletonLines lines={8} />
      ) : query.isError ? (
        <QueryError what="the service map" error={query.error} />
      ) : (
        <CatalogServiceMapBody
          nodes={query.data.graph.nodes}
          edges={query.data.graph.edges}
          droppedNodes={query.data.graph.dropped_nodes ?? 0}
          warnings={relevantWarnings(query.data.warnings)}
          hideExternal={hideExternal}
          selectedId={selectedId}
          onSelect={setSelectedId}
          onOpenService={openService}
          onOpenTraces={openTraces}
          onOpenErrors={openErrors}
        />
      )}
    </div>
  );
}

function CatalogServiceMapBody({
  nodes,
  edges,
  droppedNodes,
  warnings,
  hideExternal,
  selectedId,
  onSelect,
  onOpenService,
  onOpenTraces,
  onOpenErrors,
}: {
  nodes: GraphNode[];
  edges: GraphEdge[];
  droppedNodes: number;
  warnings: QueryWarning[];
  hideExternal: boolean;
  selectedId: string | null;
  onSelect: (id: string | null) => void;
  onOpenService: (name: string) => void;
  onOpenTraces: (name: string) => void;
  onOpenErrors: (name: string) => void;
}) {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const selected = selectedId ? byId.get(selectedId) : undefined;
  const { nodes: sgNodes, edges: sgEdges } = toGraphView(nodes, edges);

  return (
    <div className={`catalog-map-layout${selected ? " with-panel" : ""}`}>
      <div>
        {warnings.map((w) => (
          <div key={w.code} className="view-note catalog-map-warning">
            {w.message}
          </div>
        ))}
        <ServiceGraph
          nodes={sgNodes}
          edges={sgEdges}
          hideExternal={hideExternal}
          selected={selectedId}
          capped={
            droppedNodes > 0
              ? { shown: nodes.length, total: nodes.length + droppedNodes }
              : undefined
          }
          onNodeClick={onSelect}
        />
      </div>
      {selected && (
        <SidePanel
          node={selected}
          edges={edges}
          byId={byId}
          onClose={() => onSelect(null)}
          onOpenService={onOpenService}
          onOpenTraces={onOpenTraces}
          onOpenErrors={onOpenErrors}
        />
      )}
    </div>
  );
}

function SidePanel({
  node,
  edges,
  byId,
  onClose,
  onOpenService,
  onOpenTraces,
  onOpenErrors,
}: {
  node: GraphNode;
  edges: GraphEdge[];
  byId: Map<string, GraphNode>;
  onClose: () => void;
  onOpenService: (name: string) => void;
  onOpenTraces: (name: string) => void;
  onOpenErrors: (name: string) => void;
}) {
  const callers = neighbourRows(edges, node.id, byId, "target");
  const dependencies = neighbourRows(edges, node.id, byId, "source");
  const isService = node.kind === "service";

  return (
    <aside className="catalog-map-panel" aria-label={`${node.name} details`}>
      <div className="catalog-headline">
        <span className="catalog-title">{node.name}</span>
        <button type="button" className="btn" onClick={onClose}>
          Close
        </button>
      </div>
      {isService ? (
        <dl className="catalog-map-panel-stats">
          <div>
            <dt>Rate</dt>
            <dd>
              {node.request_rate != null
                ? formatRatePerSec(node.request_rate)
                : "—"}
            </dd>
          </div>
          <div>
            <dt>Errors</dt>
            <dd>
              {node.error_rate != null
                ? errorRatePercent(node.error_rate)
                : "—"}
            </dd>
          </div>
          <div>
            <dt>p95</dt>
            <dd>
              {node.p95_ns != null ? formatDurationMs(node.p95_ns / 1e6) : "—"}
            </dd>
          </div>
        </dl>
      ) : (
        <div className="view-note">
          External dependency ({node.dependency_kind ?? "other"})
        </div>
      )}
      <NeighbourList
        title="Callers"
        rows={callers}
        emptyMessage="No callers seen in the time range"
        onOpen={onOpenService}
      />
      <NeighbourList
        title="Dependencies"
        rows={dependencies}
        emptyMessage="No dependencies seen in the time range"
        onOpen={onOpenService}
      />
      {isService && (
        <div className="entity-detail-actions">
          <button className="btn" onClick={() => onOpenService(node.name)}>
            Service page
          </button>
          <button className="btn" onClick={() => onOpenTraces(node.name)}>
            Traces
          </button>
          <button className="btn" onClick={() => onOpenErrors(node.name)}>
            Errors
          </button>
        </div>
      )}
    </aside>
  );
}
