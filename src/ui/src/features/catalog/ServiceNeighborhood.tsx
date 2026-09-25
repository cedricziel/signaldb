// The service page's one-hop neighbourhood map ("Catalog: Service page
// neighbourhood map" requirement): the service centred, its callers to the
// left and its dependencies to the right, sourced from the `graph` envelope
// scoped to this service (`focus`/`depth=1` — see api/serviceGraph.ts). The
// same graph also drives a Map | Table switch, the table listing the same
// edges CSV-style, one row per caller/dependency.
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchServiceGraph } from "../../api/serviceGraph";
import type { GraphEdge, GraphNode } from "../../api/gen";
import { ServiceGraph } from "../../components/ServiceGraph";
import { QueryError } from "../../components/QueryError";
import { SkeletonLines } from "../explore/Skeleton";
import { formatRatePerSec } from "../../lib/traceGroups";
import { formatDurationMs } from "../../lib/waterfall";
import { errorRatePercent } from "./entityKpiFormat";
import { neighbourRows, toGraphView, type NeighbourRow } from "./graphView";
import type { ResolvedRange } from "../../lib/time";
import type { UpdateFn } from "../../lib/urlState";
import { compositeKey } from "../../lib/traceGroups";
import "./catalog.css";
// `.trace-open` (a drillable cell's link-styled button) is shared from the
// Traces tab, same reuse `MemberTable`/`OperationsTable` already make.
import "../traces/traces.css";

interface Props {
  serviceName: string;
  range: ResolvedRange;
  rangeKey: string;
  update: UpdateFn;
}

type ViewMode = "map" | "table";

/** A caller/dependency table, one row per edge partner — shared by this
 * page's Table view and the Catalog Map's side panel. */
export function NeighbourList({
  title,
  rows,
  emptyMessage,
  onOpen,
}: {
  title: string;
  rows: NeighbourRow[];
  emptyMessage: string;
  onOpen: (name: string) => void;
}) {
  return (
    <div className="neighbourhood-col">
      <div className="neighbourhood-col-head">{title}</div>
      {rows.length === 0 ? (
        <div className="view-note">{emptyMessage}</div>
      ) : (
        <table className="neighbourhood-table">
          <colgroup>
            <col className="nt-service" />
            <col className="nt-figure" />
            <col className="nt-figure" />
            <col className="nt-figure" />
          </colgroup>
          <thead>
            <tr>
              <th>Service</th>
              <th>Rate</th>
              <th>Err</th>
              <th>p95</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id}>
                <td>
                  {row.external ? (
                    <span>{row.name}</span>
                  ) : (
                    <button
                      type="button"
                      className="trace-open"
                      onClick={() => onOpen(row.name)}
                    >
                      {row.name}
                    </button>
                  )}
                </td>
                <td>{formatRatePerSec(row.rate)}</td>
                <td>{errorRatePercent(row.errorRate)}</td>
                <td>
                  {row.p95Ns != null ? formatDurationMs(row.p95Ns / 1e6) : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export function ServiceNeighborhood({
  serviceName,
  range,
  rangeKey,
  update,
}: Props) {
  const [view, setView] = useState<ViewMode>("map");
  const query = useQuery({
    queryKey: ["service-neighborhood", serviceName, rangeKey],
    queryFn: () => fetchServiceGraph(range, { focus: serviceName, depth: 1 }),
  });

  const openService = (name: string) =>
    update(
      { catalogPrimary: compositeKey([name]), catalogSecondary: "" },
      { push: true },
    );

  return (
    <div className="catalog-main map-card">
      <div className="catalog-headline">
        <span className="catalog-title">Service map</span>
        <div
          className="trace-volume-mode"
          role="group"
          aria-label="Service map view"
        >
          <button
            type="button"
            aria-pressed={view === "map"}
            onClick={() => setView("map")}
          >
            Map
          </button>
          <button
            type="button"
            aria-pressed={view === "table"}
            onClick={() => setView("table")}
          >
            Table
          </button>
        </div>
      </div>
      {query.isPending ? (
        <SkeletonLines lines={5} />
      ) : query.isError ? (
        <QueryError what="the service map" error={query.error} />
      ) : (
        <ServiceNeighborhoodBody
          serviceName={serviceName}
          nodes={query.data.graph.nodes}
          edges={query.data.graph.edges}
          droppedNodes={query.data.graph.dropped_nodes ?? 0}
          view={view}
          onOpen={openService}
        />
      )}
    </div>
  );
}

function ServiceNeighborhoodBody({
  serviceName,
  nodes,
  edges,
  droppedNodes,
  view,
  onOpen,
}: {
  serviceName: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
  droppedNodes: number;
  view: ViewMode;
  onOpen: (name: string) => void;
}) {
  const focusId =
    nodes.find((n) => n.kind === "service" && n.name === serviceName)?.id ??
    `service:${serviceName}`;
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const callers = neighbourRows(edges, focusId, byId, "target");
  const dependencies = neighbourRows(edges, focusId, byId, "source");

  if (view === "table") {
    return (
      <div className="neighbourhood-table-view">
        <NeighbourList
          title="Callers"
          rows={callers}
          emptyMessage="No callers seen in the time range"
          onOpen={onOpen}
        />
        <NeighbourList
          title="Dependencies"
          rows={dependencies}
          emptyMessage="No dependencies seen in the time range"
          onOpen={onOpen}
        />
      </div>
    );
  }

  const { nodes: sgNodes, edges: sgEdges } = toGraphView(nodes, edges);
  return (
    <>
      {callers.length === 0 && (
        <div className="view-note">No callers seen in the time range</div>
      )}
      <ServiceGraph
        nodes={sgNodes}
        edges={sgEdges}
        selected={focusId}
        capped={
          droppedNodes > 0
            ? { shown: nodes.length, total: nodes.length + droppedNodes }
            : undefined
        }
        onNodeClick={(id) => {
          const node = byId.get(id);
          if (node && node.kind === "service" && id !== focusId) {
            onOpen(node.name);
          }
        }}
      />
    </>
  );
}
