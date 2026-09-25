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
import {
  ServiceGraph,
  type ServiceGraphEdge,
  type ServiceGraphNode,
} from "../../components/ServiceGraph";
import { QueryError } from "../../components/QueryError";
import { SkeletonLines } from "../explore/Skeleton";
import { formatRatePerSec } from "../../lib/traceGroups";
import { formatDurationMs } from "../../lib/waterfall";
import { errorRatePercent } from "./entityKpiFormat";
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

/** The line shown under a node's name: a service's own RED figures, or —
 * for an external node, which has none of its own — its dependency kind
 * (database, http, …), so an uninstrumented callee still says what it is. */
function nodeMetricLine(node: GraphNode): string | undefined {
  if (node.kind !== "service") return node.dependency_kind ?? undefined;
  const parts: string[] = [];
  if (node.request_rate != null) {
    parts.push(formatRatePerSec(node.request_rate));
  }
  if (node.error_rate != null) {
    parts.push(`${errorRatePercent(node.error_rate)} err`);
  }
  if (node.p95_ns != null) {
    parts.push(`p95 ${formatDurationMs(node.p95_ns / 1e6)}`);
  }
  return parts.length > 0 ? parts.join(" · ") : undefined;
}

function toGraphView(nodes: GraphNode[], edges: GraphEdge[]) {
  return {
    nodes: nodes.map((n): ServiceGraphNode => ({
      id: n.id,
      label: n.name,
      external: n.kind === "external",
      errorRate: n.error_rate ?? undefined,
      metricLine: nodeMetricLine(n),
    })),
    edges: edges.map((e): ServiceGraphEdge => ({
      from: e.source,
      to: e.target,
      count: e.count,
      errorRate: e.error_rate,
      metricLine:
        e.p95_ns != null
          ? `p95 ${formatDurationMs(e.p95_ns / 1e6)}`
          : undefined,
    })),
  };
}

/** Navigable, non-focus edge partners for a table row: the other node's
 * name plus its own RED figures, keyed off the edge's own call rate. */
interface NeighbourRow {
  id: string;
  name: string;
  external: boolean;
  rate: number;
  errorRate: number;
  p95Ns: number | null;
}

function callerRows(
  edges: GraphEdge[],
  focusId: string,
  byId: Map<string, GraphNode>,
): NeighbourRow[] {
  return edges
    .filter((e) => e.target === focusId)
    .map((e) => {
      const node = byId.get(e.source);
      return {
        id: e.source,
        name: node?.name ?? e.source,
        external: node?.kind === "external",
        rate: e.rate,
        errorRate: e.error_rate,
        p95Ns: e.p95_ns ?? null,
      };
    })
    .sort((a, b) => b.rate - a.rate);
}

function dependencyRows(
  edges: GraphEdge[],
  focusId: string,
  byId: Map<string, GraphNode>,
): NeighbourRow[] {
  return edges
    .filter((e) => e.source === focusId)
    .map((e) => {
      const node = byId.get(e.target);
      return {
        id: e.target,
        name: node?.name ?? e.target,
        external: node?.kind === "external",
        rate: e.rate,
        errorRate: e.error_rate,
        p95Ns: e.p95_ns ?? null,
      };
    })
    .sort((a, b) => b.rate - a.rate);
}

function NeighbourList({
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
              <th>Errors</th>
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
          nodes={query.data.nodes}
          edges={query.data.edges}
          droppedNodes={query.data.dropped_nodes ?? 0}
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
  const callers = callerRows(edges, focusId, byId);
  const dependencies = dependencyRows(edges, focusId, byId);

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
