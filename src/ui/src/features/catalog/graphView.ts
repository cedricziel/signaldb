// Maps the `graph` envelope's `GraphNode`/`GraphEdge` (api/serviceGraph.ts)
// onto the shared `ServiceGraph` component's props — used by both the
// Catalog Map view and the service page neighbourhood map, so a node/edge
// reads the same way (metric line, failed styling) on either surface.
import type { GraphEdge, GraphNode } from "../../api/gen";
import type {
  ServiceGraphEdge,
  ServiceGraphNode,
} from "../../components/ServiceGraph";
import { formatRatePerSec } from "../../lib/traceGroups";
import { formatDurationMs } from "../../lib/waterfall";
import { errorRatePercent } from "./entityKpiFormat";

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

/** One of a node's edge partners — the other node's identity plus the
 * edge's own RED figures. Shared row shape for the service page
 * neighbourhood table and the Catalog Map's side panel. */
export interface NeighbourRow {
  id: string;
  name: string;
  external: boolean;
  rate: number;
  errorRate: number;
  p95Ns: number | null;
}

/** Edges touching `focusId` on the given side, as rows keyed off the *other*
 * node's identity — `"target"` for callers (the other node is `source`),
 * `"source"` for dependencies (the other node is `target`), ranked busiest
 * first. */
export function neighbourRows(
  edges: GraphEdge[],
  focusId: string,
  byId: Map<string, GraphNode>,
  focusSide: "source" | "target",
): NeighbourRow[] {
  const otherSide = focusSide === "source" ? "target" : "source";
  return edges
    .filter((e) => e[focusSide] === focusId)
    .map((e) => {
      const otherId = e[otherSide];
      const node = byId.get(otherId);
      return {
        id: otherId,
        name: node?.name ?? otherId,
        external: node?.kind === "external",
        rate: e.rate,
        errorRate: e.error_rate,
        p95Ns: e.p95_ns ?? null,
      };
    })
    .sort((a, b) => b.rate - a.rate);
}

export function toGraphView(
  nodes: GraphNode[],
  edges: GraphEdge[],
): { nodes: ServiceGraphNode[]; edges: ServiceGraphEdge[] } {
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
