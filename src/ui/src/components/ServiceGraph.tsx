/**
 * The one graph component every service-map surface (Catalog Map, the
 * service page neighbourhood, the trace map) renders through — props-driven
 * nodes/edges, so the query-time `graph` envelope and the client-side
 * trace-to-graph derivation can both feed it. A dependency-free layered
 * left-to-right layout (longest-path from the roots), sized so a call graph
 * with mostly-forward edges reads top-to-bottom, caller-to-callee.
 */
import { useId, useMemo, useRef, useState } from "react";
import { EmptyState } from "./EmptyState";
import { useVizPointer, VizTooltip, type VizTooltipRow } from "./VizTooltip";
import { useContainerWidth } from "../hooks/useContainerWidth";
import { formatErrorRate } from "../lib/vizFormat";
import "./ServiceGraph.css";

export interface ServiceGraphNode {
  id: string;
  label: string;
  /** An uninstrumented dependency inferred from a client span (database,
   * broker, external host) rather than a service that reported spans of
   * its own. */
  external?: boolean;
  /** 0..1 error share, when known (the server `graph` envelope). Drives the
   * neutral/warn/critical dot coloring, same thresholds as an edge; falls
   * back to `failed` when absent (the trace-derived graph, which only
   * knows pass/fail per call, not a rate). */
  errorRate?: number;
  /** At least one call into this node failed. */
  failed?: boolean;
  /** Shown under the name, e.g. "420ms in service" or "120 req/s · p95 40ms"
   * — for an external node, its kind ("database", "http", …). */
  metricLine?: string;
}

export interface ServiceGraphEdge {
  from: string;
  to: string;
  /** Call count, used to scale the edge's line width. */
  count: number;
  /** 0..1 error share, when known (the server `graph` envelope). Drives the
   * neutral/warn/critical coloring; falls back to `failed` when absent (the
   * trace-derived graph, which only knows pass/fail per call, not a rate). */
  errorRate?: number;
  failed?: boolean;
  metricLine?: string;
}

export interface ServiceGraphProps {
  nodes: ServiceGraphNode[];
  edges: ServiceGraphEdge[];
  selected?: string | null;
  onNodeClick?: (id: string) => void;
  /** Filters external nodes (and edges touching them) out of the render. */
  hideExternal?: boolean;
  /** Set when the caller capped the node set (`[querier].graph_max_nodes`);
   * `total` is how many nodes existed before the cap. */
  capped?: { shown: number; total: number };
  loading?: boolean;
  error?: string;
  emptyMessage?: string;
}

const NODE_W = 168;
const NODE_H = 60;
const LAYER_GAP = 96;
const ROW_GAP = 20;
const PADDING = 24;

interface Placed {
  node: ServiceGraphNode;
  x: number;
  y: number;
}

/** Longest-path layering: a node's layer is one past its deepest incoming
 * edge, iterated to a fixed point. Capped at `nodes.length` passes so a
 * cycle (rare in a call graph, but not impossible with retries/callbacks)
 * can't loop forever — it just settles wherever the cap left it. */
function layerNodes(
  nodes: ServiceGraphNode[],
  edges: ServiceGraphEdge[],
): Map<string, number> {
  const layer = new Map(nodes.map((n) => [n.id, 0]));
  const ids = new Set(nodes.map((n) => n.id));
  const relevant = edges.filter((e) => ids.has(e.from) && ids.has(e.to));
  for (let pass = 0; pass < nodes.length; pass++) {
    let changed = false;
    for (const e of relevant) {
      const next = (layer.get(e.from) ?? 0) + 1;
      if (next > (layer.get(e.to) ?? 0)) {
        layer.set(e.to, next);
        changed = true;
      }
    }
    if (!changed) break;
  }
  return layer;
}

function layoutGraph(
  nodes: ServiceGraphNode[],
  edges: ServiceGraphEdge[],
): { placed: Placed[]; width: number; height: number } {
  const layer = layerNodes(nodes, edges);
  const byLayer = new Map<number, ServiceGraphNode[]>();
  for (const n of nodes) {
    const l = layer.get(n.id) ?? 0;
    const list = byLayer.get(l) ?? [];
    list.push(n);
    byLayer.set(l, list);
  }
  const layers = [...byLayer.keys()].sort((a, b) => a - b);
  const placed: Placed[] = [];
  let maxRows = 1;
  for (const l of layers) {
    const rows = byLayer.get(l)!;
    maxRows = Math.max(maxRows, rows.length);
    rows.forEach((node, i) => {
      placed.push({
        node,
        x: PADDING + l * (NODE_W + LAYER_GAP),
        y: PADDING + i * (NODE_H + ROW_GAP),
      });
    });
  }
  const numLayers = Math.max(1, layers.length);
  return {
    placed,
    width: PADDING * 2 + numLayers * NODE_W + (numLayers - 1) * LAYER_GAP,
    height: PADDING * 2 + maxRows * NODE_H + (maxRows - 1) * ROW_GAP,
  };
}

type Severity = "neutral" | "warn" | "critical";

/** Shared neutral/warn/critical thresholds (0.5%/2%) for a node or an edge:
 * a known error-rate fraction wins, an unknown one falls back to a bare
 * pass/fail flag (the trace-derived graph, which has no rate, only whether
 * any call failed). */
function severityFor(
  errorRate: number | undefined,
  failed: boolean | undefined,
): Severity {
  if (errorRate !== undefined) {
    if (errorRate >= 0.02) return "critical";
    if (errorRate >= 0.005) return "warn";
    return "neutral";
  }
  return failed ? "critical" : "neutral";
}

function edgeSeverity(edge: ServiceGraphEdge): Severity {
  return severityFor(edge.errorRate, edge.failed);
}

function nodeSeverity(node: ServiceGraphNode): Severity {
  return severityFor(node.errorRate, node.failed);
}

function edgeWidth(count: number, maxCount: number): number {
  if (maxCount <= 0) return 1.5;
  return 1.5 + (Math.min(count, maxCount) / maxCount) * 4.5;
}

type Hovered =
  | { kind: "node"; node: ServiceGraphNode }
  | { kind: "edge"; edge: ServiceGraphEdge };

function tooltipRows(hovered: Hovered): VizTooltipRow[] {
  if (hovered.kind === "node") {
    const rows: VizTooltipRow[] = [];
    if (hovered.node.metricLine) {
      rows.push({ label: "time", value: hovered.node.metricLine });
    }
    if (hovered.node.errorRate !== undefined) {
      rows.push({
        label: "errors",
        value: formatErrorRate(hovered.node.errorRate, 1),
      });
    } else {
      rows.push({
        label: "status",
        value: hovered.node.failed ? "failed calls seen" : "ok",
      });
    }
    if (hovered.node.external) rows.push({ label: "type", value: "external" });
    return rows;
  }
  const rows: VizTooltipRow[] = [
    { label: "calls", value: String(hovered.edge.count) },
  ];
  if (hovered.edge.errorRate !== undefined) {
    rows.push({
      label: "errors",
      value: formatErrorRate(
        Math.round(hovered.edge.errorRate * hovered.edge.count),
        hovered.edge.count,
      ),
    });
  } else if (hovered.edge.failed) {
    rows.push({ label: "status", value: "failed" });
  }
  if (hovered.edge.metricLine) {
    rows.push({ label: "latency", value: hovered.edge.metricLine });
  }
  return rows;
}

export function ServiceGraph({
  nodes,
  edges,
  selected = null,
  onNodeClick,
  hideExternal = false,
  capped,
  loading = false,
  error,
  emptyMessage = "No services seen in this range",
}: ServiceGraphProps) {
  const outerRef = useRef<HTMLDivElement>(null);
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [hovered, setHovered] = useState<Hovered | null>(null);
  const tipId = useId();
  const arrowIdBase = useId();

  const visibleNodes = useMemo(
    () => (hideExternal ? nodes.filter((n) => !n.external) : nodes),
    [nodes, hideExternal],
  );
  const visibleIds = useMemo(
    () => new Set(visibleNodes.map((n) => n.id)),
    [visibleNodes],
  );
  const visibleEdges = useMemo(
    () => edges.filter((e) => visibleIds.has(e.from) && visibleIds.has(e.to)),
    [edges, visibleIds],
  );

  const { placed, width, height } = useMemo(
    () => layoutGraph(visibleNodes, visibleEdges),
    [visibleNodes, visibleEdges],
  );
  const position = useMemo(() => {
    const m = new Map<string, Placed>();
    for (const p of placed) m.set(p.node.id, p);
    return m;
  }, [placed]);
  const maxCount = useMemo(
    () => visibleEdges.reduce((m, e) => Math.max(m, e.count), 0),
    [visibleEdges],
  );

  // Fit-to-width: never let the laid-out graph overflow its container. Only
  // scales down (a small graph in a wide panel keeps its natural size) —
  // see `useContainerWidth`, the same live-width pattern every inline-SVG
  // chart here uses.
  const containerWidth = useContainerWidth(outerRef, width);
  const scale = containerWidth > 0 ? Math.min(1, containerWidth / width) : 1;
  const scaledWidth = width * scale;
  const scaledHeight = height * scale;

  const clearHover = () => {
    setHovered(null);
    pointer.clear();
  };

  if (loading) {
    return (
      <div className="service-graph service-graph-status" aria-busy="true">
        Loading…
      </div>
    );
  }
  if (error) {
    return (
      <div
        className="service-graph service-graph-status error-text"
        role="alert"
      >
        {error}
      </div>
    );
  }
  if (visibleNodes.length === 0) {
    return (
      <div className="service-graph">
        <EmptyState title={emptyMessage} />
      </div>
    );
  }

  return (
    <div className="service-graph" ref={outerRef}>
      {capped && capped.total > capped.shown && (
        <div className="warn-callout service-graph-cap">
          Showing the busiest {capped.shown} of {capped.total} services.
        </div>
      )}
      <div
        className="service-graph-viewport"
        style={{ width: scaledWidth, height: scaledHeight }}
      >
        <div
          className="service-graph-host viz-host"
          ref={hostRef}
          style={{ width, height, transform: `scale(${scale})` }}
        >
          <svg className="service-graph-edges" width={width} height={height}>
            <defs>
              {(["neutral", "warn", "critical"] as const).map((sev) => (
                <marker
                  key={sev}
                  id={`${arrowIdBase}-${sev}`}
                  className={`sg-arrow sg-arrow-${sev}`}
                  viewBox="0 0 11 11"
                  markerWidth={11}
                  markerHeight={11}
                  markerUnits="userSpaceOnUse"
                  refX={11}
                  refY={5.5}
                  orient="auto"
                >
                  <path d="M0,0 L11,5.5 L0,11 Z" />
                </marker>
              ))}
            </defs>
            {visibleEdges.map((e) => {
              const from = position.get(e.from);
              const to = position.get(e.to);
              if (!from || !to) return null;
              const x1 = from.x + NODE_W;
              const y1 = from.y + NODE_H / 2;
              const x2 = to.x;
              const y2 = to.y + NODE_H / 2;
              const severity = edgeSeverity(e);
              return (
                <line
                  key={`${e.from}->${e.to}`}
                  className={`sg-edge sg-edge-${severity}`}
                  x1={x1}
                  y1={y1}
                  x2={x2}
                  y2={y2}
                  strokeWidth={edgeWidth(e.count, maxCount)}
                  markerEnd={`url(#${arrowIdBase}-${severity})`}
                  onPointerMove={(ev) => {
                    setHovered({ kind: "edge", edge: e });
                    pointer.track(ev);
                  }}
                  onPointerLeave={clearHover}
                />
              );
            })}
          </svg>
          {placed.map(({ node, x, y }) => {
            const severity = nodeSeverity(node);
            return (
              <button
                key={node.id}
                type="button"
                className={`sg-node${node.external ? " external" : ""}${
                  node.id === selected ? " selected" : ""
                }`}
                style={{ left: x, top: y, width: NODE_W, height: NODE_H }}
                aria-pressed={node.id === selected}
                aria-describedby={
                  hovered?.kind === "node" && hovered.node.id === node.id
                    ? tipId
                    : undefined
                }
                onClick={() => onNodeClick?.(node.id)}
                onPointerMove={(e) => {
                  setHovered({ kind: "node", node });
                  pointer.track(e);
                }}
                onFocus={(e) => {
                  setHovered({ kind: "node", node });
                  pointer.anchorTo(e.currentTarget);
                }}
                onBlur={clearHover}
                onPointerLeave={clearHover}
              >
                <span className="sg-node-name">
                  {!node.external && (
                    <span
                      className={`sg-node-dot sg-node-dot-${
                        severity === "neutral" ? "healthy" : severity
                      }`}
                      aria-hidden
                    />
                  )}
                  {node.label}
                </span>
                {node.metricLine && (
                  <span className="sg-node-metric">{node.metricLine}</span>
                )}
              </button>
            );
          })}
        </div>
        {hovered && pointer.anchor && (
          <VizTooltip
            id={tipId}
            anchor={pointer.anchor}
            host={pointer.host}
            title={
              hovered.kind === "node"
                ? hovered.node.label
                : `${hovered.edge.from} → ${hovered.edge.to}`
            }
            rows={tooltipRows(hovered)}
          />
        )}
      </div>
    </div>
  );
}
