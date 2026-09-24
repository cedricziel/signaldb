// Pure client-side derivation of a service graph from a trace's already-
// loaded spans — no extra request. Mirrors the shape of the (future) server
// `graph` envelope closely enough that ServiceGraph can render either.

import type { TempoSpan } from "../api/traceTypes";

export interface GraphNode {
  service: string;
  /** Sum of this service's own span durations in the trace, in ms. */
  durationMs: number;
  spanCount: number;
  /** True when any span on this service errored. */
  failed: boolean;
  /** Instrumented services are never external in a trace-derived graph. */
  external: boolean;
}

export interface GraphEdge {
  from: string;
  to: string;
  /** Number of calls from `from` to `to` in this trace. */
  count: number;
  /** True when at least one of the calls errored. */
  failed: boolean;
}

export interface ServiceGraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

/**
 * Nodes are services with total time spent; edges are parent->child calls
 * between *different* services, found by walking past same-service spans
 * (an internal span nested under its own service's server span does not
 * break the caller/callee relationship). An edge is failed when any of the
 * calls it summarizes reached a span with error status.
 */
export function traceToGraph(spans: TempoSpan[]): ServiceGraphData {
  if (spans.length === 0) return { nodes: [], edges: [] };

  const byId = new Map(spans.map((s) => [s.spanId, s]));

  const nodes = new Map<string, GraphNode>();
  const node = (service: string): GraphNode => {
    let n = nodes.get(service);
    if (!n) {
      n = {
        service,
        durationMs: 0,
        spanCount: 0,
        failed: false,
        external: false,
      };
      nodes.set(service, n);
    }
    return n;
  };

  for (const span of spans) {
    const n = node(span.serviceName);
    n.durationMs += Number(BigInt(span.durNs)) / 1e6;
    n.spanCount += 1;
    if (span.status === "error") n.failed = true;
  }

  // The direct parent's service — same-service parent/child pairs are
  // collapsed into the child's own node rather than drawn as a self-edge.
  const callerService = (span: TempoSpan): string | null => {
    if (span.parentSpanId === null) return null;
    return byId.get(span.parentSpanId)?.serviceName ?? null;
  };

  const edges = new Map<string, GraphEdge>();
  for (const span of spans) {
    const from = callerService(span);
    if (from === null || from === span.serviceName) continue;
    const key = `${from}\u0000${span.serviceName}`;
    let edge = edges.get(key);
    if (!edge) {
      edge = { from, to: span.serviceName, count: 0, failed: false };
      edges.set(key, edge);
    }
    edge.count += 1;
    if (span.status === "error") edge.failed = true;
  }

  return { nodes: [...nodes.values()], edges: [...edges.values()] };
}
