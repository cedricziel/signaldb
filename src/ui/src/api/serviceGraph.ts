// The `graph` envelope (`docs/users/querying-ir.md`'s "Service graphs"
// section) over `POST /api/v1/query`: one server-side graph definition the
// Catalog Map and service page neighbourhood map both render through the
// shared `ServiceGraph` component.
import type { ServiceGraph } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

const GRAPH_IR_VERSION = 8;

export interface FetchServiceGraphOptions {
  /** Restrict to this service's neighbourhood — omitted for the whole-tenant
   * graph the Catalog Map view shows. */
  focus?: string;
  /** Hops from `focus`, 1-3. Ignored (server default: 1) without `focus`. */
  depth?: number;
}

export async function fetchServiceGraph(
  range: ResolvedRange,
  options: FetchServiceGraphOptions = {},
): Promise<ServiceGraph> {
  const res = await runIrQuery({
    from: "traces",
    irVersion: GRAPH_IR_VERSION,
    result: "graph",
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    ...(options.focus !== undefined ? { focus: options.focus } : {}),
    ...(options.depth !== undefined ? { depth: options.depth } : {}),
  });
  return res.graph ?? { nodes: [], edges: [] };
}
