/**
 * The session detail timeline's "in browser + network" vs "backend" split
 * for a selected Network-lane event's trace — the spec's "time spent in the
 * browser and network (client span minus its first server child)". Computed
 * from the already-fetched trace's own spans (design.md decision 4's note
 * that a single selected trace's spans already carry the parent/child
 * relation, so this doesn't need a `correlate` query of its own).
 */
import type { TempoSpan } from "../../api/traceTypes";

export interface ClientServerSplit {
  /** ms spent in the browser and network — the client span's own duration
   * minus its first server child's, clamped to zero. */
  browserNetworkMs: number;
  /** ms spent in the backend — the first server-kind child's own duration. */
  backendMs: number;
  backendServiceName: string;
  backendSpanId: string;
}

const NS_PER_MS = 1_000_000;

/** `undefined` when the client span isn't in `spans`, or has no server-kind
 * child — an untraced request the caller shows plainly instead. */
export function clientServerSplit(
  spans: TempoSpan[],
  clientSpanId: string,
): ClientServerSplit | undefined {
  const client = spans.find((s) => s.spanId === clientSpanId);
  if (!client) return undefined;

  const serverChildren = spans.filter(
    (s) => s.parentSpanId === clientSpanId && s.kind === "Server",
  );
  if (serverChildren.length === 0) return undefined;

  const firstChild = serverChildren.reduce((earliest, s) =>
    BigInt(s.startNs) < BigInt(earliest.startNs) ? s : earliest,
  );

  const clientMs = Number(BigInt(client.durNs)) / NS_PER_MS;
  const backendMs = Number(BigInt(firstChild.durNs)) / NS_PER_MS;

  return {
    browserNetworkMs: Math.max(0, clientMs - backendMs),
    backendMs,
    backendServiceName: firstChild.serviceName,
    backendSpanId: firstChild.spanId,
  };
}
