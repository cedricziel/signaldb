// Grouping of trace search results along a user-picked dimension: the root
// span's name (default), its service, or any root span / resource attribute
// observed in the results. The dimension and selected group are URL params so
// a drill-in is deep-linkable.

import type { TraceSummary } from "../api/tempo";

export const DEFAULT_GROUP_BY = "span.name";
export const BUILTIN_DIMENSIONS = [DEFAULT_GROUP_BY, "service.name"];

/** Bucket for traces whose root span lacks the grouping attribute. */
export const NOT_SET = "(not set)";

export interface TraceGroup {
  value: string;
  /** Newest first. */
  traces: TraceSummary[];
  /** Distinct root services in the group, sorted. */
  services: string[];
  p50Ms: number;
  p95Ms: number;
  lastStartNs: string;
}

export function groupValue(t: TraceSummary, dimension: string): string {
  if (dimension === "span.name") return t.rootTraceName;
  if (dimension === "service.name") return t.rootServiceName;
  const v = t.rootAttributes[dimension];
  return v === undefined ? NOT_SET : String(v);
}

/** Nearest-rank percentile over an ascending-sorted array. */
function percentile(sorted: number[], p: number): number {
  const idx = Math.max(0, Math.ceil(p * sorted.length) - 1);
  return sorted[idx] ?? 0;
}

export function groupTraces(
  traces: TraceSummary[],
  dimension: string,
): TraceGroup[] {
  const byValue = new Map<string, TraceSummary[]>();
  for (const t of traces) {
    const value = groupValue(t, dimension);
    const bucket = byValue.get(value);
    if (bucket) bucket.push(t);
    else byValue.set(value, [t]);
  }
  const groups = [...byValue.entries()].map(([value, members]): TraceGroup => {
    const sorted = [...members].sort((a, b) =>
      BigInt(a.startNs) < BigInt(b.startNs) ? 1 : -1,
    );
    const durations = members.map((t) => t.durationMs).sort((a, b) => a - b);
    return {
      value,
      traces: sorted,
      services: [...new Set(members.map((t) => t.rootServiceName))].sort(),
      p50Ms: percentile(durations, 0.5),
      p95Ms: percentile(durations, 0.95),
      lastStartNs: sorted[0]?.startNs ?? "0",
    };
  });
  return groups.sort(
    (a, b) =>
      b.traces.length - a.traces.length || a.value.localeCompare(b.value),
  );
}

/**
 * Dimensions offered by the picker: the built-ins plus every attribute key
 * observed on a root span (resource attributes arrive prefixed "resource.").
 */
export function groupDimensions(traces: TraceSummary[]): string[] {
  const attrs = new Set<string>();
  for (const t of traces) {
    for (const key of Object.keys(t.rootAttributes)) attrs.add(key);
  }
  return [...BUILTIN_DIMENSIONS, ...[...attrs].sort()];
}
