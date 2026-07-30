// Grouping of trace search results along user-picked dimensions: the root
// span's name (default), its service, or any root span / resource attribute
// observed in the results — optionally two combined (e.g. span.name then
// service.name). The dimensions and selected group are URL params so a
// drill-in is deep-linkable.

import type { TraceSummary } from "../api/tempo";

export const DEFAULT_GROUP_BY = "span.name";
export const BUILTIN_DIMENSIONS = [DEFAULT_GROUP_BY, "service.name"];

/** Bucket for traces whose root span lacks the grouping attribute. */
export const NOT_SET = "(not set)";

/** Joins dimension values into a group key; unit separator avoids collisions. */
const KEY_SEP = "\u001f";

export interface TraceGroup {
  key: string;
  /** One value per grouping dimension. */
  values: string[];
  /** Newest first. */
  traces: TraceSummary[];
  /** Distinct root services in the group, sorted. */
  services: string[];
  errorCount: number;
  p50Ms: number;
  p95Ms: number;
  lastStartNs: string;
}

/** The `groupBy` URL param: comma-separated dimensions, deduplicated. */
export function parseGroupBy(param: string): string[] {
  const dims = [
    ...new Set(
      param
        .split(",")
        .map((d) => d.trim())
        .filter((d) => d !== ""),
    ),
  ];
  return dims.length === 0 ? [DEFAULT_GROUP_BY] : dims;
}

export function groupValue(t: TraceSummary, dimension: string): string {
  if (dimension === "span.name") return t.rootTraceName;
  if (dimension === "service.name") return t.rootServiceName;
  const v = t.rootAttributes[dimension];
  return v === undefined ? NOT_SET : String(v);
}

export function groupKey(t: TraceSummary, dimensions: string[]): string {
  return dimensions.map((d) => groupValue(t, d)).join(KEY_SEP);
}

/** Human form of a group key or values list. */
export function groupLabel(key: string): string {
  return key.split(KEY_SEP).join(" · ");
}

/** Nearest-rank percentile over an ascending-sorted array. */
function percentile(sorted: number[], p: number): number {
  const idx = Math.max(0, Math.ceil(p * sorted.length) - 1);
  return sorted[idx] ?? 0;
}

export function groupTraces(
  traces: TraceSummary[],
  dimensions: string[],
): TraceGroup[] {
  const byKey = new Map<string, TraceSummary[]>();
  for (const t of traces) {
    const key = groupKey(t, dimensions);
    const bucket = byKey.get(key);
    if (bucket) bucket.push(t);
    else byKey.set(key, [t]);
  }
  const groups = [...byKey.entries()].map(([key, members]): TraceGroup => {
    const sorted = [...members].sort((a, b) =>
      BigInt(a.startNs) < BigInt(b.startNs) ? 1 : -1,
    );
    const durations = members.map((t) => t.durationMs).sort((a, b) => a - b);
    return {
      key,
      values: key.split(KEY_SEP),
      traces: sorted,
      services: [...new Set(members.map((t) => t.rootServiceName))].sort(),
      errorCount: members.filter((t) => t.rootError).length,
      p50Ms: percentile(durations, 0.5),
      p95Ms: percentile(durations, 0.95),
      lastStartNs: sorted[0]?.startNs ?? "0",
    };
  });
  return groups.sort(
    (a, b) => b.traces.length - a.traces.length || a.key.localeCompare(b.key),
  );
}

/**
 * Dimensions offered by the pickers: the built-ins plus every attribute key
 * observed on a root span (resource attributes arrive prefixed "resource.").
 */
export function groupDimensions(traces: TraceSummary[]): string[] {
  const attrs = new Set<string>();
  for (const t of traces) {
    for (const key of Object.keys(t.rootAttributes)) attrs.add(key);
  }
  return [...BUILTIN_DIMENSIONS, ...[...attrs].sort()];
}

/** Trace throughput over the queried range, in the unit that stays >= 1. */
export function formatRate(count: number, rangeSeconds: number): string {
  const perSec = count / rangeSeconds;
  const fmt = (v: number) => String(Number(v.toFixed(1)));
  if (perSec >= 1) return `${fmt(perSec)}/s`;
  if (perSec * 60 >= 1) return `${fmt(perSec * 60)}/min`;
  return `${fmt(perSec * 3600)}/h`;
}
