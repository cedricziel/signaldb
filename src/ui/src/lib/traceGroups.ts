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

/**
 * Joins dimension values into a group key; unit separator avoids collisions.
 * Exported so the server-driven group table (which has no `TraceSummary` to
 * run `groupKey` over) can build the identical key from a `TraceGroup`'s
 * `values` array.
 */
export const KEY_SEP = "\u001f";

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

/** Trace throughput over the queried range, in the unit that stays >= 1. */
export function formatRate(count: number, rangeSeconds: number): string {
  const perSec = count / rangeSeconds;
  const fmt = (v: number) => String(Number(v.toFixed(1)));
  if (perSec >= 1) return `${fmt(perSec)}/s`;
  if (perSec * 60 >= 1) return `${fmt(perSec * 60)}/min`;
  return `${fmt(perSec * 3600)}/h`;
}
