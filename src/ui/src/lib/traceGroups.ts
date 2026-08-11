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

/**
 * Encodes a multi-dimension value tuple (a trace group's, or a catalog
 * entity's, identity) into one URL-safe string — the not-set marker stands
 * in for null so the round trip is lossless. Not trace-group-specific
 * despite living alongside `groupKey`: the catalog reuses it for its own
 * `catalogPrimary`/`catalogSecondary` pins, which are the same "multiple
 * dimension values, one URL param" problem.
 */
export function compositeKey(values: (string | null)[]): string {
  return values.map((v) => v ?? NOT_SET).join(KEY_SEP);
}

/**
 * Reverses `compositeKey`. A value equal to the not-set marker — or a
 * dimension past the end of the encoded key, e.g. an older link generated
 * before a dimension was added — decodes to null, not the literal string,
 * since "(not set)" means the field is absent: drilling into it must query
 * "not exists", not `eq "(not set)"`.
 */
export function parseCompositeKey(
  key: string,
  dims: string[],
): (string | null)[] {
  const parts = key.split(KEY_SEP);
  return dims.map((_, i) => {
    const v = parts[i];
    return v === undefined || v === NOT_SET ? null : v;
  });
}

/** Trace throughput over the queried range, in the unit that stays >= 1. */
export function formatRate(count: number, rangeSeconds: number): string {
  const perSec = count / rangeSeconds;
  const fmt = (v: number) => String(Number(v.toFixed(1)));
  if (perSec >= 1) return `${fmt(perSec)}/s`;
  if (perSec * 60 >= 1) return `${fmt(perSec * 60)}/min`;
  return `${fmt(perSec * 3600)}/h`;
}
