// Small generic helpers shared across the filter/facet models
// (lib/filters.ts, lib/errorFacets.ts, lib/traceFilters.ts).

/** Escape `\` and `"` for embedding in a double-quoted LogQL/TraceQL string literal. */
export function escapeQuotedString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/**
 * Add `next` to `items`, or replace the first element `matches` identifies —
 * the "add or replace" pattern used by every filter/facet upsert.
 */
export function upsertBy<T>(
  items: T[],
  next: T,
  matches: (item: T) => boolean,
): T[] {
  const idx = items.findIndex(matches);
  if (idx === -1) return [...items, next];
  const copy = [...items];
  copy[idx] = next;
  return copy;
}
