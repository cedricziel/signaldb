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

/**
 * Toggle `item`'s membership in `set`, returning a new `Set` either way —
 * the add-or-remove pattern behind every collapsed/opened-id toggle
 * (`FieldSidebar`'s `toggleGroup`, `TraceFacets`'s `toggle`).
 */
export function toggleInSet<T>(set: ReadonlySet<T>, item: T): Set<T> {
  const next = new Set(set);
  if (next.has(item)) next.delete(item);
  else next.add(item);
  return next;
}
