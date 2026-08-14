// Faceting for the Errors tab. Unlike the traces facet sidebar (which
// facets over the full un-grouped event set via its own Query IR round
// trip — see api/traceFacets.ts), the Errors tab's group list is already a
// small, fully materialized aggregate (api/errors.ts's ~200-row budget), so
// faceting is a plain client-side fold over data already in memory — no
// extra query.
import type { ErrorGroup, ErrorSource } from "../api/errors";
import { upsertBy } from "./collections";

export type ErrorFacetField =
  "exceptionType" | "serviceName" | "source" | "escaped";

export interface ErrorFilter {
  field: ErrorFacetField;
  value: string;
}

export interface ErrorFacetValue {
  value: string;
  count: number;
}

export const ERROR_FACET_FIELDS: ErrorFacetField[] = [
  "exceptionType",
  "serviceName",
  "source",
  "escaped",
];

const LABELS: Record<ErrorFacetField, string> = {
  exceptionType: "Type",
  serviceName: "Service",
  source: "Source",
  escaped: "Handled",
};

export function errorFacetLabel(field: ErrorFacetField): string {
  return LABELS[field];
}

/**
 * `exception.escaped` per OTel semconv: "true" means it escaped the scope
 * it was recorded in — i.e. it was *not* handled there — "false" means it
 * was caught. Displayed the way error-tracking tools commonly label
 * handled-vs-unhandled facets; the underlying filter value stays the raw
 * "true"/"false" string.
 */
export function errorFacetValueLabel(
  field: ErrorFacetField,
  value: string,
): string {
  if (field === "escaped") return value === "true" ? "Unhandled" : "Handled";
  return value;
}

function fieldValue(group: ErrorGroup, field: ErrorFacetField): string | null {
  if (field === "source") return group.source satisfies ErrorSource;
  return group[field];
}

function matches(group: ErrorGroup, filter: ErrorFilter): boolean {
  return fieldValue(group, filter.field) === filter.value;
}

/**
 * Add or replace a filter, at most one per field — a group has exactly one
 * value for each facet field, so two simultaneous filters on the same field
 * could never both match (an already-filtered facet's new selection
 * replaces the old one rather than stacking into an impossible AND).
 */
export function upsertErrorFilter(
  filters: ErrorFilter[],
  next: ErrorFilter,
): ErrorFilter[] {
  return upsertBy(filters, next, (f) => f.field === next.field);
}

/** Groups satisfying every active filter. */
export function applyErrorFilters(
  groups: ErrorGroup[],
  filters: ErrorFilter[],
): ErrorGroup[] {
  return groups.filter((g) => filters.every((f) => matches(g, f)));
}

/**
 * Distinct values of `field` with their summed occurrence count, ranked by
 * count. Filters on *other* facets narrow the groups counted; a filter on
 * `field` itself does not, so its alternatives stay visible and switchable
 * — same rule as the traces facet sidebar.
 */
export function errorFacetValues(
  groups: ErrorGroup[],
  field: ErrorFacetField,
  filters: ErrorFilter[],
): ErrorFacetValue[] {
  const narrowing = filters.filter((f) => f.field !== field);
  const narrowed = applyErrorFilters(groups, narrowing);
  const counts = new Map<string, number>();
  for (const g of narrowed) {
    const value = fieldValue(g, field);
    if (value === null) continue;
    counts.set(value, (counts.get(value) ?? 0) + g.count);
  }
  return [...counts.entries()]
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count);
}
