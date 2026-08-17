/**
 * Facet value counts for the traces sidebar.
 *
 * Counts come from a Query IR `table` aggregate over the whole selected
 * window, never from the row-limited Tempo search response — a facet whose
 * counts described only the newest N traces would mislead about exactly the
 * thing it exists to guide.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { filterStages, type TraceFilter } from "../lib/traceFilters";

/** Values shown per facet before the list is disclosed as truncated. */
export const FACET_VALUE_LIMIT = 10;

export interface FacetValue {
  /** `null` is a real result: records where the field is absent. */
  value: string | null;
  count: number;
}

export interface FacetResult {
  values: FacetValue[];
  truncated: boolean;
  /**
   * False when the server could not resolve the field. It answers 200 with a
   * single group labelled `"null"` rather than erroring (#1070), which would
   * otherwise render as a bogus facet holding the window total.
   */
  resolved: boolean;
}

/**
 * Build the counting query for one facet. Other facets' active filters narrow
 * it, so the counts match what the trace list shows — but a facet never
 * filters itself, or selecting a value would collapse its own alternatives and
 * leave no way to switch.
 */
export function buildFacetDoc(
  irField: string,
  range: ResolvedRange,
  filters: TraceFilter[],
): QueryIrRequest {
  const others = filterStages(filters, irField);
  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "table",
    pipeline: [
      ...others,
      {
        aggregate: {
          by: [irField],
          aggs: [{ fn: "count", as: "n" }],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      // One over the display limit, so truncation is detectable rather than
      // a silently short list.
      { limit: FACET_VALUE_LIMIT + 1 },
    ],
  };
}

const EMPTY: FacetResult = { values: [], truncated: false, resolved: true };

export function facetFromIrResponse(res: QueryIrResponse): FacetResult {
  const rows = res.rows ?? [];
  if (rows.length === 0) return EMPTY;

  const parsed: FacetValue[] = rows.flatMap((row): FacetValue[] => {
    const [value, count] = row as [unknown, unknown];
    if (typeof count !== "number") return [];
    if (value === null || value === undefined) return [{ value: null, count }];
    return [{ value: String(value), count }];
  });

  // The unresolved-field signature: exactly one group, labelled with the
  // literal string "null". A genuine `null` arrives as an actual null, and a
  // real "null"-valued row would sit alongside others.
  if (parsed.length === 1 && parsed[0]!.value === "null") {
    return { values: [], truncated: false, resolved: false };
  }

  return {
    values: parsed.slice(0, FACET_VALUE_LIMIT),
    truncated: parsed.length > FACET_VALUE_LIMIT,
    resolved: true,
  };
}

export async function fetchFacet(
  irField: string,
  range: ResolvedRange,
  filters: TraceFilter[],
): Promise<FacetResult> {
  return facetFromIrResponse(
    await runIrQuery(buildFacetDoc(irField, range, filters)),
  );
}
