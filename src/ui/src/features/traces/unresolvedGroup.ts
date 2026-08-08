/**
 * Guard against #1070: when a grouping dimension cannot be resolved (a typo
 * in the free-text "Custom dimension" field, most likely), the server does
 * not error — it answers 200 with a single group labelled `null`, holding
 * the count for the *entire window*. Rendered naively that reads as a real
 * "(not set)" row covering everything: a silent wrong answer.
 *
 * The trap: a dimension that IS resolvable, but that every remaining record
 * (after filters) simply lacks, also collapses to a single null-labelled
 * group — legitimately. Neither "there is a null group" nor "the result has
 * one group" alone tells the two apart. The distinguishing signature is:
 * exactly one group, every dimension value null, AND its count equals the
 * window total under the same scope. The last part needs a companion
 * query — this module builds and runs it.
 *
 * Kept local to the traces feature rather than in api/traceGroups.ts: it
 * exists only to feed this one guard, not as a general-purpose aggregate.
 */
import type { QueryIrRequest, QueryIrResponse } from "../../api/gen";
import { runIrQuery } from "../../api/queryIr";
import {
  ROOT_SPAN_SENTINEL,
  type GroupGrain,
  type TraceGroup,
} from "../../api/traceGroups";
import { msToNanos, type ResolvedRange } from "../../lib/time";
import { facetField, type TraceFilter } from "../../lib/traceFilters";

/**
 * True when a group result has the *shape* an unresolved dimension produces:
 * one group, every value null. Does not check the count — see
 * `buildWindowTotalDoc`/`fetchWindowTotal` for the other half of the
 * signature.
 */
export function looksUnresolved(groups: TraceGroup[]): boolean {
  return groups.length === 1 && groups[0]!.values.every((v) => v === null);
}

/**
 * The same scope `buildGroupDoc` builds (grain predicate + active filters,
 * range) but ungrouped (`by: []`) and without order/limit — a single-row
 * count for the whole window under identical conditions.
 */
export function buildWindowTotalDoc(
  range: ResolvedRange,
  filters: TraceFilter[],
  grain: GroupGrain,
): QueryIrRequest {
  const scope: Record<string, unknown>[] =
    grain === "traces"
      ? [
          {
            where: {
              field: "parent_span_id",
              op: "eq",
              value: ROOT_SPAN_SENTINEL,
            },
          },
        ]
      : [];

  const active = filters.flatMap((f) => {
    const facet = facetField(f.field);
    if (!facet) return [];
    return [
      { where: { field: facet.irField, op: "eq", value: f.value } },
    ] as Record<string, unknown>[];
  });

  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "table",
    pipeline: [
      ...scope,
      ...active,
      { aggregate: { by: [], aggs: [{ fn: "count", as: "n" }] } },
    ],
  };
}

function totalFromIrResponse(res: QueryIrResponse): number {
  const n = res.rows?.[0]?.[0];
  return typeof n === "number" ? n : 0;
}

export async function fetchWindowTotal(
  range: ResolvedRange,
  filters: TraceFilter[],
  grain: GroupGrain,
): Promise<number> {
  return totalFromIrResponse(
    await runIrQuery(buildWindowTotalDoc(range, filters, grain)),
  );
}
