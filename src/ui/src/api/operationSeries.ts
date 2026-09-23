/**
 * Per-operation rate series for a catalog entity's breakdown table (e.g. a
 * service's `span.name` operations) — one grouped query, not one per row, so
 * a table of fifty operations costs the same round trip as a table of one.
 * Same grouped-query shape as `api/entitySparkline.ts`'s column, but keyed
 * by the breakdown field's own value rather than the entity's identity.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { pinsWhere, type EntityPin } from "./catalog";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { toLokiLabel } from "../lib/labelSuggestions";
import type { EntityTypeDef } from "../features/catalog/entityTypes";
import type { SeriesPoint } from "./entityDetailStats";

const NANOS_PER_MS = 1_000_000;

/** A breakdown value the series carries no label for — the same "(not set)"
 * a `null` identity value renders as elsewhere in the catalog. */
const NOT_SET = "(not set)";

/** One stepped count series per distinct value of `breakdownField`, pinned
 * to the entity's own scope (span kind, identity). */
export function buildOperationSeriesDoc(
  entity: EntityTypeDef,
  breakdownField: string,
  range: ResolvedRange,
  pinned: EntityPin[],
  stepSeconds: number,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: msToNanos(range.fromMs),
      to: msToNanos(range.toMs),
    },
    result: "series",
    pipeline: [
      ...(entity.spanKindScope
        ? [
            {
              where: {
                field: "span_kind",
                op: "eq",
                value: entity.spanKindScope,
              },
            },
          ]
        : []),
      ...pinsWhere(pinned),
      {
        aggregate: {
          by: [breakdownField],
          aggs: [{ fn: "count", as: "n" }],
          step: `${stepSeconds}s`,
        },
      },
    ],
  };
}

function decodePoints(points: Array<Array<unknown>>): SeriesPoint[] {
  return points.flatMap((p): SeriesPoint[] => {
    const [tNs, v] = p as [unknown, unknown];
    if (typeof tNs !== "number" || typeof v !== "number") return [];
    return [{ tMs: Math.round(tNs / NANOS_PER_MS), value: v }];
  });
}

/** One breakdown table's per-operation series, keyed by operation name. */
export type SeriesByOperation = Map<string, SeriesPoint[]>;

/** The breakdown's per-operation series, one query, indexed by operation
 * name for the caller to look up per table row. */
export async function fetchOperationSeries(
  entity: EntityTypeDef,
  breakdownField: string,
  range: ResolvedRange,
  pinned: EntityPin[],
  stepSeconds: number,
): Promise<SeriesByOperation> {
  const res: QueryIrResponse = await runIrQuery(
    buildOperationSeriesDoc(entity, breakdownField, range, pinned, stepSeconds),
  );
  const label = toLokiLabel(breakdownField);
  const byOperation: SeriesByOperation = new Map();
  for (const s of res.series ?? []) {
    byOperation.set(s.labels[label] ?? NOT_SET, decodePoints(s.points));
  }
  return byOperation;
}
