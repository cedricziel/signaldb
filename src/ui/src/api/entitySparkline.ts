/**
 * The entity list's sparkline column.
 *
 * One metric, every row, one query: the series are grouped by the entity
 * type's identity dimensions, so a table of fifty containers costs the same
 * round trip as a table of one. Rows the window holds no points for get no
 * entry at all, which is what lets the cell stay empty instead of drawing a
 * flat line through zero next to columns that are real measurements.
 */
import type { QueryIrRequest } from "./gen";
import type { MetricHit } from "../features/schema/api";
import { compositeKey } from "../lib/traceGroups";
import {
  aggFor,
  HISTOGRAM_QUANTILE,
  isHistogram,
  METRIC_SOURCES,
  type IrSeries,
} from "./entityMetrics";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

/**
 * The metric this entity type's column charts.
 *
 * The first of its observed associations, in registry order — deterministic,
 * and derived rather than curated. Picking a "best" metric per entity type
 * would reintroduce exactly the hand-maintained mapping this feature exists
 * to remove, and the registry declares no such preference; the column header
 * names what was chosen so the reader is never guessing.
 */
export function headlineMetric(metrics: MetricHit[]): MetricHit | undefined {
  return metrics[0];
}

/** The `metric_name`-style label the querier returns for a logical field. */
function labelFor(field: string): string {
  return field.replace(/\./g, "_");
}

/** One query for the whole column, grouped by the identity of each row. */
export function buildSparklineDoc(
  metric: MetricHit,
  identity: string[],
  range: ResolvedRange,
  stepSeconds: number,
): QueryIrRequest {
  const head = {
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    result: "series" as const,
    pipeline: [
      { where: { field: "metric.name", op: "eq", value: metric.name } },
    ],
  };

  if (isHistogram(metric.instrument)) {
    return {
      irVersion: 3,
      from: METRIC_SOURCES.histogram,
      ...head,
      pipeline: [
        ...head.pipeline,
        {
          histogram_quantile: {
            q: HISTOGRAM_QUANTILE,
            by: identity,
            step: `${stepSeconds}s`,
            as: "p95",
          },
        },
      ],
    };
  }

  return {
    irVersion: 1,
    from: METRIC_SOURCES.scalar,
    ...head,
    pipeline: [
      ...head.pipeline,
      {
        aggregate: {
          by: identity,
          aggs: [
            {
              fn: aggFor(metric.instrument),
              of: "metric.value",
              as: "v",
            },
          ],
          step: `${stepSeconds}s`,
        },
      },
    ],
  };
}

/**
 * The column's series, keyed the way the table keys its rows — by the
 * composite of the identity values, so a cell is one lookup.
 */
export async function fetchEntitySparklines(
  metric: MetricHit,
  identity: string[],
  range: ResolvedRange,
  stepSeconds: number,
): Promise<Map<string, IrSeries>> {
  const res = await runIrQuery(
    buildSparklineDoc(metric, identity, range, stepSeconds),
  );
  const byRow = new Map<string, IrSeries>();
  for (const s of res.series ?? []) {
    const key = compositeKey(
      identity.map((f) => s.labels[labelFor(f)] ?? null),
    );
    byRow.set(key, [...(byRow.get(key) ?? []), s]);
  }
  return byRow;
}
