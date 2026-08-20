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
import type { EntityTypeDef } from "../features/catalog/entityTypes";
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
 * The entity's own activity over time, for an entity type the registry names
 * no metric for.
 *
 * A service is the case that matters: OTel associates no metric with the
 * `service` entity at all, so a registry-only column is permanently empty for
 * the catalog's most-used page. Activity is not a substitute for a metric —
 * it is the shape of the count the Rate column already shows, which is a real
 * measurement of the entity and the one a reader is scanning the list for.
 *
 * Scoped exactly as that column is (a service's Server-kind spans only), or
 * the sparkline would count its outbound calls too and disagree with the
 * number beside it.
 */
export function buildActivityDoc(
  entity: EntityTypeDef,
  range: ResolvedRange,
  stepSeconds: number,
): QueryIrRequest {
  const sources = entity.sources ?? ["traces"];
  // Traces where the entity has them — that is what the Rate column counts —
  // otherwise whichever signal discovered it.
  const source = sources.includes("traces") ? "traces" : sources[0]!;
  const scoped = source === "traces" && entity.spanKindScope !== undefined;

  return {
    irVersion: 1,
    from: source,
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    result: "series",
    pipeline: [
      ...(scoped
        ? [
            {
              where: {
                field: "span_kind",
                op: "eq",
                value: entity.spanKindScope!,
              },
            },
          ]
        : []),
      {
        aggregate: {
          by: entity.identity,
          aggs: [{ fn: "count", as: "n" }],
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
  return indexByRow(res.series ?? [], identity);
}

/** The activity column's series, indexed the same way. */
export async function fetchEntityActivity(
  entity: EntityTypeDef,
  range: ResolvedRange,
  stepSeconds: number,
): Promise<Map<string, IrSeries>> {
  const res = await runIrQuery(buildActivityDoc(entity, range, stepSeconds));
  return indexByRow(res.series ?? [], entity.identity);
}

/** Series keyed the way the table keys its rows, so a cell is one lookup. */
function indexByRow(series: IrSeries, identity: string[]): Map<string, IrSeries> {
  const byRow = new Map<string, IrSeries>();
  for (const s of series) {
    const key = compositeKey(
      identity.map((f) => s.labels[labelFor(f)] ?? null),
    );
    byRow.set(key, [...(byRow.get(key) ?? []), s]);
  }
  return byRow;
}
