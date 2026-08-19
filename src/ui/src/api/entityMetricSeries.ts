/**
 * The metric series behind a Catalog entity's metrics panel.
 *
 * One query covers many metrics rather than one query per metric: the names
 * are regex-alternated on `metric.name` and the response is grouped by it, so
 * a host's dozen-plus metrics cost a handful of round trips instead of a
 * dozen-plus repeats of the same scan.
 *
 * Metrics with no points in the window simply produce no series. That absence
 * *is* the observed-set filter — nothing here has to zero-fill, and nothing
 * downstream can mistake "not reported" for "reported as zero".
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import type { EntityPin } from "./catalog";
import type { MetricHit } from "../features/schema/api";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

/** Names per alternation. Past this the predicate stops being readable in a
 * query log and starts being one string the planner must match linearly. */
export const METRIC_NAMES_PER_QUERY = 25;

/** One response's series, as the IR envelope carries them. */
type IrSeries = NonNullable<QueryIrResponse["series"]>;

/**
 * How to collapse a step's worth of points, per instrument.
 *
 * A gauge and an updowncounter are levels: their average over the step is the
 * level. A counter is cumulative, and averaging it produces a number that is
 * neither the count nor the rate — its step maximum at least stays monotonic
 * and readable as a total. The honest fix for counters is a rate stage in the
 * IR, which does not exist yet (see design.md, Risks).
 */
function aggFor(instrument: string): "avg" | "max" {
  return instrument === "counter" ? "max" : "avg";
}

/** Escaped for the alternation: an unescaped `.` matches any character, so
 * `system.cpu.time` would also select metrics that merely look like it. */
function escapeName(name: string): string {
  return name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
}

/**
 * The IR documents covering these metrics for one entity.
 *
 * Grouped by aggregation kind, because a document carries a single
 * `aggregate` spec — so at most two per source, plus chunking, never one per
 * metric.
 */
export function buildEntityMetricDocs(
  source: string,
  metrics: MetricHit[],
  pinned: EntityPin[],
  range: ResolvedRange,
  stepSeconds: number,
): QueryIrRequest[] {
  const byAgg = new Map<"avg" | "max", string[]>();
  for (const m of metrics) {
    const fn = aggFor(m.instrument);
    byAgg.set(fn, [...(byAgg.get(fn) ?? []), m.name]);
  }

  const docs: QueryIrRequest[] = [];
  for (const [fn, names] of byAgg) {
    for (const batch of chunk(names, METRIC_NAMES_PER_QUERY)) {
      docs.push({
        irVersion: 1,
        from: source,
        range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
        result: "series",
        pipeline: [
          {
            where: {
              field: "metric.name",
              op: "regex",
              value: `^(${batch.map(escapeName).join("|")})$`,
            },
          },
          ...pinned.map((p) => ({
            where: { field: p.field, op: "eq", value: p.value },
          })),
          {
            aggregate: {
              by: ["metric.name"],
              aggs: [{ fn, of: "metric.value", as: "v" }],
              step: `${stepSeconds}s`,
            },
          },
        ],
      });
    }
  }
  return docs;
}

/**
 * The response's series, grouped under the metric each belongs to.
 *
 * A metric split by one of its own attributes (`cpu.mode`, `disk.io.direction`)
 * keeps every series in its group: that is the shape of one metric, not
 * several.
 */
export function splitSeriesByMetric(series: IrSeries): Map<string, IrSeries> {
  const groups = new Map<string, IrSeries>();
  for (const s of series) {
    const name = s.labels["metric_name"];
    if (name === undefined) continue;
    groups.set(name, [...(groups.get(name) ?? []), s]);
  }
  return groups;
}

/** Every document's series for one source, merged under their metric names. */
export async function fetchEntityMetricSeries(
  source: string,
  metrics: MetricHit[],
  pinned: EntityPin[],
  range: ResolvedRange,
  stepSeconds: number,
): Promise<Map<string, IrSeries>> {
  const docs = buildEntityMetricDocs(
    source,
    metrics,
    pinned,
    range,
    stepSeconds,
  );
  const responses = await Promise.all(docs.map((doc) => runIrQuery(doc)));
  const merged = new Map<string, IrSeries>();
  for (const res of responses) {
    for (const [name, series] of splitSeriesByMetric(res.series ?? [])) {
      merged.set(name, [...(merged.get(name) ?? []), ...series]);
    }
  }
  return merged;
}
