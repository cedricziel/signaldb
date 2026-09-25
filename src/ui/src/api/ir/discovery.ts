/**
 * Discovery via the Query IR's `describe` and `aggregate` stages — what a
 * picker can offer, without reading signal data
 * (`docs/users/querying-ir.md#discovery`). Replaces the Loki label endpoints
 * (`api/loki.ts`), the Tempo tag-name endpoint (`tempoSearchTags`) and the
 * Pyroscope discovery endpoints (formerly `api/pyroscope.ts`, types now in `api/profileTypes.ts`).
 */
import type { QueryIrRequest, QueryIrResponse, DiscoveredField } from "../gen";
import { msToNanos, type ResolvedRange } from "../../lib/time";
import { runIrQuery } from "../queryIr";
import type { ProfileType } from "../profileTypes";

const IR_VERSION = 4;

function describeDoc(
  source: string,
  range: ResolvedRange,
  target: Record<string, unknown>,
): QueryIrRequest {
  return {
    irVersion: IR_VERSION,
    from: source,
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    result: "metadata",
    pipeline: [{ describe: target }],
  };
}

/** A value suggestion, plus whether it comes from a bounded/approximate
 * tier — a picker shows this as "partial list" rather than claiming
 * completeness. */
export interface DiscoveredValueView {
  value: string;
  partial: boolean;
}

/** Every field `source` can be filtered or projected on, declared-first. */
export async function fields(
  source: string,
  range: ResolvedRange,
): Promise<DiscoveredField[]> {
  const res = await runIrQuery(
    describeDoc(source, range, { target: "fields" }),
  );
  return res.metadata?.fields ?? [];
}

/** Suggested values for `field` on `source`. `partial: true` covers every
 * tier short of a free, exact, declared value set (statistics sketches and
 * sampled scans alike) — the picker hint is the same either way. */
export async function values(
  source: string,
  field: string,
  range: ResolvedRange,
  limit?: number,
): Promise<DiscoveredValueView[]> {
  const res = await runIrQuery(
    describeDoc(source, range, {
      target: "values",
      field,
      ...(limit != null ? { limit } : {}),
    }),
  );
  const meta = res.metadata;
  if (!meta) return [];
  return (meta.values ?? []).map((v) => ({
    value: v.value,
    partial: meta.cost.approximate,
  }));
}

/** A discovered metric name plus whether the metrics-query builder can
 * actually chart it: names that exist only in `metrics_histogram` are worth
 * surfacing (so a user searching for one learns it exists) but the builder
 * always compiles to the `metrics` source, so picking one would silently
 * return nothing — `chartable: false` lets a picker show, but not let you
 * pick, that case. */
export interface DiscoveredMetricName extends DiscoveredValueView {
  chartable: boolean;
}

/** Distinct metric names in the window: `metric.name` on `metrics` (gauges
 * and sums) unioned with `metrics_histogram`, which is a separate IR source
 * (its bucketed row shape has no place in `metrics`'s scalar-value schema —
 * see `SourcePlan::for_source` in the querier) and so needs its own
 * discovery call to be part of name suggestions at all. */
export async function metricNames(
  range: ResolvedRange,
): Promise<DiscoveredMetricName[]> {
  const [scalar, histogram] = await Promise.all([
    values("metrics", "metric.name", range),
    values("metrics_histogram", "metric.name", range),
  ]);
  const byValue = new Map<string, DiscoveredMetricName>();
  for (const v of scalar) {
    byValue.set(v.value, { ...v, chartable: true });
  }
  for (const v of histogram) {
    const existing = byValue.get(v.value);
    if (!existing) {
      byValue.set(v.value, { ...v, chartable: false });
    } else if (existing.partial && !v.partial) {
      byValue.set(v.value, { ...existing, partial: false });
    }
  }
  return [...byValue.values()];
}

/**
 * Distinct profile types in the window, as the `ProfilesView` picker shape:
 * one entry per `(sample.type, sample.unit, period.type, period.unit)`
 * combination on `profiles`, grouped via `aggregate` rather than `describe
 * values` — the picker needs the unit fields alongside the type, and
 * `describe` only answers one field at a time.
 */
export async function profileTypes(
  range: ResolvedRange,
): Promise<ProfileType[]> {
  const res = await runIrQuery({
    irVersion: IR_VERSION,
    from: "profiles",
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    result: "table",
    pipeline: [
      {
        aggregate: {
          by: ["sample.type", "sample.unit", "period.type", "period.unit"],
          aggs: [{ fn: "count", as: "n" }],
        },
      },
    ],
  });
  return profileTypesFromRows(res);
}

function profileTypesFromRows(res: QueryIrResponse): ProfileType[] {
  const rows = res.rows ?? [];
  const seen = new Set<string>();
  const out: ProfileType[] = [];
  for (const row of rows) {
    const [sampleType, sampleUnit, periodType, periodUnit] = row as [
      unknown,
      unknown,
      unknown,
      unknown,
      unknown, // count, unused
    ];
    if (typeof sampleType !== "string" || sampleType === "") continue;
    const unit = typeof sampleUnit === "string" ? sampleUnit : "";
    const id = unit ? `${sampleType}:${unit}` : sampleType;
    if (seen.has(id)) continue;
    seen.add(id);
    out.push({
      ID: id,
      name: sampleType,
      sampleType,
      sampleUnit: unit,
      periodType: typeof periodType === "string" ? periodType : undefined,
      periodUnit: typeof periodUnit === "string" ? periodUnit : undefined,
    });
  }
  return out;
}
