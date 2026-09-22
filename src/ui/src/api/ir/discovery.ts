/**
 * Discovery via the Query IR's `describe` stage — what a picker can offer,
 * without reading signal data (`docs/users/querying-ir.md#discovery`).
 * Replaces the Loki label endpoints (`api/loki.ts`), the Tempo tag-name
 * endpoint (`tempoSearchTags`) and the Pyroscope discovery endpoints
 * (`api/pyroscope.ts`).
 */
import type { DiscoveredField, QueryIrRequest } from "../gen";
import { msToNanos, type ResolvedRange } from "../../lib/time";
import { runIrQuery } from "../queryIr";

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

/** Distinct metric names in the window (`metric.name` on `metrics`). */
export async function metricNames(
  range: ResolvedRange,
): Promise<DiscoveredValueView[]> {
  return values("metrics", "metric.name", range);
}

/** Distinct profile types in the window (`profile.type` on `profiles`). */
export async function profileTypes(
  range: ResolvedRange,
): Promise<DiscoveredValueView[]> {
  return values("profiles", "profile.type", range);
}
