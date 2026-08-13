/**
 * "Where does this service's outbound time go" — CLIENT-span duration
 * summed and bucketed by which semconv attribute family the span carries
 * (db.system.name -> database, http.request.method -> http, rpc.system ->
 * rpc, messaging.system -> messaging), with the remainder as "other".
 *
 * No dedicated backend aggregation exists for this (Query IR's `aggregate`
 * stage groups by literal field names only, not a derived/CASE-like
 * expression), so it's five small `sum(duration)` queries — one baseline
 * over every CLIENT span, one per known category filtered by `exists` on
 * that category's attribute — combined client-side. "Other" is the
 * baseline minus the four known categories, not its own query.
 *
 * This sums raw span duration, not exclusive/self time, so concurrent
 * overlapping calls double-count — the same caveat any RED-metrics
 * duration figure in this app already has (see buildEntityDoc's p50/p95).
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

export interface DependencyCategory {
  key: string;
  label: string;
  durationNs: number;
  count: number;
}

const CATEGORIES: { key: string; label: string; attr: string }[] = [
  { key: "database", label: "Database", attr: "db.system.name" },
  { key: "http", label: "HTTP", attr: "http.request.method" },
  { key: "rpc", label: "RPC", attr: "rpc.system" },
  { key: "messaging", label: "Messaging", attr: "messaging.system" },
];

function sumCountDoc(
  serviceName: string,
  range: ResolvedRange,
  extraWhere?: Record<string, unknown>,
): QueryIrRequest {
  const pipeline: Record<string, unknown>[] = [
    { where: { field: "service.name", op: "eq", value: serviceName } },
    { where: { field: "span_kind", op: "eq", value: "Client" } },
  ];
  if (extraWhere) pipeline.push({ where: extraWhere });
  pipeline.push({
    aggregate: {
      by: ["service.name"],
      aggs: [
        { fn: "sum", of: "duration", as: "total" },
        { fn: "count", as: "n" },
      ],
    },
  });

  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "table",
    pipeline,
  };
}

function decodeSumCount(res: QueryIrResponse): {
  durationNs: number;
  count: number;
} {
  const row = res.rows?.[0] as unknown[] | undefined;
  if (!row) return { durationNs: 0, count: 0 };
  // Positional: [service.name (by), total (sum), n (count)].
  const total = row[1];
  const n = row[2];
  return {
    durationNs: typeof total === "number" ? total : 0,
    count: typeof n === "number" ? n : 0,
  };
}

export async function fetchDependencyBreakdown(
  serviceName: string,
  range: ResolvedRange,
): Promise<DependencyCategory[]> {
  const [baseline, ...perCategory] = await Promise.all([
    runIrQuery(sumCountDoc(serviceName, range)).then(decodeSumCount),
    ...CATEGORIES.map((c) =>
      runIrQuery(
        sumCountDoc(serviceName, range, { field: c.attr, op: "exists" }),
      ).then(decodeSumCount),
    ),
  ]);

  const categories: DependencyCategory[] = CATEGORIES.map((c, i) => ({
    key: c.key,
    label: c.label,
    durationNs: perCategory[i]!.durationNs,
    count: perCategory[i]!.count,
  }));

  const knownDuration = categories.reduce((sum, c) => sum + c.durationNs, 0);
  const knownCount = categories.reduce((sum, c) => sum + c.count, 0);
  const otherDuration = Math.max(0, baseline.durationNs - knownDuration);
  const otherCount = Math.max(0, baseline.count - knownCount);
  if (otherDuration > 0 || otherCount > 0) {
    categories.push({
      key: "other",
      label: "Other",
      durationNs: otherDuration,
      count: otherCount,
    });
  }

  return categories.filter((c) => c.durationNs > 0 || c.count > 0);
}
