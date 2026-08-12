/**
 * Compiles the Metrics tab's structured builder query to Query IR, for the
 * subset the minimal `metrics` source covers: no range function (rate/
 * irate/increase/*_over_time have no IR pipeline-stage equivalent yet — see
 * the catalog/metrics IR plan) and no cross-series formula. Everything
 * outside that returns `null`; the caller falls back to the existing
 * PromQL path unchanged. This is what lets a plain metric-name query (e.g.
 * `signaldb.wal.entries_processed`, whose dotted OTel-native name PromQL's
 * grammar can't even lex) run at all.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import type { PromSeries } from "./prom";
import type { LabelFilter } from "../lib/filters";
import { msToNanos, type ResolvedRange } from "../lib/time";
import type { MetricQuery } from "../features/metrics/buildPromQL";

/** Builder labels come from `promLabelNames`/`promLabelValues` — Prometheus'
 * physical, underscored label spelling. The IR's `metrics` source registers
 * `service.name` as the logical alias for the physical `service_name`
 * column, and rejects the physical spelling directly (a document must name
 * a logical field, never storage — see ir_planner.rs's `SourcePlan.aliases`
 * and the resolver's physical-addressing guard). Every other label a user
 * might pick (e.g. `region`) isn't a registered physical column name, so it
 * resolves fine as a bare attribute reference without this mapping. */
function irFieldForLabel(label: string): string {
  return label === "service_name" ? "service.name" : label;
}

/** One `LabelFilter` as an IR `where` clause, or `null` for an op the IR
 * predicate grammar can't express directly (none today — kept total rather
 * than partial so a future `FilterOp` addition fails loudly here). */
function filterWhere(f: LabelFilter): Record<string, unknown> {
  const field = irFieldForLabel(f.label);
  switch (f.op) {
    case "=":
      return { field, op: "eq", value: f.value };
    case "!=":
      return { field, op: "ne", value: f.value };
    case "=~":
      return { field, op: "regex", value: f.value };
    case "!~":
      return { not: { field, op: "regex", value: f.value } };
  }
}

export function buildMetricIrDoc(
  query: MetricQuery,
  range: ResolvedRange,
  stepSeconds: number,
): QueryIrRequest | null {
  if (query.metric.trim() === "") return null;
  // rate()/irate()/increase()/*_over_time have no IR stage — stays PromQL.
  if (query.range) return null;

  const fn = query.agg?.op ?? "sum";
  // count takes no `of` field; every other space-agg aggregates the point's
  // value, via metric.value — "value" is itself the physical column name,
  // which the resolver rejects as a bare field reference (see ir_planner.rs).
  const agg =
    fn === "count" ? { fn, as: "v" } : { fn, of: "metric.value", as: "v" };

  return {
    irVersion: 1,
    from: "metrics",
    range: {
      from: msToNanos(range.fromMs),
      to: msToNanos(range.toMs),
    },
    result: "series",
    pipeline: [
      { where: { field: "metric.name", op: "eq", value: query.metric } },
      ...query.filters.map((f) => ({ where: filterWhere(f) })),
      {
        aggregate: {
          by: (query.agg?.by ?? []).map(irFieldForLabel),
          aggs: [agg],
          step: `${stepSeconds}s`,
        },
      },
    ],
  };
}

/** Adapts the IR `series` envelope to the shape `MetricsChart` already
 * renders (nanoseconds → ms, unknown → number) — so the chart component
 * itself needs no changes to accept either source. */
export function irSeriesToPromSeries(res: QueryIrResponse): PromSeries[] {
  return (res.series ?? []).map((s) => ({
    labels: s.labels,
    points: s.points.map((p): [number, number] => {
      const [tNs, v] = p;
      return [Number(tNs) / 1_000_000, Number(v)];
    }),
  }));
}
