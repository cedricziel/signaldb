/**
 * Compiles the Metrics tab's structured builder query/queries to Query IR
 * (see docs/users/querying-ir.md). A solo query with no formula compiles to
 * a single-document request; a formula, or more than one query, compiles to
 * the multi-query `{queries, formulas, result: "series"}` shape (D5). Every
 * builder option here has an IR equivalent — the per-series range functions
 * (`rate`/`increase`/`irate`/`*_over_time`, IR v6/v7's `aggregate` stage),
 * IR v7's `across` reducer and `window` lookback — see `RangeFn`/`RangeFnSpec`
 * in `features/metrics/metricQuery.ts`.
 */
import type {
  MultiQueryIrRequest,
  QueryFormula,
  QueryIrRequest,
  QueryIrResponse,
} from "../gen";
import type { LabelFilter } from "../../lib/filters";
import { msToNanos, type ResolvedRange } from "../../lib/time";
import {
  isCompilableFilter,
  type MetricQuery,
} from "../../features/metrics/metricQuery";

export interface PromSeries {
  labels: Record<string, string>;
  /** [timestampMs, value] pairs, ascending. */
  points: [number, number][];
}

/** Prometheus-style series label: `name{k="v", …}` — used for the chart
 * legend/tooltip regardless of which path produced the series. */
export function seriesName(labels: Record<string, string>): string {
  const { __name__: name, ...rest } = labels;
  const pairs = Object.entries(rest)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${v}"`);
  if (pairs.length === 0) return name ?? "value";
  return `${name ?? ""}{${pairs.join(", ")}}`;
}

/** A user can still free-type a label the picker didn't offer, so this stays
 * a safety net for Prometheus' physical, underscored spelling even though
 * the picker itself (`api/ir/discovery.ts`) now offers logical names
 * directly. The IR's `metrics` source registers
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

const V6_RANGE_FNS = ["rate", "increase"];

/** The lowest `irVersion` a row's range function needs: `across`/`window`
 * (IR v7) or a v7-only function (`irate`/`*_over_time`) need 7; plain
 * `rate`/`increase` need 6; no range function needs 1. */
function rangeIrVersion(range: MetricQuery["range"]): number {
  if (!range) return 1;
  if (range.across !== undefined || range.window !== undefined) return 7;
  return V6_RANGE_FNS.includes(range.fn) ? 6 : 7;
}

/** Compile one builder row to an IR document. Returns `null` when no metric
 * is selected yet, so callers can gate the run on a non-empty result. */
export function buildMetricIrDoc(
  query: MetricQuery,
  range: ResolvedRange,
  stepSeconds: number,
): QueryIrRequest | null {
  if (query.metric.trim() === "") return null;

  // A per-series range function (rate/increase, IR v6; irate/*_over_time,
  // IR v7) replaces the outer space-aggregation function: it's computed per
  // individual series and then folded by `by` via its own `across` reducer,
  // the same partitioning a plain space aggregate uses — see
  // docs/users/querying-ir.md, "Counter rate" and "More range functions".
  const fn = query.range ? query.range.fn : (query.agg?.op ?? "sum");
  // count takes no `of` field; every other aggregate (including the range
  // functions) aggregates the point's value, via metric.value — "value" is
  // itself the physical column name, which the resolver rejects as a bare
  // field reference (see ir_planner.rs).
  const agg: Record<string, unknown> =
    fn === "count" ? { fn, as: "v" } : { fn, of: "metric.value", as: "v" };
  if (query.range?.across !== undefined) {
    agg.across = query.range.across;
  }
  if (query.range?.window !== undefined) {
    agg.window = query.range.window;
  }

  return {
    irVersion: rangeIrVersion(query.range),
    from: "metrics",
    range: {
      from: msToNanos(range.fromMs),
      to: msToNanos(range.toMs),
    },
    result: "series",
    pipeline: [
      { where: { field: "metric.name", op: "eq", value: query.metric } },
      ...query.filters
        .filter(isCompilableFilter)
        .map((f) => ({ where: filterWhere(f) })),
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

/**
 * Compile a set of builder rows plus a formula expression to a multi-query
 * IR document (D5). Each query letter referenced by the formula's grammar
 * (`+ - * /`, parentheses, numeric constants, and the request's own query
 * names) becomes a named inner query keyed by its ref letter. Returns `null`
 * when the formula is blank (the caller should run the solo query instead)
 * or when any query the formula could reference has no metric selected yet.
 */
export function buildFormulaIrDoc(
  queries: MetricQuery[],
  formula: string,
  range: ResolvedRange,
  stepSeconds: number,
): MultiQueryIrRequest | null {
  const expr = formula.trim();
  if (expr === "") return null;

  const irQueries: Record<string, QueryIrRequest> = {};
  for (const q of queries) {
    const doc = buildMetricIrDoc(q, range, stepSeconds);
    if (doc === null) return null;
    irQueries[q.ref] = doc;
  }
  if (Object.keys(irQueries).length === 0) return null;

  const formulas: QueryFormula[] = [{ name: "formula", expr }];
  return { queries: irQueries, formulas, result: "series" };
}

/** Adapts the IR `series` envelope to the shape `MetricsChart` renders
 * (nanoseconds → ms, unknown → number). */
export function irSeriesToPromSeries(
  series: NonNullable<QueryIrResponse["series"]>,
): PromSeries[] {
  return series.map((s) => ({
    labels: s.labels,
    points: s.points.map((p): [number, number] => {
      const [tNs, v] = p;
      return [Number(tNs) / 1_000_000, Number(v)];
    }),
  }));
}
