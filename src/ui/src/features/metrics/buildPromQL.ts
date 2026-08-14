// Structured metric-query model and its PromQL compilation. The visual builder
// is the primary input; "edit as PromQL" switches to a raw string. The model
// mirrors the querier's lowering steps (querier/src/query/promql.rs): a bucketed
// selector, an optional range-vector function (rate/*_over_time), and an
// optional outer space aggregation with grouping.

import { isValidLabelName, type LabelFilter } from "../../lib/filters";
import { escapeQuotedString } from "../../lib/collections";

export type SpaceAgg = "sum" | "avg" | "min" | "max" | "count";

export const SPACE_AGGS: SpaceAgg[] = ["sum", "avg", "min", "max", "count"];

/**
 * Range-vector functions that turn a step-bucketed selector into a single
 * value per bucket. `rate`/`irate`/`increase` are counter functions; the
 * `*_over_time` family are gauge rollups. All take a lookback `window`
 * (a PromQL duration like `5m`).
 */
export type RangeFn =
  | "rate"
  | "irate"
  | "increase"
  | "avg_over_time"
  | "min_over_time"
  | "max_over_time"
  | "sum_over_time";

export const RANGE_FNS: RangeFn[] = [
  "rate",
  "irate",
  "increase",
  "avg_over_time",
  "min_over_time",
  "max_over_time",
  "sum_over_time",
];

export interface RangeFnSpec {
  fn: RangeFn;
  /** PromQL duration, e.g. "5m". */
  window: string;
}

export interface SpaceAggSpec {
  op: SpaceAgg;
  /** Group-by labels; empty aggregates every series into one. */
  by: string[];
}

export interface MetricQuery {
  /** Query letter (a, b, …) — display/formula reference, not part of PromQL. */
  ref: string;
  /** Prometheus metric name (already sanitized, from /labels __name__). */
  metric: string;
  filters: LabelFilter[];
  /** Optional range-vector function applied to the raw selector. */
  range?: RangeFnSpec;
  /** Optional space aggregation applied outermost. */
  agg?: SpaceAggSpec;
}

export function emptyQuery(ref: string): MetricQuery {
  return { ref, metric: "", filters: [] };
}

/** `metric{label=~"…", …}` — or bare `metric` when there are no valid filters. */
export function buildSelector(metric: string, filters: LabelFilter[]): string {
  const matchers = filters
    .filter((f) => isValidLabelName(f.label))
    .map((f) => `${f.label}${f.op}"${escapeQuotedString(f.value)}"`);
  if (matchers.length === 0) return metric;
  return `${metric}{${matchers.join(", ")}}`;
}

/**
 * Compile a structured metric query to PromQL. Returns "" when no metric is
 * selected yet, so callers can gate the chart query on a non-empty result.
 */
export function buildPromQL(q: MetricQuery): string {
  if (q.metric.trim() === "") return "";

  let expr = buildSelector(q.metric, q.filters);

  if (q.range) {
    expr = `${q.range.fn}(${expr}[${q.range.window}])`;
  }

  if (q.agg) {
    const by = q.agg.by.filter(isValidLabelName);
    const grouping = by.length > 0 ? ` by (${by.join(", ")})` : "";
    expr = `${q.agg.op}${grouping}(${expr})`;
  }

  return expr;
}

/** The reference letters a query can take, in assignment order. */
export const QUERY_REFS = "abcdefghij".split("");

/** Next unused ref letter for a set of queries (falls back to "a"). */
export function nextRef(queries: MetricQuery[]): string {
  const used = new Set(queries.map((q) => q.ref));
  return QUERY_REFS.find((r) => !used.has(r)) ?? "a";
}

/**
 * Compile a set of queries plus an optional formula into a single PromQL
 * expression. Each single-letter reference in the formula (a, b, …) is
 * replaced by the parenthesized compilation of that query, so a formula like
 * `(a / b) * 100` becomes a ratio expression. PromQL function names (rate,
 * sum, time, …) are multi-character and are left untouched.
 *
 * With no formula, only the first query charts. Returns "" when nothing is
 * runnable yet (empty first query, or a formula referencing empty queries).
 */
export function buildFormula(queries: MetricQuery[], formula: string): string {
  const trimmed = formula.trim();
  if (trimmed === "") {
    const first = queries[0];
    return first ? buildPromQL(first) : "";
  }

  let referencedEmpty = false;
  const substituted = trimmed.replace(/\b([a-j])\b/g, (match, ref: string) => {
    const q = queries.find((qq) => qq.ref === ref);
    if (!q) return match;
    const compiled = buildPromQL(q);
    if (compiled === "") {
      referencedEmpty = true;
      return match;
    }
    return `(${compiled})`;
  });

  // A formula that still names an empty query isn't runnable.
  return referencedEmpty ? "" : substituted;
}
