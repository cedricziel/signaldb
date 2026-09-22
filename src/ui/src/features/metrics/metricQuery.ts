// The metrics builder's structured query model. Compiled to the Query IR by
// api/ir/metrics.ts — this module owns only the data shape, its URL
// round-trip, and the letter bookkeeping (ref assignment) shared by the
// query rows and the formula box.

import {
  FILTER_OPS,
  isValidLabelName,
  type LabelFilter,
} from "../../lib/filters";

export type SpaceAgg = "sum" | "avg" | "min" | "max" | "count";

export const SPACE_AGGS: SpaceAgg[] = ["sum", "avg", "min", "max", "count"];

/**
 * Counter-rate functions the Query IR's `aggregate` stage can express (see
 * docs/users/querying-ir.md, "Counter rate: rate/increase (v6)"). PromQL's
 * `irate` and the `*_over_time` gauge rollups have no IR pipeline-stage
 * equivalent and are not offered here — see the metrics-builder report in
 * openspec/changes/explore-ui-query-ir for what that drops.
 */
export type RangeFn = "rate" | "increase";

export const RANGE_FNS: RangeFn[] = ["rate", "increase"];

export interface RangeFnSpec {
  fn: RangeFn;
}

export interface SpaceAggSpec {
  op: SpaceAgg;
  /** Group-by labels; empty aggregates every series into one. */
  by: string[];
}

export interface MetricQuery {
  /** Query letter (a, b, …) — display/formula reference. */
  ref: string;
  /** Metric name (from the IR's `metricNames` discovery). */
  metric: string;
  filters: LabelFilter[];
  /** Optional counter-rate function applied to the raw series. */
  range?: RangeFnSpec;
  /** Optional space aggregation applied outermost. */
  agg?: SpaceAggSpec;
}

export function emptyQuery(ref: string): MetricQuery {
  return { ref, metric: "", filters: [] };
}

function isLabelFilter(value: unknown): value is LabelFilter {
  return (
    value !== null &&
    typeof value === "object" &&
    typeof (value as LabelFilter).label === "string" &&
    typeof (value as LabelFilter).value === "string" &&
    FILTER_OPS.includes((value as LabelFilter).op)
  );
}

function isRangeFnSpec(value: unknown): value is RangeFnSpec {
  return (
    value !== null &&
    typeof value === "object" &&
    RANGE_FNS.includes((value as RangeFnSpec).fn)
  );
}

function isSpaceAggSpec(value: unknown): value is SpaceAggSpec {
  return (
    value !== null &&
    typeof value === "object" &&
    SPACE_AGGS.includes((value as SpaceAggSpec).op) &&
    Array.isArray((value as SpaceAggSpec).by) &&
    (value as SpaceAggSpec).by.every((b) => typeof b === "string")
  );
}

function isMetricQuery(value: unknown): value is MetricQuery {
  if (value === null || typeof value !== "object") return false;
  const q = value as MetricQuery;
  if (
    typeof q.ref !== "string" ||
    typeof q.metric !== "string" ||
    !Array.isArray(q.filters) ||
    !q.filters.every(isLabelFilter)
  ) {
    return false;
  }
  if (q.range !== undefined && !isRangeFnSpec(q.range)) return false;
  if (q.agg !== undefined && !isSpaceAggSpec(q.agg)) return false;
  return true;
}

export interface BuilderState {
  queries: MetricQuery[];
  formula: string;
}

/**
 * Defensive JSON parse for the builder state round-tripped through the URL
 * (`?mq=` — see lib/urlState.ts's `metricQuery`). Malformed JSON or a value
 * that isn't shaped like a `BuilderState` degrades to `null` rather than
 * throwing, so a corrupted/hand-edited link falls back to an empty builder
 * instead of crashing.
 *
 * Also accepts the pre-formula encoding — a single `MetricQuery` object, not
 * wrapped in `{queries, formula}` — so a link produced before this change
 * keeps loading, seeded as a one-query builder with no formula.
 */
export function parseBuilderState(raw: string): BuilderState | null {
  if (raw === "") return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== "object") return null;

  if ("queries" in parsed) {
    const s = parsed as { queries: unknown; formula: unknown };
    if (!Array.isArray(s.queries) || !s.queries.every(isMetricQuery)) {
      return null;
    }
    if (typeof s.formula !== "string") return null;
    return { queries: s.queries, formula: s.formula };
  }

  // Legacy single-query encoding.
  if (!isMetricQuery(parsed)) return null;
  return { queries: [parsed], formula: "" };
}

/** The reference letters a query can take, in assignment order. */
export const QUERY_REFS = "abcdefghij".split("");

/** Next unused ref letter for a set of queries (falls back to "a"). */
export function nextRef(queries: MetricQuery[]): string {
  const used = new Set(queries.map((q) => q.ref));
  return QUERY_REFS.find((r) => !used.has(r)) ?? "a";
}

/** Whether a filter's label is well-formed enough to compile — mirrors the
 * IR's own field-name validation, so a half-typed filter row is silently
 * dropped from the compiled document rather than sent as a broken `where`. */
export function isCompilableFilter(f: LabelFilter): boolean {
  return isValidLabelName(f.label);
}
