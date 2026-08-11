/**
 * RED metrics per group for the traces tab's group table.
 *
 * The aggregation is a Query IR `table` aggregate evaluated server-side over
 * the whole selected window — never a client-side fold of the row-limited
 * Tempo search response, whose counts and percentiles would describe only the
 * newest N records. The row budget below therefore bounds the number of
 * *groups* returned, not the records their aggregates cover.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { facetField, type TraceFilter } from "../lib/traceFilters";

/** Groups shown before the table is disclosed as truncated. */
export const GROUP_BUDGET = 500;

/**
 * A trace's root span carries this sentinel as its parent, not an empty string
 * and not null — a wire-format fact, so `eq ""` and `not exists` both match
 * nothing and read as "no data". Do not "fix" this to `""`.
 */
export const ROOT_SPAN_SENTINEL = "0000000000000000";

/**
 * Matches every spelling of an error status the server may return (`Error`,
 * `STATUS_CODE_ERROR`). A scoping predicate cannot call `normalizeStatus()`,
 * so this is the same rule expressed in the predicate grammar; the two are
 * kept in step by a test.
 */
export const ERROR_PATTERN = "(?i)error";

/** What one group row counts. */
export type GroupGrain = "traces" | "spans";

/**
 * A sortable column, named as the IR aggregate it orders by.
 *
 * Sorting is a server-side `order` stage, not a client-side sort of the
 * fetched page: the page is the top {@link GROUP_BUDGET} groups under the
 * *current* sort, so re-sorting it locally would rank within the groups the
 * previous sort selected — clicking "p95" would show the slowest of the most
 * frequent groups, not the slowest groups. Changing the sort refetches.
 *
 * `dim:<i>` orders by the i-th grouping dimension. Rate is deliberately absent:
 * it is `count / window`, a strictly increasing function of `n` over a fixed
 * window, so it sorts identically to `n`.
 */
export type GroupSortKey = "n" | "errors" | "p50" | "p95" | "last" | string;

export interface GroupSort {
  key: GroupSortKey;
  dir: "asc" | "desc";
}

export const DEFAULT_GROUP_SORT: GroupSort = { key: "n", dir: "desc" };

export interface TraceGroup {
  /** One value per grouping dimension; `null` where the record has none. */
  values: (string | null)[];
  count: number;
  errors: number;
  p50Ms: number;
  p95Ms: number;
  /** Start of the most recent record in the group, epoch nanoseconds. */
  lastNs: string;
}

export interface TraceGroupResult {
  groups: TraceGroup[];
  /** More groups exist than the budget displays. */
  truncated: boolean;
}

/**
 * Build the RED aggregate for the group table.
 *
 * At `traces` grain a root-span predicate restricts the scan to one record per
 * trace, so `count` counts traces and the percentiles measure end-to-end trace
 * duration. At `spans` grain every matching record contributes, agreeing with
 * the span-level volume chart and facet counts.
 */
export function buildGroupDoc(
  dimensions: string[],
  range: ResolvedRange,
  filters: TraceFilter[],
  grain: GroupGrain,
  sort: GroupSort = DEFAULT_GROUP_SORT,
): QueryIrRequest {
  const scope: Record<string, unknown>[] =
    grain === "traces"
      ? [
          {
            where: {
              field: "parent_span_id",
              op: "eq",
              value: ROOT_SPAN_SENTINEL,
            },
          },
        ]
      : [];

  const active = filters.flatMap((f) => {
    const facet = facetField(f.field);
    if (!facet) return [];
    return [
      { where: { field: facet.irField, op: "eq", value: f.value } },
    ] as Record<string, unknown>[];
  });

  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "table",
    pipeline: [
      ...scope,
      ...active,
      {
        aggregate: {
          by: dimensions,
          aggs: [
            { fn: "count", as: "n" },
            // Scoped to errors only — one row per group keeps the percentiles
            // whole-group, which a status grouping dimension could not.
            {
              fn: "count",
              as: "errors",
              where: {
                field: "status.code",
                op: "regex",
                value: ERROR_PATTERN,
              },
            },
            { fn: "quantile", of: "duration", arg: 0.5, as: "p50" },
            { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
            { fn: "max", of: "start_time_unix_nano", as: "last" },
          ],
        },
      },
      { order: [orderKey(sort, dimensions)] },
      // One more than displayed, so truncation is detectable.
      { limit: GROUP_BUDGET + 1 },
    ],
  };
}

/**
 * Resolve a sort key to an `order` operand. `dim:<i>` addresses the i-th
 * grouping dimension by name; anything unrecognised falls back to the count,
 * so a stale URL sorts sensibly instead of erroring.
 */
function orderKey(
  sort: GroupSort,
  dimensions: string[],
): { of: string; dir: "asc" | "desc" } {
  const dim = /^dim:(\d+)$/.exec(sort.key);
  const of = dim
    ? (dimensions[Number(dim[1])] ?? dimensions[0] ?? "n")
    : ["n", "errors", "p50", "p95", "last"].includes(sort.key)
      ? sort.key
      : "n";
  return { of, dir: sort.dir };
}

const NANOS_PER_MS = 1_000_000;

/**
 * Decode a `table` envelope into group rows.
 *
 * Values are read **positionally**: the server answers with physical column
 * names (`span.name` in, `span_name` out), so looking them up by the logical
 * name that was sent would silently miss. The measure columns follow the
 * grouping dimensions in the order `buildGroupDoc` declared them.
 */
export function groupsFromIrResponse(
  res: QueryIrResponse,
  dimensionCount: number,
): TraceGroupResult {
  const rows = res.rows ?? [];
  const d = dimensionCount;
  const groups = rows.slice(0, GROUP_BUDGET).map((row): TraceGroup => {
    const cells = row as unknown[];
    const values = cells.slice(0, d).map((v) => (v == null ? null : String(v)));
    const num = (i: number) => {
      const v = cells[d + i];
      return typeof v === "number" ? v : 0;
    };
    const cell = cells[d + 4];
    return {
      values,
      count: num(0),
      errors: num(1),
      p50Ms: num(2) / NANOS_PER_MS,
      p95Ms: num(3) / NANOS_PER_MS,
      lastNs: cell == null ? "0" : String(cell),
    };
  });
  return { groups, truncated: rows.length > GROUP_BUDGET };
}

export async function fetchTraceGroups(
  dimensions: string[],
  range: ResolvedRange,
  filters: TraceFilter[],
  grain: GroupGrain,
  sort: GroupSort = DEFAULT_GROUP_SORT,
): Promise<TraceGroupResult> {
  return groupsFromIrResponse(
    await runIrQuery(buildGroupDoc(dimensions, range, filters, grain, sort)),
    dimensions.length,
  );
}
