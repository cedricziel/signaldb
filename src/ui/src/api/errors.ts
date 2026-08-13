/**
 * Errors & Exceptions: exceptions grouped by (type, message, service),
 * combined across two sources that record them differently:
 *
 * - **traces**: an exception is a span *event*, not a span attribute — see
 *   `exception.type` resolution in `query-ir-core`'s traces source.
 * - **logs**: per the exceptions-on-logs semconv, the same attribute names
 *   are ordinary LogRecord attributes.
 *
 * There is no single query spanning both, so this fetches each source's
 * aggregate independently and merges client-side, ranked by count — the
 * same pattern as `api/dependencyBreakdown.ts`'s multi-query combine.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

export type ErrorSource = "traces" | "logs";

export interface ErrorGroup {
  source: ErrorSource;
  exceptionType: string | null;
  exceptionMessage: string | null;
  serviceName: string | null;
  count: number;
  firstNs: string;
  lastNs: string;
}

export interface ErrorGroupResult {
  groups: ErrorGroup[];
  /** More groups exist in one or both sources than the budget displays. */
  truncated: boolean;
}

export interface ErrorExample {
  traceId: string | null;
  stacktrace: string | null;
}

/** Groups shown per source before the list is disclosed as truncated. */
const GROUP_BUDGET = 200;

const GROUP_DIMENSIONS = [
  "exception.type",
  "exception.message",
  "service.name",
];

function rangeDoc(range: ResolvedRange) {
  return {
    from: String(msToNanos(range.fromMs)),
    to: String(msToNanos(range.toMs)),
  };
}

/** The field that carries a record's own timestamp, per source. */
function timeField(source: ErrorSource): string {
  return source === "traces" ? "start_time_unix_nano" : "timestamp";
}

export function buildErrorGroupDoc(
  source: ErrorSource,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: source,
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      { where: { field: "exception.type", op: "exists" } },
      {
        aggregate: {
          by: GROUP_DIMENSIONS,
          aggs: [
            { fn: "count", as: "n" },
            { fn: "min", of: timeField(source), as: "first" },
            { fn: "max", of: timeField(source), as: "last" },
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: GROUP_BUDGET + 1 },
    ],
  };
}

/**
 * Decode a `table` envelope of `[type, message, service, count, first, last]`
 * rows, positionally — the server answers with physical column names, so a
 * lookup by the logical names sent would silently miss.
 */
function groupsFromResponse(
  res: QueryIrResponse,
  source: ErrorSource,
): ErrorGroup[] {
  const rows = res.rows ?? [];
  return rows.slice(0, GROUP_BUDGET).map((row): ErrorGroup => {
    const cells = row as unknown[];
    const str = (v: unknown) => (v == null ? null : String(v));
    const num = (v: unknown) => (typeof v === "number" ? v : 0);
    const ns = (v: unknown) => (v == null ? "0" : String(v));
    return {
      source,
      exceptionType: str(cells[0]),
      exceptionMessage: str(cells[1]),
      serviceName: str(cells[2]),
      count: num(cells[3]),
      firstNs: ns(cells[4]),
      lastNs: ns(cells[5]),
    };
  });
}

export async function fetchErrorGroups(
  range: ResolvedRange,
): Promise<ErrorGroupResult> {
  const [tracesRes, logsRes] = await Promise.all([
    runIrQuery(buildErrorGroupDoc("traces", range)),
    runIrQuery(buildErrorGroupDoc("logs", range)),
  ]);
  const groups = [
    ...groupsFromResponse(tracesRes, "traces"),
    ...groupsFromResponse(logsRes, "logs"),
  ].sort((a, b) => b.count - a.count);
  const truncated =
    (tracesRes.rows?.length ?? 0) > GROUP_BUDGET ||
    (logsRes.rows?.length ?? 0) > GROUP_BUDGET;
  return { groups, truncated };
}

export function buildErrorExampleDoc(
  group: ErrorGroup,
  range: ResolvedRange,
): QueryIrRequest {
  const pins: Record<string, unknown>[] = [
    {
      where: {
        field: "exception.type",
        op: "eq",
        value: group.exceptionType ?? "",
      },
    },
  ];
  if (group.exceptionMessage != null) {
    pins.push({
      where: {
        field: "exception.message",
        op: "eq",
        value: group.exceptionMessage,
      },
    });
  }
  if (group.serviceName != null) {
    pins.push({
      where: { field: "service.name", op: "eq", value: group.serviceName },
    });
  }
  return {
    irVersion: 1,
    from: group.source,
    range: rangeDoc(range),
    result: "rows",
    fields: ["trace_id", "exception.stacktrace"],
    pipeline: [...pins, { limit: 1 }],
  };
}

/** One concrete occurrence of a group — its stacktrace and, when the record
 * carries one, a trace id to open in the waterfall. */
export async function fetchErrorExample(
  group: ErrorGroup,
  range: ResolvedRange,
): Promise<ErrorExample | null> {
  const res = await runIrQuery(buildErrorExampleDoc(group, range));
  const row = res.rows?.[0] as unknown[] | undefined;
  if (!row) return null;
  const [traceId, stacktrace] = row;
  return {
    traceId: traceId == null ? null : String(traceId),
    stacktrace: stacktrace == null ? null : String(stacktrace),
  };
}
