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
import type { VolumeSeries } from "../features/explore/SignalHistogram";

export type ErrorSource = "traces" | "logs";

export interface ErrorGroup {
  source: ErrorSource;
  exceptionType: string | null;
  exceptionMessage: string | null;
  serviceName: string | null;
  /** `exception.escaped`: "true" when the exception escaped the scope it
   * was recorded in (uncaught/unhandled), "false" when it did not
   * (caught), `null` when the record carries no such attribute. */
  escaped: string | null;
  count: number;
  firstNs: string;
  lastNs: string;
}

export interface ErrorGroupResult {
  groups: ErrorGroup[];
  /** More groups exist in one or both sources than the budget displays. */
  truncated: boolean;
}

export interface ErrorOccurrence {
  timestampNs: string;
  /** Absent when the record carries no active trace (e.g. a log-only
   * exception outside a traced request) — never assumed present. */
  traceId: string | null;
  stacktrace: string | null;
}

/** Groups shown per source before the list is disclosed as truncated. */
const GROUP_BUDGET = 200;

/** Occurrences listed per selected group, newest first. */
const OCCURRENCE_LIMIT = 25;

const GROUP_DIMENSIONS = [
  "exception.type",
  "exception.message",
  "service.name",
  "exception.escaped",
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
 * Decode a `table` envelope of
 * `[type, message, service, escaped, count, first, last]` rows, positionally
 * — the server answers with physical column names, so a lookup by the
 * logical names sent would silently miss.
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
      escaped: str(cells[3]),
      count: num(cells[4]),
      firstNs: ns(cells[5]),
      lastNs: ns(cells[6]),
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

/** A field pinned to an exact value, or — when the group's own value for it
 * is absent — pinned to "absent on this record" via `not exists`, never
 * left unconstrained. An omitted pin would let occurrence/volume queries
 * for a "no service" group match records from *any* service instead of
 * only those genuinely missing one. */
function pin(field: string, value: string | null): Record<string, unknown> {
  return {
    where:
      value == null
        ? { not: { field, op: "exists" } }
        : { field, op: "eq", value },
  };
}

function pinnedWhere(group: ErrorGroup): Record<string, unknown>[] {
  return [
    {
      where: {
        field: "exception.type",
        op: "eq",
        value: group.exceptionType ?? "",
      },
    },
    pin("exception.message", group.exceptionMessage),
    pin("service.name", group.serviceName),
    pin("exception.escaped", group.escaped),
  ];
}

/**
 * The IR document for a group's individual occurrences — the same
 * group→instances drill-in already used for trace groups
 * (`api/traceGroupMembers.ts`) and catalog entities, applied here: a group
 * is an aggregate, and only its instances carry a stacktrace or a trace id
 * to link to.
 */
export function buildErrorOccurrencesDoc(
  group: ErrorGroup,
  range: ResolvedRange,
): QueryIrRequest {
  const time = timeField(group.source);
  return {
    irVersion: 1,
    from: group.source,
    range: rangeDoc(range),
    result: "rows",
    fields: [time, "trace_id", "exception.stacktrace"],
    pipeline: [
      ...pinnedWhere(group),
      { order: [{ of: time, dir: "desc" }] },
      { limit: OCCURRENCE_LIMIT },
    ],
  };
}

function occurrencesFromResponse(res: QueryIrResponse): ErrorOccurrence[] {
  const rows = res.rows ?? [];
  return rows.map((row): ErrorOccurrence => {
    const cells = row as unknown[];
    const [timestamp, traceId, stacktrace] = cells;
    return {
      timestampNs: timestamp == null ? "0" : String(timestamp),
      traceId: traceId == null ? null : String(traceId),
      stacktrace: stacktrace == null ? null : String(stacktrace),
    };
  });
}

/** A group's individual occurrences, newest first — each one's own
 * stacktrace and, where the record carries one, its own trace id to open in
 * the waterfall. Not every occurrence of the same (type, message, service)
 * shares a trace: only some may have occurred inside an active trace. */
export async function fetchErrorOccurrences(
  group: ErrorGroup,
  range: ResolvedRange,
): Promise<ErrorOccurrence[]> {
  const res = await runIrQuery(buildErrorOccurrencesDoc(group, range));
  return occurrencesFromResponse(res);
}

/**
 * The IR document for a group's own step-bucketed occurrence count — the
 * "count over time" chart error-tracking issue views commonly show above
 * the sample list. `by` regroups on the already-pinned exception.type only
 * to guarantee a single named series; the pins already scope it to the
 * exact group.
 */
export function buildErrorGroupVolumeDoc(
  group: ErrorGroup,
  range: ResolvedRange,
  step: string,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: group.source,
    range: rangeDoc(range),
    result: "series",
    pipeline: [
      ...pinnedWhere(group),
      {
        aggregate: {
          by: ["exception.type"],
          aggs: [{ fn: "count", as: "n" }],
          step,
        },
      },
    ],
  };
}

function volumeFromResponse(res: QueryIrResponse): VolumeSeries[] {
  return (res.series ?? []).map((s, i) => ({
    key: `s${i}`,
    points: s.points.flatMap((p): [number, number][] => {
      const [tNs, value] = p as [unknown, unknown];
      if (typeof tNs !== "number" || typeof value !== "number") return [];
      return [[Math.round(tNs / 1e6), value]];
    }),
  }));
}

/** A group's own occurrence count over time, one point per step bucket. */
export async function fetchErrorGroupVolume(
  group: ErrorGroup,
  range: ResolvedRange,
  step: string,
): Promise<VolumeSeries[]> {
  const res = await runIrQuery(buildErrorGroupVolumeDoc(group, range, step));
  return volumeFromResponse(res);
}
