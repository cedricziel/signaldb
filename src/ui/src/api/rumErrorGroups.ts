/**
 * Real user monitoring: the Errors tab's group list and its "backend cause"
 * pill — see `openspec/changes/rum-explore-tabs/specs/explore-ui-rum/
 * spec.md`'s "Errors tab" requirement and design.md decisions 3 and 4.
 *
 * The list is one `logs` aggregate scoped to the app, grouped by the same
 * dimensions `api/errors.ts` uses for the standalone Errors page minus
 * `service.name` (already pinned by the scope): type, message, escaped.
 * It carries everything the tab needs in that single read — each group's
 * latest session (for "Latest session" and the backend-cause join below),
 * distinct users/sessions (irVersion 9 `count_distinct`), and a scoped
 * count of records from any *other* `resource.service.version` (zero of
 * those means the group is "new in <version>", per the spec).
 *
 * "Backend cause" is a second, separate read, batched over every listed
 * group rather than issued per row (design.md decision 3): one `traces`
 * query fetches the app's failed client spans whose `session.id` is among
 * the groups' own latest sessions. `joinBackendCause` then matches each
 * group's latest event to the latest such span that started no more than
 * `BACKEND_CAUSE_WINDOW_MS` earlier in the same session — session/page-view
 * boundaries aren't tracked on the client, so a fixed window stands in for
 * "the same page view".
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import {
  irColumn,
  namedRows,
  rangeDoc,
  runIrQuery,
  type IrRow,
} from "./queryIr";
import { msToNanos, nanosToMs, type ResolvedRange } from "../lib/time";
import { serviceWhere } from "./rum";
import type { ErrorGroup } from "./errors";

/** Groups shown before the list would need its own truncation notice. */
const GROUP_LIMIT = 200;

const GROUP_DIMENSIONS = [
  "exception.type",
  "exception.message",
  "exception.escaped",
];

export interface RumErrorGroup {
  exceptionType: string | null;
  exceptionMessage: string | null;
  escaped: string | null;
  count: number;
  firstMs: number;
  lastMs: number;
  /** The group's most recent `session.id` — `null` when no occurrence
   * carries one. */
  lastSessionId: string | null;
  users: number;
  sessions: number;
  /** `true` when every occurrence in the window carries the app's current
   * `resource.service.version`; `undefined` when the current version isn't
   * known, so there's nothing to compare against. */
  newInCurrentRelease: boolean | undefined;
}

/** ns (as a JS number, from a `table` aggregate) → ms — same bounded
 * precision loss `api/rumSessions.ts`'s own table reads already accept. */
function nsNumberToMs(v: unknown): number {
  return typeof v === "number" ? Math.round(v / 1_000_000) : 0;
}

export function buildRumErrorGroupsDoc(
  app: string,
  range: ResolvedRange,
  /** The app's current `service.version` (`RumApp.version`) — omitted or
   * `null` when unknown, in which case the "new in release" comparison is
   * skipped rather than guessed at. */
  currentVersion?: string | null,
): QueryIrRequest {
  return {
    irVersion: 9,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      { where: { field: "exception.type", op: "exists" } },
      {
        aggregate: {
          by: GROUP_DIMENSIONS,
          aggs: [
            { fn: "count", as: "n" },
            { fn: "min", of: "timestamp", as: "first" },
            { fn: "max", of: "timestamp", as: "last" },
            { fn: "last", of: "session.id", as: "last_session" },
            { fn: "count_distinct", of: "user.id", as: "users" },
            { fn: "count_distinct", of: "session.id", as: "sessions" },
            ...(currentVersion
              ? [
                  {
                    fn: "count" as const,
                    as: "other_version",
                    where: {
                      field: "resource.service.version",
                      op: "ne" as const,
                      value: currentVersion,
                    },
                  },
                ]
              : []),
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: GROUP_LIMIT },
    ],
  };
}

/**
 * Decodes the list read's rows. The trailing `other_version` column is only
 * present when `currentVersion` was passed to {@link buildRumErrorGroupsDoc}
 * — a row without it decodes to `undefined` for that cell, same as
 * `newInCurrentRelease` being unknown.
 */
export function errorGroupsFromResponse(res: QueryIrResponse): RumErrorGroup[] {
  return (res.rows ?? []).map((row) => {
    const cells = row as unknown[];
    const [
      exceptionType,
      exceptionMessage,
      escaped,
      n,
      first,
      last,
      lastSession,
      users,
      sessions,
      otherVersion,
    ] = cells as [
      string | null,
      string | null,
      string | null,
      number,
      number,
      number,
      string | null,
      number,
      number,
      number | undefined,
    ];
    return {
      exceptionType: exceptionType ?? null,
      exceptionMessage: exceptionMessage ?? null,
      escaped: escaped ?? null,
      count: typeof n === "number" ? n : 0,
      firstMs: nsNumberToMs(first),
      lastMs: nsNumberToMs(last),
      lastSessionId: lastSession ?? null,
      users: typeof users === "number" ? users : 0,
      sessions: typeof sessions === "number" ? sessions : 0,
      newInCurrentRelease:
        otherVersion === undefined ? undefined : otherVersion === 0,
    };
  });
}

export async function fetchRumErrorGroups(
  app: string,
  range: ResolvedRange,
  currentVersion?: string | null,
): Promise<RumErrorGroup[]> {
  return errorGroupsFromResponse(
    await runIrQuery(buildRumErrorGroupsDoc(app, range, currentVersion)),
  );
}

/** A stable identity for a group, used as the `?errgroup=` value and the
 * row key. JSON keeps a `null` field distinct from any literal string and
 * keeps field boundaries unambiguous. */
export function errorGroupKey(
  group: Pick<RumErrorGroup, "exceptionType" | "exceptionMessage" | "escaped">,
): string {
  return JSON.stringify([
    group.exceptionType,
    group.exceptionMessage,
    group.escaped,
  ]);
}

/** Adapts a RUM group to `api/errors.ts`'s `ErrorGroup` shape so the Errors
 * tab's detail panel can reuse that module's already-shipped occurrences and
 * volume queries instead of re-implementing them — the two only disagree on
 * ns-vs-ms timestamps and on always being a `logs` group here (RUM
 * exceptions are logs, not traced span events — see `api/rum.ts`'s module
 * doc). */
export function toErrorsPageGroup(
  group: RumErrorGroup,
  app: string,
): ErrorGroup {
  return {
    source: "logs",
    exceptionType: group.exceptionType,
    exceptionMessage: group.exceptionMessage,
    serviceName: app,
    escaped: group.escaped,
    count: group.count,
    firstNs: msToNanos(group.firstMs),
    lastNs: msToNanos(group.lastMs),
  };
}

// ---- Backend cause: one batched read over every listed group's session --

/** A failed request that started at most this long before a group's latest
 * occurrence still counts as its likely backend cause (see the module doc). */
export const BACKEND_CAUSE_WINDOW_MS = 30_000;

/** Failed client requests fetched per batch, over every session id in one
 * `in` filter. */
const BACKEND_CAUSE_REQUEST_LIMIT = 5000;

export interface RumFailedRequest {
  sessionId: string;
  startMs: number;
  traceId: string;
  spanId: string;
  method: string | null;
  urlFull: string | null;
  statusCode: number | null;
  durationNs: string;
}

const FAILED_REQUEST_FIELDS = [
  "session.id",
  "start_time_unix_nano",
  "trace_id",
  "span_id",
  "http.request.method",
  "url.full",
  "http.response.status_code",
  "duration",
] as const;

/** ≥400, or the span itself recorded an error — `api/rum.ts`'s
 * `HTTP_ERROR_WHERE` predicate, applied to the app's client spans here. */
const FAILED_REQUEST_WHERE = {
  or: [
    { field: "status.code", op: "eq", value: "Error" },
    { field: "http.response.status_code", op: "gte", value: 400 },
  ],
};

export function buildBackendCauseRequestsDoc(
  app: string,
  range: ResolvedRange,
  sessionIds: string[],
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "traces",
    range: rangeDoc(range),
    result: "rows",
    fields: [...FAILED_REQUEST_FIELDS],
    pipeline: [
      { where: { field: "service.name", op: "eq", value: app } },
      { where: { field: "span_kind", op: "eq", value: "Client" } },
      { where: FAILED_REQUEST_WHERE },
      { where: { field: "session.id", op: "in", value: sessionIds } },
      { order: [{ of: "start_time_unix_nano", dir: "desc" }] },
      { limit: BACKEND_CAUSE_REQUEST_LIMIT },
    ],
  };
}

/** Reads a projected field off a `rows` result, trying the response's own
 * column-name convention first and the field's last segment as a fallback —
 * same defensive lookup as `api/rumSessionDetail.ts`'s `pick`, duplicated
 * here since that module treats it as a private decoding detail. */
function pick(row: IrRow, field: string): unknown {
  const byConvention = row[irColumn(field)];
  if (byConvention !== undefined) return byConvention;
  return row[field.split(".").pop()!];
}

const str = (v: unknown): string => (v == null ? "" : String(v));
const strOrNull = (v: unknown): string | null => (v == null ? null : String(v));
const numOrNull = (v: unknown): number | null =>
  typeof v === "number" ? v : null;

export function failedRequestsFromResponse(
  res: QueryIrResponse,
): RumFailedRequest[] {
  return namedRows(res).map((row) => ({
    sessionId: str(pick(row, "session.id")),
    startMs: nanosToMs(str(pick(row, "start_time_unix_nano"))),
    traceId: str(pick(row, "trace_id")),
    spanId: str(pick(row, "span_id")),
    method: strOrNull(pick(row, "http.request.method")),
    urlFull: strOrNull(pick(row, "url.full")),
    statusCode: numOrNull(pick(row, "http.response.status_code")),
    durationNs: str(pick(row, "duration")),
  }));
}

export interface RumErrorGroupWithCause extends RumErrorGroup {
  /** The failed request most likely behind this group's latest occurrence
   * — absent when none was found within the window (see the module doc). */
  backendCause?: RumFailedRequest;
}

/** Matches each group to the latest failed request in its own last session
 * that started before (and within {@link BACKEND_CAUSE_WINDOW_MS} of) the
 * group's own latest occurrence — client-side, from the two batched reads
 * above (design.md decision 3). */
export function joinBackendCause(
  groups: RumErrorGroup[],
  failedRequests: RumFailedRequest[],
): RumErrorGroupWithCause[] {
  const bySession = new Map<string, RumFailedRequest[]>();
  for (const r of failedRequests) {
    const list = bySession.get(r.sessionId);
    if (list) list.push(r);
    else bySession.set(r.sessionId, [r]);
  }
  return groups.map((g) => {
    if (!g.lastSessionId) return g;
    const candidates = bySession.get(g.lastSessionId) ?? [];
    let best: RumFailedRequest | undefined;
    for (const c of candidates) {
      if (c.startMs > g.lastMs) continue;
      if (g.lastMs - c.startMs > BACKEND_CAUSE_WINDOW_MS) continue;
      if (!best || c.startMs > best.startMs) best = c;
    }
    return best ? { ...g, backendCause: best } : g;
  });
}

/** The list read, then — batched over every distinct session id it
 * returned — the backend-cause read, joined client-side. Skips the second
 * read entirely when no group has a session id to look up. */
export async function fetchRumErrorGroupsWithBackendCause(
  app: string,
  range: ResolvedRange,
  currentVersion?: string | null,
): Promise<RumErrorGroupWithCause[]> {
  const groups = await fetchRumErrorGroups(app, range, currentVersion);
  const sessionIds = Array.from(
    new Set(groups.flatMap((g) => (g.lastSessionId ? [g.lastSessionId] : []))),
  );
  if (sessionIds.length === 0) return groups;
  // The backend cause is an enrichment: if its read fails, still show the
  // groups rather than failing the whole list.
  let failedRequests: RumFailedRequest[];
  try {
    failedRequests = failedRequestsFromResponse(
      await runIrQuery(buildBackendCauseRequestsDoc(app, range, sessionIds)),
    );
  } catch {
    return groups;
  }
  return joinBackendCause(groups, failedRequests);
}
