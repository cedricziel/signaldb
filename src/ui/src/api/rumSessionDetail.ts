/**
 * Real user monitoring: one session's detail timeline — see
 * `openspec/changes/rum-explore-tabs/specs/explore-ui-rum/spec.md`'s
 * "Session detail timeline" requirement.
 *
 * Two bounded reads, both filtered on the one `session.id` (design.md
 * decision 4): every span of the session's traces, and every log record but
 * `browser.resource_timing` (the Network tab already covers that signal in
 * aggregate). They're merged client-side into one ordered event list, each
 * event classified into a display lane. Both reads ask for one page over the
 * combined 2,000-record cap so an overflow is detectable without a third
 * count query; "Load more" re-issues both with a `timestamp`/
 * `start_time_unix_nano` cursor set to the last loaded event's own
 * timestamp, per the spec's "starting after the last loaded timestamp".
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import {
  irColumn,
  namedRows,
  rangeDoc,
  runIrQuery,
  type IrRow,
} from "./queryIr";
import { nanosToMs, type ResolvedRange } from "../lib/time";

/** Records shown on the timeline, across both reads — the spec's "up to a
 * cap of 2,000 records". */
export const SESSION_DETAIL_CAP = 2000;

/** One page's request limit per source: one more than the cap, so a source
 * alone holding more than the cap is detectable. */
const PAGE_LIMIT = SESSION_DETAIL_CAP + 1;

export type SessionLane =
  "views" | "actions" | "network" | "perf" | "errors" | "logs";

interface SessionEventBase {
  /** ns since the epoch, as a string — exact precision from a `rows` result
   * (unlike `rumSessions.ts`'s aggregate `table` result), needed for the
   * `>` cursor on "Load more" and for a stable sort. */
  tsNs: string;
  lane: SessionLane;
}

export interface SessionSpanEvent extends SessionEventBase {
  kind: "span";
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  spanKind: string | null;
  serviceName: string;
  durationNs: string;
  isError: boolean;
  httpMethod: string | null;
  urlFull: string | null;
  httpStatusCode: number | null;
}

export interface SessionLogEvent extends SessionEventBase {
  kind: "log";
  eventName: string | null;
  traceId: string | null;
  urlTemplate: string | null;
  urlFull: string | null;
  vitalName: string | null;
  vitalRating: string | null;
  vitalValue: number | null;
  cssSelector: string | null;
  tagName: string | null;
  exceptionType: string | null;
  exceptionMessage: string | null;
  exceptionStacktrace: string | null;
}

export type SessionEvent = SessionSpanEvent | SessionLogEvent;

function whereSession(sessionId: string): Record<string, unknown> {
  return { where: { field: "session.id", op: "eq", value: sessionId } };
}

const SPAN_FIELDS = [
  "trace_id",
  "span_id",
  "parent_span_id",
  "span.name",
  "span_kind",
  "service.name",
  "start_time_unix_nano",
  "duration",
  "status.code",
  "http.request.method",
  "url.full",
  "http.response.status_code",
] as const;

export function buildSessionSpansDoc(
  sessionId: string,
  range: ResolvedRange,
  afterNs?: string,
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "traces",
    range: rangeDoc(range),
    result: "rows",
    fields: [...SPAN_FIELDS],
    pipeline: [
      whereSession(sessionId),
      ...(afterNs
        ? [
            {
              where: {
                field: "start_time_unix_nano",
                op: "gt",
                value: afterNs,
              },
            },
          ]
        : []),
      { order: [{ of: "start_time_unix_nano", dir: "asc" }] },
      { limit: PAGE_LIMIT },
    ],
  };
}

const LOG_FIELDS = [
  "timestamp",
  "event_name",
  "trace_id",
  "url.template",
  "url.full",
  "browser.web_vital.name",
  "browser.web_vital.rating",
  "browser.web_vital.value",
  "browser.css_selector",
  "browser.tag_name",
  "exception.type",
  "exception.message",
  "exception.stacktrace",
] as const;

export function buildSessionLogsDoc(
  sessionId: string,
  range: ResolvedRange,
  afterNs?: string,
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "logs",
    range: rangeDoc(range),
    result: "rows",
    fields: [...LOG_FIELDS],
    pipeline: [
      whereSession(sessionId),
      {
        where: {
          field: "event_name",
          op: "ne",
          value: "browser.resource_timing",
        },
      },
      ...(afterNs
        ? [{ where: { field: "timestamp", op: "gt", value: afterNs } }]
        : []),
      { order: [{ of: "timestamp", dir: "asc" }] },
      { limit: PAGE_LIMIT },
    ],
  };
}

/** Reads a projected field off a `rows` result, trying the response's own
 * column-name convention (`irColumn`) first and the field's last segment as
 * a fallback — `rows` responses have been seen naming a projected field
 * differently from the naive dot-to-underscore mapping (`traceDetail.ts`'s
 * `duration` → `duration_nanos`), and this module has no live server to
 * confirm every field's exact name against. */
function pick(row: IrRow, field: string): unknown {
  const byConvention = row[irColumn(field)];
  if (byConvention !== undefined) return byConvention;
  const lastSegment = field.split(".").pop()!;
  return row[lastSegment];
}

const str = (v: unknown): string => (v == null ? "" : String(v));
const strOrNull = (v: unknown): string | null => (v == null ? null : String(v));
const numOrNull = (v: unknown): number | null =>
  typeof v === "number" ? v : null;

/** ≥400, or the span itself recorded an error — `api/rum.ts`'s
 * `HTTP_ERROR_WHERE` predicate, applied client-side here since it decides
 * lane classification rather than filtering a query. */
function spanIsError(
  statusCode: string | null,
  httpStatus: number | null,
): boolean {
  return statusCode === "Error" || (httpStatus !== null && httpStatus >= 400);
}

function spanEventFromRow(row: IrRow): SessionSpanEvent {
  const statusCode = strOrNull(pick(row, "status.code"));
  const httpStatusCode = numOrNull(pick(row, "http.response.status_code"));
  const isError = spanIsError(statusCode, httpStatusCode);
  const spanKind = strOrNull(pick(row, "span_kind"));
  return {
    kind: "span",
    tsNs: str(pick(row, "start_time_unix_nano")),
    // Errors take lane priority over Network (the spec's own lane list
    // order) — a failed request is still drawn with its request details,
    // just under the Errors lane rather than Network.
    lane: isError ? "errors" : spanKind === "Client" ? "network" : "logs",
    traceId: str(pick(row, "trace_id")),
    spanId: str(pick(row, "span_id")),
    parentSpanId: strOrNull(pick(row, "parent_span_id")),
    name: str(pick(row, "span.name")),
    spanKind,
    serviceName: str(pick(row, "service.name")),
    durationNs: str(pick(row, "duration")),
    isError,
    httpMethod: strOrNull(pick(row, "http.request.method")),
    urlFull: strOrNull(pick(row, "url.full")),
    httpStatusCode,
  };
}

const VIEW_EVENT = "browser.navigation";
const ACTION_EVENT = "browser.user_action.click";
const PERF_EVENTS = new Set(["browser.web_vital", "browser.navigation_timing"]);
const EXCEPTION_EVENT = "exception";

function logLane(eventName: string | null): SessionLane {
  if (eventName === EXCEPTION_EVENT) return "errors";
  if (eventName === VIEW_EVENT) return "views";
  if (eventName === ACTION_EVENT) return "actions";
  if (eventName !== null && PERF_EVENTS.has(eventName)) return "perf";
  return "logs";
}

function logEventFromRow(row: IrRow): SessionLogEvent {
  const eventName = strOrNull(pick(row, "event_name"));
  return {
    kind: "log",
    tsNs: str(pick(row, "timestamp")),
    lane: logLane(eventName),
    eventName,
    traceId: strOrNull(pick(row, "trace_id")),
    urlTemplate: strOrNull(pick(row, "url.template")),
    urlFull: strOrNull(pick(row, "url.full")),
    vitalName: strOrNull(pick(row, "browser.web_vital.name")),
    vitalRating: strOrNull(pick(row, "browser.web_vital.rating")),
    vitalValue: numOrNull(pick(row, "browser.web_vital.value")),
    cssSelector: strOrNull(pick(row, "browser.css_selector")),
    tagName: strOrNull(pick(row, "browser.tag_name")),
    exceptionType: strOrNull(pick(row, "exception.type")),
    exceptionMessage: strOrNull(pick(row, "exception.message")),
    exceptionStacktrace: strOrNull(pick(row, "exception.stacktrace")),
  };
}

function tsAsc(a: SessionEvent, b: SessionEvent): number {
  const an = BigInt(a.tsNs || "0");
  const bn = BigInt(b.tsNs || "0");
  return an < bn ? -1 : an > bn ? 1 : 0;
}

export interface SessionDetailPage {
  events: SessionEvent[];
  /** More records exist beyond this page — the spec's "the page shall say
   * so ... and offer to load the next page". */
  hasMore: boolean;
  /** How many more, when both sources returned fewer than their own page
   * limit (so the true remainder is knowable); `undefined` means "more
   * records exist" with no known count — one or both sources were
   * themselves truncated at their own request limit. */
  moreCount?: number;
  /** The cursor for "Load more" — the last returned event's own timestamp;
   * `undefined` when there's nothing more to load. */
  nextCursorNs?: string;
}

/** Merges the two session-detail reads into one ordered, capped page (see
 * the module doc). */
export function sessionDetailFromResponses(
  spansRes: QueryIrResponse,
  logsRes: QueryIrResponse,
  cap = SESSION_DETAIL_CAP,
): SessionDetailPage {
  const spanRows = namedRows(spansRes);
  const logRows = namedRows(logsRes);
  // Each source was asked for `cap + 1` rows (see `PAGE_LIMIT`, and this
  // function's own `cap` parameter for tests) — getting that many back means
  // the source may hold more beyond what was fetched.
  const pageLimit = cap + 1;
  const spansTruncated = spanRows.length >= pageLimit;
  const logsTruncated = logRows.length >= pageLimit;

  const merged = [
    ...spanRows.map(spanEventFromRow),
    ...logRows.map(logEventFromRow),
  ].sort(tsAsc);

  const page = merged.slice(0, cap);
  const hasMore = merged.length > cap || spansTruncated || logsTruncated;
  const moreCount =
    !spansTruncated && !logsTruncated && merged.length > cap
      ? merged.length - cap
      : undefined;

  return {
    events: page,
    hasMore,
    moreCount,
    nextCursorNs: hasMore ? page[page.length - 1]?.tsNs : undefined,
  };
}

export async function fetchSessionDetail(
  sessionId: string,
  range: ResolvedRange,
  afterNs?: string,
): Promise<SessionDetailPage> {
  const [spansRes, logsRes] = await Promise.all([
    runIrQuery(buildSessionSpansDoc(sessionId, range, afterNs)),
    runIrQuery(buildSessionLogsDoc(sessionId, range, afterNs)),
  ]);
  return sessionDetailFromResponses(spansRes, logsRes);
}

/** ms, for display — a thin wrapper so callers don't reach past this module
 * for the one conversion the timeline needs. */
export function sessionEventTimeMs(event: SessionEvent): number {
  return nanosToMs(event.tsNs || "0");
}
