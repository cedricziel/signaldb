/**
 * Real user monitoring: the Sessions tab's list and one session's detail
 * timeline — see `openspec/changes/rum-explore-tabs/specs/explore-ui-rum/
 * spec.md`'s "Sessions list" and "Session detail timeline" requirements.
 *
 * The list is one aggregate read grouped by `session.id` (verified against
 * the hive deployment — see the change's task notes): `first`/`last`
 * resolve including nulls, so a session whose first or last record lacks
 * `url.template` shows a null entry/exit rather than skipping the session.
 * The quick filters ("With errors" / "Slow load") are applied client-side
 * from the aggregated `errors`/`slow` counts already in the row — filtering
 * them server-side on `event_name = exception` would drop every
 * non-exception record from the aggregate and corrupt the other columns.
 * The free-text filter (`session.id`/`user.id` equality, or `key=value` for
 * an arbitrary attribute) is server-side, since it narrows which sessions
 * are aggregated at all — but it can't be a `where` stage on the *list*
 * aggregate itself: a `where` there filters records, not sessions, so a
 * session with only some records carrying the matched attribute would lose
 * its other records from the aggregate (wrong views/duration/entry/exit/
 * error counts). Instead a filter runs as a first, separate bounded read —
 * the same shape as the list read but aggregating down to just the matching
 * `session.id`s — and the list read then scopes to `session.id in [...]`,
 * with no attribute `where` of its own. One request when no filter is set,
 * two when one is.
 */
import type { IrStage, QueryIrRequest, QueryIrResponse } from "./gen";
import { rangeDoc, runIrQuery } from "./queryIr";
import type { ResolvedRange } from "../lib/time";
import { parseBrowserFromUserAgent } from "../features/rum/rumModel";
import { serviceWhere } from "./rum";

const NS_PER_MS = 1_000_000;

/** `topk`'s `n` — sessions ranked by their own `last_ts` (most recently
 * active first), matching the verified query shape. */
const SESSION_LIST_LIMIT = 100;

export interface RumSessionRow {
  sessionId: string;
  firstMs: number;
  lastMs: number;
  durationMs: number;
  views: number;
  errors: number;
  slow: number;
  entry: string | null;
  exit: string | null;
  userId: string | null;
  browser: string | null;
  mobile: boolean | null;
}

/** `session.id` / `user.id` equality, or `key=value` for an arbitrary
 * attribute — the spec's "free-text filter SHALL accept `session.id`,
 * `user.id` or `attribute=value`". Empty/blank text has no filter. */
export function sessionsTextFilterWhere(text: string): IrStage | null {
  const trimmed = text.trim();
  if (trimmed === "") return null;
  const eq = trimmed.indexOf("=");
  if (eq > 0) {
    const field = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim();
    if (field !== "" && value !== "") {
      return { where: { field, op: "eq", value } };
    }
  }
  return {
    where: {
      or: [
        { field: "session.id", op: "eq", value: trimmed },
        { field: "user.id", op: "eq", value: trimmed },
      ],
    },
  };
}

/** `topk`'s `n` for the free-text filter's own id lookup — same budget as
 * the list itself, since a filter can never surface more sessions than an
 * unfiltered list would show anyway. */
const SESSION_ID_LOOKUP_LIMIT = SESSION_LIST_LIMIT;

/** The free-text filter's own read: which sessions have at least one
 * matching record, ranked and capped the same way the list is (see the
 * module doc). `null` for blank filter text — nothing to look up, so
 * `fetchSessions` skips this read entirely and goes straight to the list. */
export function buildSessionIdLookupDoc(
  app: string,
  range: ResolvedRange,
  filterText: string,
): QueryIrRequest | null {
  const textWhere = sessionsTextFilterWhere(filterText);
  if (!textWhere) return null;
  return {
    irVersion: 9,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      { where: { field: "session.id", op: "exists" } },
      textWhere,
      {
        aggregate: {
          by: ["session.id"],
          aggs: [{ fn: "max", of: "timestamp", as: "last_ts" }],
        },
      },
      { topk: { n: SESSION_ID_LOOKUP_LIMIT, of: "last_ts" } },
    ],
  };
}

/** The `session.id` column of a `buildSessionIdLookupDoc` response, busiest
 * (most recently active) first — the list read's `session.id in [...]`
 * scope. */
export function sessionIdsFromResponse(res: QueryIrResponse): string[] {
  return (res.rows ?? []).map((row) => (row as unknown[])[0] as string);
}

export function buildSessionsListDoc(
  app: string,
  range: ResolvedRange,
  sessionIds?: string[],
): QueryIrRequest {
  return {
    irVersion: 9,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      { where: { field: "session.id", op: "exists" } },
      ...(sessionIds
        ? ([
            { where: { field: "session.id", op: "in", value: sessionIds } },
          ] satisfies IrStage[])
        : []),
      {
        aggregate: {
          by: ["session.id"],
          aggs: [
            { fn: "min", of: "timestamp", as: "first_ts" },
            { fn: "max", of: "timestamp", as: "last_ts" },
            {
              fn: "count",
              as: "views",
              where: {
                field: "event_name",
                op: "eq",
                value: "browser.navigation",
              },
            },
            {
              fn: "count",
              as: "errors",
              where: { field: "event_name", op: "eq", value: "exception" },
            },
            {
              fn: "count",
              as: "slow",
              where: {
                and: [
                  { field: "browser.web_vital.name", op: "eq", value: "lcp" },
                  {
                    field: "browser.web_vital.rating",
                    op: "eq",
                    value: "poor",
                  },
                ],
              },
            },
            { fn: "first", of: "url.template", as: "entry" },
            { fn: "last", of: "url.template", as: "exit" },
            { fn: "last", of: "user.id", as: "user" },
            {
              fn: "last",
              of: "resource.user_agent.original",
              as: "ua",
            },
            { fn: "last", of: "resource.browser.mobile", as: "mobile" },
          ],
        },
      },
      { topk: { n: SESSION_LIST_LIMIT, of: "last_ts" } },
    ],
  };
}

/** ns → ms — the aggregate response's timestamps arrive as JS numbers (a
 * `table` result, unlike a `rows` result's exact-precision strings), so the
 * same bounded precision loss `decodePoints` already accepts elsewhere. */
function nsNumberToMs(v: unknown): number {
  return typeof v === "number" ? Math.round(v / NS_PER_MS) : 0;
}

export function sessionsFromResponse(res: QueryIrResponse): RumSessionRow[] {
  return (res.rows ?? []).map((row) => {
    const [
      sessionId,
      firstTs,
      lastTs,
      views,
      errors,
      slow,
      entry,
      exit,
      user,
      ua,
      mobile,
    ] = row as [
      string,
      number,
      number,
      number,
      number,
      number,
      string | null,
      string | null,
      string | null,
      string | null,
      boolean | null,
    ];
    const firstMs = nsNumberToMs(firstTs);
    const lastMs = nsNumberToMs(lastTs);
    return {
      sessionId,
      firstMs,
      lastMs,
      durationMs: Math.max(0, lastMs - firstMs),
      views: typeof views === "number" ? views : 0,
      errors: typeof errors === "number" ? errors : 0,
      slow: typeof slow === "number" ? slow : 0,
      entry: entry ?? null,
      exit: exit ?? null,
      userId: user ?? null,
      browser: parseBrowserFromUserAgent(ua ?? null),
      mobile: mobile ?? null,
    };
  });
}

export interface RumSessionQuickFilters {
  onlyErrors?: boolean;
  onlySlow?: boolean;
}

/** The Sessions tab's "With errors" / "Slow load (LCP poor)" quick filters
 * — client-side, from the already-aggregated counts (see the module doc). */
export function filterSessionsRows(
  rows: RumSessionRow[],
  filters: RumSessionQuickFilters,
): RumSessionRow[] {
  return rows.filter(
    (r) =>
      (!filters.onlyErrors || r.errors > 0) &&
      (!filters.onlySlow || r.slow > 0),
  );
}

export async function fetchSessions(
  app: string,
  range: ResolvedRange,
  filterText = "",
): Promise<RumSessionRow[]> {
  const lookupDoc = buildSessionIdLookupDoc(app, range, filterText);
  if (!lookupDoc) {
    return sessionsFromResponse(
      await runIrQuery(buildSessionsListDoc(app, range)),
    );
  }
  const sessionIds = sessionIdsFromResponse(await runIrQuery(lookupDoc));
  if (sessionIds.length === 0) return [];
  return sessionsFromResponse(
    await runIrQuery(buildSessionsListDoc(app, range, sessionIds)),
  );
}

// ---- Command palette: jump to a pasted session id -----------------------
//
// One bounded read — `session.id = <query>`, grouped by `service.name`,
// capped at one row — answers "does this session exist, and which app does
// it belong to" without scanning every session in the window (the spec's
// "Pasting a session id" scenario).

export function buildSessionLookupDoc(
  query: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      { where: { field: "session.id", op: "eq", value: query } },
      { aggregate: { by: ["service.name"], aggs: [{ fn: "count", as: "n" }] } },
      { limit: 1 },
    ],
  };
}

/** The session's app (`service.name`), or `null` when no record with that
 * `session.id` exists in the window. */
export function sessionLookupFromResponse(res: QueryIrResponse): string | null {
  const row = res.rows?.[0] as unknown[] | undefined;
  const app = row?.[0];
  return typeof app === "string" && app !== "" ? app : null;
}

export async function fetchSessionLookup(
  query: string,
  range: ResolvedRange,
): Promise<string | null> {
  return sessionLookupFromResponse(
    await runIrQuery(buildSessionLookupDoc(query, range)),
  );
}
