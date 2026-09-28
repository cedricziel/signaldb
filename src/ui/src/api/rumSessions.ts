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
 * The free-text filter is a later addition to this module (a separate
 * change): it needs its own bounded read to narrow which *sessions* this
 * list aggregates, not a `where` on this aggregate itself.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
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

export function buildSessionsListDoc(
  app: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 9,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      { where: { field: "session.id", op: "exists" } },
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
): Promise<RumSessionRow[]> {
  return sessionsFromResponse(
    await runIrQuery(buildSessionsListDoc(app, range)),
  );
}
