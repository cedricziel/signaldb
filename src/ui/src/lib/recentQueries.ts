// The command palette's "Recent queries" group: the last few logs/traces
// queries this browser ran, remembered in localStorage. There's no
// server-side query history, so this is per-browser by design.

import type { ExploreState } from "./urlState";

const STORAGE_KEY = "sdb.recentQueries";
const MAX_ENTRIES = 10;

export interface RecentQuery {
  /** The query text as typed. */
  text: string;
  /** Which explore view ran it — shown as the palette row's meta. */
  signal: "logs" | "traces";
  /** Path + search that reopens it. */
  href: string;
}

export function loadRecentQueries(): RecentQuery[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(isRecentQuery).slice(0, MAX_ENTRIES);
  } catch {
    return [];
  }
}

/** Put `entry` first, dropping any older entry with the same text and
 * signal, and cap the list. */
export function recordRecentQuery(entry: RecentQuery): void {
  const text = entry.text.trim();
  if (text === "") return;
  const next = [
    { ...entry, text },
    ...loadRecentQueries().filter(
      (q) => !(q.text === text && q.signal === entry.signal),
    ),
  ].slice(0, MAX_ENTRIES);
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  } catch {
    // localStorage unavailable (private mode, quota) — history is optional.
  }
}

function isRecentQuery(value: unknown): value is RecentQuery {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.text === "string" &&
    (v.signal === "logs" || v.signal === "traces") &&
    typeof v.href === "string" &&
    isInAppPath(v.href)
  );
}

/** A same-origin path: leading `/`, but not `//host` or `/\host`, which a
 * browser (and React Router's `<Link>`) resolves to another origin. */
function isInAppPath(href: string): boolean {
  return href.startsWith("/") && href[1] !== "/" && href[1] !== "\\";
}

/** The logs or traces query `state` describes, as one line of text — the
 * label filters then the search text for logs, the facet filters for
 * traces. "" for other views, or when nothing narrows the query. */
export function recentQueryText(
  state: Pick<ExploreState, "signal" | "filters" | "search" | "traceFilters">,
): string {
  if (state.signal === "logs") {
    return [
      ...state.filters.map((f) => `${f.label}${f.op}${f.value}`),
      state.search.trim(),
    ]
      .filter(Boolean)
      .join(" ");
  }
  if (state.signal === "traces") {
    return state.traceFilters
      .map((f) => (f.op === "absent" ? `!${f.field}` : `${f.field}=${f.value}`))
      .join(" ");
  }
  return "";
}
