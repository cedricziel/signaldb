// The app navigation's information architecture: which pages exist, how
// they group in the sidebar, and which one a pathname belongs to. Pure data
// + functions so the sidebar, the page header's breadcrumb, the mobile top
// bar and the command palette all agree on one list.

import { crossSignalSearch, type ExploreState } from "../../lib/urlState";

export type PageId =
  | "overview"
  | "errors"
  | "catalog"
  | "logs"
  | "traces"
  | "metrics"
  | "profiles"
  | "query"
  | "schema"
  | "processors"
  | "instrumentation"
  | "evals"
  | "compare"
  | "runs"
  | "evaluators"
  | "manage"
  | "api-keys"
  | "integrations";

export interface NavPage {
  id: PageId;
  label: string;
  /** Route path, without search. */
  path: string;
  /** Hidden in the read-only demo account: the page's purpose is editing. */
  mutating?: boolean;
}

export interface NavGroup {
  title: "Monitor" | "Investigate" | "Evaluate" | "Configure" | "Settings";
  pages: NavPage[];
  /** Shown only to tenant and instance admins. */
  adminOnly?: boolean;
}

export const NAV_GROUPS: NavGroup[] = [
  {
    title: "Monitor",
    pages: [
      { id: "overview", label: "Overview", path: "/overview" },
      { id: "errors", label: "Errors", path: "/errors" },
      { id: "catalog", label: "Catalog", path: "/catalog" },
    ],
  },
  {
    title: "Investigate",
    pages: [
      { id: "logs", label: "Logs", path: "/logs" },
      { id: "traces", label: "Traces", path: "/traces" },
      { id: "metrics", label: "Metrics", path: "/metrics" },
      { id: "profiles", label: "Profiles", path: "/profiles" },
      { id: "query", label: "Query", path: "/query" },
    ],
  },
  {
    title: "Evaluate",
    pages: [
      { id: "evals", label: "Agents & scores", path: "/evals" },
      { id: "compare", label: "Compare", path: "/evals/compare" },
      { id: "runs", label: "Runs", path: "/evals/runs" },
      { id: "evaluators", label: "Evaluators", path: "/evals/evaluators" },
    ],
  },
  {
    title: "Configure",
    pages: [
      { id: "schema", label: "Schema", path: "/schema", mutating: true },
      {
        id: "processors",
        label: "Processors",
        path: "/processors",
        mutating: true,
      },
      { id: "instrumentation", label: "Send data", path: "/instrumentation" },
    ],
  },
  {
    title: "Settings",
    adminOnly: true,
    pages: [
      { id: "manage", label: "Manage", path: "/manage" },
      { id: "api-keys", label: "API keys", path: "/api-keys" },
      {
        id: "integrations",
        label: "Integrations",
        path: "/integrations/github",
      },
    ],
  },
];

const ALL_PAGES: NavPage[] = NAV_GROUPS.flatMap((g) => g.pages);

/** The groups and pages this viewer gets: Settings only for admins, and no
 * editing pages in the read-only demo account. Every nav surface (sidebar,
 * drawer, palette) lists this, so they can't disagree. */
export function visibleNavGroups({
  canManage,
  isDemo,
}: {
  canManage: boolean;
  isDemo: boolean;
}): NavGroup[] {
  return NAV_GROUPS.filter((g) => canManage || !g.adminOnly)
    .map((g) => ({
      ...g,
      pages: g.pages.filter((p) => !(isDemo && p.mutating)),
    }))
    .filter((g) => g.pages.length > 0);
}

export function pageById(id: PageId): NavPage | undefined {
  return ALL_PAGES.find((p) => p.id === id);
}

/** Where the brand mark and the bare `/` route land. */
export const HOME_PATH = "/overview";

/** Explore pages share the URL-backed window and tenant context; their
 * links carry it over (see `crossSignalSearch`). Configure and Settings
 * pages don't read the range, and the tenant context is sticky in `App`
 * anyway. */
const EXPLORE_PAGES = new Set<PageId>(
  NAV_GROUPS.filter(
    (g) => g.title !== "Configure" && g.title !== "Settings",
  ).flatMap((g) => g.pages.map((p) => p.id)),
);

export function pageHref(page: NavPage, state: ExploreState): string {
  return EXPLORE_PAGES.has(page.id)
    ? `${page.path}${crossSignalSearch(state)}`
    : page.path;
}

/** Routes outside the sidebar that still need a breadcrumb. */
const UNLISTED_PATHS: Record<string, string> = {
  "select-tenant": "Switch tenant",
};

export interface CurrentPage {
  /** The sidebar item to highlight, if the route has one. */
  id: PageId | null;
  group: string;
  label: string;
}

/** The page a pathname belongs to — the longest page path that prefixes
 * it wins, so deep links (`/traces/:id`, `/catalog/service/...`,
 * `/evals/compare/case`) keep their own section highlighted. */
export function currentPageFor(pathname: string): CurrentPage {
  let best: CurrentPage | null = null;
  let bestLen = 0;
  for (const group of NAV_GROUPS) {
    for (const page of group.pages) {
      const hit =
        pathname === page.path || pathname.startsWith(`${page.path}/`);
      if (hit && page.path.length > bestLen) {
        best = { id: page.id, group: group.title, label: page.label };
        bestLen = page.path.length;
      }
    }
  }
  if (best) return best;
  const label = UNLISTED_PATHS[pathname.split("/")[1] ?? ""];
  return { id: null, group: "", label: label ?? "" };
}
