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
  | "manage";

export interface NavPage {
  id: PageId;
  label: string;
  /** Route path, without search. */
  path: string;
}

export interface NavGroup {
  title: "Monitor" | "Investigate" | "Evaluate" | "Configure";
  pages: NavPage[];
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
      { id: "schema", label: "Schema", path: "/schema" },
      { id: "processors", label: "Processors", path: "/processors" },
      {
        id: "instrumentation",
        label: "Instrumentation",
        path: "/instrumentation",
      },
    ],
  },
];

export const MANAGE_PAGE: NavPage = {
  id: "manage",
  label: "Manage",
  path: "/manage",
};

const ALL_PAGES: NavPage[] = [
  ...NAV_GROUPS.flatMap((g) => g.pages),
  MANAGE_PAGE,
];

export function pageById(id: PageId): NavPage | undefined {
  return ALL_PAGES.find((p) => p.id === id);
}

/** Where the brand mark and the bare `/` route land. */
export const HOME_PATH = "/overview";

/** Explore pages share the URL-backed window and tenant context; their
 * links carry it over (see `crossSignalSearch`). Configure/admin pages
 * don't read the range, and the tenant context is sticky in `App` anyway. */
const EXPLORE_PAGES = new Set<PageId>(
  NAV_GROUPS.filter((g) => g.title !== "Configure").flatMap((g) =>
    g.pages.map((p) => p.id),
  ),
);

export function pageHref(page: NavPage, state: ExploreState): string {
  return EXPLORE_PAGES.has(page.id)
    ? `${page.path}${crossSignalSearch(state)}`
    : page.path;
}

/** Admin-only routes that live outside the sidebar groups but still need
 * a breadcrumb. */
const ADMIN_PATHS: Record<string, string> = {
  manage: "Manage",
  "api-keys": "API keys",
  integrations: "GitHub",
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
  const first = pathname.split("/")[1] ?? "";
  const admin = ADMIN_PATHS[first];
  if (admin) {
    return {
      id: first === "manage" ? "manage" : null,
      group: "Admin",
      label: admin,
    };
  }
  return { id: null, group: "", label: "" };
}
