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
  | "manage";

export interface NavPage {
  id: PageId;
  label: string;
  /** Route path, without search. */
  path: string;
}

export interface NavGroup {
  title: "Monitor" | "Investigate" | "Configure";
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

/** Where the brand mark and the bare `/` route land. */
export const HOME_PATH = "/overview";

/** Explore pages share the URL-backed window and tenant context; their
 * links carry it over (see `crossSignalSearch`). Configure/admin pages
 * don't read the range, and the tenant context is sticky in `App` anyway. */
const EXPLORE_PAGES = new Set<PageId>([
  "overview",
  "errors",
  "catalog",
  "logs",
  "traces",
  "metrics",
  "profiles",
  "query",
]);

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

/** The page a pathname belongs to — the first segment decides, so deep
 * links (`/traces/:id`, `/catalog/service/...`, `/schema/...`) keep their
 * section highlighted. */
export function currentPageFor(pathname: string): CurrentPage {
  const first = pathname.split("/")[1] ?? "";
  for (const group of NAV_GROUPS) {
    const page = group.pages.find((p) => p.path === `/${first}`);
    if (page) return { id: page.id, group: group.title, label: page.label };
  }
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
