// The main column's sticky 52px header on desktop and tablet: a
// "{Group} / {Page} / {Leaf}" breadcrumb (the leaf only on a detail page,
// see breadcrumbLeaf.ts), the search field that opens the ⌘K palette and
// the Connect button for MCP, CLI and API access.
// Its bottom border lines up with the sidebar's brand row.

import { Link, useLocation } from "react-router";
import type { ExploreState } from "../../lib/urlState";
import { useBreadcrumbLeafValue } from "./breadcrumbLeaf";
import { NavIcon } from "./NavIcon";
import { currentPageFor, pageById, pageHref } from "./navModel";

const isMac =
  typeof navigator !== "undefined" &&
  /mac|iphone|ipad/i.test(navigator.platform || navigator.userAgent);

export function PageHeader({
  state,
  onOpenPalette,
  onOpenConnect,
}: {
  state: ExploreState;
  onOpenPalette: () => void;
  onOpenConnect: () => void;
}) {
  const { pathname } = useLocation();
  const page = currentPageFor(pathname);
  const leaf = useBreadcrumbLeafValue();
  const section = page.id ? pageById(page.id) : undefined;
  const shortcut = isMac ? "⌘K" : "Ctrl K";
  return (
    <header className="app-page-header">
      <nav aria-label="Current page" className="app-breadcrumb">
        {page.group && (
          <>
            <span className="app-breadcrumb-group">{page.group}</span>
            <span className="app-breadcrumb-sep" aria-hidden="true">
              /
            </span>
          </>
        )}
        {leaf ? (
          <>
            {section ? (
              <Link
                className="app-breadcrumb-group app-breadcrumb-link"
                to={pageHref(section, state)}
              >
                {page.label}
              </Link>
            ) : (
              <span className="app-breadcrumb-group">{page.label}</span>
            )}
            <span className="app-breadcrumb-sep" aria-hidden="true">
              /
            </span>
            <span className="app-breadcrumb-page" aria-current="page">
              {leaf}
            </span>
          </>
        ) : (
          <span className="app-breadcrumb-page" aria-current="page">
            {page.label}
          </span>
        )}
      </nav>
      <span className="app-page-header-spacer" />
      <button
        type="button"
        className="app-search-trigger"
        onClick={onOpenPalette}
        aria-label={`Search (${shortcut})`}
        aria-haspopup="dialog"
      >
        <NavIcon name="search" size={15} />
        <span className="app-search-trigger-text">
          Search pages, services, trace IDs…
        </span>
        <kbd className="nav-kbd">{shortcut}</kbd>
      </button>
      <button
        type="button"
        className="app-connect-trigger"
        onClick={onOpenConnect}
        aria-haspopup="dialog"
      >
        <NavIcon name="connect" size={15} />
        Connect
      </button>
    </header>
  );
}
