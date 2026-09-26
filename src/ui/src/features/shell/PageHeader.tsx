// The main column's sticky 52px header on desktop and tablet: a
// "{Group} / {Page}" breadcrumb and the search field that opens the ⌘K
// palette. Its bottom border lines up with the sidebar's brand row.

import { useLocation } from "react-router";
import { NavIcon } from "./NavIcon";
import { currentPageFor } from "./navModel";

const isMac =
  typeof navigator !== "undefined" &&
  /mac|iphone|ipad/i.test(navigator.platform || navigator.userAgent);

export function PageHeader({ onOpenPalette }: { onOpenPalette: () => void }) {
  const { pathname } = useLocation();
  const page = currentPageFor(pathname);
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
        <span className="app-breadcrumb-page" aria-current="page">
          {page.label}
        </span>
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
    </header>
  );
}
