import type { ReactNode } from "react";

interface Props {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  /**
   * Which edge the drawer slides in from. Left (default) backs the facet/
   * field sidebars; right backs the trace waterfall's span-detail pane,
   * which sits on the right of its content on desktop (see traces.css).
   */
  side?: "left" | "right";
}

/**
 * The toggle button that opens a `MobileSidebarDrawer`. A sibling of the
 * drawer rather than part of it: it sits outside the `.logs-body`/
 * `.traces-body`/`.errors-body` grid (see explore.css) so it never becomes a
 * grid item, while the drawer's content does.
 */
export function MobileFiltersToggle({
  open,
  onToggle,
  label = "Filters",
}: {
  open: boolean;
  onToggle: () => void;
  /** Button text; "Filters" for the facet/field drawers, "Details" for the
   * trace waterfall's span-detail drawer. */
  label?: string;
}) {
  return (
    <button
      type="button"
      className="mobile-filters-toggle"
      aria-expanded={open}
      onClick={onToggle}
    >
      {label}
    </button>
  );
}

/**
 * Wraps a facet/field `<aside className="sidebar">` (FieldSidebar,
 * TraceFacets, ErrorFacets) or the trace waterfall's `<aside
 * className="span-detail">` so the same markup can also serve as a
 * dismissible mobile drawer below the tablet breakpoint — see
 * `useMobileSidebar` for the open state and explore.css's
 * `.mobile-sidebar-*` rules for the styling. Dismissible via the close
 * button, a backdrop click, or Escape (the latter from the hook itself).
 */
export function MobileSidebarDrawer({
  open,
  onClose,
  children,
  side = "left",
}: Props) {
  return (
    <>
      {open && (
        <div
          className="mobile-sidebar-backdrop"
          onClick={onClose}
          aria-hidden="true"
        />
      )}
      <div
        className={[
          "mobile-sidebar-drawer",
          side === "right" && "mobile-sidebar-drawer--right",
          open && "open",
        ]
          .filter(Boolean)
          .join(" ")}
      >
        {open && (
          <button
            type="button"
            className="mobile-sidebar-close"
            onClick={onClose}
          >
            Close ×
          </button>
        )}
        {children}
      </div>
    </>
  );
}
