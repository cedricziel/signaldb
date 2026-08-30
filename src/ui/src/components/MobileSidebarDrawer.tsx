import type { ReactNode } from "react";

interface Props {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
}

/**
 * The "Filters" button that opens a `MobileSidebarDrawer`. A sibling of the
 * drawer rather than part of it: it sits outside the `.logs-body`/
 * `.traces-body`/`.errors-body` grid (see explore.css) so it never becomes a
 * grid item, while the drawer's content does.
 */
export function MobileFiltersToggle({
  open,
  onToggle,
}: {
  open: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      className="mobile-filters-toggle"
      aria-expanded={open}
      onClick={onToggle}
    >
      Filters
    </button>
  );
}

/**
 * Wraps a facet/field `<aside className="sidebar">` (FieldSidebar,
 * TraceFacets, ErrorFacets) so the same markup can also serve as a
 * dismissible mobile drawer below the tablet breakpoint — see
 * `useMobileSidebar` for the open state and explore.css's
 * `.mobile-sidebar-*` rules for the styling. Dismissible via the close
 * button, a backdrop click, or Escape (the latter from the hook itself).
 */
export function MobileSidebarDrawer({ open, onClose, children }: Props) {
  return (
    <>
      {open && (
        <div
          className="mobile-sidebar-backdrop"
          onClick={onClose}
          aria-hidden="true"
        />
      )}
      <div className={`mobile-sidebar-drawer${open ? " open" : ""}`}>
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
