import { useCallback, useState } from "react";
import { useEscapeKey } from "./useEscapeKey";

/**
 * Open/close state for the facet/field sidebar's mobile drawer (see
 * MobileSidebarDrawer and explore.css's `.mobile-sidebar-*` rules, active
 * below the tablet breakpoint). Desktop behavior — the sidebar as an
 * always-visible, resizable column — is untouched; this only backs the
 * drawer. Switching signal tabs unmounts the Logs/Traces/Errors view (see
 * ExploreView), which drops this state along with it, so there's nothing
 * extra to wire up for cross-tab navigation.
 */
export function useMobileSidebar() {
  const [open, setOpen] = useState(false);
  const close = useCallback(() => setOpen(false), []);
  const toggle = useCallback(() => setOpen((prev) => !prev), []);
  // Unconditional open, for callers (e.g. a waterfall span click) that must
  // reveal the drawer regardless of its current state, unlike `toggle`.
  const show = useCallback(() => setOpen(true), []);

  useEscapeKey(open, close);

  return { open, toggle, close, show };
}
