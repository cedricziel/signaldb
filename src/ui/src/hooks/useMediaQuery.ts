import { useSyncExternalStore } from "react";

/** Whether a CSS media query currently matches, kept live as the viewport
 * changes. For layout that has to change the rendered tree (not just its
 * styling), such as the app nav swapping its sidebar for a mobile top bar. */
export function useMediaQuery(query: string): boolean {
  return useSyncExternalStore(
    (onChange) => {
      const mql = window.matchMedia(query);
      mql.addEventListener?.("change", onChange);
      return () => mql.removeEventListener?.("change", onChange);
    },
    () => window.matchMedia(query).matches,
    () => false,
  );
}
