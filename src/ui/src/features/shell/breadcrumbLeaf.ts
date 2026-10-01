// The breadcrumb's optional last crumb — "Investigate / Traces / 4bf92f35".
// A detail page names what it shows with `useBreadcrumbLeaf`; the shell's
// page header and mobile top bar read it back with `useBreadcrumbLeafValue`.

import { createContext, useContext, useEffect } from "react";

export const SetBreadcrumbLeafContext = createContext<
  ((leaf: string | null) => void) | null
>(null);

export const BreadcrumbLeafContext = createContext<string | null>(null);

/** Shows `label` as the breadcrumb's leaf while the calling page is mounted.
 * An empty or missing label shows none. A no-op outside the shell. */
export function useBreadcrumbLeaf(label: string | null | undefined) {
  const setLeaf = useContext(SetBreadcrumbLeafContext);
  const leaf = label || null;
  useEffect(() => {
    if (!setLeaf || leaf === null) return;
    setLeaf(leaf);
    return () => setLeaf(null);
  }, [setLeaf, leaf]);
}

export function useBreadcrumbLeafValue(): string | null {
  return useContext(BreadcrumbLeafContext);
}
