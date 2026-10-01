// In-app links out of the Overview. Every row and legend item is a plain
// link into an existing view (Catalog, Errors, Traces, Logs, …) carrying the
// window and tenant context — nothing filters in place.

import { buildPath, viewHref, type ExploreState } from "../../lib/urlState";

export { viewHref };

/** A service's catalog entry, by its catalog composite identity key. */
export function serviceHref(key: string, state: ExploreState): string {
  return viewHref(
    buildPath("catalog", "", {
      catalogEntity: "service",
      catalogPrimary: key,
      catalogSecondary: "",
    }),
    state,
  );
}
