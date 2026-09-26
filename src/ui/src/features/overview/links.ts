// In-app links out of the Overview. Every row and legend item is a plain
// link into an existing view (Catalog, Errors, Traces, Logs, …) carrying the
// window and tenant context — nothing filters in place.

import {
  buildPath,
  buildSearch,
  DEFAULT_STATE,
  type ExploreState,
} from "../../lib/urlState";

export function viewHref(
  path: string,
  state: ExploreState,
  patch: Partial<ExploreState> = {},
): string {
  return `${path}${buildSearch({
    ...DEFAULT_STATE,
    range: state.range,
    tenant: state.tenant,
    dataset: state.dataset,
    ...patch,
  })}`;
}

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
