// Typed accessor for the ExploreState/update pair the shell (App) passes
// down via <Outlet context>, shared by every route nested under it.

import { useOutletContext } from "react-router";
import type { ExploreState } from "./urlState";

export interface ShellContext {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function useOutletState(): ShellContext {
  return useOutletContext<ShellContext>();
}
