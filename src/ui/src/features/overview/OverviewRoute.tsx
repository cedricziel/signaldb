import { useOutletState } from "../../lib/outletState";
import { OverviewView } from "./OverviewView";

/** `/overview` — the landing page, inside the app shell. */
export function OverviewRoute() {
  const { state, update } = useOutletState();
  return <OverviewView state={state} update={update} />;
}
