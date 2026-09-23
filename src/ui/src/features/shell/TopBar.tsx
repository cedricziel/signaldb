// Thin container: wires the tenant-scoped whoami query and the demo-account
// probe, and hands the result to the presentational `components/TopBar`.
import { TopBar as TopBarView } from "../../components/TopBar";
import { useIsDemo, useWhoami } from "../../lib/useWhoami";
import type { ExploreState } from "../../lib/urlState";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function TopBar({ state, update }: Props) {
  const { data: who, canManage } = useWhoami(state);
  const isDemo = useIsDemo();
  return (
    <TopBarView
      state={state}
      update={update}
      who={who}
      canManage={canManage}
      isDemo={isDemo}
    />
  );
}
