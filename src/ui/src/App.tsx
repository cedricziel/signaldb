import { setTenantContext } from "./api/http";
import { ExploreView } from "./features/explore/ExploreView";
import { TopBar } from "./features/shell/TopBar";
import { useExploreState } from "./lib/urlState";

export function App() {
  const [state, update] = useExploreState();
  // Keep the API clients' tenant headers in sync with the URL state.
  setTenantContext({ tenant: state.tenant, dataset: state.dataset });

  return (
    <div className="app-frame">
      <TopBar state={state} update={update} />
      <main className="app-main">
        <ExploreView state={state} update={update} />
      </main>
    </div>
  );
}
