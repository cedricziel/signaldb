import { Outlet } from "react-router";
import { setTenantContext } from "./api/http";
import { LoginGate } from "./features/shell/LoginPanel";
import { TopBar } from "./features/shell/TopBar";
import { useExploreState } from "./lib/urlState";

/**
 * The persistent shell (top bar + login gate) around whichever route is
 * active — the explore view for a signal, or the management panel. Renders
 * state/update via outlet context so route children share the one
 * URL-backed ExploreState instead of re-deriving it.
 */
export function App() {
  const [state, update] = useExploreState();
  // Keep the API clients' tenant headers in sync with the URL state.
  setTenantContext({ tenant: state.tenant, dataset: state.dataset });

  return (
    <div className="app-frame">
      <TopBar state={state} update={update} />
      <main className="app-main">
        <Outlet context={{ state, update }} />
      </main>
      <LoginGate
        onLoggedIn={({ tenant, dataset }) => update({ tenant, dataset })}
      />
    </div>
  );
}
