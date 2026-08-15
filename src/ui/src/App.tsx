import { useEffect, useRef } from "react";
import { Outlet } from "react-router";
import {
  loadPersistedTenantContext,
  persistTenantContext,
  setTenantContext,
} from "./api/http";
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

  // The tenant/dataset context lives in the URL (`?tenant=&dataset=`), but
  // plain links (user menu → Schema, /manage, deep links inside the schema
  // hub, …) drop the search string, and a bookmark or new tab starts with
  // none at all. A session-authenticated request without `X-Tenant-ID` is a
  // 401, which the login gate would misread as "logged out". So the last
  // non-empty context is sticky — within this tab via a ref, across tabs via
  // localStorage — it keeps feeding the API clients and is written back into
  // the URL so subsequent links carry it.
  const remembered = useRef(
    state.tenant
      ? { tenant: state.tenant, dataset: state.dataset }
      : (loadPersistedTenantContext() ?? { tenant: "", dataset: "" }),
  );
  if (state.tenant) {
    remembered.current = { tenant: state.tenant, dataset: state.dataset };
    persistTenantContext(remembered.current);
  }
  const effective = state.tenant ? state : { ...state, ...remembered.current };
  // Keep the API clients' tenant headers in sync with the (effective) state.
  setTenantContext({ tenant: effective.tenant, dataset: effective.dataset });
  useEffect(() => {
    if (!state.tenant && remembered.current.tenant) {
      update(remembered.current);
    }
  }, [state.tenant, update]);

  return (
    <div className="app-frame">
      <TopBar state={effective} update={update} />
      <main className="app-main">
        <Outlet context={{ state: effective, update }} />
      </main>
      <LoginGate
        onLoggedIn={({ tenant, dataset }) => update({ tenant, dataset })}
      />
    </div>
  );
}
