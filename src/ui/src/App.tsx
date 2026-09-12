import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Outlet, useLocation, useNavigate } from "react-router";
import {
  isAuthError,
  loadPersistedTenantContext,
  persistTenantContext,
  setTenantContext,
} from "./api/http";
import { ThrottleBanner } from "./features/shell/ThrottleBanner";
import { TopBar } from "./features/shell/TopBar";
import { loginRedirectPath, safeRedirectTarget } from "./lib/redirectTarget";
import { useExploreState } from "./lib/urlState";
import { useCurrentSession } from "./lib/useWhoami";

/**
 * The persistent shell (top bar + 401-to-`/login` redirect) around whichever
 * route is active — the explore view for a signal, or the management panel.
 * Renders state/update via outlet context so route children share the one
 * URL-backed ExploreState instead of re-deriving it.
 */
export function App() {
  const [state, update] = useExploreState();
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();

  // The tenant/dataset context lives in the URL (`?tenant=&dataset=`), but
  // plain links (user menu → Schema, /manage, deep links inside the schema
  // hub, …) drop the search string, and a bookmark or new tab starts with
  // none at all. A session-authenticated request without `X-Tenant-ID` is a
  // 401, which the redirect effect below would misread as "logged out". So
  // the last non-empty context is sticky — within this tab via a ref, across
  // tabs via localStorage — it keeps feeding the API clients and is written
  // back into the URL so subsequent links carry it.
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
  // `update` keeps the current path off the explore routes (see
  // useExploreState), so this only adds the search params.
  useEffect(() => {
    if (!state.tenant && remembered.current.tenant) {
      update(remembered.current);
    }
  }, [state.tenant, update]);

  // Nothing in the URL or remembered locally (a fresh browser, or a bookmark
  // predating any visit) — the only way left to place the visitor is a
  // session cookie, e.g. one an SSO callback just set landing on the return
  // target (not `/login`, so `LoginRoute`'s own resolution never runs). A
  // sole membership (or an SSO/session response that already names one)
  // goes straight into the URL; anything else — several memberships, or
  // none — defers to `/select-tenant`, which owns rendering a picker or the
  // no-access explanation.
  const needsTenantResolution = !state.tenant && !remembered.current.tenant;
  const sessionQuery = useCurrentSession(needsTenantResolution);
  useEffect(() => {
    if (!needsTenantResolution || !sessionQuery.isSuccess) return;
    const session = sessionQuery.data;
    if (session.tenant) {
      update({ tenant: session.tenant, dataset: session.dataset ?? "" });
      return;
    }
    if (location.pathname === "/select-tenant") return;
    const target = safeRedirectTarget(
      `${location.pathname}${location.search}${location.hash}`,
    );
    navigate(`/select-tenant?redirect=${encodeURIComponent(target)}`, {
      replace: true,
    });
  }, [
    needsTenantResolution,
    sessionQuery.isSuccess,
    sessionQuery.data,
    location.pathname,
    location.search,
    location.hash,
    navigate,
    update,
  ]);

  // A 401 anywhere in the app (session expiry, a request that outran the
  // cookie) sends the visitor to the dedicated login page rather than
  // popping a dialog over the current one — `LoginRoute` lands them back
  // here via `?redirect=` once signed in.
  useEffect(
    () =>
      queryClient.getQueryCache().subscribe((event) => {
        if (
          event.type === "updated" &&
          event.action.type === "error" &&
          isAuthError(event.action.error)
        ) {
          navigate(
            loginRedirectPath(
              `${location.pathname}${location.search}${location.hash}`,
            ),
            { replace: true },
          );
        }
      }),
    [queryClient, location.pathname, location.search, location.hash, navigate],
  );

  return (
    <div className="app-frame">
      <TopBar state={effective} update={update} />
      <ThrottleBanner />
      <main className="app-main">
        <Outlet context={{ state: effective, update }} />
      </main>
    </div>
  );
}
