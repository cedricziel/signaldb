// The SPA's route tree. `/oauth/consent` and `/login` are distinct top-level
// views that bypass the explore shell entirely; everything else nests under
// `App` (the shell), which shares its URL-backed ExploreState with children
// via outlet context so `/logs`, `/traces`, ... and `/manage` all read/write
// the same tenant, dataset, and range without re-deriving them.

import { useEffect } from "react";
import {
  createBrowserRouter,
  createRoutesFromElements,
  Navigate,
  Outlet,
  matchRoutes,
  Route,
  useLocation,
  useParams,
  type RouteObject,
} from "react-router";
import { App } from "./App";
import { ConsentView } from "./features/consent/ConsentView";
import { ExploreView } from "./features/explore/ExploreView";
import { evalsRoutes } from "./features/evals/routes";
import { OverviewRoute } from "./features/overview/OverviewRoute";
import { GitHubIntegrationRoute } from "./features/integrations/GitHubIntegrationRoute";
import { ApiKeysRoute } from "./features/management/ApiKeysRoute";
import { InstrumentationRoute } from "./features/management/InstrumentationRoute";
import { ManagementRoute } from "./features/management/ManagementRoute";
import { SelectTenantRoute } from "./features/management/SelectTenantRoute";
import { LoginRoute } from "./features/shell/LoginRoute";
import { HOME_PATH } from "./features/shell/navModel";
import { RouteErrorBoundary } from "./features/shell/RouteErrorBoundary";
import { UnsavedChangesGuard } from "./features/shell/UnsavedChangesGuard";
import { processorsRoutes } from "./features/processors/routes";
import { RealUsersRoute } from "./features/rum/RealUsersRoute";
import { schemaRoutes } from "./features/schema/routes";
import { useOutletState } from "./lib/outletState";
import { signalFromParam } from "./lib/urlState";
import { buildRouteTemplate } from "./telemetry/routeTemplate";
import { setRouteTemplate } from "./telemetry/routeTemplateLogRecordProcessor";

let routeTree: RouteObject[] | undefined;

/** The telemetry `url.template` for a pathname, from the declared paths of
 * the routes it matches. */
export function routeTemplateFor(pathname: string): string | undefined {
  routeTree ??= createRoutesFromElements(routeElements());
  const matches = matchRoutes(routeTree, pathname);
  const leaf = matches?.at(-1);
  if (!matches || !leaf) return undefined;
  return buildRouteTemplate(
    matches.map((m) => m.route.path),
    leaf.params,
  );
}

/** Keeps the telemetry `url.template` in step with the matched route. */
function RouteTemplateReporter() {
  const { pathname } = useLocation();
  useEffect(() => {
    setRouteTemplate(routeTemplateFor(pathname));
  }, [pathname]);
  return null;
}

/**
 * Pathless root layout above every route, including `/oauth/consent` and
 * `/login` which sit outside `App`'s explore shell — `UnsavedChangesGuard`
 * needs to intercept in-app navigation away from *any* dirty form (see
 * lib/dirtyForms.ts), not only ones nested under the shell.
 */
function RootLayout() {
  return (
    <>
      <UnsavedChangesGuard />
      <RouteTemplateReporter />
      <Outlet />
    </>
  );
}

/** Redirects home (`/overview`, the landing page) — for `/` and any
 * unrecognized path — preserving the query string, so a deep link's
 * `?tenant=&dataset=` survives the redirect. */
function RedirectHome() {
  const location = useLocation();
  return <Navigate to={`${HOME_PATH}${location.search}`} replace />;
}

/** `/rum` opens its default tab, preserving the query string. */
function RedirectToRumOverview() {
  const location = useLocation();
  return <Navigate to={`/rum/overview${location.search}`} replace />;
}

function ExploreRoute() {
  const { signal, traceId } = useParams<{
    signal?: string;
    traceId?: string;
  }>();
  const { state, update } = useOutletState();
  // An unknown path segment (typo, stale bookmark) settles on the home page
  // instead of silently rendering the logs view under the wrong URL. Only the
  // generic `:signal` route needs this guard — `traces/:traceId`'s static
  // "traces" segment is always valid, and `signal` isn't even matched there.
  // Routes without a `:signal` param (traces/:traceId, catalog/...) are
  // valid by construction.
  if (
    traceId === undefined &&
    signal !== undefined &&
    signalFromParam(signal) !== signal
  ) {
    return <RedirectHome />;
  }
  return <ExploreView state={state} update={update} />;
}

/**
 * The route tree as JSX `<Route>` elements — the single source
 * `createAppRouter` below turns into a data router (via
 * `createRoutesFromElements`), which is what `useBlocker` needs for the
 * unsaved-edit guard.
 */
export function routeElements() {
  return (
    <Route element={<RootLayout />} errorElement={<RouteErrorBoundary />}>
      <Route path="/oauth/consent" element={<ConsentView />} />
      <Route path="/login" element={<LoginRoute />} />
      <Route path="/" element={<App />}>
        <Route index element={<RedirectHome />} />
        <Route path="overview" element={<OverviewRoute />} />
        <Route path="manage" element={<ManagementRoute />} />
        <Route path="select-tenant" element={<SelectTenantRoute />} />
        <Route path="api-keys" element={<ApiKeysRoute />} />
        <Route
          path="integrations/github"
          element={<GitHubIntegrationRoute />}
        />
        <Route path="instrumentation" element={<InstrumentationRoute />} />
        {/* `/rum` opens Overview, preserving the query string — mirrors
            RedirectToOverview. The tab lives in the path (see
            RealUsersRoute), not a search param. */}
        <Route path="rum" element={<RedirectToRumOverview />} />
        <Route path="rum/:tab" element={<RealUsersRoute />} />
        {schemaRoutes()}
        {processorsRoutes()}
        {evalsRoutes()}
        {/* Single-trace view is a route, not a `?trace=` param on /traces —
            see buildPath in lib/urlState.ts. Matched by React Router's
            specificity ranking regardless of declaration order relative to
            :signal below, but listed first for readability. */}
        <Route path="traces/:traceId" element={<ExploreRoute />} />
        {/* Catalog selection is path state too — entity type, then the
            drilled-into entity and breakdown row (see lib/urlState.ts's
            buildPath / parseCatalogPath). */}
        <Route path="catalog/:entity" element={<ExploreRoute />} />
        <Route path="catalog/:entity/:primary" element={<ExploreRoute />} />
        <Route
          path="catalog/:entity/:primary/:secondary"
          element={<ExploreRoute />}
        />
        <Route path=":signal" element={<ExploreRoute />} />
        <Route path="*" element={<RedirectHome />} />
      </Route>
    </Route>
  );
}

/**
 * A data router over the same tree — the only one that supports
 * `useBlocker`, which `RootLayout`'s `UnsavedChangesGuard` needs to
 * intercept in-app navigation (links, the tab strip, the user menu, browser
 * Back/Forward) while a form is dirty. Shared by `main.tsx` and any test
 * that renders the real shell (App.test.tsx), so both stay on the same route
 * tree and router construction.
 */
export function createAppRouter() {
  return createBrowserRouter(createRoutesFromElements(routeElements()));
}
