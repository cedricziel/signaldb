// The SPA's route tree. `/oauth/consent` is a distinct top-level view that
// bypasses the explore shell entirely; everything else nests under `App`
// (the shell), which shares its URL-backed ExploreState with children via
// outlet context so `/logs`, `/traces`, ... and `/manage` all read/write the
// same tenant, dataset, and range without re-deriving them.

import { Navigate, Route, Routes, useLocation, useParams } from "react-router";
import { App } from "./App";
import { ConsentView } from "./features/consent/ConsentView";
import { ExploreView } from "./features/explore/ExploreView";
import { ApiKeysRoute } from "./features/management/ApiKeysRoute";
import { InstrumentationRoute } from "./features/management/InstrumentationRoute";
import { ManagementRoute } from "./features/management/ManagementRoute";
import { SelectTenantRoute } from "./features/management/SelectTenantRoute";
import { schemaRoutes } from "./features/schema/routes";
import { useOutletState } from "./lib/outletState";
import { signalFromParam } from "./lib/urlState";

function ExploreRoute() {
  const { signal, traceId } = useParams<{
    signal?: string;
    traceId?: string;
  }>();
  const location = useLocation();
  const { state, update } = useOutletState();
  // An unknown path segment (typo, stale bookmark) settles on /logs instead
  // of silently rendering the logs view under the wrong URL. Only the
  // generic `:signal` route needs this guard — `traces/:traceId`'s static
  // "traces" segment is always valid, and `signal` isn't even matched there.
  // Routes without a `:signal` param (traces/:traceId, catalog/...) are
  // valid by construction.
  if (
    traceId === undefined &&
    signal !== undefined &&
    signalFromParam(signal) !== signal
  ) {
    return <Navigate to={`/logs${location.search}`} replace />;
  }
  return <ExploreView state={state} update={update} />;
}

export function AppRoutes() {
  return (
    <Routes>
      <Route path="/oauth/consent" element={<ConsentView />} />
      <Route path="/" element={<App />}>
        <Route index element={<Navigate to="/logs" replace />} />
        <Route path="manage" element={<ManagementRoute />} />
        <Route path="select-tenant" element={<SelectTenantRoute />} />
        <Route path="api-keys" element={<ApiKeysRoute />} />
        <Route path="instrumentation" element={<InstrumentationRoute />} />
        {schemaRoutes()}
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
        <Route path="*" element={<Navigate to="/logs" replace />} />
      </Route>
    </Routes>
  );
}
