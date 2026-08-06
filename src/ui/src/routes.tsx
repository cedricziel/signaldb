// The SPA's route tree. `/oauth/consent` is a distinct top-level view that
// bypasses the explore shell entirely; everything else nests under `App`
// (the shell), which shares its URL-backed ExploreState with children via
// outlet context so `/logs`, `/traces`, ... and `/manage` all read/write the
// same tenant, dataset, and range without re-deriving them.

import { Navigate, Route, Routes, useLocation, useParams } from "react-router";
import { App } from "./App";
import { ConsentView } from "./features/consent/ConsentView";
import { ExploreView } from "./features/explore/ExploreView";
import { ManagementRoute } from "./features/management/ManagementRoute";
import { useOutletState } from "./lib/outletState";
import { signalFromParam } from "./lib/urlState";

function ExploreRoute() {
  const { signal } = useParams<{ signal: string }>();
  const location = useLocation();
  const { state, update } = useOutletState();
  // An unknown path segment (typo, stale bookmark) settles on /logs instead
  // of silently rendering the logs view under the wrong URL.
  if (signalFromParam(signal) !== signal) {
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
        <Route path=":signal" element={<ExploreRoute />} />
        <Route path="*" element={<Navigate to="/logs" replace />} />
      </Route>
    </Routes>
  );
}
