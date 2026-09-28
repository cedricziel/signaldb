// `/rum/:tab` — redirects `/rum` to `/rum/overview` and an unknown tab to
// `overview`, preserving the query string (mirrors `RedirectToOverview` in
// routes.tsx). Inside the shell, so it shares tenant/dataset/range with
// every other explore page via outlet context.
import { Navigate, useLocation, useNavigate, useParams } from "react-router";
import { useOutletState } from "../../lib/outletState";
import { RealUsersView } from "./RealUsersView";
import { rumTabFromParam, type RumTab } from "./rumModel";

export function RealUsersRoute() {
  const { tab } = useParams<{ tab?: string }>();
  const location = useLocation();
  const navigate = useNavigate();
  const { state, update } = useOutletState();

  const known = rumTabFromParam(tab);
  if (tab !== known) {
    return <Navigate to={`/rum/${known}${location.search}`} replace />;
  }

  // The tab lives in the path, like the signal tabs (`buildPath`) — not a
  // search param — so switching it navigates rather than going through
  // `update()`, which only ever rewrites the search half of the URL.
  function onTabChange(next: RumTab) {
    navigate(`/rum/${next}${location.search}`);
  }

  return (
    <RealUsersView
      state={state}
      update={update}
      tab={known}
      onTabChange={onTabChange}
    />
  );
}
