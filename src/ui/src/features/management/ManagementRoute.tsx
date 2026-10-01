import { Navigate, useNavigate } from "react-router";
import { whoamiQueryError } from "../../components/QueryError";
import { useOutletState } from "../../lib/outletState";
import { goBackOr } from "../../lib/router";
import { crossSignalSearch } from "../../lib/urlState";
import { useWhoami } from "../../lib/useWhoami";
import { HOME_PATH } from "../shell/navModel";
import { ManagementPanel } from "./ManagementPanel";

/**
 * `/manage` — a real, deep-linkable URL for the panel the old top bar used to render
 * as ad hoc component state (which couldn't be bookmarked and didn't close
 * on browser back). Redirects non-admins home; a 401 is handled globally
 * (the app shell sends it to `/login`); any other failure (5xx, network, an
 * older server without the endpoint) shows an inline error instead of
 * silently bouncing home.
 */
export function ManagementRoute() {
  const navigate = useNavigate();
  const { state, update } = useOutletState();
  const {
    data: who,
    isLoading,
    isError,
    error,
    canManage,
  } = useWhoami(state);

  if (isLoading) return null;
  if (isError) return whoamiQueryError("your account", error);

  if (!who || !canManage) {
    return <Navigate to={`${HOME_PATH}${crossSignalSearch(state)}`} replace />;
  }

  return (
    <ManagementPanel
      who={who}
      onClose={() =>
        goBackOr(navigate, () =>
          navigate(`${HOME_PATH}${crossSignalSearch(state)}`, {
            replace: true,
          }),
        )
      }
      onTenantCreated={(tenant, dataset) => {
        // Setting the signal explicitly leaves /manage for /logs, which
        // closes the panel as a side effect.
        update({ tenant, dataset, signal: "logs" });
      }}
    />
  );
}
