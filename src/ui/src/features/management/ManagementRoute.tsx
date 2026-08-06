import { useQuery } from "@tanstack/react-query";
import { Navigate, useNavigate } from "react-router";
import { whoami } from "../../api/session";
import { useOutletState } from "../../lib/outletState";
import { ManagementPanel } from "./ManagementPanel";

/**
 * `/manage` — a real, deep-linkable URL for the panel TopBar used to render
 * as ad hoc component state (which couldn't be bookmarked and didn't close
 * on browser back). Redirects non-admins and unauthenticated users back to
 * the logs view rather than rendering an empty panel.
 */
export function ManagementRoute() {
  const navigate = useNavigate();
  const { state, update } = useOutletState();
  const { data: who, isLoading } = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });

  if (isLoading) return null;

  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const canManage = who?.user?.is_instance_admin || role === "admin";
  if (!who || !canManage) {
    return <Navigate to="/logs" replace />;
  }

  return (
    <ManagementPanel
      who={who}
      onClose={() => navigate(-1)}
      onTenantCreated={(tenant, dataset) => {
        // update() navigates to /logs itself (the route's signal defaults to
        // "logs" off /manage), which closes the panel as a side effect.
        update({ tenant, dataset });
      }}
    />
  );
}
