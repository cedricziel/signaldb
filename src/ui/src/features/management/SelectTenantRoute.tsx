import { Navigate } from "react-router";
import { useWhoamiGate } from "../../lib/useWhoami";
import { SelectTenant } from "./SelectTenant";

/**
 * `/select-tenant` — route for selecting a tenant after authentication.
 * Requires authentication (whoami succeeds) but no admin role.
 * Redirects unauthenticated users to /logs.
 */
export function SelectTenantRoute() {
  const { who, isLoading } = useWhoamiGate();

  if (isLoading) return null;

  if (!who) {
    return <Navigate to="/logs" replace />;
  }

  return <SelectTenant />;
}
