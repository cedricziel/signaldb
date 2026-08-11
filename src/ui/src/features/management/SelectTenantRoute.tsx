import { useQuery } from "@tanstack/react-query";
import { Navigate } from "react-router";
import { whoami } from "../../api/session";
import { SelectTenant } from "./SelectTenant";

/**
 * `/select-tenant` — route for selecting a tenant after authentication.
 * Requires authentication (whoami succeeds) but no admin role.
 * Redirects unauthenticated users to /logs.
 */
export function SelectTenantRoute() {
  const { data: who, isLoading } = useQuery({
    queryKey: ["whoami"],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });

  if (isLoading) return null;

  if (!who) {
    return <Navigate to="/logs" replace />;
  }

  return <SelectTenant />;
}
