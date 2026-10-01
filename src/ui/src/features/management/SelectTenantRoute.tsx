import { Navigate } from "react-router";
import { useCurrentSession } from "../../lib/useWhoami";
import { HOME_PATH } from "../shell/navModel";
import { SelectTenant } from "./SelectTenant";

/**
 * `/select-tenant` — route for selecting a tenant after authentication.
 * Requires only a session cookie (`GET /ui/session`, tenant-independent) —
 * unlike the tenant-scoped `whoami()`, this resolves even before any tenant
 * is known, which is exactly the state a fresh SSO landing with several (or
 * zero) memberships lands in. Redirects unauthenticated visitors home.
 */
export function SelectTenantRoute() {
  const sessionQuery = useCurrentSession();

  if (sessionQuery.isLoading) return null;
  if (!sessionQuery.isSuccess) return <Navigate to={HOME_PATH} replace />;

  return <SelectTenant session={sessionQuery.data} />;
}
