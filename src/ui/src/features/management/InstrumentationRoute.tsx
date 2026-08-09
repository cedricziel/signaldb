import { useQuery } from "@tanstack/react-query";
import { Navigate } from "react-router";
import { whoami } from "../../api/session";
import { useOutletState } from "../../lib/outletState";
import { Instrumentation } from "./Instrumentation";

/**
 * `/instrumentation` — route for the instrumentation guide.
 * Requires authentication (whoami succeeds) but no admin role.
 * Redirects unauthenticated users to /logs.
 */
export function InstrumentationRoute() {
  const { state } = useOutletState();
  const { data: who, isLoading } = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });

  if (isLoading) return null;

  if (!who) {
    return <Navigate to="/logs" replace />;
  }

  return <Instrumentation state={state} />;
}
