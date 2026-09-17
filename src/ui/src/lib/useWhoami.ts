import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import {
  currentSession,
  whoami,
  type CurrentSessionResponse,
  type WhoamiResponse,
} from "../api/session";

/** `/login`'s (and `/select-tenant`'s, and the app shell's) "already
 * authenticated?" gate: `GET /ui/session` answers this without a tenant
 * header, unlike `whoami()` (401 by design without one).
 * `retry: false` so an expected 401 (no cookie) settles immediately.
 * `staleTime: 0` (the default, made explicit): a revisit — e.g. right after
 * an SSO round trip lands back on `/login` — must always refetch instead of
 * answering from a cached 401, so `LoginRoute` can tell "still checking" (a
 * background refetch of stale data) from "confirmed unauthenticated" and
 * never flash the credential form for what turns out to be a fresh session.
 * `enabled` defaults to `true`; the app shell passes `false` until it
 * actually needs to resolve a tenant from the session (nothing in the URL or
 * remembered locally), sharing this query's cache entry rather than issuing
 * its own. */
export function useCurrentSession(
  enabled = true,
): UseQueryResult<CurrentSessionResponse> {
  return useQuery({
    queryKey: ["current-session"],
    queryFn: currentSession,
    enabled,
    retry: false,
    staleTime: 0,
  });
}

/** {@link useWhoami}'s query result plus the derived management permission
 * every shell/admin surface otherwise recomputed from `data` itself. */
export type WhoamiResult = UseQueryResult<WhoamiResponse> & {
  /** Whether the signed-in user can reach tenant-admin surfaces: an
   * instance admin, or holding the "admin" role on the active tenant. */
  canManage: boolean;
};

/** The tenant-scoped `whoami()` query, shared by every shell/management
 * surface that needs the current user, role, and datasets. Gated on a
 * tenant being present in `state` — a request without `X-Tenant-ID` is a
 * 401 the router answers before any tenant has resolved, which the app
 * shell would otherwise misread as "logged out" — and keyed by
 * tenant/dataset so switching either refetches instead of answering from a
 * previous tenant's cached response. */
export function useWhoami(state: {
  tenant: string;
  dataset: string;
}): WhoamiResult {
  const query = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
    enabled: state.tenant !== "",
  });
  const who = query.data;
  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const canManage = Boolean(who?.user?.is_instance_admin || role === "admin");
  return { ...query, canManage };
}
