import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { currentSession, type CurrentSessionResponse } from "../api/session";

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
