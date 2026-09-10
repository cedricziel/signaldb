import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import {
  currentSession,
  whoami,
  type CurrentSessionResponse,
  type WhoamiResponse,
} from "../api/session";

/** Shared "is this visitor already authenticated" check for `/select-tenant`,
 * which is tenant-scoped: `who` is set once a session cookie resolves via
 * `whoami()`, `isLoading` is true only for the initial check (`retry:
 * false`, so a 401 settles immediately rather than retrying). */
export function useWhoamiGate(): {
  who: WhoamiResponse | undefined;
  isLoading: boolean;
} {
  const { data: who, isLoading } = useQuery({
    queryKey: ["whoami"],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });
  return { who, isLoading };
}

/** `/login`'s "already authenticated?" gate: `GET /ui/session` answers this
 * without a tenant header, unlike `whoami()` (401 by design without one).
 * `retry: false` so an expected 401 (no cookie) settles immediately.
 * `staleTime: 0` (the default, made explicit): a revisit — e.g. right after
 * an SSO round trip lands back on `/login` — must always refetch instead of
 * answering from a cached 401, so `LoginRoute` can tell "still checking" (a
 * background refetch of stale data) from "confirmed unauthenticated" and
 * never flash the credential form for what turns out to be a fresh session. */
export function useCurrentSession(): UseQueryResult<CurrentSessionResponse> {
  return useQuery({
    queryKey: ["current-session"],
    queryFn: currentSession,
    retry: false,
    staleTime: 0,
  });
}
