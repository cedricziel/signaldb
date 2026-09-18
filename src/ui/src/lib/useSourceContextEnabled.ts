// Gate for the "View source" affordance (see docs/users/explore-ui.md's
// "View source (GitHub)"): it only ever renders once the tenant has linked
// at least one GitHub repository, so a deployment without `[github]`
// configured — or a tenant that hasn't linked anything yet — never shows a
// button that can only ever answer "unavailable". A read-level probe (any
// signal reader can call it), not the management-scoped installation list —
// see `fetchSourceContextAvailability`.
import { useQuery } from "@tanstack/react-query";
import { fetchSourceContextAvailability } from "../api/sourceContext";

/** Query key for the availability probe, shared with callers (e.g.
 * `GitHubIntegration`) that need to invalidate it after linking/unlinking a
 * tenant's GitHub installation. */
export const sourceContextAvailabilityKey = (tenant: string) => [
  "source-context-availability",
  tenant,
];

/** Whether `tenant` has GitHub linked. Fails closed (`false`) on a network
 * or auth error — the source-context affordance simply doesn't appear
 * rather than surfacing its own error. */
export function useSourceContextEnabled(tenant: string): boolean {
  const query = useQuery({
    queryKey: sourceContextAvailabilityKey(tenant),
    queryFn: () => fetchSourceContextAvailability(tenant),
    staleTime: 5 * 60_000,
    retry: false,
    enabled: !!tenant,
  });
  return !!query.data?.configured && !!query.data?.linked;
}
