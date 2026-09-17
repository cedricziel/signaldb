// Who is looking at the schema hub, and what may they do. Mirrors the
// tenant-admin rule used by the management pages: instance admins and
// tenant `admin` members can manage custom registries; instance admins can
// additionally open the Storage tab.
import { useOutletState } from "../../lib/outletState";
import { useWhoami } from "../../lib/useWhoami";
import type { WhoamiResponse } from "../../api/session";

export interface SchemaSession {
  who: WhoamiResponse | undefined;
  isLoading: boolean;
  isInstanceAdmin: boolean;
  isTenantAdmin: boolean;
  /** The active tenant/dataset (from the shell's outlet state), for callers
   * that need it in their own query keys. */
  tenant: string;
  dataset: string;
}

export function useSchemaSession(): SchemaSession {
  // Reading the outlet state (rather than the imperative `getTenantContext`)
  // makes every caller a subscriber of the shell's context, so a tenant
  // switch re-renders the schema pages instead of leaving them on stale
  // props/query keys until something else happens to re-render them.
  const { state } = useOutletState();
  const { tenant, dataset } = state;
  const { data: who, isLoading } = useWhoami(state);
  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const isInstanceAdmin = !!who?.user?.is_instance_admin;
  return {
    who,
    isLoading,
    isInstanceAdmin,
    isTenantAdmin: isInstanceAdmin || role === "admin",
    tenant,
    dataset,
  };
}
