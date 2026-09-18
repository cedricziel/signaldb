// Who is looking at the processors page, what may they do, and which
// datasets can they scope a processor to. Mirrors
// `features/schema/useSchemaSession.ts`'s tenant-admin rule: instance admins
// and tenant `admin` members may create, edit, and delete processors;
// everyone else sees the page read-only.
import { useOutletState } from "../../lib/outletState";
import { useWhoami } from "../../lib/useWhoami";
import type { WhoamiDataset, WhoamiResponse } from "../../api/session";

export interface ProcessorsSession {
  who: WhoamiResponse | undefined;
  isLoading: boolean;
  isTenantAdmin: boolean;
  tenant: string;
  dataset: string;
  datasets: WhoamiDataset[];
}

export function useProcessorsSession(): ProcessorsSession {
  const { state } = useOutletState();
  const { tenant, dataset } = state;
  const { data: who, isLoading } = useWhoami(state);
  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const isInstanceAdmin = !!who?.user?.is_instance_admin;
  return {
    who,
    isLoading: tenant === "" || isLoading,
    isTenantAdmin: isInstanceAdmin || role === "admin",
    tenant,
    dataset,
    datasets: who?.datasets ?? [],
  };
}
