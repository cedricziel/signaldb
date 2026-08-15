// Who is looking at the schema hub, and what may they do. Mirrors the
// tenant-admin rule used by the management pages: instance admins and
// tenant `admin` members can manage custom registries; instance admins can
// additionally open the Storage tab.
import { useQuery } from "@tanstack/react-query";
import { whoami, type WhoamiResponse } from "../../api/session";

export interface SchemaSession {
  who: WhoamiResponse | undefined;
  isLoading: boolean;
  isInstanceAdmin: boolean;
  isTenantAdmin: boolean;
}

export function useSchemaSession(): SchemaSession {
  const { data: who, isLoading } = useQuery({
    queryKey: ["whoami"],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });
  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const isInstanceAdmin = !!who?.user?.is_instance_admin;
  return {
    who,
    isLoading,
    isInstanceAdmin,
    isTenantAdmin: isInstanceAdmin || role === "admin",
  };
}
