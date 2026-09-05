import { useQuery } from "@tanstack/react-query";
import { whoami, type WhoamiResponse } from "../api/session";

/** Shared "is this visitor already authenticated" check for routes that
 * gate on it (`/select-tenant`, `/login`): `who` is set once a session
 * cookie resolves via `whoami()`, `isLoading` is true only for the initial
 * check (`retry: false`, so a 401 settles immediately rather than retrying). */
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
