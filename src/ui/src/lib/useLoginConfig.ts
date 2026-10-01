import { useQuery } from "@tanstack/react-query";
import { loginConfig, type LoginConfigResponse } from "../api/session";

/** Shown by LoginRoute in place of the credential controls while
 * `useLoginConfig()` is still loading (`undefined`) — never the
 * "unavailable" fallback, which would misreport a probe that just hasn't
 * answered yet. */
export const CHECKING_LOGIN_OPTIONS_HINT = "Checking sign-in options…";

/** The login-configuration probe, wrapped for the credential step: `undefined`
 * while the initial fetch is in flight, `"unavailable"` on any failure (a
 * network error, or a 404 from an older router) so the password-form
 * fallback (design decision 4) never depends on distinguishing failure
 * modes, and the parsed response once it succeeds. `retry: false` so a
 * failure settles immediately instead of retrying an unreachable probe. */
export function useLoginConfig():
  LoginConfigResponse | "unavailable" | undefined {
  const { data, isError, isLoading } = useQuery({
    queryKey: ["login-config"],
    queryFn: loginConfig,
    retry: false,
    staleTime: 60_000,
  });
  if (isLoading) return undefined;
  if (isError) return "unavailable";
  return data;
}
