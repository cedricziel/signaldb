import { useState } from "react";
import {
  whoami,
  type SessionMembership,
  type SessionResult,
} from "../api/session";

/** Copy shown by the "choose a tenant" render (LoginRoute) — the account has
 * several memberships and none was auto-selected. */
export const CHOOSE_TENANT_HINT =
  "Your account belongs to several tenants. Pick the one to explore — you can switch later from the top bar.";

/** Resolve a tenant's default dataset once a session cookie names it — the
 * context step LoginRoute runs after any credential establishes a session.
 * The cookie is already set at this point, so a lookup failure only means
 * starting without a dataset rather than blocking the sign-in. */
export async function resolveDefaultDataset(tenant: string): Promise<string> {
  try {
    const info = await whoami(tenant);
    return info.default_dataset ?? "";
  } catch {
    return "";
  }
}

/** The credential -> context hand-off (design decision 2): any credential
 * either resolves a tenant directly or reports several memberships to
 * choose from (`pending`); picking one resolves its default dataset the
 * same way. LoginRoute wires its own `onResolved` action (a navigate) onto
 * this state machine. */
export function useTenantStep(
  onResolved: (tenant: string, dataset: string) => void,
): {
  pending: SessionMembership[] | null;
  onAuthenticated: (result: SessionResult) => void;
  pick: (tenant: string) => void;
  busy: boolean;
} {
  const [pending, setPending] = useState<SessionMembership[] | null>(null);
  const [busy, setBusy] = useState(false);

  const onAuthenticated = (result: SessionResult) => {
    if (result.tenant) {
      onResolved(result.tenant, result.dataset ?? "");
    } else {
      setPending(result.memberships);
    }
  };

  const pick = (tenant: string) => {
    setBusy(true);
    resolveDefaultDataset(tenant)
      .then((dataset) => onResolved(tenant, dataset))
      .finally(() => setBusy(false));
  };

  return { pending, onAuthenticated, pick, busy };
}
