// Tenant picker shown when a login resolves to several memberships instead
// of one (design decision 2, the shared context step). Rendered by
// LoginRoute.

import type { SessionMembership } from "../../api/session";

interface Props {
  memberships: SessionMembership[];
  onPicked: (tenantId: string) => void;
  busy: boolean;
}

export function TenantPicker({ memberships, onPicked, busy }: Props) {
  return (
    <ul className="login-tenants">
      {memberships.map((membership) => (
        <li key={membership.tenant_id}>
          <button
            type="button"
            disabled={busy}
            onClick={() => onPicked(membership.tenant_id)}
          >
            <span className="login-tenant-name">{membership.name}</span>
            <span className="login-tenant-meta">
              {membership.tenant_id} · {membership.role}
            </span>
          </button>
        </li>
      ))}
    </ul>
  );
}
