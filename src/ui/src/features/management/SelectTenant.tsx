import { useQuery } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router";
import { useState, type ReactNode } from "react";
import { toErrorMessage } from "../../api/http";
import { whoami, type CurrentSessionResponse } from "../../api/session";
import { safeRedirectTarget } from "../../lib/redirectTarget";
import { useOutletState } from "../../lib/outletState";
import "./SelectTenant.css";

const mpMountTime = 5 * 60_000; // 5 minutes

interface TenantDataset {
  id: string;
  slug: string;
  is_default: boolean;
}

interface TenantRowProps {
  tenantId: string;
  role: string;
  isExpanded: boolean;
  isActive: boolean;
  onTenantClick: (tenantId: string) => void;
  onDatasetClick: (tenantId: string, datasetId: string) => void;
}

function TenantRow({
  tenantId,
  role,
  isExpanded,
  isActive,
  onTenantClick,
  onDatasetClick,
}: TenantRowProps) {
  // Always scoped to `tenantId` explicitly (via the `X-Tenant-ID` header
  // `whoami(tenant)` sends) rather than relying on the app's current tenant
  // context — this renders correctly even before any tenant is chosen (a
  // fresh SSO landing with several memberships), when there is no "current"
  // tenant to special-case.
  const {
    data: tenantData,
    isLoading,
    isError,
    error,
    refetch,
  } = useQuery<{
    datasets: TenantDataset[];
  }>({
    queryKey: ["whoami", tenantId],
    queryFn: () => whoami(tenantId),
    staleTime: mpMountTime,
    retry: false,
    enabled: isExpanded,
  });

  const datasets = tenantData?.datasets ?? [];

  return (
    <div className={`tenant-row ${isActive ? "tenant-row-active" : ""}`}>
      <button
        className="tenant-row-header"
        onClick={() => onTenantClick(tenantId)}
        aria-expanded={isExpanded}
      >
        <span className="tenant-name">
          {isExpanded ? "▼" : "▶"} {tenantId}
        </span>
        <span className={`tenant-role tenant-role-${role}`}>{role}</span>
      </button>

      {isExpanded && (
        <div className="dataset-list">
          {isLoading ? (
            <div className="dataset-loading">Loading…</div>
          ) : isError ? (
            <div className="dataset-error" role="alert">
              <p>Failed to load datasets: {toErrorMessage(error)}</p>
              <button type="button" onClick={() => void refetch()}>
                Retry
              </button>
            </div>
          ) : (
            datasets.map((dataset) => (
              <button
                key={dataset.id}
                className="dataset-row"
                onClick={() => onDatasetClick(tenantId, dataset.id)}
              >
                <span className="dataset-name">· {dataset.id}</span>
                {dataset.is_default && (
                  <span className="dataset-default">default</span>
                )}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}

/** Shared wrapper for the two no-tenant-access explanations below — same
 * panel chrome, different heading and copy. */
function NoAccessPanel({
  heading,
  children,
}: {
  heading: string;
  children: ReactNode;
}) {
  return (
    <div className="select-tenant">
      <div className="select-tenant-panel">
        <h2>{heading}</h2>
        <p className="select-tenant-subtitle">{children}</p>
      </div>
    </div>
  );
}

export interface SelectTenantProps {
  /** The signed-in session, already resolved by `SelectTenantRoute` — no
   * tenant context is required to reach this page (a fresh SSO landing with
   * several memberships, or none, lands here with an empty tenant). */
  session: CurrentSessionResponse;
}

export function SelectTenant({ session }: SelectTenantProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { state, update } = useOutletState();
  const [expandedTenants, setExpandedTenants] = useState<string[]>(
    state.tenant ? [state.tenant] : [],
  );

  const handleTenantClick = (tenantId: string) => {
    setExpandedTenants((prev) =>
      prev.includes(tenantId)
        ? prev.filter((id) => id !== tenantId)
        : [...prev, tenantId],
    );
  };

  const handleDatasetClick = (tenantId: string, datasetId: string) => {
    update({ tenant: tenantId, dataset: datasetId });
    navigate(safeRedirectTarget(searchParams.get("redirect")));
  };

  if (session.memberships.length === 0 && session.user.is_instance_admin) {
    return (
      <NoAccessPanel heading="Instance admin, no tenant memberships yet">
        Your account <strong>{session.user.email}</strong> has instance-admin
        access but isn't a member of any tenant, so there's nothing to pick
        here. Ask another instance admin to add you to a tenant from that
        tenant's Members panel (<code>/manage</code>), or use{" "}
        <code>signaldb-cli</code> to add yourself directly.
      </NoAccessPanel>
    );
  }

  if (session.memberships.length === 0 && !session.user.is_instance_admin) {
    return (
      <NoAccessPanel heading="No tenant access yet">
        Your account <strong>{session.user.email}</strong> isn't a member of
        any tenant. Ask a tenant admin to add you.
      </NoAccessPanel>
    );
  }

  return (
    <div className="select-tenant">
      <div className="select-tenant-panel">
        <h2>Select tenant</h2>
        <p className="select-tenant-subtitle">Pick which tenant to explore.</p>

        <div className="tenant-list">
          {session.memberships.map((membership) => {
            const tenantId = membership.tenant_id;
            return (
              <TenantRow
                key={tenantId}
                tenantId={tenantId}
                role={membership.role}
                isExpanded={expandedTenants.includes(tenantId)}
                isActive={state.tenant === tenantId}
                onTenantClick={handleTenantClick}
                onDatasetClick={handleDatasetClick}
              />
            );
          })}
        </div>
      </div>
    </div>
  );
}
