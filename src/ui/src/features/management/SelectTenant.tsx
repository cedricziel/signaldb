import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router";
import { useState } from "react";
import { whoami } from "../../api/session";
import { useOutletState } from "../../lib/outletState";
import "./SelectTenant.css";

const mpMountTime = 5 * 60_000; // 5 minutes

interface TenantDataset {
  id: string;
  slug: string;
  is_default: boolean;
}

interface TenantData {
  user?: {
    id: string;
    email: string;
    display_name: string | null;
    is_instance_admin: boolean;
  };
  memberships: Array<{
    tenant_id: string;
    role: "admin" | "member" | "viewer";
  }>;
  tenant: { id: string; slug: string; name: string };
  datasets: TenantDataset[];
  default_dataset: string | null;
}

interface TenantRowProps {
  tenantId: string;
  role: string;
  isExpanded: boolean;
  isActive: boolean;
  currentTenantId: string;
  /** The signed-in user's own tenant data, already fetched by the parent. */
  currentWho: TenantData;
  onTenantClick: (tenantId: string) => void;
  onDatasetClick: (tenantId: string, datasetId: string) => void;
}

function TenantRow({
  tenantId,
  role,
  isExpanded,
  isActive,
  currentTenantId,
  currentWho,
  onTenantClick,
  onDatasetClick,
}: TenantRowProps) {
  const { data: tenantData, isLoading } = useQuery<TenantData>({
    queryKey: ["whoami", tenantId],
    queryFn: () => whoami(tenantId),
    staleTime: mpMountTime,
    retry: false,
    enabled: isExpanded && tenantId !== currentTenantId,
  });

  const datasets =
    tenantId === currentTenantId
      ? currentWho?.datasets || []
      : tenantData?.datasets || [];

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
            <div className="dataset-loading">Loading...</div>
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

export function SelectTenant() {
  const navigate = useNavigate();
  const { state, update } = useOutletState();
  const [expandedTenants, setExpandedTenants] = useState<string[]>([
    state.tenant,
  ]);

  // Fetch current tenant info
  const { data: currentWho, isLoading } = useQuery<TenantData>({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: mpMountTime,
    retry: false,
  });

  if (isLoading || !currentWho) {
    return null;
  }

  const handleTenantClick = (tenantId: string) => {
    setExpandedTenants((prev) =>
      prev.includes(tenantId)
        ? prev.filter((id) => id !== tenantId)
        : [...prev, tenantId],
    );
  };

  const handleDatasetClick = (tenantId: string, datasetId: string) => {
    update({ tenant: tenantId, dataset: datasetId });
    navigate("/logs");
  };

  const isCurrentTenantActive = (tenantId: string) => state.tenant === tenantId;

  return (
    <div className="select-tenant">
      <div className="select-tenant-panel">
        <h2>Select tenant</h2>
        <p className="select-tenant-subtitle">Pick which tenant to explore.</p>

        <div className="tenant-list">
          {currentWho.memberships.map((membership) => {
            const tenantId = membership.tenant_id;
            const isExpanded = expandedTenants.includes(tenantId);
            const isActive = isCurrentTenantActive(tenantId);
            const role = membership.role;

            return (
              <TenantRow
                key={tenantId}
                tenantId={tenantId}
                role={role}
                isExpanded={isExpanded}
                isActive={isActive}
                currentTenantId={state.tenant}
                currentWho={currentWho}
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
