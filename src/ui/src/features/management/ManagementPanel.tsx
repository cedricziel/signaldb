import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import {
  createApiKey,
  createDataset,
  createTenant,
  deleteDataset,
  listApiKeys,
  listMemberships,
  removeMembership,
  revokeApiKey,
  upsertMembership,
  type IngestScope,
} from "../../api/management";
import type { WhoamiResponse } from "../../api/session";
import "./management.css";

const scopes: IngestScope[] = [
  "metrics:write",
  "logs:write",
  "traces:write",
  "profiles:write",
];

interface Props {
  who: WhoamiResponse;
  onClose: () => void;
  onTenantCreated: (tenant: string, dataset: string) => void;
}

export function ManagementPanel({ who, onClose, onTenantCreated }: Props) {
  const tenant = who.tenant.id;
  const client = useQueryClient();
  const [secret, setSecret] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const keys = useQuery({
    queryKey: ["managed-api-keys", tenant],
    queryFn: () => listApiKeys(tenant),
  });
  const memberships = useQuery({
    queryKey: ["managed-memberships", tenant],
    queryFn: () => listMemberships(tenant),
  });
  const refresh = () => {
    void client.invalidateQueries({ queryKey: ["managed-api-keys", tenant] });
    void client.invalidateQueries({ queryKey: ["whoami"] });
    void client.invalidateQueries({
      queryKey: ["managed-memberships", tenant],
    });
  };
  const datasetMutation = useMutation({
    mutationFn: (name: string) => createDataset(tenant, name),
    onSuccess: refresh,
    onError: (value) =>
      setError(value instanceof Error ? value.message : String(value)),
  });
  const keyMutation = useMutation({
    mutationFn: (input: {
      name?: string;
      dataset_id?: string;
      scopes: IngestScope[];
    }) => createApiKey(tenant, input),
    onSuccess: (result) => {
      setSecret(result.key);
      setError(null);
      refresh();
    },
    onError: (value) =>
      setError(value instanceof Error ? value.message : String(value)),
  });

  return (
    <div className="manage-backdrop" role="dialog" aria-label="Manage tenant">
      <section className="manage-panel">
        <header>
          <div>
            <span className="eyebrow">Administration</span>
            <h2>{tenant}</h2>
          </div>
          <button onClick={onClose} aria-label="Close management">
            Close
          </button>
        </header>

        {error && <p className="manage-error">{error}</p>}
        {secret && (
          <div className="secret-once">
            <strong>Copy this key now</strong>
            <code>{secret}</code>
            <span>It will not be shown again.</span>
          </div>
        )}

        <div className="manage-grid">
          <section>
            <h3>Datasets</h3>
            <ul className="compact-list">
              {who.datasets.map((dataset) => (
                <li key={dataset.id}>
                  <div>
                    <code>{dataset.id}</code>
                    {dataset.is_default && <span>default</span>}
                  </div>
                  {!dataset.is_default && (
                    <button
                      onClick={() =>
                        deleteDataset(tenant, dataset.id)
                          .then(refresh)
                          .catch((value) =>
                            setError(
                              value instanceof Error
                                ? value.message
                                : String(value),
                            ),
                          )
                      }
                    >
                      Delete
                    </button>
                  )}
                </li>
              ))}
            </ul>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                const form = event.currentTarget;
                const name = String(new FormData(form).get("name") ?? "").trim();
                if (name) datasetMutation.mutate(name);
                form.reset();
              }}
            >
              <input name="name" placeholder="new-dataset" required />
              <button disabled={datasetMutation.isPending}>Create dataset</button>
            </form>
          </section>

          <section>
            <h3>API keys</h3>
            <ul className="compact-list">
              {(keys.data ?? []).map((key) => (
                <li key={key.id}>
                  <div>
                    <strong>{key.name || "Unnamed key"}</strong>
                    <span>
                      {key.dataset_id || "all datasets"} ·{" "}
                      {key.scopes?.join(", ") || "legacy unrestricted"}
                    </span>
                  </div>
                  {!key.revoked && (
                    <button
                      onClick={() =>
                        revokeApiKey(tenant, key.id).then(refresh).catch((value) =>
                          setError(
                            value instanceof Error ? value.message : String(value),
                          ),
                        )
                      }
                    >
                      Revoke
                    </button>
                  )}
                </li>
              ))}
            </ul>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                const data = new FormData(event.currentTarget);
                keyMutation.mutate({
                  name: String(data.get("name") ?? "").trim() || undefined,
                  dataset_id:
                    String(data.get("dataset") ?? "").trim() || undefined,
                  scopes: scopes.filter((scope) => data.has(scope)),
                });
              }}
            >
              <input name="name" placeholder="collector-production" />
              <select name="dataset" defaultValue="">
                <option value="">All datasets</option>
                {who.datasets.map((dataset) => (
                  <option key={dataset.id} value={dataset.id}>
                    {dataset.id}
                  </option>
                ))}
              </select>
              <fieldset>
                <legend>Ingestion scopes</legend>
                {scopes.map((scope) => (
                  <label key={scope}>
                    <input
                      type="checkbox"
                      name={scope}
                      defaultChecked={scope === "metrics:write"}
                    />
                    {scope}
                  </label>
                ))}
              </fieldset>
              <button disabled={keyMutation.isPending}>Create API key</button>
            </form>
          </section>
        </div>

        <section className="memberships">
          <h3>Members</h3>
          <ul className="compact-list">
            {(memberships.data ?? []).map((membership) => (
              <li key={membership.user_id}>
                <div>
                  <strong>{membership.email}</strong>
                  <span>{membership.role}</span>
                </div>
                {membership.user_id !== who.user?.id && (
                  <button
                    onClick={() =>
                      removeMembership(tenant, membership.user_id)
                        .then(refresh)
                        .catch((value) =>
                          setError(
                            value instanceof Error ? value.message : String(value),
                          ),
                        )
                    }
                  >
                    Remove
                  </button>
                )}
              </li>
            ))}
          </ul>
          <form
            className="membership-form"
            onSubmit={(event) => {
              event.preventDefault();
              const form = event.currentTarget;
              const data = new FormData(form);
              upsertMembership(tenant, {
                email: String(data.get("email") ?? "").trim(),
                role: String(data.get("role")) as
                  | "admin"
                  | "member"
                  | "viewer",
              })
                .then(() => {
                  form.reset();
                  refresh();
                })
                .catch((value) =>
                  setError(value instanceof Error ? value.message : String(value)),
                );
            }}
          >
            <input name="email" type="email" placeholder="user@example.com" required />
            <select name="role" defaultValue="viewer">
              <option value="viewer">Viewer</option>
              <option value="member">Member</option>
              <option value="admin">Admin</option>
            </select>
            <button>Add or update member</button>
          </form>
        </section>

        {who.user?.is_instance_admin && (
          <section className="instance-admin">
            <h3>New tenant</h3>
            <form
              onSubmit={(event) => {
                event.preventDefault();
                const form = event.currentTarget;
                const data = new FormData(form);
                const id = String(data.get("id") ?? "").trim();
                const dataset = String(data.get("dataset") ?? "").trim();
                createTenant({
                  id,
                  name: String(data.get("name") ?? "").trim(),
                  default_dataset: dataset || undefined,
                })
                  .then(() => onTenantCreated(id, dataset))
                  .catch((value) =>
                    setError(value instanceof Error ? value.message : String(value)),
                  );
              }}
            >
              <input name="id" placeholder="tenant-id" required />
              <input name="name" placeholder="Tenant name" required />
              <input name="dataset" placeholder="default dataset" />
              <button>Create tenant</button>
            </form>
          </section>
        )}
      </section>
    </div>
  );
}
