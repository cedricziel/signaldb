import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Navigate } from "react-router";
import {
  createApiKey,
  listApiKeys,
  revokeApiKey,
  type IngestScope,
} from "../../api/management";
import { whoami } from "../../api/session";
import { CopyValueButton } from "../../components/CopyValueButton";
import "./ApiKeys.css";

const scopes: IngestScope[] = [
  "metrics:write",
  "logs:write",
  "traces:write",
  "profiles:write",
];

export function ApiKeys() {
  const queryClient = useQueryClient();
  const { data: who, isLoading } = useQuery({
    queryKey: ["whoami"],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });

  const [secret, setSecret] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const tenant = who?.tenant.id;
  const keys = useQuery({
    queryKey: ["managed-api-keys", tenant],
    queryFn: () => listApiKeys(tenant!),
    enabled: !!tenant,
  });

  const createMutation = useMutation({
    mutationFn: (input: {
      name?: string;
      dataset_id?: string;
      scopes: IngestScope[];
    }) => createApiKey(tenant!, input),
    onSuccess: (result) => {
      setSecret(result.key);
      setError(null);
      void queryClient.invalidateQueries({
        queryKey: ["managed-api-keys", tenant],
      });
    },
    onError: (value) =>
      setError(value instanceof Error ? value.message : String(value)),
  });

  const revokeMutation = useMutation({
    mutationFn: (keyId: string) => revokeApiKey(tenant!, keyId),
    onSuccess: () => {
      void queryClient.invalidateQueries({
        queryKey: ["managed-api-keys", tenant],
      });
    },
    onError: (value) =>
      setError(value instanceof Error ? value.message : String(value)),
  });

  if (isLoading) return null;

  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const canManage = who?.user?.is_instance_admin || role === "admin";
  if (!who || !canManage) {
    return <Navigate to="/logs" replace />;
  }

  const datasets = who.datasets;

  const handleCreate = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = event.currentTarget;
    const data = new FormData(form);
    createMutation.mutate({
      name: String(data.get("name") ?? "").trim() || undefined,
      dataset_id: String(data.get("dataset") ?? "").trim() || undefined,
      scopes: scopes.filter((scope) => data.has(scope)),
    });
    form.reset();
  };

  const handleRevoke = (keyId: string) => {
    revokeMutation.mutate(keyId);
  };

  return (
    <div className="api-keys-page">
      <h1 className="api-keys-title">API keys</h1>
      <p className="api-keys-subtitle">
        Manage ingestion keys for <strong>{who.tenant.id}</strong>.
      </p>

      {error && <p className="manage-error">{error}</p>}

      <section className="api-keys-create">
        <h2>Create new key</h2>
        <form className="api-keys-form" onSubmit={handleCreate}>
          <input
            name="name"
            placeholder="collector-production"
            aria-label="Key name (optional)"
          />
          <select name="dataset" aria-label="Dataset" defaultValue="">
            <option value="">All datasets</option>
            {datasets.map((dataset) => (
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
          <button type="submit" disabled={createMutation.isPending}>
            Create API key
          </button>
        </form>
      </section>

      <section className="api-keys-list">
        <h2>Existing keys</h2>
        <ul>
          {(keys.data ?? []).map((key) => (
            <li
              key={key.id}
              className={`api-key-row ${key.revoked ? "revoked" : ""}`}
            >
              <div>
                <div className={`api-key-name ${key.revoked ? "revoked" : ""}`}>
                  {key.name || "Unnamed key"}
                </div>
                <div className="api-key-meta">
                  {key.dataset_id || "all datasets"} ·{" "}
                  {key.scopes?.length
                    ? key.scopes.join(", ")
                    : "legacy unrestricted"}
                  {"created_at" in key &&
                    typeof key.created_at === "string" && (
                      <>{` · created ${new Date(key.created_at).toLocaleDateString()}`}</>
                    )}
                  {key.revoked && " · revoked"}
                </div>
              </div>
              {!key.revoked && (
                <button
                  className="api-key-revoke"
                  onClick={() => handleRevoke(key.id)}
                  disabled={revokeMutation.isPending}
                >
                  Revoke
                </button>
              )}
            </li>
          ))}
        </ul>
      </section>

      {secret && (
        <div
          className="secret-modal-backdrop"
          role="dialog"
          aria-label="API key secret"
        >
          <div className="secret-modal">
            <strong>Copy this key now</strong>
            <span> — it will not be shown again.</span>
            <code>{secret}</code>
            <div className="secret-modal-footer">
              <CopyValueButton value={secret} label="API key" />
              <button onClick={() => setSecret(null)}>Done</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
