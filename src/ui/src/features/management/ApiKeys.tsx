import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Navigate } from "react-router";
import {
  ALL_SCOPES,
  INGEST_SCOPES,
  SCOPE_GROUPS,
  createApiKey,
  listApiKeys,
  revokeApiKey,
  updateApiKey,
  type ApiKeyScope,
} from "../../api/management";
import { whoami } from "../../api/session";
import { toErrorMessage } from "../../api/http";
import { ConfirmButton } from "../../components/ConfirmButton";
import { CopyValueButton } from "../../components/CopyValueButton";
import { Dialog } from "../../components/Dialog";
import {
  DatasetPicker,
  datasetRestrictionLabel,
  restrictionSet,
} from "./DatasetPicker";
import "./ApiKeys.css";

/** Scopes checked in a form, in vocabulary order. */
function selectedScopes(data: FormData): ApiKeyScope[] {
  return ALL_SCOPES.filter((scope) => data.has(scope));
}

/** Datasets checked in a form, via the shared `dataset` checkbox name. */
function selectedDatasetIds(data: FormData): string[] {
  return data.getAll("dataset").map(String);
}

/** The grouped scope picker (Ingestion / Schema / Management) shared by the create form
 * and the per-key editor. `idPrefix` keeps input ids unique per instance. */
function ScopePicker({
  idPrefix,
  checked,
}: {
  idPrefix: string;
  checked: (scope: ApiKeyScope) => boolean;
}) {
  return (
    <div className="scope-picker">
      {SCOPE_GROUPS.map((group) => (
        <fieldset key={group.name}>
          <legend>{group.name}</legend>
          {group.scopes.map(({ scope, description }) => {
            const id = `${idPrefix}-${scope.replace(":", "-")}`;
            return (
              <div key={scope} className="scope-option">
                <input
                  type="checkbox"
                  id={id}
                  name={scope}
                  defaultChecked={checked(scope)}
                />
                <label htmlFor={id}>{scope}</label>
                <span className="scope-description">{description}</span>
              </div>
            );
          })}
        </fieldset>
      ))}
    </div>
  );
}

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
  const [editingKeyId, setEditingKeyId] = useState<string | null>(null);
  // A distinct clear-restriction control for the update form (D1a): unlike
  // the dataset checkboxes, this is real React state so choosing it can
  // visibly disable the picker, rather than relying on "every box happens
  // to be unchecked" to mean the same thing.
  const [clearRestriction, setClearRestriction] = useState(false);

  const tenant = who?.tenant.id;
  const keys = useQuery({
    queryKey: ["managed-api-keys", tenant],
    queryFn: () => listApiKeys(tenant!),
    enabled: !!tenant,
  });

  const invalidateKeys = () =>
    queryClient.invalidateQueries({ queryKey: ["managed-api-keys", tenant] });

  const createMutation = useMutation({
    mutationFn: (input: {
      name?: string;
      dataset_ids?: string[];
      scopes: ApiKeyScope[];
    }) => createApiKey(tenant!, input),
    onSuccess: (result) => {
      setSecret(result.key);
      setError(null);
      void invalidateKeys();
    },
    onError: (value) => setError(toErrorMessage(value)),
  });

  const updateMutation = useMutation({
    mutationFn: (input: {
      keyId: string;
      scopes: ApiKeyScope[];
      dataset_ids?: string[];
      clear_dataset_restriction?: boolean;
    }) =>
      updateApiKey(tenant!, input.keyId, {
        scopes: input.scopes,
        dataset_ids: input.dataset_ids,
        clear_dataset_restriction: input.clear_dataset_restriction,
      }),
    onSuccess: () => {
      setEditingKeyId(null);
      setClearRestriction(false);
      setError(null);
      void invalidateKeys();
    },
    onError: (value) => setError(toErrorMessage(value)),
  });

  const revokeMutation = useMutation({
    mutationFn: (keyId: string) => revokeApiKey(tenant!, keyId),
    onSuccess: () => {
      void invalidateKeys();
    },
    onError: (value) => setError(toErrorMessage(value)),
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
    const scopes = selectedScopes(data);
    if (scopes.length === 0) {
      setError("Select at least one scope.");
      return;
    }
    // Omitting every dataset means unrestricted (D1a) — there is nothing to
    // clear on create, so an empty selection is never ambiguous here.
    const datasetIds = selectedDatasetIds(data);
    createMutation.mutate({
      name: String(data.get("name") ?? "").trim() || undefined,
      dataset_ids: datasetIds.length > 0 ? datasetIds : undefined,
      scopes,
    });
    form.reset();
  };

  const handleUpdate = (
    keyId: string,
    event: React.FormEvent<HTMLFormElement>,
  ) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const scopes = selectedScopes(data);
    if (scopes.length === 0) {
      setError("Select at least one scope.");
      return;
    }
    if (clearRestriction) {
      // The explicit clear signal (D1a): never paired with dataset_ids, and
      // never implied by an empty selection alone.
      updateMutation.mutate({ keyId, scopes, clear_dataset_restriction: true });
      return;
    }
    const datasetIds = selectedDatasetIds(data);
    updateMutation.mutate({
      keyId,
      scopes,
      // An empty selection here is never sent as `dataset_ids: []` (D1a) —
      // it leaves the key's current restriction untouched instead.
      ...(datasetIds.length > 0 ? { dataset_ids: datasetIds } : {}),
    });
  };

  const handleRevoke = (keyId: string) => {
    revokeMutation.mutate(keyId);
  };

  return (
    <div className="api-keys-page">
      <h1 className="api-keys-title">API keys</h1>
      <p className="api-keys-subtitle">
        Manage API keys for <strong>{who.tenant.id}</strong>. Every key carries
        explicit scopes; edit them any time without rotating the secret.
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
          <DatasetPicker
            idPrefix="create"
            datasets={datasets}
            checked={() => false}
          />
          <ScopePicker
            idPrefix="create"
            checked={(scope) => INGEST_SCOPES.includes(scope)}
          />
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
              <div className="api-key-main">
                <div className={`api-key-name ${key.revoked ? "revoked" : ""}`}>
                  {key.name || "Unnamed key"}
                </div>
                <div className="api-key-meta">
                  {datasetRestrictionLabel(key)} ·{" "}
                  {key.scopes?.length
                    ? key.scopes.join(", ")
                    : "legacy unrestricted"}
                  {` · created ${new Date(key.created_at).toLocaleDateString()}`}
                  {key.revoked && " · revoked"}
                </div>
                {editingKeyId === key.id && (
                  <form
                    className="api-key-editor"
                    aria-label="Edit scopes"
                    onSubmit={(event) => handleUpdate(key.id, event)}
                  >
                    <ScopePicker
                      idPrefix={`edit-${key.id}`}
                      checked={(scope) => key.scopes?.includes(scope) ?? false}
                    />
                    <DatasetPicker
                      idPrefix={`edit-${key.id}`}
                      datasets={datasets}
                      checked={(id) => restrictionSet(key).includes(id)}
                      disabled={clearRestriction}
                    />
                    <label className="dataset-clear">
                      <input
                        type="checkbox"
                        checked={clearRestriction}
                        onChange={(event) =>
                          setClearRestriction(event.target.checked)
                        }
                      />
                      Remove dataset restriction
                    </label>
                    <div className="api-key-editor-actions">
                      <button type="submit" disabled={updateMutation.isPending}>
                        Save scopes
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setEditingKeyId(null);
                          setClearRestriction(false);
                        }}
                      >
                        Cancel
                      </button>
                    </div>
                  </form>
                )}
              </div>
              {!key.revoked && (
                <div className="api-key-actions">
                  <button
                    className="api-key-edit"
                    onClick={() => {
                      setEditingKeyId(editingKeyId === key.id ? null : key.id);
                      setClearRestriction(false);
                    }}
                  >
                    Edit scopes
                  </button>
                  <ConfirmButton
                    className="api-key-revoke"
                    label="Revoke"
                    prompt={`Revoke ${key.name || "this key"}?`}
                    disabled={revokeMutation.isPending}
                    onConfirm={() => handleRevoke(key.id)}
                  />
                </div>
              )}
            </li>
          ))}
        </ul>
      </section>

      {secret && (
        <Dialog
          label="API key secret"
          onClose={() => setSecret(null)}
          className="secret-modal"
        >
          <strong>Copy this key now</strong>
          <span> — it will not be shown again.</span>
          <code>{secret}</code>
          <div className="secret-modal-footer">
            <CopyValueButton value={secret} label="API key" />
            <button onClick={() => setSecret(null)}>Done</button>
          </div>
        </Dialog>
      )}
    </div>
  );
}
