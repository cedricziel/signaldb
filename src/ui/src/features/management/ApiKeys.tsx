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
  selectedDatasetIds,
} from "./DatasetPicker";
import {
  OriginPicker,
  allowedOriginsLabel,
  allowedOriginsSet,
} from "./OriginPicker";
import "./ApiKeys.css";

/** Scopes checked in a form, in vocabulary order. */
function selectedScopes(data: FormData): ApiKeyScope[] {
  return ALL_SCOPES.filter((scope) => data.has(scope));
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
  // Allowed-origins are free-form strings, not a fixed checkable set, so the
  // picker's add/remove list is real React state (both for the create form
  // and the per-key editor) rather than form-derived like the scope/dataset
  // checkboxes.
  const [createOrigins, setCreateOrigins] = useState<string[]>([]);
  const [editOrigins, setEditOrigins] = useState<string[]>([]);
  const [clearOriginRestriction, setClearOriginRestriction] = useState(false);

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
      allowed_origins?: string[];
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
      allowed_origins?: string[];
      clear_allowed_origins?: boolean;
    }) =>
      updateApiKey(tenant!, input.keyId, {
        scopes: input.scopes,
        dataset_ids: input.dataset_ids,
        clear_dataset_restriction: input.clear_dataset_restriction,
        allowed_origins: input.allowed_origins,
        clear_allowed_origins: input.clear_allowed_origins,
      }),
    onSuccess: () => {
      setEditingKeyId(null);
      setClearRestriction(false);
      setClearOriginRestriction(false);
      setEditOrigins([]);
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
    // clear on create, so an empty selection is never ambiguous here. Same
    // reasoning for allowed_origins.
    const datasetIds = selectedDatasetIds(data);
    createMutation.mutate({
      name: String(data.get("name") ?? "").trim() || undefined,
      dataset_ids: datasetIds.length > 0 ? datasetIds : undefined,
      allowed_origins: createOrigins.length > 0 ? createOrigins : undefined,
      scopes,
    });
    form.reset();
    setCreateOrigins([]);
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
    // The dataset and allowed-origins restrictions are independent, each
    // with its own explicit clear signal (D1a) — never paired with a
    // non-empty replacement set, and never implied by an empty
    // picker/list alone.
    const datasetIds = selectedDatasetIds(data);
    updateMutation.mutate({
      keyId,
      scopes,
      ...(clearRestriction
        ? { clear_dataset_restriction: true }
        : datasetIds.length > 0
          ? { dataset_ids: datasetIds }
          : {}),
      ...(clearOriginRestriction
        ? { clear_allowed_origins: true }
        : editOrigins.length > 0
          ? { allowed_origins: editOrigins }
          : {}),
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
          <OriginPicker
            idPrefix="create"
            origins={createOrigins}
            onChange={setCreateOrigins}
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
                  {datasetRestrictionLabel(key)} · {allowedOriginsLabel(key)} ·{" "}
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
                    <OriginPicker
                      idPrefix={`edit-${key.id}`}
                      origins={editOrigins}
                      onChange={setEditOrigins}
                      disabled={clearOriginRestriction}
                      mode="update"
                    />
                    <label className="dataset-clear">
                      <input
                        type="checkbox"
                        checked={clearOriginRestriction}
                        onChange={(event) =>
                          setClearOriginRestriction(event.target.checked)
                        }
                      />
                      Remove allowed-origins restriction
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
                          setClearOriginRestriction(false);
                          setEditOrigins([]);
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
                      const opening = editingKeyId !== key.id;
                      setEditingKeyId(opening ? key.id : null);
                      setClearRestriction(false);
                      setClearOriginRestriction(false);
                      setEditOrigins(opening ? allowedOriginsSet(key) : []);
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
