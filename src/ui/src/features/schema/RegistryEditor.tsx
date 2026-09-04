import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  Link,
  Navigate,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router";
import YAML from "yaml";
import { ConfirmButton } from "../../components/ConfirmButton";
import {
  createRegistry,
  deleteRegistry,
  getRegistry,
  replaceRegistry,
  validateRegistry,
  type RegistryDocument,
  type RegistryResponse,
  type ValidationReport,
} from "./api";
import { CONVENTIONS, registryPath } from "./paths";
import {
  diffRegistries,
  indexRegistry,
  type DiffSummary,
} from "./registryIndex";
import { useSchemaSession } from "./useSchemaSession";
import { toErrorMessage } from "../../api/http";

/**
 * `/schema/conventions/new` and `/schema/conventions/:ns/:version/edit` —
 * the custom-registry editor: a plain source editor over the Weaver-format
 * document (YAML or JSON, typed, pasted, or uploaded), server-side
 * Validate, and Save/Replace/Save-as-new-version/Delete. Tenant admins
 * only; bundled registries are never editable.
 */
export function RegistryEditor() {
  const { ns, version } = useParams<{ ns?: string; version?: string }>();
  const { isTenantAdmin, isLoading } = useSchemaSession();
  const editing = ns !== undefined && version !== undefined;
  const stored = useQuery({
    queryKey: ["schema-registry", ns, version],
    queryFn: () => getRegistry(ns!, version!),
    enabled: editing && isTenantAdmin,
    staleTime: 60_000,
  });

  if (isLoading) return null;
  if (!isTenantAdmin) return <Navigate to={CONVENTIONS} replace />;
  if (!editing) return <EditorForm stored={undefined} />;

  if (stored.isPending) return <p className="schema-note">Loading…</p>;
  if (stored.isError) {
    return (
      <div className="schema-page">
        <p className="schema-error">
          Could not load {ns}@{version}: {toErrorMessage(stored.error)}
        </p>
        <Link to={CONVENTIONS}>Back to conventions</Link>
      </div>
    );
  }
  if (stored.data.read_only) {
    return <Navigate to={registryPath(ns, version)} replace />;
  }
  return <EditorForm stored={stored.data} />;
}

type ParseOutcome =
  { ok: true; document: RegistryDocument } | { ok: false; message: string };

/** Parse the editor text as YAML (a superset of JSON) into a document. */
export function parseDocument(text: string): ParseOutcome {
  let value: unknown;
  try {
    value = YAML.parse(text);
  } catch (e) {
    return {
      ok: false,
      message: `Cannot parse document: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {
      ok: false,
      message: "Cannot parse document: expected a mapping with `groups`",
    };
  }
  return { ok: true, document: value as RegistryDocument };
}

type Report =
  | { kind: "parse-error"; message: string }
  | { kind: "validated"; report: ValidationReport; document: RegistryDocument };

const count = (n: number, one: string, many: string) =>
  `${n} ${n === 1 ? one : many}`;

function EditorForm({ stored }: { stored: RegistryResponse | undefined }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchParams] = useSearchParams();
  const fileInput = useRef<HTMLInputElement>(null);
  const [text, setText] = useState(() =>
    stored ? YAML.stringify(stored.document) : "",
  );
  const [outcome, setOutcome] = useState<Report | null>(null);
  const [validatedText, setValidatedText] = useState<string | null>(null);
  const [newVersion, setNewVersion] = useState("");
  const [error, setError] = useState<string | null>(null);

  // "Upload registry" from the list opens the file picker on arrival.
  useEffect(() => {
    if (searchParams.get("upload")) fileInput.current?.click();
  }, [searchParams]);

  const validation = useMutation({
    mutationFn: async (source: string): Promise<Report> => {
      const parsed = parseDocument(source);
      if (!parsed.ok) return { kind: "parse-error", message: parsed.message };
      const report = await validateRegistry(parsed.document);
      return { kind: "validated", report, document: parsed.document };
    },
    onSuccess: (result, source) => {
      setOutcome(result);
      setValidatedText(source);
      setError(null);
    },
    onError: (e) => setError(e instanceof Error ? e.message : String(e)),
  });

  const validated =
    outcome?.kind === "validated" &&
    outcome.report.errors.length === 0 &&
    validatedText === text
      ? outcome
      : null;

  const diff: DiffSummary | null = useMemo(() => {
    if (!stored || !validated) return null;
    return diffRegistries(
      indexRegistry(stored.document),
      indexRegistry(validated.document),
    );
  }, [stored, validated]);

  const finish = (namespace: string, version: string) => {
    void queryClient.invalidateQueries({ queryKey: ["schema-registries"] });
    void queryClient.invalidateQueries({ queryKey: ["schema-registry"] });
    void queryClient.invalidateQueries({ queryKey: ["schema-resolve"] });
    navigate(registryPath(namespace, version));
  };
  const onError = (e: unknown) =>
    setError(e instanceof Error ? e.message : String(e));

  const save = useMutation({
    mutationFn: async () => {
      if (!validated) throw new Error("Validate the document first");
      if (stored) {
        await replaceRegistry(
          stored.namespace,
          stored.version,
          validated.document,
        );
        return { namespace: stored.namespace, version: stored.version };
      }
      const created = await createRegistry(validated.document);
      return { namespace: created.namespace, version: created.version };
    },
    onSuccess: (r) => finish(r.namespace, r.version),
    onError,
  });

  const saveAsNew = useMutation({
    mutationFn: async () => {
      if (!validated) throw new Error("Validate the document first");
      const version = newVersion.trim();
      if (!version) throw new Error("Enter the new version");
      const created = await createRegistry({ ...validated.document, version });
      return { namespace: created.namespace, version: created.version };
    },
    onSuccess: (r) => finish(r.namespace, r.version),
    onError,
  });

  const remove = useMutation({
    mutationFn: async () => {
      if (!stored) return;
      await deleteRegistry(stored.namespace, stored.version);
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["schema-registries"] });
      navigate(CONVENTIONS);
    },
    onError,
  });

  const onFile = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      setText(String(reader.result ?? ""));
      setOutcome(null);
    };
    reader.onerror = () => setError(`Could not read ${file.name}`);
    reader.readAsText(file);
  };

  const busy =
    validation.isPending ||
    save.isPending ||
    saveAsNew.isPending ||
    remove.isPending;
  const title = stored
    ? `${stored.namespace}@${stored.version}`
    : "New registry";

  return (
    <div className="schema-page">
      <p className="schema-crumbs">
        <Link to={CONVENTIONS}>Conventions</Link> ›{" "}
        {stored ? (
          <>
            <Link to={registryPath(stored.namespace, stored.version)}>
              {title}
            </Link>{" "}
            › edit
          </>
        ) : (
          "new"
        )}
      </p>
      <h1 className="schema-title">{title}</h1>
      <p className="schema-subtitle">
        A registry in the OpenTelemetry Weaver semantic-convention model (
        <code>name</code>, <code>version</code>, <code>groups</code>), as YAML
        or JSON. Validate before saving; nothing is stored until Save.
      </p>

      <div className="schema-editor">
        <div className="schema-editor-toolbar">
          <label className="schema-button">
            Upload file
            <input
              ref={fileInput}
              type="file"
              accept=".yaml,.yml,.json,application/yaml,application/json"
              aria-label="Upload registry file"
              onChange={onFile}
              hidden
            />
          </label>
          <span className="spacer" />
          <button
            type="button"
            className="schema-button"
            disabled={busy || text.trim() === ""}
            onClick={() => validation.mutate(text)}
          >
            Validate
          </button>
          <button
            type="button"
            className="schema-button primary"
            disabled={busy || !validated}
            onClick={() => save.mutate()}
          >
            {stored ? "Replace" : "Save"}
          </button>
          {stored && (
            <>
              <input
                type="text"
                aria-label="New version"
                placeholder="1.1.0"
                value={newVersion}
                onChange={(e) => setNewVersion(e.target.value)}
              />
              <button
                type="button"
                className="schema-button"
                disabled={busy || !validated || newVersion.trim() === ""}
                onClick={() => saveAsNew.mutate()}
              >
                Save as new version
              </button>
              <ConfirmButton
                className="schema-button danger"
                label="Delete"
                prompt={`Delete ${title}?`}
                disabled={busy}
                onConfirm={() => remove.mutate()}
              />
            </>
          )}
        </div>

        <textarea
          aria-label="Registry document"
          spellCheck={false}
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder={
            "name: acme\nversion: 1.0.0\ngroups:\n  - id: registry.acme\n    type: attribute_group\n    attributes: []"
          }
        />

        {error && <p className="schema-error">{error}</p>}

        {outcome && (
          <ValidationOutcome outcome={outcome} stale={validatedText !== text} />
        )}

        {diff && <DiffView diff={diff} />}
      </div>
    </div>
  );
}

function ValidationOutcome({
  outcome,
  stale,
}: {
  outcome: Report;
  stale: boolean;
}) {
  if (outcome.kind === "parse-error") {
    return (
      <div className="schema-report failed" role="status">
        {outcome.message}
      </div>
    );
  }
  const { report } = outcome;
  const failed = report.errors.length > 0;
  return (
    <div className={`schema-report ${failed ? "failed" : "ok"}`} role="status">
      <strong>{failed ? "Invalid" : "Valid"}</strong> — {report.namespace}@
      {report.version} ·{" "}
      {count(report.attribute_count, "attribute", "attributes")} ·{" "}
      {count(report.entity_count, "entity", "entities")} ·{" "}
      {count(report.metric_count, "metric", "metrics")}
      {stale && " (document changed since — validate again)"}
      {failed && (
        <ul>
          {report.errors.map((e, i) => (
            <li key={`${e.path}-${i}`}>
              <code>{e.path}</code>: {e.message}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function DiffView({ diff }: { diff: DiffSummary }) {
  const row = (label: string, cls: string, items: string[]) => (
    <div className="schema-diff-row">
      <strong className={cls}>
        {label} ({items.length})
      </strong>
      <span>{items.length === 0 ? "—" : items.join(", ")}</span>
    </div>
  );
  return (
    <section className="schema-report schema-diff" aria-label="Changes">
      <strong>Changes vs. stored registry</strong>
      {row("Added", "added", diff.added)}
      {row("Changed", "changed", diff.changed)}
      {row("Removed", "removed", diff.removed)}
    </section>
  );
}
