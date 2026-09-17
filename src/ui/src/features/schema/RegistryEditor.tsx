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
import { invalidateSemantics } from "../../hooks/useSemantics";
import { useDirtyForm } from "../../lib/dirtyForms";
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
  const { isTenantAdmin, isLoading, tenant, dataset } = useSchemaSession();
  const editing = ns !== undefined && version !== undefined;
  const stored = useQuery({
    queryKey: ["schema-registry", ns, version, tenant, dataset],
    queryFn: () => getRegistry(ns!, version!),
    enabled: editing && isTenantAdmin,
    staleTime: 60_000,
  });

  if (isLoading) return null;
  if (!isTenantAdmin) return <Navigate to={CONVENTIONS} replace />;
  // Keyed on tenant/dataset/namespace/version: without it, navigating
  // between two registries whose data is already cached (no intervening
  // "Loading…" render to unmount the old instance) keeps the same
  // `EditorForm` mounted, letting unsaved text from the previous registry
  // carry into the new one instead of resetting to its own document.
  const formKey = `${tenant}|${dataset}|${ns ?? ""}|${version ?? ""}`;
  if (!editing) return <EditorForm key={formKey} stored={undefined} />;

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
  return <EditorForm key={formKey} stored={stored.data} />;
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
  const [searchParams, setSearchParams] = useSearchParams();
  const fileInput = useRef<HTMLInputElement>(null);
  const [text, setText] = useState(() =>
    stored ? YAML.stringify(stored.document) : "",
  );
  // The text as loaded (or last saved); compared against `text` to decide
  // whether there are unsaved edits to guard navigation against.
  const initialTextRef = useRef(text);
  const [pendingNav, setPendingNav] = useState<string | null>(null);
  const [outcome, setOutcome] = useState<Report | null>(null);
  const [validatedText, setValidatedText] = useState<string | null>(null);
  const [newVersion, setNewVersion] = useState("");
  const [error, setError] = useState<string | null>(null);
  const isDirty = text !== initialTextRef.current;
  // Protects an in-progress edit from a PWA update reload (see
  // lib/dirtyForms.ts) — a single id since only one editor instance is ever
  // mounted at a time (one route per registry/new-document).
  useDirtyForm("schema-registry-editor", isDirty);

  // "Upload registry" from the list opens the file picker on arrival, once —
  // the shell rewrites `?tenant=` on the way in, which would otherwise
  // re-fire this on every resulting `searchParams` change.
  const uploadFired = useRef(false);
  useEffect(() => {
    if (uploadFired.current || !searchParams.get("upload")) return;
    uploadFired.current = true;
    fileInput.current?.click();
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete("upload");
        return next;
      },
      { replace: true },
    );
  }, [searchParams, setSearchParams]);

  // A dirty document survives an in-app link click (confirmed inline below)
  // and warns on tab close/reload; it does not block Save/Replace/Delete's
  // own `navigate()` calls, which run after the edit is already persisted
  // (or discarded, for Delete).
  useEffect(() => {
    if (!isDirty) return;
    const onBeforeUnload = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = "";
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => window.removeEventListener("beforeunload", onBeforeUnload);
  }, [isDirty]);

  // Guards only this editor's own crumb links (below), each wired up
  // individually with `onClick={guardedNav(...)}` — there is no data router
  // here, so no `useBlocker` to intercept the top bar's links or the
  // browser's Back button; `beforeunload` above still covers reload/close.
  const guardedNav = (to: string) => (e: React.MouseEvent) => {
    if (!isDirty) return;
    e.preventDefault();
    setPendingNav(to);
  };

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

  // Every mutation that changes or removes a registry (save, replace, save-as,
  // delete) invalidates the same caches: the list, any stored copy, its
  // resolved definitions, and the tooltip/combobox semantics cache — leaving
  // any of these out lets stale data (or a deleted registry) keep rendering.
  const invalidateSchemaCaches = () => {
    void queryClient.invalidateQueries({ queryKey: ["schema-registries"] });
    void queryClient.invalidateQueries({ queryKey: ["schema-registry"] });
    void queryClient.invalidateQueries({ queryKey: ["schema-resolve"] });
    invalidateSemantics();
  };

  const finish = (namespace: string, version: string) => {
    invalidateSchemaCaches();
    // The document just saved is the new baseline: no unsaved edits remain,
    // so the navigation below isn't blocked by the dirty-document guard.
    initialTextRef.current = text;
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
      invalidateSchemaCaches();
      initialTextRef.current = text;
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
        <Link to={CONVENTIONS} onClick={guardedNav(CONVENTIONS)}>
          Conventions
        </Link>{" "}
        ›{" "}
        {stored ? (
          <>
            <Link
              to={registryPath(stored.namespace, stored.version)}
              onClick={guardedNav(registryPath(stored.namespace, stored.version))}
            >
              {title}
            </Link>{" "}
            › edit
          </>
        ) : (
          "new"
        )}
      </p>
      {pendingNav && (
        <p
          className="schema-report"
          role="alertdialog"
          aria-label="Unsaved changes"
        >
          You have unsaved changes.{" "}
          <button
            type="button"
            className="schema-button btn"
            onClick={() => {
              const to = pendingNav;
              setPendingNav(null);
              navigate(to);
            }}
          >
            Leave
          </button>{" "}
          <button
            type="button"
            className="schema-button btn"
            onClick={() => setPendingNav(null)}
          >
            Stay
          </button>
        </p>
      )}
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
            className="schema-button btn"
            disabled={busy || text.trim() === ""}
            onClick={() => validation.mutate(text)}
          >
            Validate
          </button>
          <button
            type="button"
            className="schema-button btn btn-primary"
            disabled={busy || !validated}
            title={validated ? undefined : "Validate first"}
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
                className="schema-button btn"
                disabled={busy || !validated || newVersion.trim() === ""}
                onClick={() => saveAsNew.mutate()}
              >
                Save as new version
              </button>
              <ConfirmButton
                className="schema-button"
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
          onChange={(e) => {
            setText(e.target.value);
            setError(null);
          }}
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
