import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, Navigate, NavLink, useParams } from "react-router";
import { getRegistry, listRegistries, type DefinitionKind } from "./api";
import { DefinitionPane } from "./DefinitionPane";
import {
  CONVENTIONS,
  LATEST,
  definitionPath,
  editorPath,
  isKind,
  registryPath,
} from "./paths";
import { filterByName, indexRegistry } from "./registryIndex";
import { useSchemaSession } from "./useSchemaSession";

/**
 * `/schema/conventions/:ns/:version[/:kind/:name]` — one registry: a search
 * box over its attributes, entities, and metrics (indexed client-side from
 * the stored document) and, when a definition is selected, its resolved
 * definition pane. Definition URLs are addressable deep links.
 */
export function RegistryBrowser() {
  const params = useParams<{
    ns: string;
    version: string;
    kind?: string;
    name?: string;
  }>();
  const namespace = params.ns ?? "";
  const version = params.version ?? "";
  if (version === LATEST) {
    return (
      <LatestRedirect
        namespace={namespace}
        kind={isKind(params.kind) ? params.kind : undefined}
        name={params.name}
      />
    );
  }
  return (
    <RegistryView
      namespace={namespace}
      version={version}
      kind={isKind(params.kind) ? params.kind : undefined}
      name={params.name}
    />
  );
}

/** Resolve the `latest` version placeholder against the registry list. */
function LatestRedirect({
  namespace,
  kind,
  name,
}: {
  namespace: string;
  kind?: DefinitionKind;
  name?: string;
}) {
  const registries = useQuery({
    queryKey: ["schema-registries"],
    queryFn: listRegistries,
    staleTime: 60_000,
  });
  if (registries.isPending) return <p className="schema-note">Loading…</p>;
  const found = registries.data?.find((r) => r.namespace === namespace);
  if (!found) {
    return (
      <div className="schema-page">
        <p className="schema-error">
          No visible registry named <code>{namespace}</code>.
        </p>
        <Link to={CONVENTIONS}>Back to conventions</Link>
      </div>
    );
  }
  const to =
    kind && name
      ? definitionPath(found.namespace, found.version, kind, name)
      : registryPath(found.namespace, found.version);
  return <Navigate to={to} replace />;
}

function RegistryView({
  namespace,
  version,
  kind,
  name,
}: {
  namespace: string;
  version: string;
  kind?: DefinitionKind;
  name?: string;
}) {
  const { isTenantAdmin } = useSchemaSession();
  const [query, setQuery] = useState("");
  const registry = useQuery({
    queryKey: ["schema-registry", namespace, version],
    queryFn: () => getRegistry(namespace, version),
    staleTime: 60_000,
  });
  const index = useMemo(
    () => (registry.data ? indexRegistry(registry.data.document) : undefined),
    [registry.data],
  );

  if (registry.isPending) return <p className="schema-note">Loading…</p>;
  if (registry.isError || !index) {
    return (
      <div className="schema-page">
        <p className="schema-error">
          Failed to load {namespace}@{version}:{" "}
          {registry.error instanceof Error
            ? registry.error.message
            : String(registry.error)}
        </p>
        <Link to={CONVENTIONS}>Back to conventions</Link>
      </div>
    );
  }

  const summary = registry.data;
  const canEdit = isTenantAdmin && !summary.read_only;
  const attributes = filterByName(index.attributes, (a) => a.key, query);
  const entities = filterByName(index.entities, (e) => e.name, query);
  const metrics = filterByName(index.metrics, (m) => m.name, query);

  return (
    <div className="schema-page">
      <p className="schema-crumbs">
        <Link to={CONVENTIONS}>Conventions</Link> ›{" "}
        <Link to={registryPath(namespace, version)}>
          {namespace}@{version}
        </Link>
        {kind && name && (
          <>
            {" "}
            › {kind}/{name}
          </>
        )}
      </p>
      <div className="schema-browser-head">
        <h1>
          {namespace}@{version}
        </h1>
        <span className={`schema-source schema-source-${summary.source}`}>
          {summary.source}
        </span>
        {summary.read_only && (
          <span
            className="schema-readonly"
            role="img"
            aria-label="read-only"
            title="Read-only (bundled)"
          >
            🔒
          </span>
        )}
        <span className="schema-note">
          {summary.attribute_count} attributes · {summary.entity_count} entities
          · {summary.metric_count} metrics
        </span>
        {canEdit && (
          <div className="schema-actions">
            <Link className="schema-button" to={editorPath(namespace, version)}>
              Edit
            </Link>
          </div>
        )}
      </div>
      {summary.description && (
        <p className="schema-subtitle">{summary.description}</p>
      )}

      <div className="schema-browser">
        <nav className="schema-browser-nav" aria-label="Definitions">
          <input
            className="schema-search"
            aria-label="Search definitions"
            placeholder="Filter by name…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <Section
            title="Attributes"
            count={attributes.length}
            items={attributes.map((a) => ({ name: a.key, title: a.brief }))}
            kind="attributes"
            namespace={namespace}
            version={version}
          />
          <Section
            title="Entities"
            count={entities.length}
            items={entities.map((e) => ({ name: e.name, title: e.brief }))}
            kind="entities"
            namespace={namespace}
            version={version}
          />
          <Section
            title="Metrics"
            count={metrics.length}
            items={metrics.map((m) => ({ name: m.name, title: m.brief }))}
            kind="metrics"
            namespace={namespace}
            version={version}
          />
        </nav>
        {kind && name ? (
          <DefinitionPane
            namespace={namespace}
            version={version}
            kind={kind}
            name={name}
          />
        ) : (
          <div className="schema-definition">
            <p className="schema-note">
              Select an attribute, entity, or metric to see its definition.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

const MAX_LISTED = 500;

function Section({
  title,
  count,
  items,
  kind,
  namespace,
  version,
}: {
  title: string;
  count: number;
  items: Array<{ name: string; title: string }>;
  kind: DefinitionKind;
  namespace: string;
  version: string;
}) {
  const shown = items.slice(0, MAX_LISTED);
  return (
    <section className="schema-section" aria-label={title}>
      <h2>
        <span>{title}</span>
        <span>{count}</span>
      </h2>
      {shown.length === 0 ? (
        <p className="schema-note">No matches.</p>
      ) : (
        <ul>
          {shown.map((item) => (
            <li key={item.name}>
              <NavLink
                to={definitionPath(namespace, version, kind, item.name)}
                title={item.title}
              >
                {item.name}
              </NavLink>
            </li>
          ))}
          {items.length > shown.length && (
            <li className="schema-note">
              …{items.length - shown.length} more — refine the filter
            </li>
          )}
        </ul>
      )}
    </section>
  );
}
