import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import {
  resolveAttribute,
  resolveEntity,
  resolveMetric,
  type AttributeHit,
  type DefinitionKind,
  type EntityHit,
  type MetricHit,
} from "./api";
import { LATEST, definitionPath, kindLabel } from "./paths";
import { humanizeGroupId } from "./registryIndex";

type AnyHit = AttributeHit | EntityHit | MetricHit;

interface Props {
  namespace: string;
  version: string;
  kind: DefinitionKind;
  name: string;
}

/**
 * One definition (attribute / entity / metric) of a registry, resolved via
 * the server so the pane shows the same fully-resolved shape as MCP and
 * tooltips do, plus every other visible registry defining the same name.
 */
export function DefinitionPane({ namespace, version, kind, name }: Props) {
  const resolution = useQuery({
    queryKey: ["schema-resolve", kind, name],
    queryFn: async (): Promise<AnyHit[]> => {
      switch (kind) {
        case "attributes":
          return (await resolveAttribute(name)).hits;
        case "entities":
          return (await resolveEntity(name)).hits;
        case "metrics":
          return (await resolveMetric(name)).hits;
      }
    },
    staleTime: 60_000,
  });

  if (resolution.isPending) {
    return (
      <div className="schema-definition" aria-busy="true">
        <p className="schema-note">Loading…</p>
      </div>
    );
  }
  if (resolution.isError) {
    return (
      <div className="schema-definition">
        <p className="schema-error">
          Failed to resolve <code>{name}</code>:{" "}
          {resolution.error instanceof Error
            ? resolution.error.message
            : String(resolution.error)}
        </p>
      </div>
    );
  }

  const hits = resolution.data;
  const index = hits.findIndex(
    (h) => h.namespace === namespace && h.version === version,
  );
  const hit = index >= 0 ? hits[index] : undefined;
  if (!hit) {
    return (
      <article className="schema-definition">
        <h2>{name}</h2>
        <p className="schema-note">
          Not defined in {namespace}@{version}.
        </p>
        <Alternatives hits={hits} kind={kind} name={name} self={undefined} />
      </article>
    );
  }
  const isPrimary = index === 0;

  return (
    <article
      className="schema-definition"
      aria-label={`${kindLabel(kind)} ${name}`}
    >
      <div className="schema-definition-kind">
        {kindLabel(kind)} · {hit.namespace} · {hit.version} · {hit.source}{" "}
        {isPrimary ? (
          <span className="schema-badge">primary</span>
        ) : (
          <span className="schema-badge muted">shadowed</span>
        )}
      </div>
      <h2>{name}</h2>
      <p className="schema-definition-brief">{hit.brief}</p>
      {hit.deprecated && (
        <p className="schema-deprecated">
          Deprecated
          {hit.deprecated.reason ? ` (${hit.deprecated.reason})` : ""}
          {hit.deprecated.renamed_to && (
            <>
              {" — replaced by "}
              <Link
                to={definitionPath(
                  hit.namespace,
                  hit.version,
                  kind,
                  hit.deprecated.renamed_to,
                )}
              >
                {hit.deprecated.renamed_to}
              </Link>
            </>
          )}
          {hit.deprecated.note ? `. ${hit.deprecated.note}` : ""}
        </p>
      )}
      {hit.note && <p className="schema-definition-note">{hit.note}</p>}
      {kind === "attributes" && <AttributeBody hit={hit as AttributeHit} />}
      {kind === "entities" && <EntityBody hit={hit as EntityHit} />}
      {kind === "metrics" && <MetricBody hit={hit as MetricHit} />}
      <Alternatives hits={hits} kind={kind} name={name} self={hit} />
    </article>
  );
}

/** `namespace/entity` → that entity's page (same version when it is this
 * registry, otherwise the namespace's visible version). */
function qualifiedEntityPath(qualified: string, hit: AnyHit): string {
  const slash = qualified.indexOf("/");
  const namespace = slash >= 0 ? qualified.slice(0, slash) : hit.namespace;
  const entity = slash >= 0 ? qualified.slice(slash + 1) : qualified;
  const version = namespace === hit.namespace ? hit.version : LATEST;
  return definitionPath(namespace, version, "entities", entity);
}

function AttributeBody({ hit }: { hit: AttributeHit }) {
  return (
    <>
      <dl className="schema-props">
        <dt>Type</dt>
        <dd>{hit.type}</dd>
        <dt>Group</dt>
        <dd className="prose">
          {hit.group_display_name || humanizeGroupId(hit.group_id)}{" "}
          <code>({hit.group_id})</code>
        </dd>
        {hit.stability && (
          <>
            <dt>Stability</dt>
            <dd>{hit.stability}</dd>
          </>
        )}
        {hit.requirement_level && (
          <>
            <dt>Requirement level</dt>
            <dd>{hit.requirement_level}</dd>
          </>
        )}
        {hit.examples && hit.examples.length > 0 && (
          <>
            <dt>Examples</dt>
            <dd>
              {hit.examples
                .map((e) => (typeof e === "string" ? e : JSON.stringify(e)))
                .join(", ")}
            </dd>
          </>
        )}
      </dl>
      {hit.enum_members && hit.enum_members.length > 0 && (
        <>
          <h3>Enum members</h3>
          <ul aria-label="Enum members">
            {hit.enum_members.map((m) => (
              <li key={m.id}>
                <code>{JSON.stringify(m.value)}</code>
                {m.brief ? ` — ${m.brief}` : ""}
              </li>
            ))}
          </ul>
        </>
      )}
      {hit.entity_roles && hit.entity_roles.length > 0 && (
        <>
          <h3>Entity roles</h3>
          <ul aria-label="Entity roles">
            {hit.entity_roles.map((r) => (
              <li key={`${r.namespace}/${r.entity}`}>
                <Link
                  to={definitionPath(
                    r.namespace,
                    r.namespace === hit.namespace ? hit.version : LATEST,
                    "entities",
                    r.entity,
                  )}
                >
                  {r.namespace}/{r.entity}
                </Link>
                <span className="schema-role">{r.role}</span>
              </li>
            ))}
          </ul>
        </>
      )}
    </>
  );
}

function EntityBody({ hit }: { hit: EntityHit }) {
  const attrList = (label: string, items: EntityHit["identifying"]) => (
    <>
      <h3>{label}</h3>
      {items.length === 0 ? (
        <p className="schema-note">None.</p>
      ) : (
        <ul aria-label={label}>
          {items.map((a) => (
            <li key={a.key}>
              <Link
                to={definitionPath(
                  hit.namespace,
                  hit.version,
                  "attributes",
                  a.key,
                )}
              >
                {a.key}
              </Link>
              {a.requirement_level && (
                <span className="schema-role">{a.requirement_level}</span>
              )}
            </li>
          ))}
        </ul>
      )}
    </>
  );
  return (
    <>
      <dl className="schema-props">
        {hit.stability && (
          <>
            <dt>Stability</dt>
            <dd>{hit.stability}</dd>
          </>
        )}
        {hit.extends && (
          <>
            <dt>Extends</dt>
            <dd>{hit.extends}</dd>
          </>
        )}
      </dl>
      {attrList("Identifying", hit.identifying)}
      {attrList("Descriptive", hit.descriptive)}
      <h3>Associated metrics</h3>
      {hit.metrics && hit.metrics.length > 0 ? (
        <ul aria-label="Associated metrics">
          {hit.metrics.map((m) => (
            <li key={m}>
              <Link
                to={definitionPath(hit.namespace, hit.version, "metrics", m)}
              >
                {m}
              </Link>
            </li>
          ))}
        </ul>
      ) : (
        <p className="schema-note">None.</p>
      )}
      {hit.extended_by && hit.extended_by.length > 0 && (
        <>
          <h3>Extended by</h3>
          <ul aria-label="Extended by">
            {hit.extended_by.map((e) => (
              <li key={e}>
                <Link to={qualifiedEntityPath(e, hit)}>{e}</Link>
              </li>
            ))}
          </ul>
        </>
      )}
    </>
  );
}

function MetricBody({ hit }: { hit: MetricHit }) {
  return (
    <>
      <dl className="schema-props">
        <dt>Instrument</dt>
        <dd>{hit.instrument}</dd>
        <dt>Unit</dt>
        <dd>{hit.unit}</dd>
        {hit.stability && (
          <>
            <dt>Stability</dt>
            <dd>{hit.stability}</dd>
          </>
        )}
      </dl>
      <h3>Attributes</h3>
      {hit.attributes.length > 0 ? (
        <ul aria-label="Attributes">
          {hit.attributes.map((a) => (
            <li key={a.key}>
              <Link
                to={definitionPath(
                  hit.namespace,
                  hit.version,
                  "attributes",
                  a.key,
                )}
              >
                {a.key}
              </Link>
              {a.requirement_level && (
                <span className="schema-role">{a.requirement_level}</span>
              )}
            </li>
          ))}
        </ul>
      ) : (
        <p className="schema-note">None declared.</p>
      )}
      <h3>Associated entities</h3>
      {hit.entity_associations.length > 0 ? (
        <ul aria-label="Associated entities">
          {hit.entity_associations.map((e) => (
            <li key={e}>
              <Link
                to={definitionPath(hit.namespace, hit.version, "entities", e)}
              >
                {e}
              </Link>
            </li>
          ))}
        </ul>
      ) : (
        <p className="schema-note">None.</p>
      )}
    </>
  );
}

function Alternatives({
  hits,
  kind,
  name,
  self,
}: {
  hits: AnyHit[];
  kind: DefinitionKind;
  name: string;
  self: AnyHit | undefined;
}) {
  const others = hits.filter((h) => h !== self);
  if (others.length === 0) return null;
  return (
    <>
      <h3>Also defined in</h3>
      <ul aria-label="Also defined in">
        {others.map((h) => (
          <li key={`${h.namespace}@${h.version}`}>
            <Link to={definitionPath(h.namespace, h.version, kind, name)}>
              {h.namespace}/{name}
            </Link>{" "}
            <span className="schema-hit-meta">
              {h.version} · {h.source}
            </span>{" "}
            {hits[0] === h && <span className="schema-badge">primary</span>}
          </li>
        ))}
      </ul>
    </>
  );
}
