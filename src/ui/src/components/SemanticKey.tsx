/**
 * Attribute-key labels enriched with schema-registry semantics.
 *
 * `SemanticKey` is the detail-row form (span/log attribute tables): the raw
 * key stays first and copyable; underneath sit the brief, the entity role
 * markers, and a deprecation marker, with the defining namespace tagged on
 * the right. `SemanticInfo` is the compact form for sidebars and facet
 * headers: an info glyph that only appears when the registry knows the key.
 * Both open the same hover/focus tooltip. Without semantics they render
 * exactly what the raw key would — a plain text node, or nothing.
 */
import { useId, useState, type ReactNode } from "react";
import type { AttributeHit } from "../api/gen";
import type { AttributeSemantics } from "../lib/semantics";

const ROLE_GLYPH: Record<string, string> = {
  identifying: "◆",
  descriptive: "○",
};

const registryLabel = (hit: AttributeHit) => `${hit.namespace}@${hit.version}`;

function formatExamples(examples: unknown[] | undefined): string | null {
  if (!examples || examples.length === 0) return null;
  return examples
    .slice(0, 4)
    .map((e) => (typeof e === "string" ? e : JSON.stringify(e)))
    .join(", ");
}

/** Tooltip body: everything the registry says about the key. */
export function SemanticTooltip({
  semantics,
}: {
  semantics: AttributeSemantics;
}) {
  const { primary, alternatives, title, deprecated } = semantics;
  const facts = [title, primary.type, primary.stability].filter(Boolean);
  const examples = formatExamples(primary.examples);
  return (
    <span className="sem-tip-body">
      <span className="sem-tip-key">{semantics.key}</span>
      <span className="sem-tip-registry">{registryLabel(primary)}</span>
      <span className="sem-tip-facts">{facts.join(" · ")}</span>
      <span className="sem-tip-brief">{primary.brief}</span>
      {examples && <span className="sem-tip-examples">e.g. {examples}</span>}
      {primary.entity_roles && primary.entity_roles.length > 0 && (
        <span className="sem-tip-roles">
          {primary.entity_roles.map((r) => (
            <span key={`${r.namespace}/${r.entity}/${r.role}`}>
              {ROLE_GLYPH[r.role] ?? "·"} {r.role} for {r.entity}
            </span>
          ))}
        </span>
      )}
      {deprecated && (
        <span className="sem-tip-deprecated">
          ⚠ deprecated
          {deprecated.renamed_to ? ` → ${deprecated.renamed_to}` : ""}
          {deprecated.note ? ` — ${deprecated.note}` : ""}
        </span>
      )}
      {alternatives.length > 0 && (
        <span className="sem-tip-alts">
          Also defined in: {alternatives.map(registryLabel).join(", ")}
        </span>
      )}
    </span>
  );
}

/** Hover/focus target that reveals the tooltip. */
function SemanticHover({
  semantics,
  className,
  children,
}: {
  semantics: AttributeSemantics;
  className: string;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const id = useId();
  return (
    <span
      className={className}
      tabIndex={0}
      aria-describedby={open ? id : undefined}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
    >
      {children}
      {open && (
        <span role="tooltip" id={id} className="sem-tip">
          <SemanticTooltip semantics={semantics} />
        </span>
      )}
    </span>
  );
}

interface SemanticKeyProps {
  name: string;
  semantics: AttributeSemantics | undefined;
  /** Show the semantic title inline (when rows are not grouped by title). */
  showTitle?: boolean;
}

/** Detail-row key label. Falls back to the bare key when unresolved. */
export function SemanticKey({ name, semantics, showTitle }: SemanticKeyProps) {
  if (!semantics) return <>{name}</>;
  const { primary, deprecated } = semantics;
  const roles = primary.entity_roles ?? [];
  return (
    <span className="semkey" data-deprecated={deprecated ? "" : undefined}>
      <SemanticHover semantics={semantics} className="semkey-head">
        <span className="semkey-name">{name}</span>
        <span className="semkey-ns" data-source={primary.source}>
          {primary.namespace}
        </span>
      </SemanticHover>
      <span className="semkey-brief">{primary.brief}</span>
      {(showTitle || roles.length > 0 || deprecated) && (
        <span className="semkey-meta">
          {showTitle && <span className="semkey-title">{semantics.title}</span>}
          {roles.map((r) => (
            <span
              className={`semkey-role semkey-role-${r.role}`}
              key={`${r.namespace}/${r.entity}/${r.role}`}
            >
              {ROLE_GLYPH[r.role] ?? "·"} {r.role} · {r.entity}
            </span>
          ))}
          {deprecated && (
            <span className="semkey-dep">
              ⚠ deprecated
              {deprecated.renamed_to ? ` → ${deprecated.renamed_to}` : ""}
            </span>
          )}
        </span>
      )}
    </span>
  );
}

/** Info glyph with tooltip for compact key lists; nothing when unresolved. */
export function SemanticInfo({
  name,
  semantics,
}: {
  name: string;
  semantics: AttributeSemantics | undefined;
}) {
  if (!semantics) return null;
  return (
    <SemanticHover semantics={semantics} className="sem-info">
      <span aria-label={`About ${name}`} role="img">
        ⓘ
      </span>
    </SemanticHover>
  );
}
