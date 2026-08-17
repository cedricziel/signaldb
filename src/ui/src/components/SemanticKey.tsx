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
import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { Link, useInRouterContext } from "react-router";
import type { AttributeHit } from "../api/gen";
import type { AttributeSemantics } from "../lib/semantics";

const ROLE_GLYPH: Record<string, string> = {
  identifying: "◆",
  descriptive: "○",
};

const registryLabel = (hit: AttributeHit) => `${hit.namespace}@${hit.version}`;

/** Definition page of `hit`'s attribute in the schema hub. */
const attributeHref = (hit: AttributeHit) =>
  `/schema/conventions/${encodeURIComponent(hit.namespace)}/${encodeURIComponent(hit.version)}/attributes/${encodeURIComponent(hit.key)}`;

/** Entity page in the schema hub (`latest` resolves to the newest version). */
const entityHref = (namespace: string, entity: string) =>
  `/schema/conventions/${encodeURIComponent(namespace)}/latest/entities/${encodeURIComponent(entity)}`;

/**
 * Link into the schema hub. Uses the SPA router when one is mounted (the
 * app), and a plain anchor otherwise (isolated component renders), so the
 * tooltip never depends on router context.
 */
function HubLink({ to, children }: { to: string; children: ReactNode }) {
  const inRouter = useInRouterContext();
  // Keep the click from bubbling to row handlers (e.g. "add filter").
  const stop = (e: MouseEvent) => e.stopPropagation();
  return inRouter ? (
    <Link to={to} className="sem-tip-link" onClick={stop}>
      {children}
    </Link>
  ) : (
    <a href={to} className="sem-tip-link" onClick={stop}>
      {children}
    </a>
  );
}

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
      <span className="sem-tip-registry">
        <HubLink to={attributeHref(primary)}>{registryLabel(primary)}</HubLink>
      </span>
      <span className="sem-tip-facts">{facts.join(" · ")}</span>
      <span className="sem-tip-brief">{primary.brief}</span>
      {examples && <span className="sem-tip-examples">e.g. {examples}</span>}
      {primary.entity_roles && primary.entity_roles.length > 0 && (
        <span className="sem-tip-roles">
          {primary.entity_roles.map((r) => (
            <span key={`${r.namespace}/${r.entity}/${r.role}`}>
              {ROLE_GLYPH[r.role] ?? "·"} {r.role} for{" "}
              <HubLink to={entityHref(r.namespace, r.entity)}>
                {r.entity}
              </HubLink>
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
          Also defined in:{" "}
          {alternatives.map((alt, i) => (
            <span key={registryLabel(alt)}>
              {i > 0 ? ", " : ""}
              <HubLink to={attributeHref(alt)}>{registryLabel(alt)}</HubLink>
            </span>
          ))}
        </span>
      )}
    </span>
  );
}

/** Gap between the trigger and the tooltip; `.sem-tip::before` bridges it. */
const TIP_GAP = 4;
/** `.sem-tip` max-width, for deciding whether to right-align. */
const TIP_MAX_WIDTH = 360;
/** Height guess before the tooltip has been measured, for vertical flipping. */
const TIP_HEIGHT_GUESS = 160;

/**
 * Fixed-position placement for the tooltip: below the trigger, left-aligned
 * with it; right-aligned when it would run past the viewport's right edge,
 * and above the trigger when it would run past the bottom.
 */
function tipStyle(anchor: DOMRect, height: number): CSSProperties {
  const style: CSSProperties = { position: "fixed" };
  if (anchor.left + TIP_MAX_WIDTH > window.innerWidth) {
    style.right = Math.max(0, window.innerWidth - anchor.right);
  } else {
    style.left = anchor.left;
  }
  if (anchor.bottom + TIP_GAP + height > window.innerHeight) {
    style.bottom = window.innerHeight - anchor.top + TIP_GAP;
  } else {
    style.top = anchor.bottom + TIP_GAP;
  }
  return style;
}

/**
 * Hover/focus target that reveals the tooltip.
 *
 * The tooltip is portaled to `<body>` and placed with fixed coordinates
 * from the trigger's rect: its triggers live in scrolling panes (facet
 * sidebars, attribute tables — `overflow: auto`), which would clip a tooltip
 * positioned inside them at the pane's edge. Leaving the trigger for the
 * tooltip keeps it open so its links stay clickable; leaving either for
 * anywhere else, or scrolling, closes it.
 */
function SemanticHover({
  semantics,
  className,
  children,
}: {
  semantics: AttributeSemantics;
  className: string;
  children: ReactNode;
}) {
  const [anchor, setAnchor] = useState<DOMRect | null>(null);
  const [height, setHeight] = useState(TIP_HEIGHT_GUESS);
  const triggerRef = useRef<HTMLSpanElement>(null);
  const tipRef = useRef<HTMLSpanElement>(null);
  const id = useId();

  const open = () => {
    if (triggerRef.current)
      setAnchor(triggerRef.current.getBoundingClientRect());
  };
  const close = () => setAnchor(null);
  // Pointer travelling between the trigger and the tooltip does not close it.
  const leaveTo = (e: MouseEvent, other: HTMLElement | null) => {
    if (
      other &&
      e.relatedTarget instanceof Node &&
      other.contains(e.relatedTarget)
    )
      return;
    close();
  };

  useLayoutEffect(() => {
    const h = tipRef.current?.offsetHeight ?? 0;
    if (h > 0 && h !== height) setHeight(h);
  });

  useEffect(() => {
    if (!anchor) return;
    // Fixed placement goes stale as soon as the pane scrolls.
    window.addEventListener("scroll", close, true);
    return () => window.removeEventListener("scroll", close, true);
  }, [anchor]);

  return (
    <span
      ref={triggerRef}
      className={className}
      tabIndex={0}
      aria-describedby={anchor ? id : undefined}
      onMouseEnter={open}
      onMouseLeave={(e) => leaveTo(e, tipRef.current)}
      onFocus={open}
      onBlur={close}
    >
      {children}
      {anchor &&
        createPortal(
          <span
            role="tooltip"
            id={id}
            ref={tipRef}
            className="sem-tip"
            style={tipStyle(anchor, height)}
            onMouseLeave={(e) => leaveTo(e, triggerRef.current)}
          >
            <SemanticTooltip semantics={semantics} />
          </span>,
          document.body,
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
