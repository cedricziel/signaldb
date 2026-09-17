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

/** Gap between the trigger and the tooltip; bridged by an in-tooltip element
 * (see `HoverBridge`) so the pointer can cross it without closing the tip. */
const TIP_GAP = 4;
/** `.sem-tip` max-width, for deciding whether to right-align. */
const TIP_MAX_WIDTH = 360;
/** Height/width guesses before the tooltip has been measured, for flipping
 * above the trigger and clamping horizontally. */
const TIP_HEIGHT_GUESS = 160;
const TIP_WIDTH_GUESS = 220;
/** Minimum on-screen margin the tooltip is clamped to on either edge. */
const TIP_EDGE_MARGIN = 8;

type Placement = "above" | "below";

/**
 * Fixed-position placement for the tooltip: below the trigger, left-aligned
 * with it and right-aligned when it would otherwise run past the viewport's
 * right edge — either way, `left` is then clamped to stay fully on screen
 * (a right-aligned tip narrower than its own min-width could otherwise start
 * left of x=0 on a narrow viewport). Flips above the trigger when it would
 * run past the bottom.
 */
function tipStyle(
  anchor: DOMRect,
  height: number,
  width: number,
): { style: CSSProperties; placement: Placement } {
  const preferRight = anchor.left + TIP_MAX_WIDTH > window.innerWidth;
  const rawLeft = preferRight ? anchor.right - width : anchor.left;
  const maxLeft = Math.max(TIP_EDGE_MARGIN, window.innerWidth - width - TIP_EDGE_MARGIN);
  const left = Math.min(Math.max(rawLeft, TIP_EDGE_MARGIN), maxLeft);
  const placement: Placement =
    anchor.bottom + TIP_GAP + height > window.innerHeight ? "above" : "below";
  const style: CSSProperties = { position: "fixed", left };
  if (placement === "above") {
    style.bottom = window.innerHeight - anchor.top + TIP_GAP;
  } else {
    style.top = anchor.bottom + TIP_GAP;
  }
  return { style, placement };
}

/**
 * Invisible strip covering the gap on the side facing the trigger, so the
 * pointer can travel from one to the other without the tooltip closing in
 * between. A real DOM element (not `::before`) because it needs to sit on
 * whichever side the tooltip was flipped to.
 */
function HoverBridge({ placement }: { placement: Placement }) {
  const style: CSSProperties = {
    position: "absolute",
    left: 0,
    right: 0,
    height: TIP_GAP + 2,
    ...(placement === "above" ? { bottom: -(TIP_GAP + 2) } : { top: -(TIP_GAP + 2) }),
  };
  return <span aria-hidden="true" style={style} />;
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
  const [width, setWidth] = useState(TIP_WIDTH_GUESS);
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
    const w = tipRef.current?.offsetWidth ?? 0;
    if (w > 0 && w !== width) setWidth(w);
  });

  useEffect(() => {
    if (!anchor) return;
    // Fixed placement goes stale as soon as the pane scrolls.
    window.addEventListener("scroll", close, true);
    return () => window.removeEventListener("scroll", close, true);
  }, [anchor]);

  useEffect(() => {
    // Tab reaches the tooltip's own links (it's portaled outside the
    // trigger's DOM subtree, so plain `onBlur` on the trigger would close it
    // the moment focus leaves for them). `focusout` bubbles, so one listener
    // covers focus leaving either the trigger or the tooltip; it only closes
    // once focus lands somewhere in neither.
    if (!anchor) return;
    const onFocusOut = (e: FocusEvent) => {
      const next = e.relatedTarget;
      if (
        next instanceof Node &&
        (triggerRef.current?.contains(next) || tipRef.current?.contains(next))
      )
        return;
      close();
    };
    document.addEventListener("focusout", onFocusOut, true);
    return () => document.removeEventListener("focusout", onFocusOut, true);
  }, [anchor]);

  const { style, placement } = anchor
    ? tipStyle(anchor, height, width)
    : { style: undefined, placement: "below" as Placement };

  return (
    <span
      ref={triggerRef}
      className={className}
      tabIndex={0}
      aria-describedby={anchor ? id : undefined}
      onMouseEnter={open}
      onMouseLeave={(e) => leaveTo(e, tipRef.current)}
      onFocus={open}
    >
      {children}
      {anchor &&
        createPortal(
          <span
            role="tooltip"
            id={id}
            ref={tipRef}
            className="sem-tip"
            data-placement={placement}
            style={style}
            onMouseLeave={(e) => leaveTo(e, triggerRef.current)}
          >
            <HoverBridge placement={placement} />
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
