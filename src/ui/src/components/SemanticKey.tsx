/**
 * Attribute-key labels enriched with schema-registry semantics.
 *
 * `SemanticKeyLabel` is the detail-row form (span/log attribute tables): the
 * raw key alone as the hover/focus trigger, struck through with its
 * replacement when deprecated — everything else the registry knows lives in
 * the tooltip. `SemanticInfo` is the compact form for sidebars and facet
 * headers: an info glyph that only appears when the registry knows the key.
 * Both open the same hover/focus tooltip. Without semantics they render
 * exactly what the raw key would — a plain text node, or nothing.
 */
import {
  Fragment,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type CSSProperties,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { Link, useInRouterContext } from "react-router";
import type { AttributeHit } from "../api/gen";
import { deprecationLabel, type AttributeSemantics } from "../lib/semantics";

/** Entity-role glyph, shared by the tooltip's role list and
 * `AttributeTable`'s group heading (◆ identifying, ○ descriptive). */
export const ROLE_GLYPH: Record<string, string> = {
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
      <span className="sem-tip-brief">{semantics.brief}</span>
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
  const style: CSSProperties = {
    position: "fixed",
    left,
    // The clamped `left` alone doesn't stop the tip's own CSS max-width
    // (`.sem-tip`, 360px) from running past a viewport narrower than that.
    maxWidth: `calc(100vw - ${2 * TIP_EDGE_MARGIN}px)`,
  };
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
  dataKnown,
}: {
  semantics: AttributeSemantics;
  className: string;
  children: ReactNode;
  /** Marks the trigger `data-known` for a dotted-underline affordance
   * (`AttributeTable`'s compact key label); other callers own their own
   * visual treatment and omit it. */
  dataKnown?: boolean;
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

  // Escape closes the tooltip without also collapsing whatever row/drawer
  // the trigger sits in; Tab (no shift) steps into the tooltip's first link
  // instead of leaving the trigger for whatever the DOM would reach next —
  // the tooltip is portaled to <body>, so natural tab order can otherwise
  // skip past it or land somewhere unrelated.
  const onTriggerKeyDown = (e: ReactKeyboardEvent) => {
    if (!anchor) return;
    if (e.key === "Escape") {
      e.stopPropagation();
      close();
      return;
    }
    if (e.key === "Tab" && !e.shiftKey) {
      const link = tipRef.current?.querySelector("a");
      if (link) {
        e.preventDefault();
        link.focus();
      }
    }
  };
  // Shift+Tab off the tooltip's first link returns focus to the trigger
  // rather than wherever the link would naturally precede in tab order.
  // The tooltip is portaled to <body>, so native tab order would carry
  // focus off to the end of the document from its last link; both edges
  // hand focus back to the trigger instead, forward Tab closing the tip so
  // the next Tab continues from the trigger's own position in the list.
  const onTipKeyDown = (e: ReactKeyboardEvent) => {
    if (e.key !== "Tab") return;
    // React bubbles the portaled tooltip's events to the trigger, whose own
    // Tab handler would otherwise send focus straight back into the tip.
    e.stopPropagation();
    const links = tipRef.current?.querySelectorAll("a") ?? [];
    const firstLink = links[0];
    const lastLink = links[links.length - 1];
    if (e.shiftKey && e.target === firstLink) {
      e.preventDefault();
      triggerRef.current?.focus();
    } else if (!e.shiftKey && e.target === lastLink) {
      e.preventDefault();
      triggerRef.current?.focus();
      close();
    }
  };

  return (
    <span
      ref={triggerRef}
      className={className}
      data-known={dataKnown ? "" : undefined}
      tabIndex={0}
      aria-describedby={anchor ? id : undefined}
      onMouseEnter={open}
      onMouseLeave={(e) => leaveTo(e, tipRef.current)}
      onFocus={open}
      onKeyDown={onTriggerKeyDown}
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
            onKeyDown={onTipKeyDown}
          >
            <HoverBridge placement={placement} />
            <SemanticTooltip semantics={semantics} />
          </span>,
          document.body,
        )}
    </span>
  );
}

/** Key text with a `<wbr/>` after every `.` so a long dotted key (`cloud.
 * region`, `db.statement`) wraps at a dot instead of overflowing or relying
 * on an ellipsis — used by `SemanticKeyLabel` for both a resolved and a bare
 * key, so wrapping is consistent either way. */
function DottedKey({ name }: { name: string }) {
  const segments = name.split(".");
  return (
    <>
      {segments.map((segment, i) => (
        <Fragment key={i}>
          {i > 0 && (
            <>
              .<wbr />
            </>
          )}
          {segment}
        </Fragment>
      ))}
    </>
  );
}

/**
 * Compact key label for attribute tables (`components/AttributeTable.tsx`):
 * the bare key when unresolved; otherwise the key alone as the hover/focus
 * tooltip trigger, struck through with its replacement when deprecated.
 * Everything else the registry knows — brief, roles, namespace — lives in
 * the tooltip, not stacked under the row.
 */
export function SemanticKeyLabel({
  name,
  semantics,
}: {
  name: string;
  semantics: AttributeSemantics | undefined;
}) {
  if (!semantics) return <DottedKey name={name} />;
  const { deprecated } = semantics;
  const text = <DottedKey name={name} />;
  const depLabel = deprecationLabel(deprecated);
  return (
    <>
      <SemanticHover semantics={semantics} className="semkey-name" dataKnown>
        {deprecated ? <s>{text}</s> : text}
      </SemanticHover>
      {depLabel && <span className="semkey-dep">{depLabel}</span>}
    </>
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
