/**
 * Shared attribute key/value table for the log detail (`features/logs/
 * LogList.tsx`) and the span panel (`features/traces/TracesView.tsx`).
 *
 * Rows carry only the key, the value, and whatever the caller hangs off the
 * row (a filter action strip): everything else the schema registry knows
 * about a key — brief, entity roles, deprecation detail, alternate
 * definitions — lives in `SemanticKeyLabel`'s hover tooltip instead of being
 * stacked under every row. What a resolved group's rows share (namespace,
 * entity) is stated once on the group heading rather than repeated per row.
 * A group left with a single row folds into "Other" instead of earning a
 * heading of its own — see `foldSingletonGroups`.
 */
import { Fragment, useId } from "react";
import { AttributeValue } from "./AttributeValue";
import { SemanticKeyLabel } from "./SemanticKey";
import type { AttributeSummaryResult } from "../lib/attrSummary";
import {
  foldSingletonGroups,
  groupBySemanticTitle,
  OTHER_TITLE,
  plainBrief,
  type AttributeSemantics,
  type SemanticsMap,
} from "../lib/semantics";

export interface AttributeRowAction {
  label: string;
  ariaLabel: string;
  onClick: () => void;
}

export interface AttributeTableProps {
  /** Key/value pairs, already sorted by the caller. */
  entries: [string, string][];
  semantics: SemanticsMap;
  /** Wide key|value grid vs narrow key-over-value (see AttributeTable.css
   * rules for each in styles/global.css). */
  layout: "grid" | "stacked";
  /** Reading-mode toggle, owned by the caller (see lib/attrDescriptions.ts):
   * adds a plain-text brief line under every resolved row. */
  showDescriptions: boolean;
  /** Per-row action strip (e.g. "+ filter"); omit for none. */
  actions?: (key: string, value: string) => AttributeRowAction[];
  /** `data-scope` on every row, for callers that style/query by it. */
  scope?: string;
}

const ENTITY_GLYPH = { identifying: "◆", descriptive: "○" } as const;

/** The group's rows that resolved, in order — unresolved keys (the registry
 * doesn't know them, or hasn't answered yet) drop out, since a heading fact
 * can only ever be stated over what actually resolved. */
function resolvedSemantics(
  entries: readonly [string, unknown][],
  semantics: SemanticsMap,
): AttributeSemantics[] {
  return entries
    .map(([key]) => semantics.get(key))
    .filter((sem): sem is AttributeSemantics => sem !== undefined);
}

/** The namespace/source shared by a titled group's rows, `null` unless every
 * resolved row agrees on the same one — a group can hold rows from more than
 * one namespace under one title, and a heading must not assert an origin
 * only some of the rows actually have. */
function groupNamespace(
  entries: readonly [string, unknown][],
  semantics: SemanticsMap,
): { namespace: string; source: string } | null {
  const resolved = resolvedSemantics(entries, semantics);
  if (resolved.length === 0) return null;
  const agreed = new Set(
    resolved.map((sem) => `${sem.primary.namespace} ${sem.primary.source}`),
  );
  if (agreed.size !== 1) return null;
  const { namespace, source } = resolved[0]!.primary;
  return { namespace, source };
}

/** The entity every resolved row in the group agrees on, when there is one:
 * `null` if any resolved row carries no entity role, or the rows' entity
 * sets share nothing in common. The ◆ (identifying) glyph applies only when
 * every resolved row identifies that entity; any row that merely describes
 * it downgrades the whole group to ○ (descriptive). */
function groupEntity(
  entries: readonly [string, unknown][],
  semantics: SemanticsMap,
): { entity: string; identifying: boolean } | null {
  const resolved = resolvedSemantics(entries, semantics);
  if (resolved.length === 0) return null;
  const roleSets = resolved.map(
    (sem) => new Set((sem.primary.entity_roles ?? []).map((r) => r.entity)),
  );
  if (roleSets.some((set) => set.size === 0)) return null;
  const [first, ...rest] = roleSets;
  const common = [...first!].filter((entity) =>
    rest.every((set) => set.has(entity)),
  );
  if (common.length === 0) return null;
  const entity = common[0]!;
  const identifying = resolved.every((sem) =>
    (sem.primary.entity_roles ?? []).some(
      (r) => r.entity === entity && r.role === "identifying",
    ),
  );
  return { entity, identifying };
}

function AttributeRow({
  rowKey,
  value,
  sem,
  showDescriptions,
  actions,
  scope,
}: {
  rowKey: string;
  value: string;
  sem: AttributeSemantics | undefined;
  showDescriptions: boolean;
  actions: AttributeRowAction[];
  scope: string | undefined;
}) {
  return (
    <div className="attrtable-row" data-scope={scope}>
      <dt>
        <SemanticKeyLabel name={rowKey} semantics={sem} />
      </dt>
      <dd>
        <AttributeValue value={value} label={`value for ${rowKey}`} />
        {actions.length > 0 && (
          <span className="attrtable-actions">
            {actions.map((action) => (
              <button
                key={action.ariaLabel}
                type="button"
                aria-label={action.ariaLabel}
                onClick={action.onClick}
              >
                {action.label}
              </button>
            ))}
          </span>
        )}
        {showDescriptions && sem && (
          <div className="attrtable-desc">
            {plainBrief(sem.primary.brief)}
            <span className="attrtable-desc-suffix">
              {" "}
              · {sem.primary.type} · {sem.primary.stability}
            </span>
          </div>
        )}
      </dd>
    </div>
  );
}

export function AttributeTable({
  entries,
  semantics,
  layout,
  showDescriptions,
  actions,
  scope,
}: AttributeTableProps) {
  const groups = foldSingletonGroups(groupBySemanticTitle(entries, semantics));
  const idBase = useId();
  return (
    <div className="attrtable" data-layout={layout}>
      {groups.map((group, i) => {
        // "Other" mixes folded singletons with keys no registry knows, so
        // no namespace or entity holds for all of its rows.
        const titled = group.title !== null && group.title !== OTHER_TITLE;
        const namespace = titled
          ? groupNamespace(group.entries, semantics)
          : null;
        const entity = titled ? groupEntity(group.entries, semantics) : null;
        const headingId = group.title !== null ? `${idBase}-h${i}` : undefined;
        return (
          <Fragment key={group.title ?? ""}>
            {group.title && (
              <div id={headingId} className="attrtable-group">
                <span className="attrtable-title">{group.title}</span>
                {namespace && (
                  <span className="attrtable-ns" data-source={namespace.source}>
                    {namespace.namespace}
                  </span>
                )}
                {entity && (
                  <span className="attrtable-entity">
                    {ENTITY_GLYPH[entity.identifying ? "identifying" : "descriptive"]}{" "}
                    {entity.entity}
                  </span>
                )}
              </div>
            )}
            <dl className="attrtable-list" aria-labelledby={headingId}>
              {group.entries.map(([key, value]) => (
                <AttributeRow
                  key={key}
                  rowKey={key}
                  value={value}
                  sem={semantics.get(key)}
                  showDescriptions={showDescriptions}
                  actions={actions?.(key, value) ?? []}
                  scope={scope}
                />
              ))}
            </dl>
          </Fragment>
        );
      })}
    </div>
  );
}

/**
 * A collapsed section's one-line preview (`lib/attrSummary.ts`'s curated
 * fields plus a `+N more`), rendered as key/value parts rather than one
 * joined string so each half can carry its own color and truncate on its
 * own rather than the whole line ellipsizing early.
 */
export function AttributeSummary({
  summary,
}: {
  summary: AttributeSummaryResult;
}) {
  return (
    <div className="attrtable-summary">
      {summary.parts.map((part) => (
        <span className="attrtable-summary-kv" key={part.key}>
          <span className="attrtable-summary-k">{part.key}</span>{" "}
          <span className="attrtable-summary-v">{part.value}</span>
        </span>
      ))}
      {summary.more > 0 && (
        <span className="attrtable-summary-more">+{summary.more} more</span>
      )}
    </div>
  );
}
