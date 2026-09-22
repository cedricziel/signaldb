import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef, useState } from "react";
import type { LogRow } from "../../api/ir/logs";
import {
  AttributeSection,
  AttributeSummary,
  AttributeTable,
  DescriptionsToggle,
  type AttributeRowAction,
} from "../../components/AttributeTable";
import { CopyValueButton } from "../../components/CopyValueButton";
import { EmptyState } from "../../components/EmptyState";
import { useSemantics } from "../../hooks/useSemantics";
import { useAttrDescriptions } from "../../lib/attrDescriptions";
import { pivotRowActions } from "../../lib/attrPivots";
import {
  RESOURCE_IDENTITY_FIELDS,
  summarizeAttributes,
  type SummaryField,
} from "../../lib/attrSummary";
import type { LabelFilter } from "../../lib/filters";
import { formatTimestamp } from "../../lib/time";
import type { UpdateFn } from "../../lib/urlState";
import { normalizeLevel } from "./Histogram";
import { logScopes } from "./logScopes";

interface Props {
  rows: LogRow[];
  onAddFilter: (filter: LabelFilter) => void;
  onOpenTrace: (traceId: string) => void;
  update: UpdateFn;
}

/** Cheap canonical form of a row's attribute containers — sorted so
 * insertion order never changes the result. `JSON.stringify`d over the
 * sorted `[key, value]` tuples rather than joined with plain delimiters: an
 * unescaped `,`/`=` join collapses distinct records onto the same string. */
function canonicalEntries(record: Record<string, string>): string {
  return JSON.stringify(
    Object.keys(record)
      .sort()
      .map((k) => [k, record[k]]),
  );
}

/**
 * A row's identity for expansion/React-key purposes: the virtualizer's
 * `item.index` shifts under a row's feet in live mode (a newer row
 * prepends), which used to collapse whatever was expanded. Timestamp plus
 * the body, ids, and every attribute container is stable across such a
 * shift and cheap to compute; it does not need to be a true hash, only
 * unique enough among the rows on screen.
 */
export function rowKey(row: LogRow): string {
  return [
    row.tsNs,
    row.spanId ?? "",
    row.traceId ?? "",
    canonicalEntries(row.logAttributes),
    canonicalEntries(row.scopeAttributes),
    canonicalEntries(row.resourceAttributes),
    row.body,
  ].join("|");
}

export function LogList({ rows, onAddFilter, onOpenTrace, update }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [showDescriptions, toggleDescriptions] = useAttrDescriptions();

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 26,
    overscan: 20,
  });

  if (rows.length === 0) {
    return <EmptyState title="No log lines in this range" />;
  }

  return (
    <div className="loglist" ref={scrollRef}>
      <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
        {virtualizer.getVirtualItems().map((item) => {
          const row = rows[item.index]!;
          const key = rowKey(row);
          const level = normalizeLevel(row.severityText);
          const isOpen = expanded === key;
          return (
            <div
              key={key}
              data-index={item.index}
              ref={virtualizer.measureElement}
              className="logrow-wrap"
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                transform: `translateY(${item.start}px)`,
              }}
            >
              <button
                className={`logrow level-${level}`}
                aria-expanded={isOpen}
                onClick={() => setExpanded(isOpen ? null : key)}
              >
                <span className="logrow-ts">{formatTimestamp(row.tsMs)}</span>
                <span className={`logrow-level level-${level}`}>
                  {(row.severityText || "-").toUpperCase()}
                </span>
                <span className="logrow-svc">{row.serviceName}</span>
                <span className="logrow-msg">{row.body}</span>
                {row.traceId !== null && (
                  <span className="logrow-trace">⛓</span>
                )}
              </button>
              {isOpen && (
                <LogDetail
                  row={row}
                  onAddFilter={onAddFilter}
                  onOpenTrace={onOpenTrace}
                  update={update}
                  showDescriptions={showDescriptions}
                  onToggleDescriptions={toggleDescriptions}
                />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

/** Preferred order for the collapsed resource summary line — the shared
 * resource-identity fields plus the container image (trailing). Level lives
 * on the row itself now (severity_text is a first-class field, not a
 * resource attribute), so it's no longer in this list. */
const RESOURCE_SUMMARY_FIELDS: SummaryField[] = [
  ...RESOURCE_IDENTITY_FIELDS,
  { keys: ["container.image.name"] },
];

/**
 * The expanded row's actions plus its attribute table: "This line" (per-line
 * fields, always shown), "Scope" and "Resource" (collapsed behind a summary
 * by default — see logScopes.ts for how the IR's own scopes drive the
 * split).
 */
function LogDetail({
  row,
  onAddFilter,
  onOpenTrace,
  update,
  showDescriptions,
  onToggleDescriptions,
}: {
  row: LogRow;
  onAddFilter: (filter: LabelFilter) => void;
  onOpenTrace: (traceId: string) => void;
  update: UpdateFn;
  showDescriptions: boolean;
  onToggleDescriptions: () => void;
}) {
  return (
    <div className="logdetail">
      <div className="logdetail-actions">
        {row.traceId !== null && (
          <button
            className="act-primary btn btn-primary"
            onClick={() => onOpenTrace(row.traceId!)}
          >
            View trace {row.traceId.slice(0, 8)}…
          </button>
        )}
        <CopyValueButton value={row.body} label="log message" />
        <button
          className="btn"
          onClick={() =>
            navigator.clipboard?.writeText(
              JSON.stringify(
                {
                  ...row.logAttributes,
                  ...row.scopeAttributes,
                  ...row.resourceAttributes,
                  trace_id: row.traceId,
                  span_id: row.spanId,
                  body: row.body,
                },
                null,
                2,
              ),
            )
          }
        >
          Copy JSON
        </button>
        <DescriptionsToggle
          checked={showDescriptions}
          onToggle={onToggleDescriptions}
        />
      </div>
      <LogAttributes
        row={row}
        onAddFilter={onAddFilter}
        onOpenTrace={onOpenTrace}
        update={update}
        showDescriptions={showDescriptions}
      />
    </div>
  );
}

function LogAttributes({
  row,
  onAddFilter,
  onOpenTrace,
  update,
  showDescriptions,
}: {
  row: LogRow;
  onAddFilter: (filter: LabelFilter) => void;
  onOpenTrace: (traceId: string) => void;
  update: UpdateFn;
  showDescriptions: boolean;
}) {
  const groups = useMemo(() => logScopes(row), [row]);
  const keys = useMemo(
    () => groups.flatMap((g) => g.entries.map(([k]) => k)),
    [groups],
  );
  const semantics = useSemantics(keys);
  // Every attribute on the row, for `pivotRowActions`'s catalog pivot —
  // it needs an entity's full identity, not just one row's key/value.
  const bag = useMemo(
    (): ReadonlyMap<string, string> =>
      new Map(groups.flatMap((g) => g.entries)),
    [groups],
  );
  const [scopeExpanded, setScopeExpanded] = useState(false);
  const [resourceExpanded, setResourceExpanded] = useState(false);

  const rowActions = (k: string, v: string): AttributeRowAction[] => {
    const filterActions: AttributeRowAction[] = [
      {
        label: "+ filter",
        ariaLabel: `Filter for ${k} = ${v}`,
        onClick: () => onAddFilter({ label: k, op: "=", value: v }),
      },
      {
        label: "− exclude",
        ariaLabel: `Filter out ${k} = ${v}`,
        onClick: () => onAddFilter({ label: k, op: "!=", value: v }),
      },
    ];
    // trace_id carries no identifying entity role of its own, but the trace
    // it names is always one click away — mirrors the `logdetail-actions`
    // "View trace" button for the row that has one, as a per-row action for
    // this specific field.
    const openTrace: AttributeRowAction[] =
      k === "trace_id"
        ? [
            {
              label: "open trace ↗",
              ariaLabel: `Open trace ${v}`,
              onClick: () => onOpenTrace(v),
            },
          ]
        : [];
    return [
      ...filterActions,
      ...openTrace,
      ...pivotRowActions(k, v, semantics.get(k), bag, "logs", update),
    ];
  };

  const groupByTitle = (title: string) => groups.find((g) => g.title === title);
  const lineGroup = groupByTitle("This line");
  const scopeGroup = groupByTitle("Scope");
  const resourceGroup = groupByTitle("Resource");
  const resourceSummary = useMemo(
    () =>
      summarizeAttributes(
        resourceGroup?.entries ?? [],
        RESOURCE_SUMMARY_FIELDS,
      ),
    [resourceGroup],
  );

  return (
    <>
      {lineGroup && lineGroup.entries.length > 0 && (
        <>
          <AttributeSection title="This line" />
          <AttributeTable
            entries={lineGroup.entries}
            semantics={semantics}
            layout="grid"
            showDescriptions={showDescriptions}
            scope="line"
            actions={rowActions}
          />
        </>
      )}
      {scopeGroup && (
        <>
          <AttributeSection
            title="Scope"
            count={`${scopeGroup.entries.length} fields`}
            expanded={scopeExpanded}
            onToggle={() => setScopeExpanded((current) => !current)}
          />
          {scopeExpanded && (
            <AttributeTable
              entries={scopeGroup.entries}
              semantics={semantics}
              layout="grid"
              showDescriptions={showDescriptions}
              scope="scope"
              actions={rowActions}
            />
          )}
        </>
      )}
      {resourceGroup && (
        <>
          <AttributeSection
            title="Resource"
            count={`${resourceGroup.entries.length} fields`}
            expanded={resourceExpanded}
            onToggle={() => setResourceExpanded((current) => !current)}
          />
          {resourceExpanded ? (
            <AttributeTable
              entries={resourceGroup.entries}
              semantics={semantics}
              layout="grid"
              showDescriptions={showDescriptions}
              scope="resource"
              actions={rowActions}
            />
          ) : (
            <AttributeSummary summary={resourceSummary} />
          )}
        </>
      )}
    </>
  );
}
