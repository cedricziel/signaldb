import { useVirtualizer } from "@tanstack/react-virtual";
import { useMemo, useRef, useState } from "react";
import type { LogRow } from "../../api/loki";
import {
  AttributeSummary,
  AttributeTable,
  type AttributeRowAction,
} from "../../components/AttributeTable";
import { CopyValueButton } from "../../components/CopyValueButton";
import { EmptyState } from "../../components/EmptyState";
import { useSemantics } from "../../hooks/useSemantics";
import {
  readAttrDescriptions,
  writeAttrDescriptions,
} from "../../lib/attrDescriptions";
import { pivotRowActions } from "../../lib/attrPivots";
import { summarizeAttributes, type SummaryField } from "../../lib/attrSummary";
import type { LabelFilter } from "../../lib/filters";
import { formatTimestamp } from "../../lib/time";
import type { UpdateFn } from "../../lib/urlState";
import { normalizeLevel } from "./Histogram";

interface Props {
  rows: LogRow[];
  onAddFilter: (filter: LabelFilter) => void;
  onOpenTrace: (traceId: string) => void;
  update: UpdateFn;
}

/**
 * Fallback label spellings, kept for rows from a source that promoted a
 * `trace_id`-named attribute to a label rather than sending it as
 * structured metadata.
 */
const TRACE_LABELS = ["trace_id", "traceID", "traceId"];

export function traceIdOf(row: LogRow): string | null {
  const metadataTraceId = row.metadata["trace_id"];
  if (metadataTraceId) return metadataTraceId;
  for (const key of TRACE_LABELS) {
    const v = row.labels[key];
    if (v) return v;
  }
  return null;
}

/** Cheap canonical form of a labels/metadata record for a key — sorted so
 * insertion order never changes the result. `JSON.stringify`d over the
 * sorted `[key, value]` tuples rather than joined with plain delimiters: an
 * unescaped `,`/`=` join collapses distinct records (e.g. `{ a: "b,c=d" }`
 * and `{ a: "b", c: "d" }`) onto the same string. */
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
 * the line, any span/trace id, and the row's own labels/metadata is stable
 * across such a shift and cheap to compute; it does not need to be a true
 * hash, only unique enough among the rows on screen — two streams sharing a
 * timestamp and line (e.g. the same log line from two pods) still differ in
 * at least one label.
 */
export function rowKey(row: LogRow): string {
  const spanId = row.metadata["span_id"] ?? "";
  const traceId = traceIdOf(row) ?? "";
  return `${row.tsNs}|${spanId}|${traceId}|${canonicalEntries(row.labels)}|${canonicalEntries(row.metadata)}|${row.line}`;
}

export function LogList({ rows, onAddFilter, onOpenTrace, update }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [showDescriptions, setShowDescriptions] = useState(readAttrDescriptions);
  const toggleDescriptions = () => {
    setShowDescriptions((current) => {
      const next = !current;
      writeAttrDescriptions(next);
      return next;
    });
  };

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
          const level = normalizeLevel(row.labels["level"] ?? "");
          const isOpen = expanded === key;
          const traceId = traceIdOf(row);
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
                  {(row.labels["level"] ?? "-").toUpperCase()}
                </span>
                <span className="logrow-svc">
                  {row.labels["service_name"] ?? ""}
                </span>
                <span className="logrow-msg">{row.line}</span>
                {traceId !== null && <span className="logrow-trace">⛓</span>}
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

const sortedEntries = (bag: Record<string, string>): [string, string][] =>
  Object.entries(bag).sort(([a], [b]) => a.localeCompare(b));

/** Preferred order for the collapsed stream/resource summary line — the
 * first present spelling of each field, up to 8 pairs, then `+ N more`. */
const STREAM_SUMMARY_FIELDS: SummaryField[] = [
  { keys: ["service_name", "service.name"] },
  { keys: ["service.namespace"] },
  { keys: ["deployment.environment.name"] },
  { keys: ["level"] },
  { keys: ["k8s.pod.name"] },
  { keys: ["host.name"] },
  { keys: ["cloud.region"] },
  { keys: ["container.image.name"] },
];

/**
 * The expanded row's actions plus its attribute table: `THIS LINE` (per-line
 * fields, always shown) then `STREAM · RESOURCE` (stream labels, collapsed
 * behind a summary by default — there are usually dozens, identical across
 * every line in the stream).
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
  const traceId = traceIdOf(row);
  return (
    <div className="logdetail">
      <div className="logdetail-actions">
        {traceId !== null && (
          <button
            className="act-primary btn btn-primary"
            onClick={() => onOpenTrace(traceId)}
          >
            View trace {traceId.slice(0, 8)}…
          </button>
        )}
        <CopyValueButton value={row.line} label="log message" />
        <button
          className="btn"
          onClick={() =>
            navigator.clipboard?.writeText(
              JSON.stringify(
                { ...row.labels, ...row.metadata, line: row.line },
                null,
                2,
              ),
            )
          }
        >
          Copy JSON
        </button>
        <label className="attrtable-desc-toggle">
          <input
            type="checkbox"
            checked={showDescriptions}
            onChange={onToggleDescriptions}
            aria-label="Show descriptions"
          />
          descriptions
        </label>
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
  const labels = useMemo(() => sortedEntries(row.labels), [row.labels]);
  const metadata = useMemo(() => sortedEntries(row.metadata), [row.metadata]);
  const keys = useMemo(
    () => [...labels, ...metadata].map(([k]) => k),
    [labels, metadata],
  );
  const semantics = useSemantics(keys);
  // Every label/metadata value on the row, for `pivotRowActions`'s catalog
  // pivot — it needs an entity's full identity, not just one row's key/value.
  const bag = useMemo(
    (): ReadonlyMap<string, string> => new Map([...labels, ...metadata]),
    [labels, metadata],
  );
  const [streamExpanded, setStreamExpanded] = useState(false);

  const metadataActions = (k: string, v: string): AttributeRowAction[] => {
    const actions = pivotRowActions(k, v, semantics.get(k), bag, "logs", update);
    // trace_id carries no identifying entity role of its own, but the trace
    // it names is always one click away — mirrors the `logdetail-actions`
    // "View trace" button for the row that has one, as a per-row action for
    // this specific field once the metadata table is open.
    return k === "trace_id"
      ? [
          {
            label: "open trace ↗",
            ariaLabel: `Open trace ${v}`,
            onClick: () => onOpenTrace(v),
          },
          ...actions,
        ]
      : actions;
  };

  return (
    <>
      {metadata.length > 0 && (
        <>
          <div className="attrtable-section">This line</div>
          {/* No filter/exclude actions — the label-filter model compiles to
              a stream selector, which is the wrong shape for a field that
              varies per line. Filtering on these arrives with the Query IR
              migration, where the predicate is built server-side. */}
          <AttributeTable
            entries={metadata}
            semantics={semantics}
            layout="grid"
            showDescriptions={showDescriptions}
            scope="metadata"
            actions={metadataActions}
          />
        </>
      )}
      {labels.length > 0 && (
        <>
          <button
            type="button"
            className="attrtable-section"
            aria-expanded={streamExpanded}
            onClick={() => setStreamExpanded((current) => !current)}
          >
            Stream · resource
            <span className="attrtable-count">{labels.length} labels</span>
          </button>
          {streamExpanded ? (
            <AttributeTable
              entries={labels}
              semantics={semantics}
              layout="grid"
              showDescriptions={showDescriptions}
              scope="label"
              actions={(k, v) => [
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
                ...pivotRowActions(k, v, semantics.get(k), bag, "logs", update),
              ]}
            />
          ) : (
            <AttributeSummary
              summary={summarizeAttributes(labels, STREAM_SUMMARY_FIELDS)}
            />
          )}
        </>
      )}
    </>
  );
}
