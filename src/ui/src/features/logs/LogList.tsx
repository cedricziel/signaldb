import { useVirtualizer } from "@tanstack/react-virtual";
import { useRef, useState } from "react";
import type { LogRow } from "../../api/loki";
import type { LabelFilter } from "../../lib/filters";
import { formatTimestamp } from "../../lib/time";
import { normalizeLevel } from "./Histogram";

interface Props {
  rows: LogRow[];
  onAddFilter: (filter: LabelFilter) => void;
  onOpenTrace: (traceId: string) => void;
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

export function LogList({ rows, onAddFilter, onOpenTrace }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState<string | null>(null);

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 26,
    overscan: 20,
  });

  if (rows.length === 0) {
    return <div className="loglist-empty">No log lines match this query.</div>;
  }

  return (
    <div className="loglist" ref={scrollRef}>
      <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
        {virtualizer.getVirtualItems().map((item) => {
          const row = rows[item.index]!;
          const key = `${row.tsNs}-${item.index}`;
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
                <div className="logdetail">
                  <div className="logdetail-actions">
                    {traceId !== null && (
                      <button
                        className="act act-primary"
                        onClick={() => onOpenTrace(traceId)}
                      >
                        View trace {traceId.slice(0, 8)}…
                      </button>
                    )}
                    <button
                      className="act"
                      onClick={() =>
                        navigator.clipboard?.writeText(
                          JSON.stringify(
                            { ...row.labels, line: row.line },
                            null,
                            2,
                          ),
                        )
                      }
                    >
                      Copy JSON
                    </button>
                  </div>
                  <dl className="attr-grid">
                    {Object.entries(row.labels).map(([k, v]) => (
                      <div className="attr-row" key={k}>
                        <dt>{k}</dt>
                        <dd>{v}</dd>
                        <span className="attr-actions">
                          <button
                            aria-label={`Filter for ${k} = ${v}`}
                            onClick={() =>
                              onAddFilter({ label: k, op: "=", value: v })
                            }
                          >
                            + filter
                          </button>
                          <button
                            aria-label={`Filter out ${k} = ${v}`}
                            onClick={() =>
                              onAddFilter({ label: k, op: "!=", value: v })
                            }
                          >
                            − exclude
                          </button>
                        </span>
                      </div>
                    ))}
                  </dl>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
