// A sortable table of trace/span records — extracted from the traces tab's
// group drill-in so the catalog's entity detail page can show the same
// "recent matching spans" shape without duplicating it. Purely
// presentational: the caller fetches (`fetchTraceGroupMembers` or similar)
// and supplies rows, loading/error state, and the copy that differs by
// context (empty message, footnote, what the identity column is called).
import { SkeletonRows } from "../explore/Skeleton";
import { SortTh, sortRows, useSort, type SortValue } from "../../lib/sortTable";
import { formatTimestamp, nanosToMs } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";
import type { TraceGroupMember } from "../../api/traceGroupMembers";

function memberSortValue(m: TraceGroupMember, key: string): SortValue {
  switch (key) {
    case "root":
      return m.spanName;
    case "service":
      return m.serviceName;
    case "duration":
      return BigInt(m.durationNanos);
    case "id":
      return m.traceId;
    default:
      return BigInt(m.startNs);
  }
}

export interface MemberTableProps {
  /** `undefined` means the query is still pending. */
  members: TraceGroupMember[] | undefined;
  isError: boolean;
  errorMessage?: string;
  /** First column's header: "Root" for trace-grain rows (always the root
   * span), "Span" for span-grain or otherwise-unscoped rows (whatever
   * matched, not necessarily the root). */
  identityLabel: string;
  /** Shown when the query resolved with zero rows. */
  emptyMessage: string;
  /** Shown below the table once rows are rendered, e.g. "Showing up to 500
   * spans, newest first." Omitted entirely when not supplied. */
  footnote?: string;
  onOpenTrace: (traceId: string) => void;
}

export function MemberTable({
  members,
  isError,
  errorMessage,
  identityLabel,
  emptyMessage,
  footnote,
  onOpenTrace,
}: MemberTableProps) {
  const [sort, toggle] = useSort("time", "desc");
  const rows = members ? sortRows(members, sort, memberSortValue) : [];
  const pending = members === undefined && !isError;

  const header = (
    <tr>
      <SortTh
        label={identityLabel}
        sortKey="root"
        sort={sort}
        toggle={toggle}
      />
      <SortTh label="Service" sortKey="service" sort={sort} toggle={toggle} />
      <SortTh
        label="Time"
        sortKey="time"
        sort={sort}
        toggle={toggle}
        firstDir="desc"
      />
      <SortTh
        label="Duration"
        sortKey="duration"
        sort={sort}
        toggle={toggle}
        numeric
      />
      <SortTh label="Trace ID" sortKey="id" sort={sort} toggle={toggle} />
    </tr>
  );

  if (isError) {
    return (
      <div className="query-error" role="alert">
        {errorMessage ?? "Failed to load."}
      </div>
    );
  }

  if (pending) {
    return (
      <table className="trace-table" aria-busy="true">
        <thead>{header}</thead>
        <tbody>
          <SkeletonRows rows={8} columns={5} numericFrom={3} />
        </tbody>
      </table>
    );
  }

  if (rows.length === 0) {
    return <div className="traces-note">{emptyMessage}</div>;
  }

  return (
    <>
      <table className="trace-table">
        <thead>{header}</thead>
        <tbody>
          {rows.map((m) => (
            <tr
              key={`${m.traceId}-${m.spanId}`}
              onClick={() => onOpenTrace(m.traceId)}
            >
              <td>
                <button className="trace-open">{m.spanName}</button>
              </td>
              <td>{m.serviceName}</td>
              <td>{formatTimestamp(nanosToMs(m.startNs))}</td>
              <td className="num">
                {formatDurationMs(nanosToMs(m.durationNanos))}
              </td>
              <td className="trace-id">{m.traceId}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {footnote && <div className="traces-note">{footnote}</div>}
    </>
  );
}
