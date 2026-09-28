// The Real users page's Sessions tab: the session list (grouped by
// session.id, `useRumSessions`), quick filters ("With errors" / "Slow load")
// and a free-text filter (session.id/user.id or key=value, sent server-side
// — see `api/rumSessions.ts`'s module doc). Session detail (`?session=`)
// ships in a later branch of this same change.
import { useState } from "react";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { compactCount } from "../../lib/vizFormat";
import { formatTimestamp } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";
import { Panel } from "./Panel";
import { filterSessionsRows, type RumSessionRow } from "../../api/rumSessions";
import { useRumSessions, type RumScope } from "./useRumData";

interface Props {
  scope: RumScope;
  onSelectSession: (sessionId: string) => void;
}

export function SessionsTab({ scope, onSelectSession }: Props) {
  const [filterText, setFilterText] = useState("");
  const [onlyErrors, setOnlyErrors] = useState(false);
  const [onlySlow, setOnlySlow] = useState(false);
  const sessions = useRumSessions(scope, filterText);
  const rows = filterSessionsRows(sessions.data ?? [], {
    onlyErrors,
    onlySlow,
  });

  return (
    <div className="rum-stack">
      <Panel
        title="Sessions"
        meta="grouped by session.id · sorted by most recently active"
        actions={
          <div className="rum-session-toolbar">
            <input
              type="search"
              className="rum-session-filter-input"
              placeholder="session.id, user.id or attribute=value"
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              aria-label="Filter sessions"
            />
            <button
              type="button"
              className={
                onlyErrors ? "rum-quick-filter on" : "rum-quick-filter"
              }
              aria-pressed={onlyErrors}
              onClick={() => setOnlyErrors((v) => !v)}
            >
              With errors
            </button>
            <button
              type="button"
              className={onlySlow ? "rum-quick-filter on" : "rum-quick-filter"}
              aria-pressed={onlySlow}
              onClick={() => setOnlySlow((v) => !v)}
            >
              Slow load (LCP poor)
            </button>
          </div>
        }
      >
        {sessions.isError ? (
          <QueryError what="sessions" error={sessions.error} />
        ) : sessions.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : rows.length === 0 ? (
          <EmptyState
            title={
              sessions.data?.length
                ? "No sessions match this filter"
                : "No sessions recorded in this window"
            }
          />
        ) : (
          <SessionsTable rows={rows} onSelect={onSelectSession} />
        )}
      </Panel>
    </div>
  );
}

function SessionsTable({
  rows,
  onSelect,
}: {
  rows: RumSessionRow[];
  onSelect: (sessionId: string) => void;
}) {
  return (
    <div className="rum-sessions-table">
      <div className="rum-sessions-head">
        <span>Session</span>
        <span>User</span>
        <span>Browser</span>
        <span>Device</span>
        <span>Started</span>
        <span className="num">Duration</span>
        <span className="num">Views</span>
        <span>Entry → Exit</span>
        <span>Signals</span>
      </div>
      {rows.map((r) => (
        <button
          key={r.sessionId}
          type="button"
          className="rum-sessions-row"
          onClick={() => onSelect(r.sessionId)}
        >
          <span className="mono ell" title={r.sessionId}>
            {r.sessionId}
          </span>
          <span className="mono dim ell">{r.userId ?? "—"}</span>
          <span className="dim">{r.browser ?? "—"}</span>
          <span className="dim">
            {r.mobile === null ? "—" : r.mobile ? "Mobile" : "Desktop"}
          </span>
          <span className="mono dim">{formatTimestamp(r.firstMs)}</span>
          <span className="mono num">{formatDurationMs(r.durationMs)}</span>
          <span className="mono num dim">{compactCount(r.views)}</span>
          <span
            className="mono dim ell"
            title={`${r.entry ?? "—"} → ${r.exit ?? "—"}`}
          >
            {r.entry ?? "—"} → {r.exit ?? "—"}
          </span>
          <span className="rum-session-signals">
            {r.errors > 0 && (
              <span className="rum-pill err">{compactCount(r.errors)} err</span>
            )}
            {r.slow > 0 && (
              <span className="rum-pill warn">{compactCount(r.slow)} slow</span>
            )}
          </span>
        </button>
      ))}
    </div>
  );
}
