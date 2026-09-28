// The Real users page's Errors tab: exception groups scoped to the selected
// app (`api/rumErrorGroups.ts`, the `explore-ui-errors` grouping restricted
// to this app's `service.name`), each row showing its "new in <version>"
// and "backend cause" pills, last-seen time and occurrence count. Selecting
// a row (`?errgroup=`) opens its detail — the stats, stack frames and
// backend-cause panel added by `ErrorDetailView`.
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { compactCount } from "../../lib/vizFormat";
import { formatTimestamp } from "../../lib/time";
import {
  errorGroupKey,
  type RumErrorGroupWithCause,
} from "../../api/rumErrorGroups";
import { Panel } from "./Panel";
import { useRumErrorGroups, type RumScope } from "./useRumData";

interface Props {
  scope: RumScope;
  currentVersion: string | null;
  selected: string;
  onSelectGroup: (groupKey: string) => void;
}

export function ErrorsTab({
  scope,
  currentVersion,
  selected,
  onSelectGroup,
}: Props) {
  const groups = useRumErrorGroups(scope, currentVersion);
  const rows = groups.data ?? [];

  return (
    <div className="rum-stack">
      <Panel
        title="Errors"
        meta="exception groups · scoped to this app · sorted by occurrences"
      >
        {groups.isError ? (
          <QueryError what="error groups" error={groups.error} />
        ) : groups.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : rows.length === 0 ? (
          <EmptyState title="No errors recorded in this window" />
        ) : (
          <ErrorsTable
            rows={rows}
            currentVersion={currentVersion}
            selected={selected}
            onSelect={onSelectGroup}
          />
        )}
      </Panel>
    </div>
  );
}

function ErrorsTable({
  rows,
  currentVersion,
  selected,
  onSelect,
}: {
  rows: RumErrorGroupWithCause[];
  currentVersion: string | null;
  selected: string;
  onSelect: (groupKey: string) => void;
}) {
  return (
    <div className="rum-errors-table">
      <div className="rum-errors-head">
        <span>Error</span>
        <span>Flags</span>
        <span>Last seen</span>
        <span className="num">Count</span>
      </div>
      {rows.map((g) => {
        const key = errorGroupKey(g);
        return (
          <button
            key={key}
            type="button"
            className={
              key === selected ? "rum-errors-row on" : "rum-errors-row"
            }
            aria-current={key === selected ? "true" : undefined}
            onClick={() => onSelect(key)}
          >
            <span className="rum-row-main">
              <span className="ell rum-row-title">
                <span style={{ color: "var(--err)" }}>
                  {g.exceptionType ?? "Error"}
                </span>{" "}
                <span className="rum-row-message">
                  {g.exceptionMessage ?? ""}
                </span>
              </span>
            </span>
            <span className="rum-session-signals">
              {g.newInCurrentRelease && currentVersion && (
                <span className="rum-pill accent">new in {currentVersion}</span>
              )}
              {g.backendCause && (
                <span className="rum-pill warn">backend cause</span>
              )}
            </span>
            <span className="mono dim">{formatTimestamp(g.lastMs)}</span>
            <span className="mono num">{compactCount(g.count)}</span>
          </button>
        );
      })}
    </div>
  );
}
