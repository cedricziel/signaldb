// "Error groups" — a service detail page's own leaderboard of its top
// exception groups (see api/errors.ts for how a group is derived), so a
// service's most common failures are visible without leaving the Catalog
// tab. Each row drills into the same group detail the standalone Errors tab
// itself opens (`ErrorsView`'s `?group=` state), via the identical key
// encoding (`groupKey`, exported from there for this reuse).
import { useQuery } from "@tanstack/react-query";
import {
  fetchErrorGroups,
  fetchErrorGroupVolume,
  type ErrorGroup,
} from "../../api/errors";
import { groupKey } from "../errors/ErrorsView";
import { ErrorSparkline } from "../errors/ErrorSparkline";
import { QueryError } from "../../components/QueryError";
import { EmptyState } from "../../components/EmptyState";
import { SkeletonRows } from "../explore/Skeleton";
import {
  durationToSeconds,
  formatTimestampForRange,
  nanosToMs,
  type ResolvedRange,
} from "../../lib/time";
import { formatValue } from "../../lib/vizFormat";
import type { UpdateFn } from "../../lib/urlState";
import "../errors/errors.css";
import "./entityErrorGroups.css";

interface Props {
  serviceName: string;
  range: ResolvedRange;
  rangeKey: string;
  update: UpdateFn;
}

/** How many of the service's own top exception groups this section shows —
 * a leaderboard, not the full list the Errors tab itself gives. */
const TOP_N = 5;

/** The row sparkline's own fixed window — always "the last hour", regardless
 * of the page's own (often much wider) range, the same "recent shape" signal
 * an issue tracker's own group list commonly leads with. */
const SPARK_STEP = "2m";
const SPARK_WINDOW_MS = 60 * 60 * 1000;

function lastHourRange(nowMs: number): ResolvedRange {
  return { fromMs: nowMs - SPARK_WINDOW_MS, toMs: nowMs };
}

function EntityErrorGroupRow({
  group,
  range,
  sparkRange,
  stepMs,
  onOpen,
}: {
  group: ErrorGroup;
  range: ResolvedRange;
  sparkRange: ResolvedRange;
  stepMs: number;
  onOpen: () => void;
}) {
  const volumeQuery = useQuery({
    queryKey: ["entity-error-group-volume", groupKey(group)],
    queryFn: () => fetchErrorGroupVolume(group, sparkRange, SPARK_STEP),
  });

  return (
    <tr
      className="error-groups-row"
      onClick={onOpen}
      data-testid="error-groups-row"
    >
      <td className="error-groups-spark">
        {volumeQuery.data && (
          <ErrorSparkline
            series={volumeQuery.data}
            rangeMs={sparkRange}
            stepMs={stepMs}
          />
        )}
      </td>
      <td className="error-groups-error">
        <span className="error-groups-type">{group.exceptionType ?? "—"}</span>
        <span
          className="error-groups-message"
          title={group.exceptionMessage ?? undefined}
        >
          {group.exceptionMessage ?? "—"}
        </span>
      </td>
      <td>
        <span className={`errors-source errors-source-${group.source}`}>
          {group.source}
        </span>
      </td>
      <td className="num">{formatValue(group.count)}</td>
      <td>{formatTimestampForRange(nanosToMs(group.lastNs), range)}</td>
    </tr>
  );
}

/**
 * The entity detail page's own "Error groups" section, `service`-only (see
 * `EntityDetail.tsx`'s guard): the same (type, message, service, escaped)
 * grouping the Errors tab shows, pinned to this one service and capped to
 * its top {@link TOP_N} by count.
 */
export function EntityErrorGroups({
  serviceName,
  range,
  rangeKey,
  update,
}: Props) {
  const query = useQuery({
    queryKey: ["entity-error-groups", serviceName, rangeKey],
    queryFn: () => fetchErrorGroups(range, serviceName),
  });
  const groups = (query.data?.groups ?? []).slice(0, TOP_N);
  const pending = query.isPending;
  // Computed once for the whole strip, not per row: every row's sparkline
  // shows the same "last hour" window, so recomputing `Date.now()` per row
  // would only risk a one-tick skew between them for no benefit.
  const sparkRange = lastHourRange(Date.now());
  const stepMs = (durationToSeconds(SPARK_STEP) ?? 0) * 1000;

  const openAllErrors = () =>
    update(
      {
        signal: "errors",
        filters: [{ label: "serviceName", op: "=", value: serviceName }],
      },
      { push: true },
    );
  const openGroup = (g: ErrorGroup) =>
    update({ signal: "errors", group: groupKey(g) }, { push: true });

  return (
    <div className="catalog-main entity-error-groups">
      <div className="catalog-headline">
        <span className="catalog-title">Error groups</span>
        <button type="button" className="btn" onClick={openAllErrors}>
          All errors for {serviceName}
        </button>
      </div>

      {query.isError && <QueryError what="error groups" error={query.error} />}
      {!pending && !query.isError && groups.length === 0 && (
        <EmptyState title={`No errors for ${serviceName} in this window.`} />
      )}
      {(pending || groups.length > 0) && (
        <div className="table-scroll">
          <table className="error-groups-table" aria-busy={pending}>
            <thead>
              <tr>
                <th>Last hour</th>
                <th>Error</th>
                <th>Source</th>
                <th>Count</th>
                <th>Last seen</th>
              </tr>
            </thead>
            <tbody>
              {pending ? (
                <SkeletonRows rows={TOP_N} columns={5} />
              ) : (
                groups.map((g) => (
                  <EntityErrorGroupRow
                    key={groupKey(g)}
                    group={g}
                    range={range}
                    sparkRange={sparkRange}
                    stepMs={stepMs}
                    onOpen={() => openGroup(g)}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
