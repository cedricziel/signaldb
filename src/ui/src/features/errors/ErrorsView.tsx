// Errors & Exceptions: exceptions grouped by (type, message, service) across
// both places SignalDB can find them (see api/errors.ts) — an issue-list
// style view built entirely on Query IR, no dedicated backend endpoint.
import { useQuery } from "@tanstack/react-query";
import { Fragment, useState } from "react";
import {
  fetchErrorGroupVolume,
  fetchErrorGroups,
  fetchErrorOccurrences,
  type ErrorGroup,
} from "../../api/errors";
import {
  MobileFiltersToggle,
  MobileSidebarDrawer,
} from "../../components/MobileSidebarDrawer";
import { QueryError } from "../../components/QueryError";
import { useMobileSidebar } from "../../hooks/useMobileSidebar";
import { SkeletonRows } from "../explore/Skeleton";
import { ErrorFacets } from "./ErrorFacets";
import { ErrorSparkline } from "./ErrorSparkline";
import {
  applyErrorFilters,
  errorFacetValueLabel,
  upsertErrorFilter,
  type ErrorFilter,
} from "../../lib/errorFacets";
import { parseStacktraceLines } from "../../lib/stacktrace";
import { SortTh, sortRows, useSort, type SortValue } from "../../lib/sortTable";
import {
  durationToSeconds,
  formatTimestamp,
  nanosToMs,
  rangeScopeKey,
  resolveRange,
  stepForRange,
} from "../../lib/time";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import "./errors.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

// A structured tuple, not a delimiter-joined string: a value containing the
// delimiter itself (e.g. an exception message with a literal "|") could
// otherwise collide with a different group's key.
function groupKey(g: ErrorGroup): string {
  return JSON.stringify([
    g.source,
    g.exceptionType,
    g.exceptionMessage,
    g.serviceName,
    g.escaped,
  ]);
}

function groupSortValue(g: ErrorGroup, key: string): SortValue {
  return key === "last" ? BigInt(g.lastNs) : g.count;
}

export function ErrorsView({ state, update }: Props) {
  const range = resolveRange(state.range, Date.now());
  const rangeKey = rangeScopeKey(state);
  const [selected, setSelected] = useState<ErrorGroup | null>(null);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [filters, setFilters] = useState<ErrorFilter[]>([]);
  const [sort, toggle] = useSort("count", "desc");
  const mobileSidebar = useMobileSidebar();

  const groupsQuery = useQuery({
    queryKey: ["error-groups", rangeKey],
    queryFn: () => fetchErrorGroups(range),
  });

  const occurrencesQuery = useQuery({
    queryKey: [
      "error-occurrences",
      rangeKey,
      selected ? groupKey(selected) : null,
    ],
    queryFn: () => fetchErrorOccurrences(selected!, range),
    enabled: selected !== null,
  });
  const occurrences = occurrencesQuery.data ?? [];

  const step = stepForRange(range, 20);
  const stepMs = (durationToSeconds(step) ?? 0) * 1000;
  const volumeQuery = useQuery({
    queryKey: [
      "error-group-volume",
      rangeKey,
      selected ? groupKey(selected) : null,
      step,
    ],
    queryFn: () => fetchErrorGroupVolume(selected!, range, step),
    enabled: selected !== null,
  });

  const selectGroup = (g: ErrorGroup) => {
    setSelected(g);
    setExpanded(null);
  };

  const allGroups = groupsQuery.data?.groups ?? [];
  const groups = sortRows(
    applyErrorFilters(allGroups, filters),
    sort,
    groupSortValue,
  );
  const pending = groupsQuery.isPending;

  const addFilter = (f: ErrorFilter) =>
    setFilters((fs) => upsertErrorFilter(fs, f));
  const removeFilter = (f: ErrorFilter) =>
    setFilters((fs) =>
      fs.filter((x) => !(x.field === f.field && x.value === f.value)),
    );

  return (
    <div className="errors-view">
      <div className="errors-head">
        <div className="catalog-headline">
          <span className="catalog-title">Errors &amp; Exceptions</span>
          <span className="catalog-sub">
            grouped from span exception events and log exception attributes
          </span>
        </div>

        {filters.length > 0 && (
          <div className="filter-chips" aria-label="Active filters">
            {filters.map((f) => (
              <button
                className="filter-chip"
                key={`${f.field}|${f.value}`}
                aria-label={`Remove filter ${f.field} = ${f.value}`}
                onClick={() => removeFilter(f)}
              >
                <span className="filter-chip-k">{f.field}</span>
                <span className="filter-chip-v">{f.value}</span>
                <span className="filter-chip-x">×</span>
              </button>
            ))}
          </div>
        )}
      </div>

      <MobileFiltersToggle
        open={mobileSidebar.open}
        onToggle={mobileSidebar.toggle}
      />

      <div className="errors-body">
        <MobileSidebarDrawer
          open={mobileSidebar.open}
          onClose={mobileSidebar.close}
        >
          <ErrorFacets
            groups={allGroups}
            filters={filters}
            onAddFilter={addFilter}
            onRemoveFilter={removeFilter}
          />
        </MobileSidebarDrawer>
        <div className="errors-main catalog-main">
          {groupsQuery.isError && (
            <QueryError what="exceptions" error={groupsQuery.error} />
          )}
          {!pending && !groupsQuery.isError && allGroups.length === 0 && (
            <div className="view-note">
              No exceptions captured in this window.
            </div>
          )}
          {!pending &&
            !groupsQuery.isError &&
            allGroups.length > 0 &&
            groups.length === 0 && (
              <div className="view-note">
                No exceptions match the active filters.
              </div>
            )}

          {(pending || groups.length > 0) && (
            <table className="errors-table" aria-busy={pending}>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Message</th>
                  <th>Service</th>
                  <th>Source</th>
                  <th>Handled</th>
                  <SortTh
                    label="Count"
                    sortKey="count"
                    sort={sort}
                    toggle={toggle}
                    numeric
                  />
                  <th>First seen</th>
                  <SortTh
                    label="Last seen"
                    sortKey="last"
                    sort={sort}
                    toggle={toggle}
                    firstDir="desc"
                  />
                </tr>
              </thead>
              <tbody>
                {pending ? (
                  <SkeletonRows rows={8} columns={8} />
                ) : (
                  groups.map((g) => {
                    const key = groupKey(g);
                    return (
                      <tr
                        key={key}
                        className="errors-row"
                        aria-selected={
                          selected !== null && groupKey(selected) === key
                        }
                        onClick={() => selectGroup(g)}
                      >
                        <td>
                          {/* No own onClick: a native button dispatches a
                              click on Enter/Space too, which bubbles to the
                              row's handler below — the same
                              keyboard-accessible-via-bubbling pattern
                              MemberTable uses. */}
                          <button type="button" className="trace-open">
                            {g.exceptionType ?? "—"}
                          </button>
                        </td>
                        <td
                          className="errors-message"
                          title={g.exceptionMessage ?? undefined}
                        >
                          {g.exceptionMessage ?? "—"}
                        </td>
                        <td>{g.serviceName ?? "—"}</td>
                        <td>
                          <span
                            className={`errors-source errors-source-${g.source}`}
                          >
                            {g.source}
                          </span>
                        </td>
                        <td>
                          {g.escaped != null
                            ? errorFacetValueLabel("escaped", g.escaped)
                            : "—"}
                        </td>
                        <td>{g.count}</td>
                        <td>{formatTimestamp(nanosToMs(g.firstNs))}</td>
                        <td>{formatTimestamp(nanosToMs(g.lastNs))}</td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          )}
          {groupsQuery.data?.truncated && (
            <div className="view-note">
              More exception groups exist than shown; narrow the time range to
              see the rest.
            </div>
          )}

          {selected && (
            <div className="errors-detail catalog-main">
              <div className="catalog-headline">
                <span className="catalog-title">
                  {selected.exceptionType ?? "Exception"}
                </span>
                <span className="catalog-sub">
                  individual occurrences — click one to view its stacktrace
                </span>
              </div>
              {volumeQuery.data && (
                <ErrorSparkline
                  series={volumeQuery.data}
                  rangeMs={range}
                  stepMs={stepMs}
                />
              )}
              {occurrencesQuery.isError && (
                <QueryError what="occurrences" error={occurrencesQuery.error} />
              )}
              {occurrencesQuery.isSuccess && occurrences.length === 0 && (
                <div className="view-note">
                  No occurrences found in this window.
                </div>
              )}
              {(occurrencesQuery.isPending || occurrences.length > 0) && (
                <table className="errors-occurrences">
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Trace</th>
                    </tr>
                  </thead>
                  <tbody>
                    {occurrencesQuery.isPending ? (
                      <SkeletonRows rows={5} columns={2} />
                    ) : (
                      occurrences.map((o, i) => (
                        <Fragment key={i}>
                          <tr
                            className="errors-occurrence-row"
                            data-testid={`occurrence-row-${i}`}
                            aria-expanded={expanded === i}
                            onClick={() =>
                              setExpanded(expanded === i ? null : i)
                            }
                          >
                            <td>
                              <button type="button" className="trace-open">
                                {formatTimestamp(nanosToMs(o.timestampNs))}
                              </button>
                            </td>
                            <td>
                              {o.traceId ? (
                                <button
                                  type="button"
                                  className="act"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    update(
                                      { signal: "traces", trace: o.traceId! },
                                      { push: true },
                                    );
                                  }}
                                >
                                  View trace →
                                </button>
                              ) : (
                                "—"
                              )}
                            </td>
                          </tr>
                          {expanded === i && (
                            <tr className="errors-occurrence-detail">
                              <td colSpan={2}>
                                {o.stacktrace ? (
                                  <Stacktrace text={o.stacktrace} />
                                ) : (
                                  <div className="view-note">
                                    No stacktrace captured for this occurrence.
                                  </div>
                                )}
                              </td>
                            </tr>
                          )}
                        </Fragment>
                      ))
                    )}
                  </tbody>
                </table>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function Stacktrace({ text }: { text: string }) {
  return (
    <pre className="errors-stacktrace">
      {parseStacktraceLines(text).map((line, i) => (
        <div
          key={i}
          className={`errors-stacktrace-line errors-stacktrace-${line.kind}`}
        >
          {line.text}
        </div>
      ))}
    </pre>
  );
}
