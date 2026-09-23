// Errors & Exceptions: exceptions grouped by (type, message, service) across
// both places SignalDB can find them (see api/errors.ts) — an issue-list
// style view built entirely on Query IR, no dedicated backend endpoint.
import { useQuery } from "@tanstack/react-query";
import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import {
  fetchErrorGroupVolume,
  fetchErrorGroups,
  fetchErrorOccurrences,
  type ErrorGroup,
  type ErrorSource,
} from "../../api/errors";
import {
  MobileFiltersToggle,
  MobileSidebarDrawer,
} from "../../components/MobileSidebarDrawer";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { useMobileSidebar } from "../../hooks/useMobileSidebar";
import { SkeletonRows } from "../explore/Skeleton";
import { ErrorFacets } from "./ErrorFacets";
import { ErrorSparkline } from "./ErrorSparkline";
import {
  applyErrorFilters,
  ERROR_FACET_FIELDS,
  errorFacetValueLabel,
  upsertErrorFilter,
  type ErrorFacetField,
  type ErrorFilter,
} from "../../lib/errorFacets";
import type { LabelFilter } from "../../lib/filters";
import { StacktraceLines } from "../../components/StacktraceLines";
import { SortTh, sortRows, useSort, type SortValue } from "../../lib/sortTable";
import {
  durationToSeconds,
  formatTimestamp,
  formatTimestampForRange,
  nanosToMs,
  rangeScopeKey,
  resolveRange,
  stepForRange,
} from "../../lib/time";
import { formatValue } from "../../lib/vizFormat";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
// `.catalog-headline`/`.catalog-title`/`.catalog-sub`/`.catalog-main`
// (the section headings) and `.trace-open`/`.backbtn` (the drill-in and
// back-navigation buttons) are shared button/heading styles this view
// reuses from the Catalog and Traces tabs rather than errors.css — each
// only renders correctly once its owning stylesheet has loaded, so this
// view must import them itself instead of relying on another tab having
// mounted first.
import "../catalog/catalog.css";
import "../traces/traces.css";
import "./errors.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

// A structured tuple, not a delimiter-joined string: a value containing the
// delimiter itself (e.g. an exception message with a literal "|") could
// otherwise collide with a different group's key.
// Exported for `EntityErrorGroups` (the service detail page's own error
// groups section), which drills into this same tab via the identical
// `?group=` encoding, and must produce a key `selectedFromState` recognizes.
export function groupKey(g: ErrorGroup): string {
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

/**
 * Decode a `groupKey()` string back into the fields a group query needs
 * (`fetchErrorOccurrences`/`fetchErrorGroupVolume` only ever read the
 * pinning fields, never `count`/`firstNs`/`lastNs`) — so a shared or
 * reloaded `?group=` link can re-run those queries immediately, without
 * waiting for the group list to load and without re-deriving a second
 * encoding just for the URL.
 */
/** `field` must be a string identity value or its absence, never some other
 * JSON shape a hand-edited or stale link might carry. */
function isIdentityField(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function decodeGroupKey(key: string): ErrorGroup | null {
  if (key === "") return null;
  try {
    const parsed: unknown = JSON.parse(key);
    if (!Array.isArray(parsed) || parsed.length !== 5) return null;
    const [source, exceptionType, exceptionMessage, serviceName, escaped] =
      parsed as unknown[];
    if (source !== "traces" && source !== "logs") return null;
    if (
      !isIdentityField(exceptionType) ||
      !isIdentityField(exceptionMessage) ||
      !isIdentityField(serviceName)
    ) {
      return null;
    }
    if (escaped !== null && escaped !== "true" && escaped !== "false") {
      return null;
    }
    return {
      source: source as ErrorSource,
      exceptionType,
      exceptionMessage,
      serviceName,
      escaped,
      // Not carried by the key, and not read by anything querying from it.
      count: 0,
      firstNs: "0",
      lastNs: "0",
    };
  } catch {
    return null;
  }
}

/** The group the URL names — the already-fetched row when the list has
 * loaded (carrying its real count/first/last for the header and sort), the
 * decoded key otherwise (see {@link decodeGroupKey}), so selecting a group
 * and reloading behave the same. */
function selectedFromState(
  groups: ErrorGroup[],
  key: string,
): ErrorGroup | null {
  if (key === "") return null;
  return groups.find((g) => groupKey(g) === key) ?? decodeGroupKey(key);
}

const ERROR_FACET_FIELD_SET = new Set<string>(ERROR_FACET_FIELDS);

/** The facet filters as `state.filters` (the shared `f`-style `LabelFilter`
 * URL encoding — see lib/urlState.ts) carry them: `label` is the facet
 * field, `op` is always `=` since a facet filter is always an equality
 * pin. Anything else the URL might carry there (a stray `f=` from a
 * hand-edited link naming a field this tab has no facet for) is dropped. */
function errorFiltersFromState(filters: LabelFilter[]): ErrorFilter[] {
  return filters
    .filter((f) => f.op === "=" && ERROR_FACET_FIELD_SET.has(f.label))
    .map((f) => ({ field: f.label as ErrorFacetField, value: f.value }));
}

function errorFiltersToState(filters: ErrorFilter[]): LabelFilter[] {
  return filters.map((f) => ({ label: f.field, op: "=", value: f.value }));
}

export function ErrorsView({ state, update }: Props) {
  const range = resolveRange(state.range, Date.now());
  const rangeKey = rangeScopeKey(state);
  const [expanded, setExpanded] = useState<number | null>(null);
  // Sort is the one piece of table state left local — a "which group is
  // open" or "which facets are active" URL is worth sharing/reloading; a
  // sort order is not.
  const [sort, toggle] = useSort("count", "desc");
  const mobileSidebar = useMobileSidebar();
  const detailRef = useRef<HTMLDivElement>(null);

  const groupsQuery = useQuery({
    queryKey: ["error-groups", rangeKey],
    queryFn: () => fetchErrorGroups(range),
  });

  const allGroups = groupsQuery.data?.groups ?? [];
  // The selected group and active facet filters live in the URL (`group`
  // and `f` — both otherwise free on /errors) so opening a group's detail,
  // narrowing the facets, then following "View trace →" and hitting Back
  // returns to the same list, not the defaults.
  // Memoised on `state.filters` so its identity is stable across renders
  // that don't change it — `groups` below depends on it, and a fresh array
  // every render would otherwise defeat that memoisation entirely.
  const filters = useMemo(
    () => errorFiltersFromState(state.filters),
    [state.filters],
  );
  const selected = useMemo(
    () => selectedFromState(allGroups, state.group),
    [allGroups, state.group],
  );

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

  // Scroll the detail panel into view on selection — it renders below the
  // full group table, which can otherwise leave a click with no visible
  // effect on a long list. Guarded: jsdom (unit tests) has no layout engine
  // and doesn't implement scrollIntoView.
  useEffect(() => {
    if (selected) detailRef.current?.scrollIntoView?.({ block: "start" });
  }, [state.group]);

  const selectGroup = (g: ErrorGroup) => {
    update({ group: groupKey(g) });
    setExpanded(null);
  };
  const closeDetail = () => update({ group: "" });

  const groups = useMemo(
    () => sortRows(applyErrorFilters(allGroups, filters), sort, groupSortValue),
    [allGroups, filters, sort],
  );
  const pending = groupsQuery.isPending;

  const addFilter = (f: ErrorFilter) =>
    update({ filters: errorFiltersToState(upsertErrorFilter(filters, f)) });
  const removeFilter = (f: ErrorFilter) =>
    update({
      filters: errorFiltersToState(
        filters.filter((x) => !(x.field === f.field && x.value === f.value)),
      ),
    });

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
                className="filter-chip chip"
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
            <EmptyState title="No exceptions in this range" />
          )}
          {!pending &&
            !groupsQuery.isError &&
            allGroups.length > 0 &&
            groups.length === 0 && (
              <EmptyState title="No exceptions in this range">
                No exceptions match the active filters.
              </EmptyState>
            )}

          {(pending || groups.length > 0) && (
            <div className="table-scroll">
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
                          <td className="num">{formatValue(g.count)}</td>
                          <td>
                            {formatTimestampForRange(
                              nanosToMs(g.firstNs),
                              range,
                            )}
                          </td>
                          <td>
                            {formatTimestampForRange(
                              nanosToMs(g.lastNs),
                              range,
                            )}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
          {groupsQuery.data?.truncated && (
            <div className="view-note">
              More exception groups exist than shown; narrow the time range to
              see the rest.
            </div>
          )}

          {selected && (
            <div className="errors-detail catalog-main" ref={detailRef}>
              <button type="button" className="backbtn" onClick={closeDetail}>
                ← all groups
              </button>
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
                <EmptyState title="No occurrences in this range" />
              )}
              {(occurrencesQuery.isPending || occurrences.length > 0) && (
                <div className="table-scroll">
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
                                    className="btn"
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
                                    // No repository/ref hints are available
                                    // for an occurrence (unlike a trace's
                                    // exception span, it carries no resource
                                    // attributes here), so the lookup probes
                                    // the tenant's linked repositories at
                                    // their default branch.
                                    <StacktraceLines
                                      text={o.stacktrace}
                                      tenant={state.tenant}
                                      variant="error"
                                    />
                                  ) : (
                                    <div className="view-note">
                                      No stacktrace captured for this
                                      occurrence.
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
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
