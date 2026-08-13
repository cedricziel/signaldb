// Errors & Exceptions: exceptions grouped by (type, message, service) across
// both places SignalDB can find them (see api/errors.ts) — a Sentry-issue-list
// style view built entirely on Query IR, no dedicated backend endpoint.
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import {
  fetchErrorGroups,
  fetchErrorExample,
  type ErrorGroup,
} from "../../api/errors";
import { ErrorFacets } from "./ErrorFacets";
import { applyErrorFilters, type ErrorFilter } from "../../lib/errorFacets";
import { parseStacktraceLines } from "../../lib/stacktrace";
import {
  formatTimestamp,
  nanosToMs,
  rangeToParam,
  resolveRange,
} from "../../lib/time";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import "./errors.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

function groupKey(g: ErrorGroup): string {
  return `${g.source}|${g.exceptionType ?? ""}|${g.exceptionMessage ?? ""}|${g.serviceName ?? ""}`;
}

export function ErrorsView({ state, update }: Props) {
  const range = resolveRange(state.range, Date.now());
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const [selected, setSelected] = useState<ErrorGroup | null>(null);
  const [filters, setFilters] = useState<ErrorFilter[]>([]);

  const groupsQuery = useQuery({
    queryKey: ["error-groups", rangeKey],
    queryFn: () => fetchErrorGroups(range),
  });

  const exampleQuery = useQuery({
    queryKey: ["error-example", rangeKey, selected ? groupKey(selected) : null],
    queryFn: () => fetchErrorExample(selected!, range),
    enabled: selected !== null,
  });

  const allGroups = groupsQuery.data?.groups ?? [];
  const groups = applyErrorFilters(allGroups, filters);
  const pending = groupsQuery.isPending;

  const addFilter = (f: ErrorFilter) => setFilters((fs) => [...fs, f]);
  const removeFilter = (f: ErrorFilter) =>
    setFilters((fs) =>
      fs.filter((x) => !(x.field === f.field && x.value === f.value)),
    );

  return (
    <div className="errors-view">
      <div className="catalog-headline">
        <span className="catalog-title">Errors &amp; Exceptions</span>
        <span className="catalog-sub">
          grouped from span exception events and log exception attributes
        </span>
      </div>

      {filters.length > 0 && (
        <div className="trace-chips" aria-label="Active filters">
          {filters.map((f) => (
            <button
              className="chip"
              key={`${f.field}|${f.value}`}
              aria-label={`Remove filter ${f.field} = ${f.value}`}
              onClick={() => removeFilter(f)}
            >
              <span className="chip-k">{f.field}</span>
              <span className="chip-v">{f.value}</span>
              <span className="chip-x">×</span>
            </button>
          ))}
        </div>
      )}

      <div className="errors-body">
        <ErrorFacets
          groups={allGroups}
          filters={filters}
          onAddFilter={addFilter}
          onRemoveFilter={removeFilter}
        />
        <div className="errors-main catalog-main">
          {groupsQuery.isError && (
            <div className="query-error" role="alert">
              Failed to load: {(groupsQuery.error as Error).message}
            </div>
          )}
          {!pending && !groupsQuery.isError && allGroups.length === 0 && (
            <div className="traces-note">
              No exceptions captured in this window.
            </div>
          )}
          {!pending &&
            !groupsQuery.isError &&
            allGroups.length > 0 &&
            groups.length === 0 && (
              <div className="traces-note">
                No exceptions match the active filters.
              </div>
            )}

          {groups.length > 0 && (
            <table className="errors-table" aria-busy={pending}>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Message</th>
                  <th>Service</th>
                  <th>Source</th>
                  <th>Count</th>
                  <th>First seen</th>
                  <th>Last seen</th>
                </tr>
              </thead>
              <tbody>
                {groups.map((g) => {
                  const key = groupKey(g);
                  return (
                    <tr
                      key={key}
                      className="errors-row"
                      aria-selected={
                        selected !== null && groupKey(selected) === key
                      }
                      onClick={() => setSelected(g)}
                    >
                      <td>{g.exceptionType ?? "—"}</td>
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
                      <td>{g.count}</td>
                      <td>{formatTimestamp(nanosToMs(g.firstNs))}</td>
                      <td>{formatTimestamp(nanosToMs(g.lastNs))}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
          {groupsQuery.data?.truncated && (
            <div className="traces-note">
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
                {exampleQuery.data?.traceId && (
                  <button
                    className="act"
                    onClick={() =>
                      update(
                        {
                          signal: "traces",
                          trace: exampleQuery.data!.traceId!,
                        },
                        { push: true },
                      )
                    }
                  >
                    View trace →
                  </button>
                )}
              </div>
              {exampleQuery.isPending && (
                <div className="traces-note">Loading…</div>
              )}
              {exampleQuery.isError && (
                <div className="query-error" role="alert">
                  Failed to load example:{" "}
                  {(exampleQuery.error as Error).message}
                </div>
              )}
              {exampleQuery.data?.stacktrace ? (
                <Stacktrace text={exampleQuery.data.stacktrace} />
              ) : (
                exampleQuery.data && (
                  <div className="traces-note">
                    No stacktrace captured for this exception.
                  </div>
                )
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
