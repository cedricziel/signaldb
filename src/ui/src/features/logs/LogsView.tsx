import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { lokiLabels, lokiQueryHistogram, lokiQueryLogs } from "../../api/loki";
import {
  compileHistogramQL,
  compileLogQL,
  upsertFilter,
  type LabelFilter,
} from "../../lib/filters";
import {
  durationToSeconds,
  rangeToParam,
  resolveRange,
  stepForRange,
} from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { FieldSidebar } from "./FieldSidebar";
import { FilterChips } from "./FilterChips";
import { Histogram } from "./Histogram";
import { LogList } from "./LogList";

const LIVE_INTERVAL_MS = 2000;

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function LogsView({ state, update }: Props) {
  const [editingRaw, setEditingRaw] = useState(false);
  const [rawDraft, setRawDraft] = useState("");
  const [searchDraft, setSearchDraft] = useState(state.search);

  const model = {
    filters: state.filters,
    search: state.search,
    raw: state.raw || undefined,
  };
  const logql = compileLogQL(model);
  // Cache scope: time range plus tenant context, so switching tenants
  // refetches instead of serving another tenant's cached results.
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  // Relative ranges resolve "now" per fetch so live mode slides forward.
  const range = () => resolveRange(state.range, Date.now());
  const refetchInterval = state.live ? LIVE_INTERVAL_MS : false;

  const logs = useQuery({
    queryKey: ["loki-logs", logql, rangeKey, state.limit],
    queryFn: () => lokiQueryLogs(logql, range(), state.limit),
    refetchInterval,
  });

  const resolvedForStep = resolveRange(state.range, Date.now());
  const step = stepForRange(resolvedForStep);
  const histogramQL = compileHistogramQL(model, step);
  const histogram = useQuery({
    queryKey: ["loki-histogram", histogramQL, rangeKey],
    queryFn: () => lokiQueryHistogram(histogramQL!, range(), step),
    enabled: histogramQL !== null,
    refetchInterval,
  });

  const labels = useQuery({
    queryKey: ["loki-labels", rangeKey],
    queryFn: () => lokiLabels(range()),
    staleTime: 60_000,
  });

  const addFilter = (f: LabelFilter) =>
    update({ filters: upsertFilter(state.filters, f), raw: "" });

  const openTrace = (traceId: string) =>
    update({ signal: "traces", trace: traceId });

  return (
    <div className="logsview">
      <div className="querybar">
        {state.raw === "" && !editingRaw && (
          <>
            <FilterChips
              filters={state.filters}
              labels={labels.data ?? []}
              onChange={(filters) => update({ filters })}
            />
            <form
              className="search-form"
              onSubmit={(e) => {
                e.preventDefault();
                update({ search: searchDraft });
              }}
            >
              <input
                type="search"
                className="search-input"
                placeholder="Search in log lines…"
                aria-label="Search in log lines"
                value={searchDraft}
                onChange={(e) => setSearchDraft(e.target.value)}
              />
            </form>
          </>
        )}
        {(state.raw !== "" || editingRaw) && (
          <form
            className="raw-form"
            onSubmit={(e) => {
              e.preventDefault();
              update({ raw: rawDraft });
              setEditingRaw(false);
            }}
          >
            <textarea
              aria-label="LogQL query"
              value={editingRaw ? rawDraft : state.raw}
              rows={2}
              onChange={(e) => {
                setEditingRaw(true);
                setRawDraft(e.target.value);
              }}
            />
            <div className="raw-actions">
              <button type="submit">Run</button>
              <button
                type="button"
                onClick={() => {
                  update({ raw: "" });
                  setEditingRaw(false);
                }}
              >
                Back to builder
              </button>
            </div>
          </form>
        )}
        {state.raw === "" && !editingRaw && (
          <button
            className="qlmode"
            title="Edit the compiled LogQL directly"
            onClick={() => {
              setRawDraft(logql);
              setEditingRaw(true);
            }}
          >
            {"{ } edit as text"}
          </button>
        )}
      </div>

      <div className="logs-body">
        <FieldSidebar
          labels={labels.data ?? []}
          range={resolvedForStep}
          rangeKey={rangeKey}
          onAddFilter={addFilter}
        />
        <div className="logs-main">
          {histogramQL !== null && (
            <div className="histo-wrap">
              <div className="histo-head">
                <span className="histo-total">
                  {logs.data ? `${logs.data.length} rows` : "…"}
                  {logs.data && logs.data.length === state.limit
                    ? ` (limit ${state.limit})`
                    : ""}
                </span>
                {logs.isFetching && (
                  <span className="histo-note">updating…</span>
                )}
              </div>
              {histogram.data && (
                <Histogram
                  series={histogram.data}
                  rangeMs={resolvedForStep}
                  stepMs={(durationToSeconds(step) ?? 60) * 1000}
                />
              )}
            </div>
          )}
          {logs.isError && (
            <div className="query-error" role="alert">
              Query failed: {(logs.error as Error).message}
            </div>
          )}
          {logs.isPending && !logs.isError && (
            <div className="loglist-empty">Loading…</div>
          )}
          {logs.data && (
            <LogList
              rows={logs.data}
              onAddFilter={addFilter}
              onOpenTrace={openTrace}
            />
          )}
        </div>
      </div>
    </div>
  );
}
