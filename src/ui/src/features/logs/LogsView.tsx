import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { lokiLabels, lokiQueryHistogram, lokiQueryLogs } from "../../api/loki";
import {
  MobileFiltersToggle,
  MobileSidebarDrawer,
} from "../../components/MobileSidebarDrawer";
import { QueryError } from "../../components/QueryError";
import { useMobileSidebar } from "../../hooks/useMobileSidebar";
import {
  compileHistogramQL,
  compileLogQL,
  upsertFilter,
  type LabelFilter,
} from "../../lib/filters";
import { liveRefetchInterval } from "../../lib/live";
import {
  durationToSeconds,
  rangeScopeKey,
  resolveRange,
  resolveStep,
  stepOptionsForRange,
} from "../../lib/time";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { FieldSidebar } from "./FieldSidebar";
import { FilterChips } from "./FilterChips";
import { Histogram } from "./Histogram";
import { LogList } from "./LogList";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

export function LogsView({ state, update }: Props) {
  const [editingRaw, setEditingRaw] = useState(false);
  const [rawDraft, setRawDraft] = useState("");
  const [searchDraft, setSearchDraft] = useState(state.search);
  const mobileSidebar = useMobileSidebar();

  // Re-clicking the Logs tab (crossSignalSearch drops `q`) or Back/Forward
  // change `state.search` without this component remounting; the draft must
  // follow rather than keep showing stale text over now-unfiltered rows.
  useEffect(() => {
    setSearchDraft(state.search);
  }, [state.search]);

  const model = {
    filters: state.filters,
    search: state.search,
    raw: state.raw || undefined,
  };
  const logql = compileLogQL(model);
  // Cache scope: time range plus tenant context, so switching tenants
  // refetches instead of serving another tenant's cached results.
  const rangeKey = rangeScopeKey(state);
  // Relative ranges resolve "now" per fetch so live mode slides forward.
  const range = () => resolveRange(state.range, Date.now());
  // Tighter than every other signal's default 15s poll — a live log tail
  // reads as broken if a burst of lines takes noticeably longer than a
  // glance to show up.
  const refetchInterval = liveRefetchInterval(state.live, 2_000);

  const logs = useQuery({
    queryKey: ["loki-logs", logql, rangeKey, state.limit],
    queryFn: () => lokiQueryLogs(logql, range(), state.limit),
    refetchInterval,
  });

  const resolvedForStep = resolveRange(state.range, Date.now());
  const step = resolveStep(resolvedForStep, state.step);
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
    update({ signal: "traces", trace: traceId }, { push: true });

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
                onChange={(e) => {
                  const next = e.target.value;
                  setSearchDraft(next);
                  // The native "×" clears the box without firing submit;
                  // an empty draft over a non-empty query would otherwise
                  // leave stale rows filtered by a query the box no longer
                  // shows.
                  if (next === "" && state.search !== "") {
                    update({ search: "" });
                  }
                }}
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
              <button type="submit" className="btn btn-primary">
                Run
              </button>
              <button
                type="button"
                className="btn"
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

      <MobileFiltersToggle
        open={mobileSidebar.open}
        onToggle={mobileSidebar.toggle}
      />

      <div className="logs-body">
        <MobileSidebarDrawer
          open={mobileSidebar.open}
          onClose={mobileSidebar.close}
        >
          <FieldSidebar
            labels={labels.data ?? []}
            range={resolvedForStep}
            rangeKey={rangeKey}
            onAddFilter={addFilter}
          />
        </MobileSidebarDrawer>
        <div className="logs-main">
          {/* The row count describes the list below, not the chart: the
              histogram is a separate unlimited aggregate, so keeping the two
              in one box made a flat chart read as "truncated by the limit". */}
          <div className="logs-rowcount">
            <span className="logs-rowcount-n">
              {logs.data ? `${logs.data.length} rows` : "…"}
              {logs.data && logs.data.length === state.limit
                ? ` (at the ${state.limit}-row limit)`
                : ""}
            </span>
            {logs.isFetching && <span className="histo-note">updating…</span>}
          </div>
          {histogramQL !== null && histogram.data && (
            <div className="histo-wrap">
              <Histogram
                series={histogram.data}
                rangeMs={resolvedForStep}
                stepMs={(durationToSeconds(step) ?? 60) * 1000}
                scale={state.scale}
                onScaleChange={(scale) => update({ scale })}
                step={state.step}
                stepOptions={stepOptionsForRange(resolvedForStep)}
                onStepChange={(step) => update({ step })}
              />
            </div>
          )}
          {logs.isError && <QueryError what="logs" error={logs.error} />}
          {logs.isPending && !logs.isError && (
            <div className="loglist-empty">Loading…</div>
          )}
          {logs.data && (
            <LogList
              rows={logs.data}
              onAddFilter={addFilter}
              onOpenTrace={openTrace}
              update={update}
            />
          )}
        </div>
      </div>
    </div>
  );
}
