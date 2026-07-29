import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { promQueryRange, seriesName } from "../../api/prom";
import {
  durationToSeconds,
  rangeToParam,
  resolveRange,
  stepForRange,
} from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { seriesColorVar } from "../../lib/promSeries";
import { MetricsChart } from "./MetricsChart";
import "./metrics.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function MetricsView({ state, update }: Props) {
  const [draft, setDraft] = useState(state.promql);
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const promql = state.promql;

  const query = useQuery({
    queryKey: ["prom-range", promql, rangeKey],
    queryFn: () => {
      const range = resolveRange(state.range, Date.now());
      const step = durationToSeconds(stepForRange(range, 120)) ?? 60;
      return promQueryRange(promql, range, step);
    },
    enabled: promql.trim() !== "",
    refetchInterval: state.live ? 15_000 : false,
  });

  return (
    <div className="metricsview">
      <form
        className="promql-form"
        onSubmit={(e) => {
          e.preventDefault();
          update({ promql: draft.trim() });
        }}
      >
        <input
          aria-label="PromQL query"
          className="promql-input"
          placeholder="PromQL, e.g. rate(http_server_duration_count[5m])"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Run</button>
      </form>

      {promql === "" && (
        <div className="metrics-note">
          Enter a PromQL expression to chart metrics.
        </div>
      )}
      {query.isError && (
        <div className="query-error" role="alert">
          Query failed: {(query.error as Error).message}
        </div>
      )}
      {query.isFetching && !query.data && (
        <div className="metrics-note">Loading…</div>
      )}
      {query.data && query.data.length === 0 && (
        <div className="metrics-note">No series returned.</div>
      )}
      {query.data && query.data.length > 0 && (
        <>
          <div className="mchart-wrap">
            <MetricsChart series={query.data} />
          </div>
          <ul className="mlegend" aria-label="Series">
            {query.data.map((s, i) => (
              <li key={seriesName(s.labels)}>
                <i style={{ background: seriesColorVar(i) }} />
                {seriesName(s.labels)}
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}
