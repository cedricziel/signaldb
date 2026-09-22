import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  buildFormulaIrDoc,
  buildMetricIrDoc,
  irSeriesToPromSeries,
  seriesName,
} from "../../api/ir/metrics";
import { runIrQuery } from "../../api/queryIr";
import { AttributeValue } from "../../components/AttributeValue";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { liveRefetchInterval } from "../../lib/live";
import {
  durationToSeconds,
  rangeScopeKey,
  resolveRange,
  stepForRange,
} from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { seriesColorVar } from "../../lib/promSeries";
import {
  emptyQuery,
  nextRef,
  parseBuilderState,
  type MetricQuery,
} from "./metricQuery";
import { MetricsChart } from "./MetricsChart";
import { QueryRow } from "./QueryRow";
import "./metrics.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function MetricsView({ state, update }: Props) {
  // Seeded once at mount from `?mq=` (see lib/urlState.ts's `metricQuery`).
  const initial = parseBuilderState(state.metricQuery);
  const [queries, setQueries] = useState<MetricQuery[]>(
    () => initial?.queries ?? [emptyQuery("a")],
  );
  const [formula, setFormula] = useState(initial?.formula ?? "");
  // The builder state (queries + formula) last run via "Run" — null before
  // the first run. Distinct from `queries`/`formula` above, which track
  // in-progress edits the user hasn't run yet.
  const [ran, setRan] = useState<{
    queries: MetricQuery[];
    formula: string;
  } | null>(initial);
  // The `metricQuery` value this component itself last wrote via `update()`
  // (seeded from the mount-time URL, which counts as already applied).
  // Distinguishes an external change — a browser Back/Forward landing on a
  // different `?mq=` — from this component observing its own write echoed
  // back through `state`; only the former should resync the builder, or
  // every Run would immediately stomp on the query state it just set.
  const lastWritten = useRef(state.metricQuery);

  useEffect(() => {
    if (state.metricQuery === lastWritten.current) return;
    lastWritten.current = state.metricQuery;
    const reseeded = parseBuilderState(state.metricQuery);
    setQueries(reseeded?.queries ?? [emptyQuery("a")]);
    setFormula(reseeded?.formula ?? "");
    setRan(reseeded);
  }, [state.metricQuery]);

  const rangeKey = rangeScopeKey(state);
  // Freeze the resolved window per range selection so metadata pickers don't
  // refetch on every render (relative ranges resolve to a shifting "now").
  const metaRange = useMemo(
    () => resolveRange(state.range, Date.now()),
    [rangeKey],
  );

  const setQuery = (i: number, next: MetricQuery) =>
    setQueries((qs) => qs.map((old, j) => (j === i ? next : old)));
  const addQuery = () => setQueries((qs) => [...qs, emptyQuery(nextRef(qs))]);
  const removeQuery = (i: number) =>
    setQueries((qs) => (qs.length > 1 ? qs.filter((_, j) => j !== i) : qs));

  const chart = useQuery({
    queryKey: ["metrics-chart", rangeKey, ran ? JSON.stringify(ran) : null],
    queryFn: async () => {
      if (!ran) return [];
      const range = resolveRange(state.range, Date.now());
      const step = durationToSeconds(stepForRange(range, 120)) ?? 60;
      const doc =
        ran.formula.trim() !== "" || ran.queries.length > 1
          ? buildFormulaIrDoc(ran.queries, ran.formula, range, step)
          : buildMetricIrDoc(ran.queries[0]!, range, step);
      if (doc === null) return [];
      return irSeriesToPromSeries((await runIrQuery(doc)).series ?? []);
    },
    enabled: ran !== null,
    refetchInterval: liveRefetchInterval(state.live),
  });

  // Runnable when every query that would be sent has a metric selected: the
  // solo case needs just the first query; a formula (or several queries)
  // needs all of them, since the formula can reference any ref letter.
  const runnable =
    formula.trim() !== "" || queries.length > 1
      ? queries.every((q) => q.metric.trim() !== "")
      : (queries[0]?.metric.trim() ?? "") !== "";

  const run = () => {
    const metricQuery = JSON.stringify({ queries, formula });
    lastWritten.current = metricQuery;
    update({ metricQuery });
    setRan({ queries, formula });
  };

  return (
    <div className="metricsview">
      <div className="metrics-builder">
        {queries.map((q, i) => (
          <div className="builder-query" key={q.ref}>
            <QueryRow
              query={q}
              range={metaRange}
              onChange={(next) => setQuery(i, next)}
            />
            {queries.length > 1 && (
              <button
                type="button"
                className="query-remove"
                aria-label={`Remove query ${q.ref}`}
                onClick={() => removeQuery(i)}
              >
                ✕
              </button>
            )}
          </div>
        ))}

        <div className="builder-formula">
          <button type="button" className="qrow-add" onClick={addQuery}>
            + query
          </button>
          <span className="formula-ref" aria-hidden>
            ƒ
          </span>
          <input
            className="formula-input"
            aria-label="Formula"
            placeholder="formula, e.g. (a / b) * 100 — optional"
            value={formula}
            onChange={(e) => setFormula(e.target.value)}
          />
        </div>

        <div className="builder-run">
          <button
            type="button"
            className="btn btn-primary"
            disabled={!runnable}
            onClick={run}
          >
            Run
          </button>
        </div>
      </div>

      {ran === null && (
        <div className="view-note">
          Pick a metric above, then Run to chart it.
        </div>
      )}
      {chart.isError && <QueryError what="metrics" error={chart.error} />}
      {chart.isFetching && !chart.data && (
        <div className="view-note">Loading…</div>
      )}
      {chart.data && chart.data.length === 0 && ran !== null && (
        <EmptyState title="No series in this range" />
      )}
      {chart.data && chart.data.length > 0 && (
        <>
          <div className="mchart-wrap">
            <MetricsChart series={chart.data} />
          </div>
          <ul className="mlegend" aria-label="Series">
            {chart.data.map((s, i) => {
              const name = seriesName(s.labels);
              return (
                <li key={name}>
                  <i style={{ background: seriesColorVar(i) }} />
                  <AttributeValue value={name} label={`series ${name}`} />
                </li>
              );
            })}
          </ul>
        </>
      )}
    </div>
  );
}
