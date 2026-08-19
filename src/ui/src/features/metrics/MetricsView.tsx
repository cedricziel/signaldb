import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { promQueryRange, seriesName } from "../../api/prom";
import { buildMetricIrDoc, irSeriesToPromSeries } from "../../api/metricsIr";
import { runIrQuery } from "../../api/queryIr";
import { AttributeValue } from "../../components/AttributeValue";
import {
  durationToSeconds,
  rangeScopeKey,
  resolveRange,
  stepForRange,
} from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { seriesColorVar } from "../../lib/promSeries";
import {
  buildFormula,
  emptyQuery,
  nextRef,
  type MetricQuery,
} from "./buildPromQL";
import { MetricsChart } from "./MetricsChart";
import { QueryRow } from "./QueryRow";
import "./metrics.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

type Mode = "builder" | "promql";

export function MetricsView({ state, update }: Props) {
  const [mode, setMode] = useState<Mode>("builder");
  const [queries, setQueries] = useState<MetricQuery[]>(() => [
    emptyQuery("a"),
  ]);
  const [formula, setFormula] = useState("");
  const [draft, setDraft] = useState(state.promql);
  // The builder query snapshotted at the moment "Run" was clicked, when it
  // was within the minimal metrics IR source's coverage (see
  // api/metricsIr.ts) — null for a PromQL-tab run, or a builder run using a
  // range function or multi-query formula, either of which stays on the
  // PromQL path below. A fresh mount (including from a shared `?promql=`
  // link) starts null, so a bookmarked raw PromQL query is never silently
  // "upgraded" to IR — only an interactive builder Run is.
  const [ranQuery, setRanQuery] = useState<MetricQuery | null>(null);

  const rangeKey = rangeScopeKey(state);
  // Freeze the resolved window per range selection so metadata pickers don't
  // refetch on every render (relative ranges resolve to a shifting "now").
  // Keyed on rangeKey rather than state.range so the window only moves when
  // the user changes the range or tenant/dataset.
  const metaRange = useMemo(
    () => resolveRange(state.range, Date.now()),
    [rangeKey],
  );

  const compiled = buildFormula(queries, formula);
  const promql = state.promql;

  const setQuery = (i: number, next: MetricQuery) =>
    setQueries((qs) => qs.map((old, j) => (j === i ? next : old)));
  const addQuery = () => setQueries((qs) => [...qs, emptyQuery(nextRef(qs))]);
  const removeQuery = (i: number) =>
    setQueries((qs) => (qs.length > 1 ? qs.filter((_, j) => j !== i) : qs));

  const chart = useQuery({
    queryKey: [
      "metrics-chart",
      rangeKey,
      ranQuery ? JSON.stringify(ranQuery) : promql,
    ],
    queryFn: async () => {
      const range = resolveRange(state.range, Date.now());
      const step = durationToSeconds(stepForRange(range, 120)) ?? 60;
      const irDoc = ranQuery ? buildMetricIrDoc(ranQuery, range, step) : null;
      if (irDoc) return irSeriesToPromSeries((await runIrQuery(irDoc)).series ?? []);
      return promQueryRange(promql, range, step);
    },
    enabled: ranQuery !== null || promql.trim() !== "",
    refetchInterval: state.live ? 15_000 : false,
  });

  // Builder → Query IR when the current builder state is within its
  // coverage (see api/metricsIr.ts); PromQL escape hatch → always PromQL,
  // unchanged.
  const runBuilder = () => {
    update({ promql: compiled.trim() });
    const solo = formula.trim() === "" && queries.length === 1;
    setRanQuery(solo ? queries[0]! : null);
  };
  const runPromQL = (q: string) => {
    update({ promql: q.trim() });
    setRanQuery(null);
  };

  const switchMode = (next: Mode) => {
    // Builder → PromQL seeds the text box with the compiled query so the raw
    // editor is the escape hatch. There is no PromQL → builder parse yet.
    if (next === "promql" && mode === "builder" && compiled !== "") {
      setDraft(compiled);
    }
    setMode(next);
  };

  return (
    <div className="metricsview">
      <div className="metrics-modes" role="tablist" aria-label="Query editor">
        <button
          type="button"
          role="tab"
          aria-selected={mode === "builder"}
          className={mode === "builder" ? "active" : ""}
          onClick={() => switchMode("builder")}
        >
          Builder
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === "promql"}
          className={mode === "promql" ? "active" : ""}
          onClick={() => switchMode("promql")}
        >
          PromQL
        </button>
      </div>

      {mode === "builder" ? (
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
            <code className="builder-preview">
              {compiled || "pick a metric to start"}
            </code>
            <button
              type="button"
              disabled={compiled === ""}
              onClick={runBuilder}
            >
              Run
            </button>
          </div>
        </div>
      ) : (
        <form
          className="promql-form"
          onSubmit={(e) => {
            e.preventDefault();
            runPromQL(draft);
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
      )}

      {promql === "" && (
        <div className="metrics-note">
          Build a query above, or switch to PromQL, then Run to chart metrics.
        </div>
      )}
      {chart.isError && (
        <div className="query-error" role="alert">
          Query failed: {(chart.error as Error).message}
        </div>
      )}
      {chart.isFetching && !chart.data && (
        <div className="metrics-note">Loading…</div>
      )}
      {chart.data && chart.data.length === 0 && (
        <div className="metrics-note">No series returned.</div>
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
