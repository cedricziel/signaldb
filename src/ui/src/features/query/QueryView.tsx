// Native Query IR builder + envelope renderers.
//
// The builder appends structured IR stages (source → where → aggregate) and
// emits a versioned IR document via the *generated* client (api/queryIr →
// api/gen), with no dialect-string compilation in the browser. The result view
// is chosen from the declared envelope (`rows`→list, `series`→chart,
// `table`→topN) before results arrive.
import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { runIrQuery } from "../../api/queryIr";
import { irSeriesToPromSeries } from "../../api/metricsIr";
import { AttributeValue } from "../../components/AttributeValue";
import { QueryError } from "../../components/QueryError";
import type { QueryIrRequest, QueryIrResponse } from "../../api/gen";
import type { PromSeries } from "../../api/prom";
import { seriesColorVar } from "../../lib/promSeries";
import { MetricsChart } from "../metrics/MetricsChart";
import "../metrics/metrics.css";
import "./query.css";
import { FilterChips } from "../logs/FilterChips";
import type { LabelFilter } from "../../lib/filters";
import {
  msToNanos,
  nanosToMs,
  resolveRange,
  stepForRange,
  type TimeRange,
} from "../../lib/time";
import { formatTimestamp } from "../../lib/vizFormat";
import type { ExploreState } from "../../lib/urlState";
import {
  buildIrDocument,
  type IrAggregate,
  type IrFilter,
  type IrResult,
  type IrSource,
} from "./buildIr";
import { viewForResult } from "./envelope";

/** Map a LogQL-style filter op (from FilterChips) to an IR predicate op. */
function mapOp(op: string): { op: string; negate?: boolean } {
  switch (op) {
    case "=":
      return { op: "eq" };
    case "!=":
      return { op: "ne" };
    case "=~":
      return { op: "regex" };
    case "!~":
      return { op: "regex", negate: true };
    default:
      return { op: "eq" };
  }
}

function toIrFilters(filters: LabelFilter[]): IrFilter[] {
  return filters.map((f) => {
    const m = mapOp(f.op);
    return { field: f.label, op: m.op, value: f.value, negate: m.negate };
  });
}

/** The aggregate implied by a declared envelope, so the emitted terminal
 * relation matches (`series` needs a step aggregate; `table` a grouped one).
 * Both sources share the `service.name` logical field. `step` is sized to
 * the selected range (see `stepForRange`) rather than a fixed bucket width,
 * so a wide range doesn't ask for thousands of one-point buckets. */
function aggregateFor(result: IrResult, step: string): IrAggregate | undefined {
  const groupField = "service.name";
  if (result === "series") {
    return { by: [groupField], aggs: [{ fn: "count", as: "n" }], step };
  }
  if (result === "table") {
    return { by: [groupField], aggs: [{ fn: "count", as: "n" }] };
  }
  return undefined;
}

/** Map the shared explore time range to IR range anchors: a relative range
 * becomes a `now-Ns` anchor (resolved once, server-side), an absolute range
 * becomes nanosecond strings. */
function irRange(range: TimeRange): { from: string; to: string } {
  if (range.type === "absolute") {
    return { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) };
  }
  return { from: `now-${range.seconds}s`, to: "now" };
}

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function QueryView({ state, update }: Props) {
  const source = state.querySource;
  const result = state.queryResult;
  const filters = state.queryFilters;
  const run = state.queryRun;

  const setSource = (v: IrSource) => update({ querySource: v });
  const setResult = (v: IrResult) => update({ queryResult: v });
  const setFilters = (fs: LabelFilter[]) => update({ queryFilters: fs });

  const resolved = useMemo(
    () => resolveRange(state.range, Date.now()),
    [state.range],
  );
  const step = stepForRange(resolved);

  const document = useMemo<QueryIrRequest>(
    () =>
      buildIrDocument({
        source,
        result,
        range: irRange(state.range),
        filters: toIrFilters(filters),
        aggregate: aggregateFor(result, step),
      }),
    [source, result, filters, state.range, step],
  );

  const query = useQuery({
    queryKey: ["ir-query", JSON.stringify(document), run],
    queryFn: () => runIrQuery(document),
    enabled: run,
  });

  // A relative range must slide forward on a second Run even though the
  // document (and therefore the query key) is unchanged — the anchors
  // resolve server-side, so only a fresh request picks up a later "now".
  const runQuery = () => {
    if (run) {
      void query.refetch();
    } else {
      update({ queryRun: true });
    }
  };

  // The view is a function of the *declared* envelope, available before results.
  const view = viewForResult(result);

  return (
    <div className="query-ir">
      <div className="query-ir-builder">
        <label>
          Source
          <select
            aria-label="source"
            value={source}
            onChange={(e) => setSource(e.target.value as IrSource)}
          >
            <option value="logs">logs</option>
            <option value="traces">traces</option>
            <option value="profiles">profile summaries</option>
          </select>
        </label>
        <label>
          Result
          <select
            aria-label="result"
            value={result}
            onChange={(e) => setResult(e.target.value as IrResult)}
          >
            <option value="rows">rows</option>
            <option value="series">series</option>
            <option value="table">table</option>
          </select>
        </label>
        <FilterChips filters={filters} labels={[]} onChange={setFilters} />
        <button type="button" onClick={runQuery}>
          Run
        </button>
      </div>

      <div className="query-ir-result" data-testid={`ir-view-${view}`}>
        {query.isError && <QueryError what="results" error={query.error} />}
        {query.isLoading && run && <div className="view-note">Loading…</div>}
        {query.data && <QueryWarnings data={query.data} />}
        {query.data && <EnvelopeResult view={view} data={query.data} />}
      </div>
    </div>
  );
}

/**
 * Non-fatal diagnostics the server attached to a result — today, a group-by
 * field nothing in the window carries, which would otherwise render as a
 * single convincing series labelled `null`.
 */
function QueryWarnings({ data }: { data: QueryIrResponse }) {
  const warnings = data.warnings ?? [];
  if (warnings.length === 0) return null;
  return (
    <div className="query-ir-warnings" role="status">
      {warnings.map((w, i) => (
        <p key={i}>
          {w.message}
          {w.suggestions && w.suggestions.length > 0
            ? ` Did you mean ${w.suggestions.join(", ")}?`
            : ""}
        </p>
      ))}
    </div>
  );
}

function EnvelopeResult({
  view,
  data,
}: {
  view: "list" | "chart" | "table";
  data: QueryIrResponse;
}) {
  if (view === "chart") {
    return <SeriesChart data={data} />;
  }
  // `list` (rows / log-span list) and `table` (topN) share a tabular renderer.
  return <RowsTable data={data} topN={view === "table"} />;
}

/** Column names that carry an absolute point in time in their entirety —
 * matched exactly, not by suffix, so a name like `runtime` (which merely
 * ends in the letters "time") never qualifies. */
const EXACT_TIME_COLUMNS = new Set([
  "timestamp",
  "time",
  "start_time_unix_nano",
  "observed_timestamp",
  "end_time_unix_nano",
]);

/** Column-name suffixes that name a timestamp regardless of the field they're
 * attached to (`span_start_time_unix_nano`, `log.timestamp`, …). */
const TIME_COLUMN_SUFFIXES = ["_time_unix_nano", "_timestamp", ".timestamp"];

/** Whether a column's own name declares it a timestamp. A value merely
 * *shaped* like an epoch-nanosecond integer (19 digits) is deliberately not
 * enough on its own — an id column can be exactly that shape by coincidence,
 * and formatting it would make it uncopyable in its real form. */
function isTimeColumnName(column: string): boolean {
  return (
    EXACT_TIME_COLUMNS.has(column) ||
    TIME_COLUMN_SUFFIXES.some((suffix) => column.endsWith(suffix))
  );
}

/** A cell that is nothing but digits — the shape the timestamp check below
 * requires before it re-parses the value as nanoseconds. */
const NUMERIC_RE = /^\d+$/;

/** Above this length a copy affordance earns its keep; below it, the value
 * is already easy to select and retyping "Copy" in every short cell (an id,
 * a count, a short string) is more chrome than help. */
const COPY_THRESHOLD = 40;

/** One rows/topN table cell: a column the server's own result metadata
 * declares as `timestamp_ns`, or whose name unambiguously names a timestamp,
 * renders as an absolute date/time; a long string gets a copy button;
 * anything else — including a value that merely happens to be
 * epoch-nanosecond-shaped, e.g. a 19-digit id — renders as plain text. */
function RowsCell({
  column,
  columnType,
  cell,
}: {
  column: string;
  columnType: string | undefined;
  cell: unknown;
}) {
  const value = formatCell(cell);
  const isTimestamp =
    columnType === "timestamp_ns" || isTimeColumnName(column);
  if (isTimestamp && NUMERIC_RE.test(value)) {
    return <span>{formatTimestamp(nanosToMs(value), 0)}</span>;
  }
  if (value.length > COPY_THRESHOLD) {
    return <AttributeValue value={value} label={`cell ${value}`} />;
  }
  return <span>{value}</span>;
}

function RowsTable({ data, topN }: { data: QueryIrResponse; topN: boolean }) {
  const columns = data.columns ?? [];
  const rows = data.rows ?? [];
  if (rows.length === 0) {
    return <div className="view-note">No rows in this window.</div>;
  }
  return (
    <div className="table-scroll">
      <table className={topN ? "ir-topn" : "ir-rows"}>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.name}>{c.name}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td key={j}>
                  <RowsCell
                    column={columns[j]?.name ?? ""}
                    columnType={columns[j]?.type}
                    cell={cell}
                  />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const irSeriesLabel = (s: PromSeries) => labelString(s.labels);

/**
 * A `series` envelope is a time-series chart: adapt it to `PromSeries` and
 * reuse the metrics chart, so it carries the same cursor tooltip.
 */
function SeriesChart({ data }: { data: QueryIrResponse }) {
  const series = useMemo(() => irSeriesToPromSeries(data.series ?? []), [data]);
  return (
    <div className="ir-series">
      {series.length === 0 && <div className="view-note">No series</div>}
      {series.length > 0 && (
        <div className="mchart-wrap">
          <MetricsChart series={series} labelOf={irSeriesLabel} />
        </div>
      )}
      <ul className="mlegend" aria-label="Series">
        {series.map((s, i) => {
          const labels = labelString(s.labels);
          return (
            <li key={i} className="ir-series-row">
              <i style={{ background: seriesColorVar(i) }} />
              <span className="ir-series-label">
                <AttributeValue value={labels} label={`series ${labels}`} />
              </span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function labelString(labels: Record<string, string>): string {
  const parts = Object.entries(labels).map(([k, v]) => `${k}=${v}`);
  return parts.length > 0 ? parts.join(", ") : "(all)";
}

function formatCell(cell: unknown): string {
  if (cell === null || cell === undefined) return "";
  if (typeof cell === "object") return JSON.stringify(cell);
  return String(cell);
}
