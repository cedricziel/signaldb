// Native Query IR builder + envelope renderers.
//
// The builder appends structured IR stages (source → where → aggregate) and
// emits a versioned IR document via the *generated* client (api/queryIr →
// api/gen), with no dialect-string compilation in the browser. The result view
// is chosen from the declared envelope (`rows`→list, `series`→chart,
// `table`→topN) before results arrive.
import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { runIrQuery } from "../../api/queryIr";
import type { QueryIrRequest, QueryIrResponse } from "../../api/gen";
import { FilterChips } from "../logs/FilterChips";
import type { LabelFilter } from "../../lib/filters";
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
 * relation matches (`series` needs a step aggregate; `table` a grouped one). */
function aggregateFor(
  result: IrResult,
  source: IrSource,
): IrAggregate | undefined {
  const groupField = source === "logs" ? "service_name" : "service.name";
  if (result === "series") {
    return { by: [groupField], aggs: [{ fn: "count", as: "n" }], step: "1m" };
  }
  if (result === "table") {
    return { by: [groupField], aggs: [{ fn: "count", as: "n" }] };
  }
  return undefined;
}

export function QueryView() {
  const [source, setSource] = useState<IrSource>("logs");
  const [result, setResult] = useState<IrResult>("rows");
  const [filters, setFilters] = useState<LabelFilter[]>([]);
  const [submitted, setSubmitted] = useState<QueryIrRequest | null>(null);

  const document = useMemo<QueryIrRequest>(
    () =>
      buildIrDocument({
        source,
        result,
        range: { from: "now-1h", to: "now" },
        filters: toIrFilters(filters),
        aggregate: aggregateFor(result, source),
      }),
    [source, result, filters],
  );

  const query = useQuery({
    queryKey: ["ir-query", JSON.stringify(submitted)],
    queryFn: () => runIrQuery(submitted as QueryIrRequest),
    enabled: submitted !== null,
  });

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
        <button type="button" onClick={() => setSubmitted(document)}>
          Run
        </button>
      </div>

      <div className="query-ir-result" data-testid={`ir-view-${view}`}>
        {query.isError && <div role="alert">Query failed</div>}
        {query.isLoading && submitted && <div>Running…</div>}
        {query.data && <EnvelopeResult view={view} data={query.data} />}
      </div>
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

function RowsTable({ data, topN }: { data: QueryIrResponse; topN: boolean }) {
  const columns = data.columns ?? [];
  const rows = data.rows ?? [];
  return (
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
              <td key={j}>{formatCell(cell)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function SeriesChart({ data }: { data: QueryIrResponse }) {
  const series = data.series ?? [];
  return (
    <div className="ir-series">
      {series.length === 0 && <div>No series</div>}
      {series.map((s, i) => (
        <div key={i} className="ir-series-row">
          <span className="ir-series-label">{labelString(s.labels)}</span>
          <span className="ir-series-points">{s.points.length} points</span>
        </div>
      ))}
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
