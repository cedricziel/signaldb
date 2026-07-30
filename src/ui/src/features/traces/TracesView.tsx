import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import {
  tempoGetTrace,
  tempoSearch,
  type TempoSpan,
  type TraceSummary,
} from "../../api/tempo";
import {
  formatTimestamp,
  nanosToMs,
  rangeToParam,
  resolveRange,
} from "../../lib/time";
import {
  formatRate,
  groupDimensions,
  groupKey,
  groupLabel,
  groupTraces,
  parseGroupBy,
  type TraceGroup,
} from "../../lib/traceGroups";
import type { ExploreState } from "../../lib/urlState";
import { buildWaterfall, formatDurationMs } from "../../lib/waterfall";
import "./traces.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

function plural(n: number, noun: string): string {
  return `${n} ${noun}${n === 1 ? "" : "s"}`;
}

export function TracesView({ state, update }: Props) {
  if (state.trace !== "") {
    return <TraceDetail state={state} update={update} />;
  }
  return <TraceSearch state={state} update={update} />;
}

function TraceSearch({ state, update }: Props) {
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const search = useQuery({
    queryKey: ["tempo-search", rangeKey, state.limit],
    queryFn: () =>
      tempoSearch(resolveRange(state.range, Date.now()), state.limit),
  });

  return (
    <div className="traces-search">
      <div className="traces-toolbar">
        <form
          className="trace-id-form"
          onSubmit={(e) => {
            e.preventDefault();
            const id = new FormData(e.currentTarget).get("traceId");
            if (typeof id === "string" && id.trim() !== "") {
              update({ trace: id.trim() });
            }
          }}
        >
          <input
            name="traceId"
            aria-label="Trace ID"
            placeholder="Open trace by ID…"
          />
          <button type="submit">Open</button>
        </form>
        {state.group === "" && (
          <DimensionPickers
            traces={search.data ?? []}
            groupBy={state.groupBy}
            update={update}
          />
        )}
      </div>

      {search.isError && (
        <div className="query-error" role="alert">
          Search failed: {(search.error as Error).message}
        </div>
      )}
      {search.isPending && <div className="traces-note">Loading…</div>}
      {search.data &&
        (state.group === "" ? (
          <GroupList
            traces={search.data}
            dims={parseGroupBy(state.groupBy)}
            rangeSeconds={rangeSeconds(state)}
            update={update}
          />
        ) : (
          <GroupDetail state={state} traces={search.data} update={update} />
        ))}
    </div>
  );
}

function rangeSeconds(state: ExploreState): number {
  const r = resolveRange(state.range, Date.now());
  return Math.max(1, (r.toMs - r.fromMs) / 1000);
}

function DimensionPickers({
  traces,
  groupBy,
  update,
}: {
  traces: TraceSummary[];
  groupBy: string;
  update: (patch: Partial<ExploreState>) => void;
}) {
  const dims = parseGroupBy(groupBy);
  const primary = dims[0] ?? "";
  const secondary = dims[1] ?? "";
  const setDims = (next: string[]) =>
    update({ groupBy: next.filter((d) => d !== "").join(","), group: "" });
  return (
    <div className="group-pickers">
      <label className="group-by">
        Group by
        <select
          aria-label="Group by"
          value={primary}
          onChange={(e) => {
            const v = e.currentTarget.value;
            setDims([v, secondary === v ? "" : secondary]);
          }}
        >
          {dimensionOptions(traces, primary).map((d) => (
            <option key={d} value={d}>
              {d}
            </option>
          ))}
        </select>
      </label>
      <label className="group-by">
        Then by
        <select
          aria-label="Then by"
          value={secondary}
          onChange={(e) => setDims([primary, e.currentTarget.value])}
        >
          <option value="">—</option>
          {dimensionOptions(traces, secondary)
            .filter((d) => d !== primary && d !== "")
            .map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
        </select>
      </label>
    </div>
  );
}

/** The picked dimension stays selectable even if absent from this range. */
function dimensionOptions(traces: TraceSummary[], picked: string): string[] {
  const dims = groupDimensions(traces);
  return picked === "" || dims.includes(picked)
    ? dims
    : [...dims, picked].sort();
}

function GroupList({
  traces,
  dims,
  rangeSeconds,
  update,
}: {
  traces: TraceSummary[];
  dims: string[];
  rangeSeconds: number;
  update: (patch: Partial<ExploreState>) => void;
}) {
  if (traces.length === 0) {
    return <div className="traces-note">No traces in this time range.</div>;
  }
  const groups = groupTraces(traces, dims);
  const showServices = !dims.includes("service.name");
  return (
    <table className="trace-table">
      <thead>
        <tr>
          {dims.map((d) => (
            <th key={d}>{d}</th>
          ))}
          {showServices && <th>Services</th>}
          <th className="num">Traces</th>
          <th className="num">Rate</th>
          <th className="num">Errors</th>
          <th className="num">P50</th>
          <th className="num">P95</th>
          <th>Last seen</th>
        </tr>
      </thead>
      <tbody>
        {groups.map((g) => (
          <tr key={g.key} onClick={() => update({ group: g.key })}>
            <td>
              <button className="trace-open">{g.values[0]}</button>
            </td>
            {g.values.slice(1).map((v, i) => (
              <td key={dims[i + 1]}>{v}</td>
            ))}
            {showServices && <td>{formatServices(g)}</td>}
            <td className="num">{g.traces.length}</td>
            <td className="num">{formatRate(g.traces.length, rangeSeconds)}</td>
            <td className={`num${g.errorCount > 0 ? " err-rate" : ""}`}>
              {g.errorCount > 0
                ? `${Math.round((100 * g.errorCount) / g.traces.length)}%`
                : "–"}
            </td>
            <td className="num">{formatDurationMs(g.p50Ms)}</td>
            <td className="num">{formatDurationMs(g.p95Ms)}</td>
            <td>{formatTimestamp(nanosToMs(g.lastStartNs))}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function formatServices(g: TraceGroup): string {
  return g.services.length > 2
    ? `${plural(g.services.length, "service")}`
    : g.services.join(", ");
}

function GroupDetail({
  state,
  traces,
  update,
}: {
  state: ExploreState;
  traces: TraceSummary[];
  update: (patch: Partial<ExploreState>) => void;
}) {
  const dims = parseGroupBy(state.groupBy);
  const members = traces
    .filter((t) => groupKey(t, dims) === state.group)
    .sort((a, b) => (BigInt(a.startNs) < BigInt(b.startNs) ? 1 : -1));
  return (
    <>
      <div className="trace-head">
        <button className="backbtn" onClick={() => update({ group: "" })}>
          ← groups
        </button>
        <h3>{groupLabel(state.group)}</h3>
        <span className="tmeta">
          {dims.join(", ")} · {plural(members.length, "trace")}
        </span>
      </div>
      {members.length === 0 ? (
        <div className="traces-note">
          No traces for this group in this time range.
        </div>
      ) : (
        <table className="trace-table">
          <thead>
            <tr>
              <th>Root</th>
              <th>Service</th>
              <th>Time</th>
              <th className="num">Duration</th>
              <th>Trace ID</th>
            </tr>
          </thead>
          <tbody>
            {members.map((t) => (
              <tr key={t.traceId} onClick={() => update({ trace: t.traceId })}>
                <td>
                  <button className="trace-open">{t.rootTraceName}</button>
                </td>
                <td>{t.rootServiceName}</td>
                <td>{formatTimestamp(nanosToMs(t.startNs))}</td>
                <td className="num">{formatDurationMs(t.durationMs)}</td>
                <td className="trace-id">{t.traceId}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

function TraceDetail({ state, update }: Props) {
  const [selected, setSelected] = useState<string | null>(null);
  const trace = useQuery({
    queryKey: ["tempo-trace", state.trace, state.tenant, state.dataset],
    queryFn: () => tempoGetTrace(state.trace),
  });

  if (trace.isError) {
    return (
      <div className="query-error" role="alert">
        Trace lookup failed: {(trace.error as Error).message}
      </div>
    );
  }
  if (trace.isPending) return <div className="traces-note">Loading…</div>;

  const waterfall = buildWaterfall(trace.data.spans);
  const selectedRow =
    waterfall.rows.find((r) => r.span.spanId === selected) ??
    waterfall.rows.find((r) => r.span.status === "error") ??
    waterfall.rows[0];

  return (
    <div className="traceview">
      <div className="trace-head">
        <button className="backbtn" onClick={() => update({ trace: "" })}>
          ← traces
        </button>
        <h3>{trace.data.rootTraceName}</h3>
        <span className="tmeta">
          {formatDurationMs(
            // The search API truncates durationMs; span extents are exact.
            trace.data.durationMs > 0
              ? trace.data.durationMs
              : Number(waterfall.traceDurationNs) / 1e6,
          )}{" "}
          · {plural(waterfall.rows.length, "span")} ·{" "}
          {plural(waterfall.services.length, "service")}
          {waterfall.errorCount > 0 && (
            <em className="tmeta-err">
              {" "}
              · {plural(waterfall.errorCount, "error")}
            </em>
          )}
        </span>
        <span className="trace-id">{trace.data.traceId}</span>
      </div>
      <div className="trace-body">
        <div className="waterfall" role="list" aria-label="Spans">
          {waterfall.rows.map((row) => (
            <button
              key={row.span.spanId}
              role="listitem"
              className="span-row"
              aria-selected={selectedRow?.span.spanId === row.span.spanId}
              onClick={() => setSelected(row.span.spanId)}
            >
              <span
                className="span-label"
                style={{ paddingLeft: row.depth * 16 }}
              >
                <span className="span-svc">{row.span.serviceName}</span>
                <span className="span-name">{row.span.name}</span>
              </span>
              <span className="span-track">
                <span
                  className={`span-bar${row.span.status === "error" ? " error" : ""}`}
                  style={{
                    left: `${row.leftPct}%`,
                    width: `${row.widthPct}%`,
                  }}
                />
              </span>
              <span
                className={`span-dur${row.span.status === "error" ? " error" : ""}`}
              >
                {formatDurationMs(row.durationMs)}
              </span>
            </button>
          ))}
        </div>
        {selectedRow && (
          <SpanDetail
            span={selectedRow.span}
            traceId={trace.data.traceId}
            update={update}
          />
        )}
      </div>
    </div>
  );
}

function SpanDetail({
  span,
  traceId,
  update,
}: {
  span: TempoSpan;
  traceId: string;
  update: (patch: Partial<ExploreState>) => void;
}) {
  const attrs = Object.entries(span.attributes);
  return (
    <aside className="span-detail" aria-label="Span details">
      <h4>{span.name}</h4>
      <div className="span-detail-sub">
        {span.serviceName}
        {span.status === "error" && <em className="tmeta-err"> · error</em>}
      </div>
      <button
        className="act act-primary"
        onClick={() =>
          update({
            signal: "logs",
            trace: "",
            raw: "",
            filters: [{ label: "trace_id", op: "=", value: traceId }],
          })
        }
      >
        Logs for this trace →
      </button>
      <div className="span-detail-sec">Attributes</div>
      {attrs.length === 0 && (
        <div className="traces-note">No attributes recorded.</div>
      )}
      <dl className="span-attrs">
        {attrs.map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd>{String(v)}</dd>
          </div>
        ))}
      </dl>
    </aside>
  );
}
