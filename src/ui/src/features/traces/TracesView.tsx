import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { tempoGetTrace, tempoSearch, type TempoSpan } from "../../api/tempo";
import {
  formatTimestamp,
  nanosToMs,
  rangeToParam,
  resolveRange,
} from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { buildWaterfall, formatDurationMs } from "../../lib/waterfall";
import "./traces.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function TracesView({ state, update }: Props) {
  if (state.trace !== "") {
    return <TraceDetail state={state} update={update} />;
  }
  return <TraceSearch state={state} update={update} />;
}

function TraceSearch({ state, update }: Props) {
  const rangeKey = rangeToParam(state.range);
  const search = useQuery({
    queryKey: ["tempo-search", rangeKey, state.limit],
    queryFn: () =>
      tempoSearch(resolveRange(state.range, Date.now()), state.limit),
  });

  return (
    <div className="traces-search">
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

      {search.isError && (
        <div className="query-error" role="alert">
          Search failed: {(search.error as Error).message}
        </div>
      )}
      {search.isPending && <div className="traces-note">Loading…</div>}
      {search.data && search.data.length === 0 && (
        <div className="traces-note">No traces in this time range.</div>
      )}
      {search.data && search.data.length > 0 && (
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
            {search.data.map((t) => (
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
    </div>
  );
}

function TraceDetail({ state, update }: Props) {
  const [selected, setSelected] = useState<string | null>(null);
  const trace = useQuery({
    queryKey: ["tempo-trace", state.trace],
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
          {formatDurationMs(trace.data.durationMs)} · {waterfall.rows.length}{" "}
          spans · {waterfall.services.length} services
          {waterfall.errorCount > 0 && (
            <em className="tmeta-err"> · {waterfall.errorCount} error(s)</em>
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
