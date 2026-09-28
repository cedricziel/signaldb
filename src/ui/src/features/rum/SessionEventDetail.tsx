// The session detail timeline's per-event panel: a selected Network-lane
// event with backend children gets an inline trace waterfall (compact,
// `lib/waterfall.ts`) split into browser+network vs backend time
// (`sessionTraceSplit.ts`), with links to the full trace and its logs.
// An exception panel ships in a later branch of this change.
import { useRef } from "react";
import { Link } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { fetchTraceDetail } from "../../api/traceDetail";
import type { SessionEvent } from "../../api/rumSessionDetail";
import { rangeScopeKey } from "../../lib/time";
import type { ExploreState } from "../../lib/urlState";
import { viewHref } from "../../lib/urlState";
import {
  buildWaterfall,
  formatDurationMs,
  rulerTicks,
} from "../../lib/waterfall";
import { sessionEventLabel } from "./rumModel";
import { clientServerSplit } from "./sessionTraceSplit";
import type { RumScope } from "./useRumData";

interface Props {
  scope: RumScope;
  state: ExploreState;
  event: SessionEvent;
  /** Every event on the timeline — unused by the network panel, but the
   * exception panel (a later branch) needs it to find its preceding failed
   * request, so this dispatcher already takes the full list. */
  events: SessionEvent[];
  onSelectEvent: (event: SessionEvent) => void;
}

export function SessionEventDetail({ scope, state, event }: Props) {
  if (event.kind === "span") {
    return <NetworkEventPanel scope={scope} state={state} event={event} />;
  }
  return null;
}

function NetworkEventPanel({
  scope,
  state,
  event,
}: {
  scope: RumScope;
  state: ExploreState;
  event: Extract<SessionEvent, { kind: "span" }>;
}) {
  const trace = useQuery({
    queryKey: ["trace-detail", event.traceId, rangeScopeKey(state)],
    queryFn: () => fetchTraceDetail(event.traceId, scope.range),
    enabled: event.traceId !== "",
  });

  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);

  if (trace.isError) return <QueryError what="trace" error={trace.error} />;
  if (trace.isPending) return <div className="rum-placeholder">Loading…</div>;
  if (!trace.data) {
    return <EmptyState title="This request's trace wasn't found in range" />;
  }

  const waterfall = buildWaterfall(trace.data.spans);
  const split = clientServerSplit(trace.data.spans, event.spanId);
  const ticks = rulerTicks(waterfall.traceDurationNs);

  return (
    <div className="rum-session-detail-panel">
      <div className="rum-session-detail-head">
        <span className="mono ell">{sessionEventLabel(event)}</span>
        <div className="rum-session-detail-links">
          <Link
            className="btn-ghost"
            to={viewHref(`/traces/${encodeURIComponent(event.traceId)}`, state)}
          >
            Open in Traces
          </Link>
          <Link
            className="btn-ghost"
            to={viewHref("/logs", state, {
              filters: [{ label: "trace_id", op: "=", value: event.traceId }],
            })}
          >
            Backend logs for trace
          </Link>
        </div>
      </div>

      {split && (
        <div
          ref={hostRef}
          tabIndex={0}
          className="rum-splitbar-wrap"
          onPointerMove={pointer.track}
          onPointerLeave={pointer.clear}
          onFocus={(e) => pointer.anchorTo(e.currentTarget)}
          onBlur={pointer.clear}
        >
          <span className="rum-splitbar-track" style={{ width: "100%" }}>
            <span
              className="rum-splitbar-client"
              style={{
                flex: split.browserNetworkMs || 0.0001,
              }}
            />
            <span
              className="rum-splitbar-backend"
              style={{ flex: split.backendMs || 0.0001 }}
            />
          </span>
          {pointer.anchor && (
            <VizTooltip
              anchor={pointer.anchor}
              host={pointer.host}
              title="Client / backend split"
              rows={[
                {
                  label: "In browser + network",
                  value: formatDurationMs(split.browserNetworkMs),
                  swatch: "var(--accent)",
                },
                {
                  label: `Backend (${split.backendServiceName})`,
                  value: formatDurationMs(split.backendMs),
                  swatch: "var(--dim)",
                },
              ]}
            />
          )}
        </div>
      )}

      <div className="rum-mini-waterfall">
        <div className="rum-mini-waterfall-content">
          <div className="rum-mini-waterfall-row rum-mini-waterfall-ruler">
            <span />
            <span className="rum-mini-waterfall-track">
              {ticks.map((t) => (
                <span key={t.pct} style={{ left: `${t.pct}%` }}>
                  {t.label}
                </span>
              ))}
            </span>
          </div>
          {waterfall.rows.map((row) => (
            <div className="rum-mini-waterfall-row" key={row.span.spanId}>
              <span
                className="mono dim ell rum-mini-waterfall-label"
                style={{ paddingLeft: `${row.depth * 10}px` }}
              >
                {row.span.serviceName} · {row.span.name}
              </span>
              <span className="rum-mini-waterfall-track">
                <span
                  className={
                    row.span.status === "error"
                      ? "rum-mini-waterfall-bar err"
                      : "rum-mini-waterfall-bar"
                  }
                  style={{
                    left: `${row.leftPct}%`,
                    width: `${row.widthPct}%`,
                  }}
                  title={`${row.span.serviceName} · ${row.span.name} · ${formatDurationMs(row.durationMs)}`}
                />
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
