// The Real users Errors tab's selected-group detail: stats, a volume
// histogram, stack frames with source context, a by-browser breakdown, the
// backend-cause panel with its own inline trace waterfall, and a link to
// the group's latest session. Occurrences and volume reuse `api/errors.ts`'s
// already-shipped queries via `toErrorsPageGroup` (design.md decision 5).
import { useRef } from "react";
import { Link } from "react-router";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { StacktraceLines } from "../../components/StacktraceLines";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { compactCount } from "../../lib/vizFormat";
import {
  durationToSeconds,
  formatTimestamp,
  stepForRange,
} from "../../lib/time";
import { useSourceContextEnabled } from "../../lib/useSourceContextEnabled";
import type { ExploreState } from "../../lib/urlState";
import { viewHref } from "../../lib/urlState";
import {
  buildWaterfall,
  formatDurationMs,
  rulerTicks,
} from "../../lib/waterfall";
import { ErrorSparkline } from "../errors/ErrorSparkline";
import {
  erroringDescendantService,
  type RumErrorBrowserRow,
  type RumErrorGroupWithCause,
  type RumFailedRequest,
} from "../../api/rumErrorGroups";
import { Panel } from "./Panel";
import {
  useRumErrorBrowserBreakdown,
  useRumErrorCauseTrace,
  useRumErrorOccurrences,
  useRumErrorRelease,
  useRumErrorVolume,
  type RumScope,
} from "./useRumData";

interface Props {
  scope: RumScope;
  state: ExploreState;
  group: RumErrorGroupWithCause;
}

export function ErrorDetailView({ scope, state, group }: Props) {
  const occurrences = useRumErrorOccurrences(scope, group);
  const volume = useRumErrorVolume(scope, group);
  const browsers = useRumErrorBrowserBreakdown(scope, group);
  const release = useRumErrorRelease(scope, group);
  const sourceContextEnabled = useSourceContextEnabled(state.tenant);

  const step = stepForRange(scope.range, 20);
  const stepMs = (durationToSeconds(step) ?? 0) * 1000;
  const latest = occurrences.data?.[0];

  return (
    <Panel
      title={group.exceptionType ?? "Exception"}
      meta={group.exceptionMessage ?? undefined}
    >
      <dl className="rum-error-detail-stats">
        <Stat label="Events" value={compactCount(group.count)} />
        <Stat label="Users" value={compactCount(group.users)} />
        <Stat label="Sessions" value={compactCount(group.sessions)} />
        <Stat label="First seen" value={formatTimestamp(group.firstMs)} />
        <Stat label="Last seen" value={formatTimestamp(group.lastMs)} />
        <Stat label="Release" value={release.data ?? "—"} />
      </dl>

      {volume.isError ? (
        <QueryError what="volume" error={volume.error} />
      ) : (
        volume.data && (
          <ErrorSparkline
            series={volume.data}
            rangeMs={scope.range}
            stepMs={stepMs}
          />
        )
      )}

      <div className="rum-error-detail-columns">
        <div className="rum-error-detail-frames">
          <span className="rum-eyebrow">Stack frames</span>
          {occurrences.isError ? (
            <QueryError what="occurrences" error={occurrences.error} />
          ) : occurrences.isPending ? (
            <div className="rum-placeholder">Loading…</div>
          ) : latest?.stacktrace ? (
            <StacktraceLines
              text={latest.stacktrace}
              tenant={sourceContextEnabled ? state.tenant : undefined}
              variant="error"
            />
          ) : (
            <EmptyState title="No stack trace recorded for this exception" />
          )}
        </div>

        <div className="rum-error-detail-side">
          <span className="rum-eyebrow">By browser</span>
          <BrowserBars rows={browsers.data} pending={browsers.isPending} />

          <span className="rum-eyebrow">Backend cause</span>
          {group.backendCause ? (
            <BackendCausePanel scope={scope} cause={group.backendCause} />
          ) : (
            <EmptyState title="No preceding failed request found" />
          )}

          {group.lastSessionId && (
            <Link
              className="btn-ghost"
              to={viewHref("/rum/sessions", state, {
                rumSession: group.lastSessionId,
              })}
            >
              Latest session →
            </Link>
          )}
        </div>
      </div>
    </Panel>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rum-error-detail-stat">
      <dt className="rum-eyebrow">{label}</dt>
      <dd className="mono">{value}</dd>
    </div>
  );
}

function BrowserBars({
  rows,
  pending,
}: {
  rows: RumErrorBrowserRow[] | undefined;
  pending: boolean;
}) {
  if (pending) return <div className="rum-placeholder">Loading…</div>;
  const list = rows ?? [];
  if (list.length === 0) return <EmptyState title="No browser data yet" />;
  const total = list.reduce((s, r) => s + r.count, 0) || 1;
  const max = Math.max(1, ...list.map((r) => r.count));
  return (
    <div className="rum-minibars">
      {list.map((r) => (
        <BrowserBarRow key={r.browser} row={r} total={total} max={max} />
      ))}
    </div>
  );
}

function BrowserBarRow({
  row,
  total,
  max,
}: {
  row: RumErrorBrowserRow;
  total: number;
  max: number;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const pct = Math.round((row.count / total) * 100);
  return (
    <div
      ref={hostRef}
      className="rum-minibars-row"
      style={{ position: "relative" }}
      onPointerMove={pointer.track}
      onPointerLeave={pointer.clear}
    >
      <span className="mono rum-minibars-label">{row.browser}</span>
      <span className="rum-minibars-track">
        <span
          className="rum-minibars-fill"
          style={{ width: `${(row.count / max) * 100}%` }}
        />
      </span>
      <span className="mono dim rum-minibars-value">{pct}%</span>
      {pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={row.browser}
          rows={[{ label: "occurrences", value: compactCount(row.count) }]}
        />
      )}
    </div>
  );
}

/** The backend-cause request's own trace, fetched only for the selected
 * group — the same inline waterfall shape `SessionEventDetail`'s
 * `NetworkEventPanel` renders, built from the same `lib/waterfall.ts`
 * helpers (not the component itself: it's keyed off a `SessionSpanEvent`,
 * a different shape than a `RumFailedRequest`). */
function BackendCausePanel({
  scope,
  cause,
}: {
  scope: RumScope;
  cause: RumFailedRequest;
}) {
  const trace = useRumErrorCauseTrace(scope, cause.traceId);

  if (trace.isError) return <QueryError what="trace" error={trace.error} />;
  if (trace.isPending) return <div className="rum-placeholder">Loading…</div>;
  if (!trace.data) {
    return <EmptyState title="This request's trace wasn't found in range" />;
  }

  const waterfall = buildWaterfall(trace.data.spans);
  const ticks = rulerTicks(waterfall.traceDurationNs);
  const backendService = erroringDescendantService(
    trace.data.spans,
    cause.spanId,
  );

  return (
    <div className="rum-session-detail-panel">
      <div className="rum-callout">
        <span>
          <code className="mono">{cause.method}</code>{" "}
          <code className="mono">{cause.urlFull}</code>
          {cause.statusCode !== null && ` → ${cause.statusCode}`}
          {backendService && ` · erroring in ${backendService}`}
        </span>
      </div>
      <div className="rum-mini-waterfall">
        <div className="rum-mini-waterfall-ruler">
          {ticks.map((t) => (
            <span key={t.pct} style={{ left: `${t.pct}%` }}>
              {t.label}
            </span>
          ))}
        </div>
        {waterfall.rows.map((row) => (
          <div
            className="rum-mini-waterfall-row"
            key={row.span.spanId}
            style={{ paddingLeft: `${row.depth * 10}px` }}
          >
            <span className="mono dim ell rum-mini-waterfall-label">
              {row.span.serviceName} · {row.span.name}
            </span>
            <span className="rum-mini-waterfall-track">
              <span
                className={
                  row.span.status === "error"
                    ? "rum-mini-waterfall-bar err"
                    : "rum-mini-waterfall-bar"
                }
                style={{ left: `${row.leftPct}%`, width: `${row.widthPct}%` }}
                title={`${row.span.serviceName} · ${row.span.name} · ${formatDurationMs(row.durationMs)}`}
              />
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
