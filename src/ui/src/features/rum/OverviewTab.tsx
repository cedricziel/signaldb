// The Real users page's Overview tab: the KPI strip, Core Web Vitals,
// sessions over time, top errors, and browser/device breakdowns. Layout and
// component shapes mirror the design prototype (rum/overview.jsx, rum.css)
// as closely as the shipped data allows.
import {
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import { KpiCard } from "../../components/KpiCard";
import { Sparkline } from "../../components/Sparkline";
import { QueryError } from "../../components/QueryError";
import { EmptyState } from "../../components/EmptyState";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { Panel } from "./Panel";
import { compactCount, formatShare } from "../../lib/vizFormat";
import { relChange } from "../../lib/relChange";
import {
  ratingLabel,
  ratingSwatchColorVar,
  ratingTextColorVar,
  vitalFigure,
  vitalThresholdBound,
  VITAL_LABELS,
  VITAL_NAMES,
  VITAL_TITLES,
  type VitalFigure,
  type VitalName,
} from "./rumModel";
import {
  useRumBreakdown,
  useRumKpis,
  useRumSessionsOverTime,
  useRumTopErrors,
  useRumVitals,
  type RumScope,
} from "./useRumData";

interface Props {
  scope: RumScope;
  onOpenSetup: () => void;
}

export function OverviewTab({ scope }: Props) {
  const kpis = useRumKpis(scope);
  const sessions = kpis.data?.sessions;
  const errors = kpis.data?.sessionsWithErrors;
  const pageViews = kpis.data?.pageViews;
  const vitals = useRumVitals(scope);
  const sessionsOverTime = useRumSessionsOverTime(scope);
  const topErrors = useRumTopErrors(scope);
  const browser = useRumBreakdown(scope, "resource.browser.brands", {
    requireField: true,
    limit: 8,
  });
  const device = useRumBreakdown(scope, "resource.browser.mobile");

  const sessionsShare =
    sessions && errors && sessions.value > 0
      ? errors.value / sessions.value
      : undefined;
  const prevShare =
    sessions?.previous && errors?.previous && sessions.previous > 0
      ? errors.previous / sessions.previous
      : undefined;
  const shareSeries = shareOverTime(
    sessions?.series ?? [],
    errors?.series ?? [],
  );

  return (
    <div className="rum-stack">
      <div className="rum-kpis">
        <KpiCard
          label="Sessions"
          value={kpis.isPending ? "–" : compactCount(sessions?.value ?? 0)}
          change={
            sessions?.previous !== undefined
              ? relChange(sessions.value, sessions.previous, false)
              : undefined
          }
          detail="distinct session.id"
        >
          <Sparkline
            points={(sessions?.series ?? []).map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
        <KpiCard
          label="Sessions with errors"
          value={
            sessionsShare === undefined
              ? "–"
              : formatShare(errors!.value, sessions!.value)
          }
          change={
            prevShare !== undefined
              ? relChange(sessionsShare ?? 0, prevShare, true)
              : undefined
          }
          detail="≥1 exception"
        >
          <Sparkline
            points={shareSeries.map((p) => ({ x: p.tMs, v: p.value }))}
            tone="error"
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
        <KpiCard
          label="Page views"
          value={kpis.isPending ? "–" : compactCount(pageViews?.value ?? 0)}
          change={
            pageViews?.previous !== undefined
              ? relChange(pageViews.value, pageViews.previous, false)
              : undefined
          }
          detail="browser.navigation"
        >
          <Sparkline
            points={(pageViews?.series ?? []).map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            height={28}
            showTooltip={false}
          />
        </KpiCard>
      </div>
      {kpis.isError && <QueryError what="RUM KPIs" error={kpis.error} />}

      <div className="rum-grid-2-1">
        <Panel
          title="Core Web Vitals"
          meta="p75 · share of page views good / needs improvement / poor"
        >
          {vitals.isError ? (
            <QueryError what="Web Vitals" error={vitals.error} />
          ) : (
            <div className="rum-vitals">
              {VITAL_NAMES.map((name) => {
                const data = vitals.data?.get(name);
                const figure = vitalFigure(name, data?.p75, data?.counts ?? {});
                return <VitalCell key={name} name={name} figure={figure} />;
              })}
            </div>
          )}
        </Panel>
        <Panel title="Sessions" meta={rangeLabelFor(scope)}>
          {sessionsOverTime.isError ? (
            <QueryError
              what="sessions over time"
              error={sessionsOverTime.error}
            />
          ) : (
            <StackedSessions data={sessionsOverTime} />
          )}
        </Panel>
      </div>

      {/* BackendPanel (Frontend → backend) and "Slowest pages" are later
          groups; this row ships with only the two panels below, same as
          the prototype's own rum-grid-3 before those land. */}
      <div className="rum-grid-3">
        <Panel title="Top errors">
          {topErrors.isError ? (
            <QueryError what="top errors" error={topErrors.error} />
          ) : topErrors.isPending ? (
            <div className="rum-placeholder">Loading…</div>
          ) : (topErrors.data?.groups.length ?? 0) === 0 ? (
            <EmptyState title="No errors in this window" />
          ) : (
            <div className="rum-error-list">
              {topErrors.data!.groups.slice(0, 6).map((g, i) => (
                <div key={i} className="rum-row">
                  <span className="rum-row-main">
                    <span className="ell rum-row-title">
                      <span style={{ color: "var(--err)" }}>
                        {g.exceptionType ?? "Error"}
                      </span>{" "}
                      <span className="rum-row-message">
                        {g.exceptionMessage ?? ""}
                      </span>
                    </span>
                  </span>
                  <span className="mono dim rum-row-count">
                    {compactCount(g.count)} occurrences
                  </span>
                </div>
              ))}
            </div>
          )}
        </Panel>

        <Panel title="Sessions by browser">
          <MiniBars
            rows={browser.data}
            pending={browser.isPending}
            emptyHint="Upgrade the SDK to capture browser.brands"
          />
          <span className="rum-eyebrow rum-device-eyebrow">Device</span>
          <MiniBars
            rows={device.data}
            pending={device.isPending}
            labelFor={(v) =>
              v === "true"
                ? "Mobile"
                : v === "false"
                  ? "Desktop"
                  : (v ?? "Unknown")
            }
          />
        </Panel>
      </div>
    </div>
  );
}

/** The window label a `Panel`'s `meta` shows next to "Sessions" — mirrors
 * the prototype's static "last 24 h" with the page's own selected range. */
function rangeLabelFor(scope: RumScope): string {
  const hours = (scope.range.toMs - scope.range.fromMs) / 3_600_000;
  return hours < 48
    ? `last ${Math.round(hours)} h`
    : `last ${Math.round(hours / 24)} d`;
}

/** Zips two bucketed series sharing the same `tMs` grid into a per-bucket
 * error share (0–1) — a bucket the error series has no point for (no
 * errors that step) reads as 0, not "missing", mirroring
 * `entityDetailStats.ts`'s `errorRateSeries`. */
function shareOverTime(
  total: { tMs: number; value: number }[],
  errors: { tMs: number; value: number }[],
): { tMs: number; value: number }[] {
  const errByT = new Map(errors.map((p) => [p.tMs, p.value]));
  return total.map((p) => ({
    tMs: p.tMs,
    value: p.value > 0 ? (errByT.get(p.tMs) ?? 0) / p.value : 0,
  }));
}

function VitalCell({ name, figure }: { name: VitalName; figure: VitalFigure }) {
  const good = figure.shares.find((s) => s.rating === "good");
  const goodPct = good ? Math.round(good.share * 100) : 0;
  return (
    <div className="rum-vital">
      <div className="rum-vital-head">
        <b className="mono rum-vital-key">{VITAL_LABELS[name]}</b>
        <span className="dim ell rum-vital-name">{VITAL_TITLES[name]}</span>
      </div>
      <div className="rum-vital-head">
        <span className="mono rum-vital-value">{figure.formatted}</span>
        {figure.rating && (
          <span
            className="rum-vital-rating"
            style={{ color: ratingTextColorVar(figure.rating) }}
          >
            {ratingLabel(figure.rating)}
          </span>
        )}
      </div>
      <div className="rum-vital-dist-wrap">
        <VitalDist name={name} figure={figure} />
        <div className="mono faint rum-vital-goodpct">{goodPct}% good</div>
      </div>
    </div>
  );
}

/** The good/needs-improvement/poor bar for one vital, with the shared
 * `VizTooltip` on hover and keyboard focus. */
function VitalDist({ name, figure }: { name: VitalName; figure: VitalFigure }) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const total = figure.shares.reduce((s, r) => s + r.count, 0);
  if (total === 0) {
    return <DistBar shares={figure.shares} />;
  }
  return (
    <div
      ref={hostRef}
      tabIndex={0}
      aria-label={`${VITAL_TITLES[name]} thresholds`}
      onPointerMove={pointer.track}
      onPointerLeave={pointer.clear}
      onFocus={(e) => pointer.anchorTo(e.currentTarget)}
      onBlur={pointer.clear}
      className="rum-vital-dist"
    >
      <DistBar shares={figure.shares} />
      {pointer.anchor && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={`${VITAL_TITLES[name]} · p75 thresholds`}
          rows={figure.shares.map((s) => ({
            swatch: ratingSwatchColorVar(s.rating),
            label:
              s.rating === "good"
                ? `Good ≤ ${vitalThresholdBound(name, "good")}`
                : s.rating === "poor"
                  ? `Poor > ${vitalThresholdBound(name, "poor")}`
                  : ratingLabel(s.rating),
            value: formatShare(s.count, total),
          }))}
          footer="share of page views"
        />
      )}
    </div>
  );
}

function DistBar({ shares }: { shares: VitalFigure["shares"] }) {
  return (
    <div className="rum-distbar">
      {shares.map((s) => (
        <span
          key={s.rating}
          title={`${ratingLabel(s.rating)} ${Math.round(s.share * 100)}%`}
          style={{
            width: `${s.share * 100}%`,
            background: ratingSwatchColorVar(s.rating),
          }}
        />
      ))}
    </div>
  );
}

/** Stacked bars of sessions with/without errors, with a hover `VizTooltip`
 * and start/middle/"now" time labels — mirrors the prototype's
 * `StackedSessions`. The deploy marker and version-keyed error-share note
 * are omitted: neither is cheap to derive from real data (see design.md's
 * "one bounded IR read" principle) without a dedicated per-version read,
 * which is out of this group's scope. */
function StackedSessions({
  data,
}: {
  data: ReturnType<typeof useRumSessionsOverTime>;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  if (data.isPending) return <div className="rum-placeholder">Loading…</div>;
  const total = data.data?.total ?? [];
  const withErrors = data.data?.withErrors ?? [];
  if (total.length === 0) {
    return <EmptyState title="No sessions in this window" />;
  }
  const errByT = new Map(withErrors.map((p) => [p.tMs, p.value]));
  const bars = total.map((p) => {
    const totalV = Math.max(0, Math.round(p.value));
    const err = Math.min(totalV, Math.round(errByT.get(p.tMs) ?? 0));
    return { tMs: p.tMs, total: totalV, err, ok: totalV - err };
  });
  const max = Math.max(1, ...bars.map((b) => b.total));

  function onMove(e: ReactPointerEvent<HTMLDivElement>) {
    const rect = hostRef.current?.getBoundingClientRect();
    if (!rect) return;
    const i = Math.max(
      0,
      Math.min(
        bars.length - 1,
        Math.floor(((e.clientX - rect.left) / rect.width) * bars.length),
      ),
    );
    setHoverIdx(i);
    pointer.track(e);
  }
  function onLeave() {
    setHoverIdx(null);
    pointer.clear();
  }

  const hb = hoverIdx !== null ? bars[hoverIdx] : undefined;
  const timeLabel = (tMs: number) =>
    `${new Date(tMs).toISOString().slice(11, 16)} UTC`;

  return (
    <div className="rum-stacked-sessions">
      <div className="rum-stacked-legend">
        <span className="rum-key">
          <i style={{ background: "var(--dim)" }} />
          without errors
        </span>
        <span className="rum-key">
          <i style={{ background: "var(--err-bar)" }} />
          with errors
        </span>
      </div>
      <div
        ref={hostRef}
        onPointerMove={onMove}
        onPointerLeave={onLeave}
        className="rum-stacked-bars"
      >
        {bars.map((b, i) => (
          <div
            key={i}
            className="rum-stacked-bar"
            style={{
              height: `${(b.total / max) * 100}%`,
              opacity: hoverIdx !== null && hoverIdx !== i ? 0.55 : 1,
            }}
          >
            <span
              className="rum-stacked-bar-ok"
              style={{ flex: b.ok || 0.0001 }}
            />
            <span
              className="rum-stacked-bar-err"
              style={{
                flex: `0 0 ${b.total > 0 ? Math.max(2, (b.err / b.total) * 100) : 0}%`,
              }}
            />
          </div>
        ))}
        {hb && pointer.anchor && (
          <VizTooltip
            anchor={pointer.anchor}
            host={pointer.host}
            title={timeLabel(hb.tMs)}
            rows={[
              {
                label: "without errors",
                value: compactCount(hb.ok),
                swatch: "var(--dim)",
              },
              {
                label: "with errors",
                value: compactCount(hb.err),
                swatch: "var(--err-bar)",
              },
            ]}
            footer={{
              label: "error share",
              value: formatShare(hb.err, hb.total),
            }}
          />
        )}
      </div>
      <div className="mono faint rum-stacked-axis">
        <span>{timeLabel(bars[0]!.tMs)}</span>
        <span>{timeLabel(bars[Math.floor(bars.length / 2)]!.tMs)}</span>
        <span>now</span>
      </div>
    </div>
  );
}

function MiniBars({
  rows,
  pending,
  emptyHint,
  labelFor,
}: {
  rows: { value: string | null; count: number }[] | undefined;
  pending: boolean;
  emptyHint?: string;
  labelFor?: (value: string | null) => string;
}) {
  if (pending) return <div className="rum-placeholder">Loading…</div>;
  const list = rows ?? [];
  if (list.length === 0) {
    return <EmptyState title="No breakdown yet">{emptyHint}</EmptyState>;
  }
  const total = list.reduce((s, r) => s + r.count, 0) || 1;
  const pcts = list
    .map((r) => ({
      label: labelFor ? labelFor(r.value) : (r.value ?? "Unknown"),
      pct: Math.round((r.count / total) * 100),
    }))
    .slice(0, 6);
  const max = Math.max(1, ...pcts.map((r) => r.pct));
  return (
    <div className="rum-minibars">
      {pcts.map((r, i) => (
        <div className="rum-minibars-row" key={i}>
          <span className="mono rum-minibars-label">{r.label}</span>
          <span className="rum-minibars-track">
            <span
              className="rum-minibars-fill"
              style={{ width: `${(r.pct / max) * 100}%` }}
            />
          </span>
          <span className="mono dim rum-minibars-value">{r.pct}%</span>
        </div>
      ))}
    </div>
  );
}
