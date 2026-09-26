// The shared deploy timeline under the KPI strip: one axis over the window,
// a pin per deploy at the same x the KPI sparklines draw their markers.

import type { Deploy } from "../../api/overview";
import { axisLabelFormatter, type ResolvedRange } from "../../lib/time";

const TICKS = [0, 0.25, 0.5, 0.75, 1];

export function DeploysLane({
  deploys,
  range,
  title,
}: {
  deploys: Deploy[];
  range: ResolvedRange;
  title: string;
}) {
  const span = range.toMs - range.fromMs || 1;
  const fmt = axisLabelFormatter(range.fromMs, range.toMs);
  const shortTime = (ms: number) => {
    const d = new Date(ms);
    return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
  };
  const tickLabel = (ms: number) => fmt(ms).replace(/:\d\d$/, "");
  const label =
    deploys.length === 0
      ? "No deploys in this window"
      : `Deploys in window: ${deploys
          .map((d) => `${d.service} ${d.version} at ${shortTime(d.atMs)}`)
          .join(", ")}`;

  return (
    <div className="overview-lane">
      <span className="overview-label">{title}</span>
      <div className="overview-lane-chart" role="img" aria-label={label}>
        <div className="overview-lane-axis" />
        {TICKS.map((f) => (
          <span
            key={f}
            className="overview-lane-tick"
            style={{ left: `${f * 100}%` }}
          />
        ))}
        {TICKS.map((f) => (
          <span
            key={`l${f}`}
            className={`overview-lane-tick-label${f === 0 ? " start" : f === 1 ? " end" : ""}`}
            style={{ left: `${f * 100}%` }}
          >
            {tickLabel(range.fromMs + f * span)}
          </span>
        ))}
        {deploys.map((d) => {
          const f = Math.min(1, Math.max(0, (d.atMs - range.fromMs) / span));
          const align = f < 0.1 ? "start" : f > 0.9 ? "end" : "mid";
          return (
            <div
              key={`${d.service}@${d.version}`}
              className="overview-lane-deploy"
              style={{ left: `${f * 100}%` }}
            >
              <span className={`overview-lane-deploy-label ${align}`}>
                {d.service} {d.version}
                <span className="overview-lane-deploy-time">
                  {" "}
                  · {shortTime(d.atMs)}
                </span>
              </span>
              <span className="overview-lane-stem" />
              <span className="overview-lane-pin" />
            </div>
          );
        })}
        {deploys.length === 0 && (
          <span className="overview-lane-empty">No deploys in this window</span>
        )}
      </div>
    </div>
  );
}
