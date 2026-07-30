import { useMemo, useState } from "react";
import type { RenderResponse } from "../../api/pyroscope";
import {
  type FlameFrame,
  type FlameView,
  colorBucket,
  decodeFlamebearer,
  formatPct,
  formatTicks,
  frameView,
  placeFrames,
  rootView,
} from "../../lib/flamebearer";

const PALETTE = ["--svc-a", "--svc-b", "--svc-c", "--svc-d", "--svc-e"];

interface Props {
  render: RenderResponse;
  /** Unit of the selected profile type, e.g. "nanoseconds", for formatting. */
  unit: string;
}

export function FlameGraph({ render, unit }: Props) {
  const fb = render.flamebearer;
  const levels = useMemo(() => decodeFlamebearer(fb), [fb]);
  const [focused, setFocused] = useState<FlameFrame | null>(null);
  const [hovered, setHovered] = useState<FlameFrame | null>(null);
  const [highlight, setHighlight] = useState("");

  const view: FlameView = focused ? frameView(focused) : rootView(fb);
  const placed = useMemo(() => placeFrames(levels, view), [levels, view]);

  // Case-insensitive substring match, and the self-time share it covers —
  // the quick answer to "how much of this is my code?".
  const needle = highlight.trim().toLowerCase();
  const matchedSelf = useMemo(() => {
    if (!needle) return 0;
    let sum = 0;
    for (const level of levels) {
      for (const f of level) {
        if (f.name.toLowerCase().includes(needle)) sum += f.self;
      }
    }
    return sum;
  }, [levels, needle]);

  const detail = hovered ?? focused ?? levels[0]?.[0] ?? null;

  return (
    <div className="flamegraph">
      <div className="flame-toolbar">
        <input
          className="flame-search"
          aria-label="Highlight frames"
          placeholder="Highlight frames… (e.g. querier, common::)"
          value={highlight}
          onChange={(e) => setHighlight(e.target.value)}
        />
        {needle && (
          <span className="flame-matched">
            {formatPct(matchedSelf, fb.numTicks)} matched
          </span>
        )}
        {focused && (
          <button
            type="button"
            className="flame-reset"
            onClick={() => setFocused(null)}
          >
            reset zoom
          </button>
        )}
      </div>

      <div className="flame-rows" onMouseLeave={() => setHovered(null)}>
        {placed.map((row, depth) =>
          row.length === 0 ? null : (
            <div className="flame-row" key={depth}>
              {row.map(({ frame, leftPct, widthPct }) => {
                const isRoot = frame.level === 0;
                const color = isRoot
                  ? "--accent"
                  : PALETTE[colorBucket(frame.name, PALETTE.length)];
                const dim =
                  needle !== "" && !frame.name.toLowerCase().includes(needle);
                return (
                  <button
                    key={`${frame.level}-${frame.x}`}
                    type="button"
                    className={`flame-frame${dim ? " dim" : ""}`}
                    style={{
                      left: `${leftPct}%`,
                      width: `${widthPct}%`,
                      background: `var(${color}-soft, var(${color}))`,
                      borderColor: `var(${color})`,
                    }}
                    title={`${frame.name} — ${formatTicks(frame.total, unit)} (${formatPct(frame.total, fb.numTicks)})`}
                    onMouseEnter={() => setHovered(frame)}
                    onFocus={() => setHovered(frame)}
                    onClick={() => setFocused(isRoot ? null : frame)}
                  >
                    <span className="flame-label">{frame.name}</span>
                  </button>
                );
              })}
            </div>
          ),
        )}
      </div>

      {detail && (
        <div className="flame-detail" aria-live="polite">
          <span className="flame-detail-name">{detail.name}</span>
          <span className="flame-detail-meta">
            self {formatTicks(detail.self, unit)} (
            {formatPct(detail.self, fb.numTicks)}) · total{" "}
            {formatTicks(detail.total, unit)} (
            {formatPct(detail.total, fb.numTicks)})
          </span>
        </div>
      )}
    </div>
  );
}
