// A thin horizontal proportional bar: either several segments sized by
// value, or a single fill against a track — for "how is this total split up"
// and "how far along is this" respectively.
import type { HTMLAttributes } from "react";
import "./ShareBar.css";

export interface ShareBarSegment {
  key: string;
  value: number;
  color: string;
  label: string;
}

export interface ShareBarProps {
  segments?: ShareBarSegment[];
  /** Single-fill mode: a fraction (0–1) of the track to fill. */
  fraction?: number;
  fillColor?: string;
  legend?: boolean;
  /**
   * Extra DOM props (event handlers, aria, tabIndex, ref) per segment. `ref`
   * takes `HTMLOrSVGElement` so a caller's roving-focus hook — typed for
   * both HTML and SVG marks — can wire straight through.
   */
  segmentProps?: (
    segment: ShareBarSegment,
    index: number,
  ) => HTMLAttributes<HTMLSpanElement> & {
    ref?: (el: HTMLOrSVGElement | null) => void;
  };
  ariaLabel?: string;
}

export function ShareBar({
  segments,
  fraction,
  fillColor = "var(--accent)",
  legend = false,
  segmentProps,
  ariaLabel,
}: ShareBarProps) {
  if (segments) {
    const total = segments.reduce((sum, s) => sum + s.value, 0);
    return (
      <div className="share-bar-wrap">
        <div className="share-bar" role="group" aria-label={ariaLabel}>
          {segments.map((s, i) => {
            const { className, ...extra } = segmentProps?.(s, i) ?? {};
            return (
              <span
                key={s.key}
                data-testid="share-bar-seg"
                className={
                  className ? `share-bar-seg ${className}` : "share-bar-seg"
                }
                style={{
                  width: `${total > 0 ? (s.value / total) * 100 : 0}%`,
                  background: s.color,
                }}
                {...extra}
              />
            );
          })}
        </div>
        {legend && (
          <dl className="share-bar-legend">
            {segments.map((s) => (
              <div key={s.key} className="share-bar-legend-item">
                <dt style={{ background: s.color }} />
                <dd>{s.label}</dd>
              </div>
            ))}
          </dl>
        )}
      </div>
    );
  }

  return (
    <div className="share-bar-track">
      <span
        data-testid="share-bar-fill"
        className="share-bar-fill"
        style={{
          width: `${Math.max(0, Math.min(1, fraction ?? 0)) * 100}%`,
          background: fillColor,
        }}
      />
    </div>
  );
}
