// A small, presentational time-series chart for a table cell or KPI card —
// a line by default, or a bar variant for occurrence counts (bars drawn from
// zero read as counts; a line reads as a level — see `ErrorSparkline`'s
// original doc comment for the reasoning this preserves).
//
// Every visualization shows its data through the shared `VizTooltip`. Some
// hosts (a table cell with `overflow: hidden` for its ellipsis) can't fit a
// tooltip without clipping it; those pass `showTooltip={false}` and read
// `onHover` instead, rendering their own tooltip elsewhere.
import {
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import { useVizPointer, VizTooltip } from "./VizTooltip";
import "./Sparkline.css";

export interface SparklinePoint {
  x: number;
  v: number;
}

/** The hovered point, plus context a caller's own tooltip may want. */
export interface SparklineHoverPoint extends SparklinePoint {
  stepMs: number;
  max: number;
}

export interface SparklineProps {
  points: SparklinePoint[];
  variant?: "line" | "bar";
  /** Maps to a token: neutral → `--dim`, error → `--err`, accent → `--accent`. */
  tone?: "neutral" | "error" | "accent";
  width?: number | string;
  height?: number;
  formatValue?: (v: number) => string;
  formatLabel?: (x: number) => string;
  /** Tooltip row label, e.g. "occurrences". */
  valueLabel?: string;
  /** Render the built-in `VizTooltip` on hover. Default true. */
  showTooltip?: boolean;
  onHover?: (
    point: SparklineHoverPoint | null,
    event?: ReactPointerEvent<SVGElement>,
  ) => void;
  /** Shown instead of the chart when there isn't enough data to draw one. */
  emptyText?: string;
  ariaLabel?: string;
  /** Only these points get a tab stop; default is every point. */
  isFocusable?: (point: SparklinePoint) => boolean;
}

const DEFAULT_WIDTH = 88;
const DEFAULT_HEIGHT = 18;

const TONE_CLASS: Record<NonNullable<SparklineProps["tone"]>, string> = {
  neutral: "sparkline-tone-neutral",
  error: "sparkline-tone-error",
  accent: "sparkline-tone-accent",
};

export function Sparkline({
  points,
  variant = "line",
  tone = "neutral",
  width = DEFAULT_WIDTH,
  height = DEFAULT_HEIGHT,
  formatValue = (v) => String(v),
  formatLabel = (x) => String(x),
  valueLabel = "value",
  showTooltip = true,
  onHover,
  emptyText,
  ariaLabel = "value over time",
  isFocusable,
}: SparklineProps) {
  const rootRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(rootRef);
  const [active, setActive] = useState<number | null>(null);

  const minPoints = variant === "line" ? 2 : 1;
  if (points.length < minPoints) {
    if (emptyText) {
      return <div className="sparkline-empty">{emptyText}</div>;
    }
    return null;
  }

  // A string width (a percentage) stretches the SVG via CSS; the viewBox
  // just needs *a* unit space to lay the points out in, since
  // `preserveAspectRatio="none"` stretches non-uniformly to fit regardless.
  const numericWidth = typeof width === "number" ? width : DEFAULT_WIDTH;
  const values = points.map((p) => p.v);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const stepMs = points.length > 1 ? points[1]!.x - points[0]!.x : 0;
  const cell =
    numericWidth / (variant === "line" ? points.length - 1 : points.length);

  const toHover = (p: SparklinePoint): SparklineHoverPoint => ({
    ...p,
    stepMs,
    max,
  });

  const activePoint =
    active === null
      ? null
      : isFocusable && !isFocusable(points[active]!)
        ? null
        : points[active]!;

  const svgStyle = typeof width === "string" ? { width } : undefined;
  // The host is `inline-block` by default so it shrink-wraps a fixed-width
  // chart; a string width (a percentage, say) needs it to fill its parent
  // instead, or the svg's own "100%" has nothing to measure against.
  const hostStyle =
    typeof width === "string"
      ? { width, display: "block" as const }
      : undefined;

  return (
    <div className="sparkline-host viz-host" ref={rootRef} style={hostStyle}>
      <svg
        className="sparkline"
        width={typeof width === "number" ? width : undefined}
        height={height}
        style={svgStyle}
        viewBox={`0 0 ${numericWidth} ${height}`}
        role="img"
        aria-label={ariaLabel}
        preserveAspectRatio="none"
      >
        {variant === "line" ? (
          <polyline
            className={`sparkline-line ${TONE_CLASS[tone]}`}
            fill="none"
            strokeWidth="1"
            points={points
              .map(
                (p, i) =>
                  `${i * cell},${height - ((p.v - min) / span) * height}`,
              )
              .join(" ")}
          />
        ) : (
          points.map((p, i) => {
            const h = max > 0 ? (p.v / max) * height : 0;
            return (
              <rect
                key={p.x}
                data-testid="sparkline-bar"
                className={`sparkline-bar ${TONE_CLASS[tone]}`}
                x={i * cell}
                y={height - h}
                width={Math.max(1, cell - 1)}
                height={h}
              />
            );
          })
        )}
        {points.map((p, i) => {
          const focusable = isFocusable ? isFocusable(p) : true;
          return (
            <rect
              key={p.x}
              data-testid="sparkline-hit"
              className="sparkline-hit"
              x={variant === "line" ? i * cell - cell / 2 : i * cell}
              y={0}
              width={cell}
              height={height}
              tabIndex={focusable ? 0 : undefined}
              aria-label={`${formatLabel(p.x)}: ${formatValue(p.v)}`}
              aria-describedby={
                showTooltip && activePoint === p ? "sparkline-tip" : undefined
              }
              onPointerEnter={(e) => {
                if (!showTooltip) onHover?.(toHover(p), e);
              }}
              onPointerMove={(e) => {
                if (showTooltip) {
                  if (!focusable) return;
                  setActive(i);
                  pointer.track(e);
                } else {
                  onHover?.(toHover(p), e);
                }
              }}
              onPointerLeave={(e) => {
                if (showTooltip) {
                  setActive((a) => (a === i ? null : a));
                  pointer.clear();
                } else {
                  onHover?.(null, e);
                }
              }}
              onFocus={(e) => {
                if (!showTooltip || !focusable) return;
                setActive(i);
                pointer.anchorTo(e.currentTarget);
              }}
              onBlur={() => {
                if (!showTooltip) return;
                setActive((a) => (a === i ? null : a));
                pointer.clear();
              }}
            />
          );
        })}
      </svg>
      {showTooltip && activePoint && pointer.anchor && (
        <VizTooltip
          id="sparkline-tip"
          anchor={pointer.anchor}
          host={pointer.host}
          title={formatLabel(activePoint.x)}
          rows={[
            {
              swatch: `var(${TONE_TOKEN[tone]})`,
              label: valueLabel,
              value: formatValue(activePoint.v),
            },
          ]}
          valueWidthCh={formatValue(max).length}
        />
      )}
    </div>
  );
}

const TONE_TOKEN: Record<NonNullable<SparklineProps["tone"]>, string> = {
  neutral: "--dim",
  error: "--err",
  accent: "--accent",
};
