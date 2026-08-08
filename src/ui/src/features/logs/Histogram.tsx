/**
 * Logs adapter over the shared volume chart: it owns the level vocabulary
 * (normalisation, stacking order, colours) and nothing else. All rendering,
 * scaling, and interaction lives in `SignalHistogram`.
 */
import type { HistogramSeries } from "../../api/loki";
import type { Scale } from "../explore/scale";
import { SignalHistogram, type VolumeSeries } from "../explore/SignalHistogram";

/** Stacking order bottom-to-top; anything else lands in "other". */
const LEVEL_ORDER = ["debug", "info", "warn", "error", "other"];

const LEVEL_VAR: Record<string, string> = {
  debug: "var(--debug-bar)",
  info: "var(--info-bar)",
  warn: "var(--warn-bar)",
  error: "var(--err-bar)",
  other: "var(--faint)",
};

export function normalizeLevel(level: string): string {
  const l = level.toLowerCase();
  if (l.startsWith("err") || l === "fatal" || l === "critical") return "error";
  if (l.startsWith("warn")) return "warn";
  if (l === "info" || l === "information") return "info";
  if (l === "debug" || l === "trace") return "debug";
  return "other";
}

/**
 * Key each Loki series by its canonical level. Series that normalise onto the
 * same level are merged downstream when the chart buckets them.
 */
export function toVolumeSeries(series: HistogramSeries[]): VolumeSeries[] {
  return series.map((s) => ({
    key: normalizeLevel(s.level),
    points: s.points,
  }));
}

interface Props {
  series: HistogramSeries[];
  /** Selected time range; empty buckets pad the full range. */
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
  scale: Scale;
  onScaleChange?: (scale: Scale) => void;
  step?: string;
  stepOptions?: string[];
  onStepChange?: (step: string) => void;
  height?: number;
}

export function Histogram({
  series,
  rangeMs,
  stepMs,
  scale,
  onScaleChange,
  step,
  stepOptions,
  onStepChange,
  height,
}: Props) {
  return (
    <SignalHistogram
      series={toVolumeSeries(series)}
      order={LEVEL_ORDER}
      colors={LEVEL_VAR}
      rangeMs={rangeMs}
      stepMs={stepMs}
      scale={scale}
      unit="lines"
      label="Log volume over time by level"
      onScaleChange={onScaleChange}
      step={step ?? ""}
      stepOptions={stepOptions ?? []}
      onStepChange={onStepChange}
      {...(height === undefined ? {} : { height })}
    />
  );
}
