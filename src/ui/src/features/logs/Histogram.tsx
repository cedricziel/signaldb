import { formatTimestamp } from "../../lib/time";
import type { HistogramSeries } from "../../api/loki";

/** Stacking order bottom-to-top; anything else lands in "other". */
const LEVEL_ORDER = ["debug", "info", "warn", "error"] as const;

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

export interface HistogramBucket {
  tMs: number;
  counts: Record<string, number>;
  total: number;
}

export function bucketize(series: HistogramSeries[]): HistogramBucket[] {
  const byTime = new Map<number, Record<string, number>>();
  for (const s of series) {
    const level = normalizeLevel(s.level);
    for (const [tMs, count] of s.points) {
      const bucket = byTime.get(tMs) ?? {};
      bucket[level] = (bucket[level] ?? 0) + count;
      byTime.set(tMs, bucket);
    }
  }
  return [...byTime.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([tMs, counts]) => ({
      tMs,
      counts,
      total: Object.values(counts).reduce((a, b) => a + b, 0),
    }));
}

interface Props {
  series: HistogramSeries[];
  height?: number;
}

export function Histogram({ series, height = 72 }: Props) {
  const buckets = bucketize(series);
  const max = Math.max(1, ...buckets.map((b) => b.total));
  const levels = [...LEVEL_ORDER, "other"];

  if (buckets.length === 0) {
    return <div className="histo histo-empty">No log volume in range</div>;
  }

  return (
    <div>
      <div
        className="histo"
        style={{ height }}
        role="img"
        aria-label="Log volume over time by level"
      >
        {buckets.map((b) => (
          <div
            className="histo-bar"
            key={b.tMs}
            data-testid="histo-bar"
            title={`${formatTimestamp(b.tMs)} — ${b.total} lines`}
          >
            {levels.map((level) => {
              const count = b.counts[level] ?? 0;
              if (count === 0) return null;
              return (
                <i
                  key={level}
                  data-level={level}
                  style={{
                    height: `${Math.max(1, (count / max) * (height - 4))}px`,
                    background: LEVEL_VAR[level],
                  }}
                />
              );
            })}
          </div>
        ))}
      </div>
      <div className="histo-axis">
        <span>{formatTimestamp(buckets[0]!.tMs)}</span>
        <span>{formatTimestamp(buckets[buckets.length - 1]!.tMs)}</span>
      </div>
    </div>
  );
}
