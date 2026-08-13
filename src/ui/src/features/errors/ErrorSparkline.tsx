// A compact "occurrences over time" chart for a selected exception group —
// the same shape error-tracking issue views commonly lead with. Unlike
// the full traces volume chart, this is a single, unlabeled series with no
// axis/legend chrome: it exists to show a shape (a spike, a steady trickle),
// not exact values per bucket.
import {
  bucketizeSeries,
  padBuckets,
  type VolumeSeries,
} from "../explore/SignalHistogram";

interface Props {
  series: VolumeSeries[];
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
}

const WIDTH = 300;
const HEIGHT = 32;

export function ErrorSparkline({ series, rangeMs, stepMs }: Props) {
  let buckets = bucketizeSeries(series);
  if (buckets.length > 0) {
    buckets = padBuckets(buckets, rangeMs.fromMs, rangeMs.toMs, stepMs);
  }
  if (buckets.length === 0 || buckets.every((b) => b.total === 0)) {
    return (
      <div className="errors-sparkline-empty">No occurrences in range</div>
    );
  }

  const max = Math.max(...buckets.map((b) => b.total));
  const barWidth = WIDTH / buckets.length;

  return (
    <svg
      className="errors-sparkline"
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      role="img"
      aria-label="Occurrences over time"
      preserveAspectRatio="none"
    >
      {buckets.map((b, i) => {
        const h = max > 0 ? (b.total / max) * HEIGHT : 0;
        return (
          <rect
            key={b.tMs}
            data-testid="sparkline-bar"
            className="errors-sparkline-bar"
            x={i * barWidth}
            y={HEIGHT - h}
            width={Math.max(1, barWidth - 1)}
            height={h}
          />
        );
      })}
    </svg>
  );
}
