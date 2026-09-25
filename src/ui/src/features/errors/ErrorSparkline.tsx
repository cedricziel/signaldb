// A compact "occurrences over time" chart for a selected exception group —
// the same shape error-tracking issue views commonly lead with. Unlike
// the full traces volume chart, this is a single, unlabeled series with no
// axis/legend chrome: it exists to show a shape (a spike, a steady trickle);
// the exact value per bucket is one hover away. A thin wrapper over the
// shared `Sparkline`'s bar variant, with bucket padding and the empty-state
// text this view needs kept local.
import {
  bucketizeSeries,
  padBuckets,
  type VolumeSeries,
} from "../../components/SignalHistogram";
import { Sparkline } from "../../components/Sparkline";
import { formatTimeBucket, formatValue } from "../../lib/vizFormat";

interface Props {
  series: VolumeSeries[];
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
}

const HEIGHT = 32;

export function ErrorSparkline({ series, rangeMs, stepMs }: Props) {
  let buckets = bucketizeSeries(series);
  if (buckets.length > 0) {
    buckets = padBuckets(buckets, rangeMs.fromMs, rangeMs.toMs, stepMs);
  }
  if (buckets.length === 0 || buckets.every((b) => b.total === 0)) {
    return (
      <div className="errors-sparkline-empty">
        No occurrences in this window
      </div>
    );
  }
  // A single bucket has no shape to show — the bar variant draws it at full
  // width and full height, an undifferentiated block that reads as a stray
  // rendering artifact rather than a trend. Say so instead of drawing it.
  if (buckets.length === 1) {
    return (
      <div className="errors-sparkline-empty">
        Not enough data in this window to show a trend
      </div>
    );
  }

  return (
    <div className="errors-sparkline-host">
      <Sparkline
        points={buckets.map((b) => ({ x: b.tMs, v: b.total }))}
        variant="bar"
        tone="neutral"
        width="100%"
        height={HEIGHT}
        ariaLabel="Occurrences over time"
        valueLabel="occurrences"
        formatValue={(v) => formatValue(v)}
        formatLabel={(x) => formatTimeBucket(x, stepMs)}
        // A padded, empty bucket has no data under the pointer: no tooltip.
        isFocusable={(p) => p.v > 0}
      />
    </div>
  );
}
