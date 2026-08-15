// A compact "occurrences over time" chart for a selected exception group —
// the same shape error-tracking issue views commonly lead with. Unlike
// the full traces volume chart, this is a single, unlabeled series with no
// axis/legend chrome: it exists to show a shape (a spike, a steady trickle);
// the exact value per bucket is one hover away.
import { useRef, useState } from "react";
import {
  bucketizeSeries,
  padBuckets,
  type VolumeSeries,
} from "../explore/SignalHistogram";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import { formatTimeBucket, formatValue } from "../../lib/vizFormat";

interface Props {
  series: VolumeSeries[];
  rangeMs: { fromMs: number; toMs: number };
  stepMs: number;
}

const WIDTH = 300;
const HEIGHT = 32;

export function ErrorSparkline({ series, rangeMs, stepMs }: Props) {
  const rootRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(rootRef);
  const [active, setActive] = useState<number | null>(null);

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
  // A padded, empty bucket has no data under the pointer: no tooltip.
  const activeBucket =
    active === null || (buckets[active]?.total ?? 0) === 0
      ? null
      : buckets[active]!;

  return (
    <div className="errors-sparkline-host viz-host" ref={rootRef}>
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
        {buckets.map((b, i) => (
          // The hit target spans the full height so a one-pixel bucket is as
          // easy to interrogate as the peak.
          <rect
            key={b.tMs}
            data-testid="sparkline-hit"
            className="errors-sparkline-hit"
            x={i * barWidth}
            y={0}
            width={barWidth}
            height={HEIGHT}
            tabIndex={b.total > 0 ? 0 : undefined}
            aria-label={`${formatTimeBucket(b.tMs, stepMs)}: ${formatValue(b.total, "occurrences")}`}
            aria-describedby={
              activeBucket === b ? "errors-sparkline-tip" : undefined
            }
            onPointerMove={(e) => {
              setActive(i);
              pointer.track(e);
            }}
            onPointerLeave={() => {
              setActive((a) => (a === i ? null : a));
              pointer.clear();
            }}
            onFocus={(e) => {
              setActive(i);
              pointer.anchorTo(e.currentTarget);
            }}
            onBlur={() => {
              setActive((a) => (a === i ? null : a));
              pointer.clear();
            }}
          />
        ))}
      </svg>
      {activeBucket && pointer.anchor && (
        <VizTooltip
          id="errors-sparkline-tip"
          anchor={pointer.anchor}
          host={pointer.host}
          title={formatTimeBucket(activeBucket.tMs, stepMs)}
          rows={[
            {
              swatch: "var(--svc-a)",
              label: "occurrences",
              value: formatValue(activeBucket.total),
            },
          ]}
          valueWidthCh={formatValue(max).length}
        />
      )}
    </div>
  );
}
