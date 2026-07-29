import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { seriesName, type PromSeries } from "../../api/prom";
import { alignSeries, seriesColorVar } from "../../lib/promSeries";

interface Props {
  series: PromSeries[];
  height?: number;
}

function cssColor(varExpr: string, el: HTMLElement): string {
  const name = /var\((--[a-z0-9-]+)\)/i.exec(varExpr)?.[1];
  if (!name) return varExpr;
  return getComputedStyle(el).getPropertyValue(name).trim() || "#888";
}

export function MetricsChart({ series, height = 260 }: Props) {
  const hostRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const host = hostRef.current;
    if (!host || series.length === 0) return;

    const data = alignSeries(series) as uPlot.AlignedData;
    const make = () =>
      new uPlot(
        {
          width: host.clientWidth || 800,
          height,
          // Timestamps are already in ms.
          ms: 1,
          series: [
            {},
            ...series.map((s, i) => ({
              label: seriesName(s.labels),
              stroke: cssColor(seriesColorVar(i), host),
              width: 1.5,
              points: { show: false },
            })),
          ],
          axes: [
            { stroke: cssColor("var(--dim)", host) },
            { stroke: cssColor("var(--dim)", host) },
          ],
          legend: { show: false },
        },
        data,
        host,
      );

    let plot = make();
    const onResize = () => {
      plot.destroy();
      plot = make();
    };
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      plot.destroy();
    };
  }, [series, height]);

  return <div ref={hostRef} data-testid="metrics-chart" />;
}
