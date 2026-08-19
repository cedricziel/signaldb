// One row's headline metric, at cell size.
//
// A line, not bars: the column charts a level (CPU utilization, memory in
// use), and bars drawn from zero make two very different levels look alike
// while implying the metric counts occurrences. `ErrorSparkline` is the bar
// version, for the counts it is right for.
//
// No axes, no legend, no tooltip: the cell shows a shape, and the entity's
// own page has the readable chart.
interface Props {
  /** Series as the IR envelope carries them — `[timestamp, value]` pairs. */
  series: { points: unknown[][] }[];
  label: string;
}

const WIDTH = 80;
const HEIGHT = 18;

export function EntitySparkline({ series, label }: Props) {
  const values = series
    .flatMap((s) => s.points.map((p) => Number(p[1])))
    .filter((v) => Number.isFinite(v));

  // Nothing to draw is not a zero line: the row's other columns are real
  // measurements, and a flat line would read as one.
  if (values.length < 2) return null;

  const min = Math.min(...values);
  const max = Math.max(...values);
  // A flat series is still a fact — draw it mid-cell rather than dividing by
  // a zero range.
  const span = max - min || 1;
  const step = WIDTH / (values.length - 1);
  const points = values
    .map((v, i) => `${i * step},${HEIGHT - ((v - min) / span) * HEIGHT}`)
    .join(" ");

  return (
    <svg
      className="entity-sparkline"
      width={WIDTH}
      height={HEIGHT}
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      role="img"
      aria-label={`${label} over the selected window`}
      preserveAspectRatio="none"
    >
      <polyline points={points} fill="none" strokeWidth="1" />
    </svg>
  );
}
