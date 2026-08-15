/**
 * Shared number/time formatting for the visualization panels, so a value read
 * off any chart tooltip or axis looks the same everywhere.
 */

const NUM = new Intl.NumberFormat();
const FRACTION = new Intl.NumberFormat(undefined, {
  maximumSignificantDigits: 3,
});
const FRACTION_LARGE = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 2,
});

/**
 * Abbreviate a count for an axis gridline, where width is scarce.
 *
 * Deliberately not `Intl`'s `notation: "compact"`: that is locale-dependent
 * and in some locales (`de`, for one) does not abbreviate at all, which both
 * overflows the axis and makes the result untestable. One decimal below ten of
 * a unit (`1.5K`), none above it (`373K`).
 */
export function compactCount(n: number): string {
  const units: [number, string][] = [
    [1e9, "B"],
    [1e6, "M"],
    [1e3, "K"],
  ];
  for (const [scale, suffix] of units) {
    if (n >= scale) {
      const v = n / scale;
      return `${v < 10 ? Math.round(v * 10) / 10 : Math.round(v)}${suffix}`;
    }
  }
  return String(Math.round(n));
}

const pad = (n: number, w = 2) => String(n).padStart(w, "0");

/**
 * An absolute timestamp at the panel's resolution: always date and time to
 * the minute, plus seconds below a minute of resolution and milliseconds
 * below a second — never more precision than the bucket can carry.
 */
export function formatTimestamp(ms: number, resolutionMs: number): string {
  const d = new Date(ms);
  let out = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  if (resolutionMs < 60_000) out += `:${pad(d.getSeconds())}`;
  if (resolutionMs < 1000) out += `.${pad(d.getMilliseconds(), 3)}`;
  return out;
}

/**
 * A time bucket as `start – end`, dropping the date from the end when it is
 * the same calendar day as the start.
 */
export function formatTimeBucket(startMs: number, stepMs: number): string {
  const start = formatTimestamp(startMs, stepMs);
  const end = formatTimestamp(startMs + stepMs, stepMs);
  const sameDay = start.slice(0, 10) === end.slice(0, 10);
  return `${start} – ${sameDay ? end.slice(11) : end}`;
}

/**
 * A value with its unit. Integers group thousands; fractions keep three
 * significant digits (two decimals once past a thousand). Missing values
 * render as an en dash so a series with a gap still gets a row.
 */
export function formatValue(v: number | null | undefined, unit = ""): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "–";
  const text = Number.isInteger(v)
    ? NUM.format(v)
    : Math.abs(v) >= 1000
      ? FRACTION_LARGE.format(v)
      : FRACTION.format(v);
  return unit ? `${text} ${unit}` : text;
}

/** A bucket range; an open upper bound reads as `lo+`. */
export function formatRange(
  lo: number,
  hi: number | undefined,
  unit = "",
): string {
  if (hi === undefined) return `${formatValue(lo, unit)}+`;
  return `${formatValue(lo, unit)} – ${formatValue(hi, unit)}`;
}

/** A part of a total as a percentage with one decimal; `0%` for no total. */
export function formatShare(part: number, total: number): string {
  if (total <= 0) return "0%";
  return `${((part / total) * 100).toFixed(1)}%`;
}
