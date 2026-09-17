/**
 * Shared number/time formatting for the visualization panels, so a value read
 * off any chart tooltip or axis looks the same everywhere.
 */

import { formatDate } from "./time";

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
  let out = `${formatDate(ms)} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
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

/**
 * A rate meant to flag trouble (an error rate, say) as a whole-percent
 * string. Unlike {@link formatShare}, a rate that rounds to zero but isn't
 * exactly zero renders `<1%` rather than a misleadingly clean `0%` — the bug
 * this exists to fix: a nonzero error count among enough traces (1 in 500,
 * say) rounded to "0%" while still carrying the "this had errors" red
 * styling, reading as a contradiction. No measurement at all (`total <= 0`)
 * and a genuinely clean `0` both render as a dash: neither is "0%", they're
 * "nothing to measure" and "measured, and it was zero" respectively, and a
 * dash — not a number — is how this codebase already says "no measurement"
 * elsewhere (see `EntityRed`'s own doc comment).
 */
export function formatErrorRate(part: number, total: number): string {
  if (total <= 0) return "–";
  const rate = part / total;
  if (rate === 0) return "–";
  if (rate < 0.005) return "<1%";
  return `${Math.round(rate * 100)}%`;
}
