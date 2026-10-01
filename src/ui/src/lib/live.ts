import type { Signal } from "./urlState";
import type { TimeRange } from "./time";

/** Signals with no query that a live tail would refresh — the tab renders a
 * static aggregate or a one-shot lookup rather than a tailable stream. */
const NO_LIVE_SIGNALS: Signal[] = ["catalog", "errors", "query"];

/**
 * Whether `signal` can be tailed live over `range`. False for the signals in
 * {@link NO_LIVE_SIGNALS}, and for an absolute range — live mode slides the
 * window forward from "now", which a fixed start/end can't do.
 */
export function supportsLive(signal: Signal, range: TimeRange): boolean {
  return !NO_LIVE_SIGNALS.includes(signal) && range.type !== "absolute";
}

/**
 * TanStack Query's `refetchInterval` for a live-tailed query: `false` (no
 * polling) when `live` is off, otherwise `intervalMs`. Logs passes its own
 * 2s interval — tighter than every other signal's default 15s — because a
 * live log tail reads as broken if a burst of lines takes noticeably longer
 * than a glance to show up.
 */
export function liveRefetchInterval(
  live: boolean,
  intervalMs = 15_000,
): number | false {
  return live ? intervalMs : false;
}
