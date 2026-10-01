// The session detail's lane timeline: six tracks (Views, Actions, Network,
// Perf, Errors, Logs) spanning the session's own first-to-last record.
// `browser.navigation` events render as Views-lane segments — a page view
// lasts from its own navigation to the next one (or the session's last
// record) — everything else as a single mark. Every mark and segment shows
// its values through the shared `VizTooltip` on hover/focus
// (`explore-ui-viz-tooltips`); clicking selects it, same as a row in the
// event list below.
import { useRef, useState } from "react";
import {
  useVizPointer,
  VizTooltip,
  type VizTooltipRow,
} from "../../components/VizTooltip";
import { formatTimestamp } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";
import type { SessionEvent, SessionLogEvent } from "../../api/rumSessionDetail";
import {
  SESSION_LANE_LABELS,
  SESSION_LANES,
  sessionEventLabel,
} from "./rumModel";

/** `index` is the event's own position in the full merged event list (not
 * the filtered per-lane subset) — a span already has a unique `spanId`, but
 * several logs (e.g. web vitals flushed together) can share the same
 * timestamp and event name, so the index is what makes a log's key unique. */
export function eventKey(event: SessionEvent, index: number): string {
  return event.kind === "span"
    ? `span:${event.spanId}`
    : `log:${index}:${event.eventName}`;
}

export interface ViewSegment {
  /** The `browser.navigation` event that started this page view — the
   * segment selects this event, same as any other mark. */
  event: SessionLogEvent;
  route: string;
  startNs: string;
  endNs: string;
}

function isPageView(event: SessionEvent): event is SessionLogEvent {
  return event.kind === "log" && event.eventName === "browser.navigation";
}

/** Each page view as a segment from its own navigation to the next one, or
 * to `sessionEndNs` for the last (still-open) view — the spec's timeline
 * showing page views as spans, and the same boundary the exception panel's
 * "preceded it in the same page view" reasoning assumes. */
export function viewSegments(
  events: SessionEvent[],
  sessionEndNs: string,
): ViewSegment[] {
  const navigations = events.filter(isPageView);
  return navigations.map((nav, i) => ({
    event: nav,
    route: nav.urlTemplate ?? nav.urlFull ?? "—",
    startNs: nav.tsNs,
    endNs: navigations[i + 1]?.tsNs ?? sessionEndNs,
  }));
}

function tsMs(ns: string): number {
  return Math.round(Number(BigInt(ns || "0")) / 1_000_000);
}

interface TooltipContent {
  title: string;
  rows: VizTooltipRow[];
}

/** A mark's accessible name: the tooltip's title plus its rows (time,
 * duration, status, …), so a screen reader announces the same values a
 * sighted user gets from hovering — not just the bare title. */
function describeTooltip(tooltip: TooltipContent): string {
  const rows = tooltip.rows.map((r) => `${r.label}: ${r.value}`);
  return [tooltip.title, ...rows].join(", ");
}

/** The tooltip content for a single-point mark (every lane but Views, which
 * renders segments instead — see `segmentTooltip`). */
function markTooltip(event: SessionEvent): TooltipContent {
  const rows: VizTooltipRow[] = [
    { label: "Time", value: formatTimestamp(tsMs(event.tsNs)) },
  ];
  if (event.kind === "span") {
    rows.push({
      label: "Duration",
      value: formatDurationMs(Number(BigInt(event.durationNs || "0")) / 1e6),
    });
    if (event.httpStatusCode !== null) {
      rows.push({ label: "Status", value: String(event.httpStatusCode) });
    }
  }
  return { title: sessionEventLabel(event), rows };
}

function segmentTooltip(segment: ViewSegment): TooltipContent {
  const durationMs = tsMs(segment.endNs) - tsMs(segment.startNs);
  return {
    title: segment.route,
    rows: [
      { label: "Started", value: formatTimestamp(tsMs(segment.startNs)) },
      { label: "Duration", value: formatDurationMs(Math.max(0, durationMs)) },
    ],
  };
}

/** One interactive mark (or, given a `widthPct`, a segment) on the
 * timeline. Hover/focus is reported up to `SessionTimeline`'s own shared
 * `VizTooltip` rather than each mark hosting its own: a mark this small
 * (6×14 px) sitting inside the horizontally-scrolling lane track would
 * clip its own tooltip on lower lanes, and give the track a hover-only
 * vertical scrollbar, if it were the tooltip's positioned ancestor (see
 * `.rum-session-timeline`'s own comment in rum.css). */
function LaneMark({
  leftPct,
  widthPct,
  tooltip,
  selected,
  onSelect,
  onHover,
  onHoverEnd,
}: {
  leftPct: number;
  widthPct?: number;
  tooltip: TooltipContent;
  selected: boolean;
  onSelect: () => void;
  onHover: (e: { clientX: number; clientY: number } | Element) => void;
  onHoverEnd: () => void;
}) {
  const isBar = widthPct !== undefined;
  const classes = ["rum-session-mark"];
  if (isBar) classes.push("segment");
  if (selected) classes.push("on");
  return (
    <button
      type="button"
      className={classes.join(" ")}
      aria-label={describeTooltip(tooltip)}
      style={{
        left: `${leftPct}%`,
        width: isBar ? `${widthPct}%` : undefined,
      }}
      onPointerMove={onHover}
      onPointerLeave={onHoverEnd}
      onFocus={(e) => onHover(e.currentTarget)}
      onBlur={onHoverEnd}
      onClick={onSelect}
    />
  );
}

export function SessionTimeline({
  events,
  span,
  selected,
  onSelect,
}: {
  events: SessionEvent[];
  span: { first: bigint; last: bigint };
  selected: SessionEvent | null;
  onSelect: (event: SessionEvent) => void;
}) {
  // Keyed by the event's own index in the full merged list (not the
  // filtered per-lane position) so `eventKey` can disambiguate same-
  // timestamp logs — see its own doc comment.
  const byLane = new Map<string, { event: SessionEvent; index: number }[]>();
  for (const lane of SESSION_LANES) byLane.set(lane, []);
  events.forEach((event, index) => {
    if (isPageView(event)) return; // rendered as segments below
    byLane.get(event.lane)?.push({ event, index });
  });

  const totalNs = span.last - span.first;
  const offsetPct = (ns: string): number => {
    if (totalNs <= 0n) return 0;
    const offset = BigInt(ns || "0") - span.first;
    return Number((offset * 1000n) / totalNs) / 10;
  };
  const spanPct = (startNs: string, endNs: string): number => {
    if (totalNs <= 0n) return 0;
    const dur = BigInt(endNs || "0") - BigInt(startNs || "0");
    return Math.max(0.5, Number((dur * 1000n) / totalNs) / 10);
  };
  /** A request span's own duration as a bar width, like a view segment's —
   * `undefined` (a point mark) only for a span with no recorded duration. */
  const durationWidthPct = (event: SessionEvent): number | undefined => {
    if (event.kind !== "span") return undefined;
    const durNs = BigInt(event.durationNs || "0");
    if (durNs <= 0n) return undefined;
    const endNs = (BigInt(event.tsNs || "0") + durNs).toString();
    return spanPct(event.tsNs, endNs);
  };

  const segments = viewSegments(events, span.last.toString());
  const selectedIndex = selected ? events.indexOf(selected) : -1;
  const selectedKey =
    selected && selectedIndex >= 0 ? eventKey(selected, selectedIndex) : null;

  // One shared pointer/tooltip for the whole timeline (see `LaneMark`'s own
  // doc comment for why, and `.rum-session-timeline`'s CSS comment): the
  // host is this non-scrolling outer element, so the tooltip's positioned
  // ancestor is never clipped by the inner element's horizontal scrollbar.
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [hovered, setHovered] = useState<TooltipContent | null>(null);

  const hoverProps = (tooltip: TooltipContent) => ({
    onHover: (e: { clientX: number; clientY: number } | Element) => {
      if (e instanceof Element) pointer.anchorTo(e);
      else pointer.track(e);
      setHovered(tooltip);
    },
    onHoverEnd: () => {
      pointer.clear();
      setHovered(null);
    },
  });

  return (
    <div
      className="rum-session-timeline"
      data-testid="rum-session-timeline"
      ref={hostRef}
    >
      <div className="rum-session-timeline-scroll">
        <div className="rum-session-timeline-content">
          {SESSION_LANES.map((lane) => (
            <div className="rum-session-lane" key={lane}>
              <span className="rum-session-lane-label">
                {SESSION_LANE_LABELS[lane]}
              </span>
              <div className="rum-session-lane-track">
                {lane === "views"
                  ? segments.map((segment) => {
                      const key = eventKey(
                        segment.event,
                        events.indexOf(segment.event),
                      );
                      const tooltip = segmentTooltip(segment);
                      return (
                        <LaneMark
                          key={key}
                          leftPct={offsetPct(segment.startNs)}
                          widthPct={spanPct(segment.startNs, segment.endNs)}
                          tooltip={tooltip}
                          selected={selectedKey === key}
                          onSelect={() => onSelect(segment.event)}
                          {...hoverProps(tooltip)}
                        />
                      );
                    })
                  : (byLane.get(lane) ?? []).map(({ event, index }) => {
                      const key = eventKey(event, index);
                      const tooltip = markTooltip(event);
                      return (
                        <LaneMark
                          key={key}
                          leftPct={offsetPct(event.tsNs)}
                          widthPct={durationWidthPct(event)}
                          tooltip={tooltip}
                          selected={selectedKey === key}
                          onSelect={() => onSelect(event)}
                          {...hoverProps(tooltip)}
                        />
                      );
                    })}
              </div>
            </div>
          ))}
        </div>
      </div>
      {pointer.anchor && hovered && (
        <VizTooltip
          anchor={pointer.anchor}
          host={pointer.host}
          title={hovered.title}
          rows={hovered.rows}
        />
      )}
    </div>
  );
}
