// Regression coverage for the shared-tooltip fix: `.rum-session-timeline`
// (the tooltip's positioned host) must not scroll — only the inner
// `.rum-session-timeline-scroll` does — so a mark's tooltip on any lane
// never gets clipped by the horizontal scrollbar. See rum.css's own comment
// on `.rum-session-timeline` for why `overflow-x: auto` there would also
// clip the y axis.
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { SessionTimeline } from "./SessionTimeline";
import type { SessionEvent, SessionLogEvent } from "../../api/rumSessionDetail";

const NETWORK_SPAN: SessionEvent = {
  kind: "span",
  tsNs: "1000",
  lane: "network",
  traceId: "t1",
  spanId: "s1",
  parentSpanId: null,
  name: "GET",
  spanKind: "Client",
  serviceName: "storefront-web",
  durationNs: "1000000",
  isError: false,
  httpMethod: "GET",
  urlFull: "https://api.example.com/checkout",
  httpStatusCode: 200,
};

const EVENTS: SessionEvent[] = [NETWORK_SPAN];

function vitalEvent(overrides: Partial<SessionLogEvent> = {}): SessionLogEvent {
  return {
    kind: "log",
    tsNs: "1500",
    lane: "perf",
    eventName: "browser.web_vital",
    traceId: null,
    urlTemplate: null,
    urlFull: null,
    vitalName: "cls",
    vitalRating: "good",
    vitalValue: 0.01,
    cssSelector: null,
    tagName: null,
    exceptionType: null,
    exceptionMessage: null,
    exceptionStacktrace: null,
    ...overrides,
  };
}

describe("SessionTimeline", () => {
  it("hosts its shared tooltip outside the scrolling lane container", () => {
    const { container } = render(
      <SessionTimeline
        events={EVENTS}
        span={{ first: 1000n, last: 2000n }}
        selected={null}
        onSelect={() => {}}
      />,
    );

    const mark = screen.getByRole("button", { name: /GET/ });
    fireEvent.pointerMove(mark, { clientX: 10, clientY: 10 });

    const tooltip = screen.getByRole("tooltip");
    const scroller = container.querySelector(".rum-session-timeline-scroll");
    expect(scroller).not.toBeNull();
    expect(scroller!.contains(tooltip)).toBe(false);

    const timeline = container.querySelector(".rum-session-timeline");
    expect(timeline!.contains(tooltip)).toBe(true);
  });

  it("gives a mark an accessible description beyond just its title", () => {
    render(
      <SessionTimeline
        events={EVENTS}
        span={{ first: 1000n, last: 2000n }}
        selected={null}
        onSelect={() => {}}
      />,
    );

    const mark = screen.getByRole("button", { name: /GET/ });
    expect(mark.getAttribute("aria-label")).toMatch(/Duration/);
    expect(mark.getAttribute("aria-label")).toMatch(/Status/);
  });

  it("renders a request span as a duration-width bar, not a point mark", () => {
    render(
      <SessionTimeline
        events={EVENTS}
        span={{ first: 1000n, last: 2000n }}
        selected={null}
        onSelect={() => {}}
      />,
    );

    const mark = screen.getByRole("button", { name: /GET/ }) as HTMLElement;
    expect(mark.className).toContain("segment");
    expect(mark.style.width).not.toBe("");
  });

  it("selects only the clicked event when two logs share a timestamp and name", () => {
    const first = vitalEvent({ vitalName: "cls" });
    const second = vitalEvent({ vitalName: "cls" });
    const events = [NETWORK_SPAN, first, second];
    let selected: SessionEvent | null = null;
    const { container, rerender } = render(
      <SessionTimeline
        events={events}
        span={{ first: 1000n, last: 2000n }}
        selected={selected}
        onSelect={(e) => {
          selected = e;
        }}
      />,
    );

    const marks = container
      .querySelectorAll(".rum-session-lane-track")[3]!
      .querySelectorAll("button");
    expect(marks).toHaveLength(2);
    fireEvent.click(marks[0]!);
    rerender(
      <SessionTimeline
        events={events}
        span={{ first: 1000n, last: 2000n }}
        selected={selected}
        onSelect={(e) => {
          selected = e;
        }}
      />,
    );

    const onMarks = container.querySelectorAll(".rum-session-mark.on");
    expect(onMarks).toHaveLength(1);
    expect(onMarks[0]).toBe(marks[0]);
  });
});
