import { describe, expect, it } from "vitest";
import { eventKey, viewSegments } from "./SessionTimeline";
import type { SessionEvent, SessionLogEvent } from "../../api/rumSessionDetail";

function navEvent(overrides: Partial<SessionLogEvent> = {}): SessionLogEvent {
  return {
    kind: "log",
    tsNs: "1",
    lane: "views",
    eventName: "browser.navigation",
    traceId: null,
    urlTemplate: "/checkout",
    urlFull: null,
    vitalName: null,
    vitalRating: null,
    vitalValue: null,
    cssSelector: null,
    tagName: null,
    exceptionType: null,
    exceptionMessage: null,
    exceptionStacktrace: null,
    ...overrides,
  };
}

describe("viewSegments", () => {
  it("spans each navigation from its own timestamp to the next one", () => {
    const nav1 = navEvent({ tsNs: "100", urlTemplate: "/checkout" });
    const nav2 = navEvent({ tsNs: "500", urlTemplate: "/thanks" });
    const segments = viewSegments([nav1, nav2], "900");
    expect(segments).toEqual([
      { event: nav1, route: "/checkout", startNs: "100", endNs: "500" },
      { event: nav2, route: "/thanks", startNs: "500", endNs: "900" },
    ]);
  });

  it("extends the last view to the session end", () => {
    const nav = navEvent({ tsNs: "100" });
    expect(viewSegments([nav], "900")[0]!.endNs).toBe("900");
  });

  it("falls back to url.full, then a dash, when there's no url.template", () => {
    const withFull = navEvent({ urlTemplate: null, urlFull: "https://x/y" });
    expect(viewSegments([withFull], "200")[0]!.route).toBe("https://x/y");
    const bare = navEvent({ urlTemplate: null, urlFull: null });
    expect(viewSegments([bare], "200")[0]!.route).toBe("—");
  });

  it("ignores every event that isn't a navigation", () => {
    const other: SessionEvent = { ...navEvent(), eventName: "exception" };
    expect(viewSegments([other], "200")).toEqual([]);
  });
});

describe("eventKey", () => {
  it("keys a span by its span id", () => {
    const span: SessionEvent = {
      kind: "span",
      tsNs: "1",
      lane: "network",
      traceId: "t",
      spanId: "s1",
      parentSpanId: null,
      name: "GET",
      spanKind: "Client",
      serviceName: "svc",
      durationNs: "1",
      isError: false,
      httpMethod: "GET",
      urlFull: null,
      httpStatusCode: null,
    };
    expect(eventKey(span, 0)).toBe("span:s1");
  });

  it("keys a log by its own index and event name", () => {
    expect(eventKey(navEvent({ tsNs: "42" }), 3)).toBe(
      "log:3:browser.navigation",
    );
  });

  it("gives two same-timestamp, same-name logs distinct keys", () => {
    const a = navEvent({ tsNs: "42", eventName: "browser.web_vital" });
    const b = navEvent({ tsNs: "42", eventName: "browser.web_vital" });
    expect(eventKey(a, 0)).not.toBe(eventKey(b, 1));
  });
});
