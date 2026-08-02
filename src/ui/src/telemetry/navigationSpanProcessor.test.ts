import { describe, expect, it } from "vitest";
import type { Span } from "@opentelemetry/sdk-trace-base";
import { NavigationSpanProcessor } from "./navigationSpanProcessor";

interface FakeSpan {
  name: string;
  setAttribute(key: string, value: unknown): FakeSpan;
  updateName(next: string): FakeSpan;
}

/** Minimal Span stand-in that records attributes and tracks name updates. */
function fakeSpan(name: string): {
  span: FakeSpan;
  attrs: Record<string, unknown>;
} {
  const attrs: Record<string, unknown> = {};
  const span: FakeSpan = {
    name,
    setAttribute(key: string, value: unknown) {
      attrs[key] = value;
      return span;
    },
    updateName(next: string) {
      span.name = next;
      return span;
    },
  };
  return { span, attrs };
}

describe("NavigationSpanProcessor", () => {
  it("collapses the URL-named navigation span to a static low-cardinality name", () => {
    const { span } = fakeSpan(
      "Navigation: /ui?signal=metrics&range=5m&promql=http.server.active_requests",
    );
    new NavigationSpanProcessor().onStart(span as unknown as Span);
    expect(span.name).toBe("Navigation");
  });

  it("moves the URL into url.full / url.path / url.query per OTel semconv", () => {
    const { span, attrs } = fakeSpan("Navigation: /ui?signal=metrics&range=5m");
    new NavigationSpanProcessor().onStart(span as unknown as Span);
    expect(attrs["url.full"]).toBe("/ui?signal=metrics&range=5m");
    expect(attrs["url.path"]).toBe("/ui");
    expect(attrs["url.query"]).toBe("signal=metrics&range=5m");
  });

  it("sets url.path but no url.query when the URL has no query string", () => {
    const { span, attrs } = fakeSpan("Navigation: /ui");
    new NavigationSpanProcessor().onStart(span as unknown as Span);
    expect(span.name).toBe("Navigation");
    expect(attrs["url.full"]).toBe("/ui");
    expect(attrs["url.path"]).toBe("/ui");
    expect(attrs).not.toHaveProperty("url.query");
  });

  it("leaves non-navigation spans untouched", () => {
    const { span, attrs } = fakeSpan("browser.error");
    new NavigationSpanProcessor().onStart(span as unknown as Span);
    expect(span.name).toBe("browser.error");
    expect(attrs).not.toHaveProperty("url.full");
    expect(attrs).not.toHaveProperty("url.path");
  });
});
