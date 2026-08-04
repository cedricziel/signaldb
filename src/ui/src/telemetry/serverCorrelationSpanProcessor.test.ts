import { describe, expect, it } from "vitest";
import type { Link } from "@opentelemetry/api";
import type { Span } from "@opentelemetry/sdk-trace-base";
import { ServerCorrelationSpanProcessor } from "./serverCorrelationSpanProcessor";

const TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
const SPAN_ID = "b7ad6b7169203331";

interface FakeSpan {
  name: string;
  links: Link[];
  addLink(link: Link): FakeSpan;
}

function fakeSpan(name: string): FakeSpan {
  const span: FakeSpan = {
    name,
    links: [],
    addLink(link: Link) {
      span.links.push(link);
      return span;
    },
  };
  return span;
}

function navigationEntry(desc: string) {
  return {
    serverTiming: [{ name: "traceparent", description: desc, duration: 0 }],
  };
}

describe("ServerCorrelationSpanProcessor", () => {
  it("links the documentLoad span to the server-returned trace context", () => {
    const processor = new ServerCorrelationSpanProcessor(() =>
      navigationEntry(`00-${TRACE_ID}-${SPAN_ID}-01`),
    );
    const span = fakeSpan("documentLoad");
    processor.onStart(span as unknown as Span);
    expect(span.links).toHaveLength(1);
    expect(span.links[0]?.context).toMatchObject({
      traceId: TRACE_ID,
      spanId: SPAN_ID,
      traceFlags: 1,
    });
  });

  it("still records the link when the server sampled out (flags 00)", () => {
    const processor = new ServerCorrelationSpanProcessor(() =>
      navigationEntry(`00-${TRACE_ID}-${SPAN_ID}-00`),
    );
    const span = fakeSpan("documentLoad");
    processor.onStart(span as unknown as Span);
    // A link to an unexported span is harmless; a parent would not be. The
    // processor must never parent, only link.
    expect(span.links).toHaveLength(1);
    expect(span.links[0]?.context).toMatchObject({ traceFlags: 0 });
  });

  it("ignores spans other than documentLoad", () => {
    const processor = new ServerCorrelationSpanProcessor(() =>
      navigationEntry(`00-${TRACE_ID}-${SPAN_ID}-01`),
    );
    const span = fakeSpan("documentFetch");
    processor.onStart(span as unknown as Span);
    expect(span.links).toHaveLength(0);
  });

  it("is a no-op without a navigation entry or traceparent metric", () => {
    for (const provider of [
      () => undefined,
      () => ({}),
      () => ({ serverTiming: [] }),
      () => navigationEntry("garbage"),
    ]) {
      const processor = new ServerCorrelationSpanProcessor(
        provider as ConstructorParameters<
          typeof ServerCorrelationSpanProcessor
        >[0],
      );
      const span = fakeSpan("documentLoad");
      expect(() => processor.onStart(span as unknown as Span)).not.toThrow();
      expect(span.links).toHaveLength(0);
    }
  });

  it("never throws when the entry provider itself throws", () => {
    const processor = new ServerCorrelationSpanProcessor(() => {
      throw new Error("performance API unavailable");
    });
    const span = fakeSpan("documentLoad");
    expect(() => processor.onStart(span as unknown as Span)).not.toThrow();
    expect(span.links).toHaveLength(0);
  });
});
