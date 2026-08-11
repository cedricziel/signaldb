import { describe, expect, it } from "vitest";
import { ROOT_CONTEXT, trace } from "@opentelemetry/api";
import { documentTraceParentContext } from "./documentTraceContext";

const TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
const SPAN_ID = "b7ad6b7169203331";

describe("documentTraceParentContext", () => {
  it("builds a remote parent context from a valid meta tag value", () => {
    const ctx = documentTraceParentContext(
      ROOT_CONTEXT,
      () => `00-${TRACE_ID}-${SPAN_ID}-01`,
    );
    expect(ctx).not.toBeNull();
    const spanContext = trace.getSpanContext(ctx!);
    expect(spanContext).toMatchObject({
      traceId: TRACE_ID,
      spanId: SPAN_ID,
      isRemote: true,
    });
  });

  it("still returns a parent context when the server sampled out (flags 00)", () => {
    // Deliberate: unconditional parenting was chosen accepting that
    // OpenTelemetry JS's default ParentBasedSampler will then drop the whole
    // documentLoad subtree for that page load. See the module doc comment.
    const ctx = documentTraceParentContext(
      ROOT_CONTEXT,
      () => `00-${TRACE_ID}-${SPAN_ID}-00`,
    );
    expect(ctx).not.toBeNull();
    expect(trace.getSpanContext(ctx!)?.traceFlags).toBe(0);
  });

  it("returns null when no meta tag is present", () => {
    expect(documentTraceParentContext(ROOT_CONTEXT, () => null)).toBeNull();
  });

  it("returns null when the meta tag content does not parse", () => {
    expect(
      documentTraceParentContext(ROOT_CONTEXT, () => "garbage"),
    ).toBeNull();
  });

  it("never throws when the provider itself throws", () => {
    expect(() =>
      documentTraceParentContext(ROOT_CONTEXT, () => {
        throw new Error("DOM unavailable");
      }),
    ).not.toThrow();
    expect(
      documentTraceParentContext(ROOT_CONTEXT, () => {
        throw new Error("DOM unavailable");
      }),
    ).toBeNull();
  });

  it("defaults to reading document.querySelector and the active context", () => {
    const meta = document.createElement("meta");
    meta.setAttribute("name", "traceparent");
    meta.setAttribute("content", `00-${TRACE_ID}-${SPAN_ID}-01`);
    document.head.appendChild(meta);
    try {
      const ctx = documentTraceParentContext();
      expect(trace.getSpanContext(ctx!)).toMatchObject({
        traceId: TRACE_ID,
        spanId: SPAN_ID,
      });
    } finally {
      meta.remove();
    }
  });
});
