import { describe, expect, it } from "vitest";
import { parseTraceparent, serverTraceContextFromEntry } from "./serverTiming";

function entryWith(desc: string | null): {
  serverTiming?: Array<{ name: string; description: string; duration: number }>;
} {
  if (desc === null) return {};
  return {
    serverTiming: [
      { name: "total", description: "", duration: 1.2 },
      { name: "traceparent", description: desc, duration: 0 },
    ],
  };
}

const TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
const SPAN_ID = "b7ad6b7169203331";

describe("serverTraceContextFromEntry", () => {
  it("parses a valid sampled traceparent entry", () => {
    const ctx = serverTraceContextFromEntry(
      entryWith(`00-${TRACE_ID}-${SPAN_ID}-01`),
    );
    expect(ctx).toEqual({
      traceId: TRACE_ID,
      spanId: SPAN_ID,
      traceFlags: 1,
      sampled: true,
    });
  });

  it("preserves unsampled trace flags", () => {
    const ctx = serverTraceContextFromEntry(
      entryWith(`00-${TRACE_ID}-${SPAN_ID}-00`),
    );
    expect(ctx).toMatchObject({ traceFlags: 0, sampled: false });
  });

  it("returns null when the entry has no serverTiming list", () => {
    expect(serverTraceContextFromEntry(entryWith(null))).toBeNull();
    expect(serverTraceContextFromEntry({ serverTiming: [] })).toBeNull();
  });

  it("returns null when no traceparent entry is present", () => {
    expect(
      serverTraceContextFromEntry({
        serverTiming: [{ name: "total", description: "", duration: 3 }],
      }),
    ).toBeNull();
  });

  it("rejects malformed descriptions", () => {
    for (const desc of [
      "garbage",
      "",
      `00-${TRACE_ID}-${SPAN_ID}`, // missing flags
      `00-${TRACE_ID.slice(1)}-${SPAN_ID}-01`, // short trace id
      `00-${TRACE_ID}-${SPAN_ID.slice(1)}-01`, // short span id
      `00-${TRACE_ID.toUpperCase()}-${SPAN_ID}-01`, // uppercase is invalid
      `ff-${TRACE_ID}-${SPAN_ID}-01`, // unknown version
      `00-${TRACE_ID}-${SPAN_ID}-01-extra`,
    ]) {
      expect(serverTraceContextFromEntry(entryWith(desc)), desc).toBeNull();
    }
  });

  it("rejects all-zero trace and span ids", () => {
    expect(
      serverTraceContextFromEntry(
        entryWith(`00-${"0".repeat(32)}-${SPAN_ID}-01`),
      ),
    ).toBeNull();
    expect(
      serverTraceContextFromEntry(
        entryWith(`00-${TRACE_ID}-${"0".repeat(16)}-01`),
      ),
    ).toBeNull();
  });
});

describe("parseTraceparent", () => {
  it("parses a valid sampled traceparent value", () => {
    expect(parseTraceparent(`00-${TRACE_ID}-${SPAN_ID}-01`)).toEqual({
      traceId: TRACE_ID,
      spanId: SPAN_ID,
      traceFlags: 1,
      sampled: true,
    });
  });

  it("returns null for null, undefined, and malformed values", () => {
    for (const value of [
      null,
      undefined,
      "garbage",
      "",
      `00-${TRACE_ID}-${SPAN_ID}`,
      `00-${"0".repeat(32)}-${SPAN_ID}-01`,
      `00-${TRACE_ID}-${"0".repeat(16)}-01`,
    ]) {
      expect(parseTraceparent(value), String(value)).toBeNull();
    }
  });

  it("backs serverTraceContextFromEntry (same parsing rules)", () => {
    const value = `00-${TRACE_ID}-${SPAN_ID}-01`;
    expect(serverTraceContextFromEntry(entryWith(value))).toEqual(
      parseTraceparent(value),
    );
  });
});
