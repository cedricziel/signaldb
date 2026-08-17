import { describe, expect, it } from "vitest";
import type { TempoSpan } from "../api/tempo";
import { buildWaterfall, formatDurationMs } from "./waterfall";

const span = (over: Partial<TempoSpan>): TempoSpan => ({
  spanId: "s",
  parentSpanId: null,
  name: "op",
  serviceName: "svc",
  status: "ok",
  startNs: "0",
  durNs: "1000000",
  attributes: {},
  events: [],
  ...over,
});

describe("buildWaterfall", () => {
  it("orders rows depth-first with children under parents", () => {
    const rows = buildWaterfall([
      span({ spanId: "child-late", parentSpanId: "root", startNs: "500" }),
      span({ spanId: "root", startNs: "0", durNs: "1000" }),
      span({ spanId: "child-early", parentSpanId: "root", startNs: "100" }),
      span({
        spanId: "grandchild",
        parentSpanId: "child-early",
        startNs: "150",
      }),
    ]).rows;
    expect(rows.map((r) => r.span.spanId)).toEqual([
      "root",
      "child-early",
      "grandchild",
      "child-late",
    ]);
    expect(rows.map((r) => r.depth)).toEqual([0, 1, 2, 1]);
  });

  it("treats spans with missing parents as roots", () => {
    const rows = buildWaterfall([
      span({ spanId: "orphan", parentSpanId: "not-in-payload" }),
      span({ spanId: "root" }),
    ]).rows;
    expect(rows).toHaveLength(2);
    expect(rows.every((r) => r.depth === 0)).toBe(true);
  });

  it("computes bar geometry as fractions of the trace duration", () => {
    const { rows } = buildWaterfall([
      span({ spanId: "root", startNs: "1000000000", durNs: "1000000000" }),
      span({
        spanId: "half",
        parentSpanId: "root",
        startNs: "1500000000",
        durNs: "250000000",
      }),
    ]);
    const half = rows.find((r) => r.span.spanId === "half")!;
    expect(half.leftPct).toBeCloseTo(50, 1);
    expect(half.widthPct).toBeCloseTo(25, 1);
    expect(half.startOffsetMs).toBeCloseTo(500);
    expect(half.durationMs).toBeCloseTo(250);
  });

  it("stretches a zero-duration parent's bar over its subtree", () => {
    // A parent with no recorded end (durNs 0 — e.g. a browser root span that
    // was never ended) would otherwise draw as a sliver; it covers whatever
    // its children cover instead. For the root that is the whole trace.
    const { rows } = buildWaterfall([
      span({ spanId: "root", startNs: "1000000000", durNs: "0" }),
      span({
        spanId: "a",
        parentSpanId: "root",
        startNs: "1000000000",
        durNs: "400000000",
      }),
      span({
        spanId: "b",
        parentSpanId: "root",
        startNs: "1600000000",
        durNs: "400000000",
      }),
    ]);
    const root = rows.find((r) => r.span.spanId === "root")!;
    expect(root.leftPct).toBe(0);
    expect(root.widthPct).toBeCloseTo(100, 1);
    expect(root.extentInferred).toBe(true);
    // Its own recorded duration is still what it was.
    expect(root.durationMs).toBe(0);
    // A zero-duration leaf stays a sliver, not inferred.
    const { rows: leafRows } = buildWaterfall([
      span({ spanId: "root", startNs: "1000000000", durNs: "1000000000" }),
      span({
        spanId: "mark",
        parentSpanId: "root",
        startNs: "1500000000",
        durNs: "0",
      }),
    ]);
    const mark = leafRows.find((r) => r.span.spanId === "mark")!;
    expect(mark.widthPct).toBe(0.3);
    expect(mark.extentInferred).toBe(false);
  });

  it("handles nanosecond timestamps beyond float precision", () => {
    const base = 1_753_776_000_123_456_789n;
    const { rows, traceDurationNs } = buildWaterfall([
      span({ spanId: "a", startNs: String(base), durNs: "2000000" }),
      span({
        spanId: "b",
        parentSpanId: "a",
        startNs: String(base + 1_000_000n),
        durNs: "1000000",
      }),
    ]);
    expect(traceDurationNs).toBe(2_000_000n);
    expect(rows.find((r) => r.span.spanId === "b")!.leftPct).toBeCloseTo(50, 1);
  });

  it("counts errors and collects services", () => {
    const wf = buildWaterfall([
      span({ spanId: "a", serviceName: "gateway" }),
      span({ spanId: "b", serviceName: "payments", status: "error" }),
      span({ spanId: "c", serviceName: "payments" }),
    ]);
    expect(wf.errorCount).toBe(1);
    expect(wf.services.sort()).toEqual(["gateway", "payments"]);
  });

  it("returns an empty waterfall for no spans", () => {
    expect(buildWaterfall([]).rows).toEqual([]);
  });
});

describe("formatDurationMs", () => {
  it("chooses sensible units", () => {
    expect(formatDurationMs(2500)).toBe("2.50 s");
    expect(formatDurationMs(412)).toBe("412 ms");
    expect(formatDurationMs(4.2)).toBe("4.2 ms");
    expect(formatDurationMs(0.25)).toBe("250 µs");
  });
});
