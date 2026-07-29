import { describe, expect, it } from "vitest";
import {
  DEFAULT_RANGE,
  durationToSeconds,
  formatRangeLabel,
  msToNanos,
  nanosToMs,
  parseRangeParam,
  rangeToParam,
  resolveRange,
  secondsToDuration,
  stepForRange,
} from "./time";

describe("resolveRange", () => {
  it("resolves a relative range against now", () => {
    const now = 1_000_000_000;
    expect(resolveRange({ type: "relative", seconds: 60 }, now)).toEqual({
      fromMs: now - 60_000,
      toMs: now,
    });
  });

  it("passes an absolute range through unchanged", () => {
    const r = { type: "absolute", fromMs: 100, toMs: 200 } as const;
    expect(resolveRange(r, 999)).toEqual({ fromMs: 100, toMs: 200 });
  });
});

describe("nanosecond conversion", () => {
  it("round-trips milliseconds through nanos", () => {
    expect(msToNanos(1234)).toBe("1234000000");
    expect(nanosToMs("1234000000")).toBe(1234);
  });

  it("handles realistic epoch timestamps without precision loss", () => {
    const ms = 1_753_776_000_123;
    expect(nanosToMs(msToNanos(ms))).toBe(ms);
  });
});

describe("range URL params", () => {
  it("round-trips relative ranges", () => {
    const r = { type: "relative", seconds: 900 } as const;
    expect(rangeToParam(r)).toBe("15m");
    expect(parseRangeParam("15m")).toEqual(r);
  });

  it("round-trips absolute ranges", () => {
    const r = { type: "absolute", fromMs: 1000, toMs: 2000 } as const;
    expect(parseRangeParam(rangeToParam(r))).toEqual(r);
  });

  it("falls back to the default for garbage input", () => {
    expect(parseRangeParam(null)).toEqual(DEFAULT_RANGE);
    expect(parseRangeParam("banana")).toEqual(DEFAULT_RANGE);
    expect(parseRangeParam("2000-1000")).toEqual(DEFAULT_RANGE);
    expect(parseRangeParam("0m")).toEqual(DEFAULT_RANGE);
  });
});

describe("durations", () => {
  it("formats seconds to the largest clean unit", () => {
    expect(secondsToDuration(45)).toBe("45s");
    expect(secondsToDuration(300)).toBe("5m");
    expect(secondsToDuration(7200)).toBe("2h");
    expect(secondsToDuration(172800)).toBe("2d");
  });

  it("parses duration strings and rejects invalid ones", () => {
    expect(durationToSeconds("90s")).toBe(90);
    expect(durationToSeconds("3h")).toBe(10800);
    expect(durationToSeconds("x")).toBeNull();
    expect(durationToSeconds("1.5h")).toBeNull();
  });
});

describe("stepForRange", () => {
  it("targets roughly the requested bucket count", () => {
    const step = stepForRange({ fromMs: 0, toMs: 3600_000 }, 45);
    // 3600s / 45 = 80s → snaps up to 120s.
    expect(step).toBe("2m");
  });

  it("never goes below one second", () => {
    expect(stepForRange({ fromMs: 0, toMs: 1000 }, 45)).toBe("1s");
  });
});

describe("formatRangeLabel", () => {
  it("uses preset labels when available", () => {
    expect(formatRangeLabel({ type: "relative", seconds: 3600 })).toBe(
      "Last 1 h",
    );
  });

  it("falls back to a duration for non-preset ranges", () => {
    expect(formatRangeLabel({ type: "relative", seconds: 120 })).toBe(
      "Last 2m",
    );
  });
});
