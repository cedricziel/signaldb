import { describe, expect, it } from "vitest";
import {
  axisLabelFormatter,
  DEFAULT_RANGE,
  durationToSeconds,
  formatRangeLabel,
  msToNanos,
  nanosToMs,
  parseRangeParam,
  rangeToParam,
  resolveRange,
  resolveStep,
  secondsToDuration,
  stepForRange,
  stepOptionsForRange,
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

describe("axisLabelFormatter", () => {
  // Local time, so build the fixtures from Date parts rather than epoch
  // literals — the assertions must hold in any TZ the suite runs in.
  const at = (y: number, mo: number, d: number, h: number, mi = 0) =>
    new Date(y, mo - 1, d, h, mi).getTime();

  it("stays time-only within a single calendar day", () => {
    const fmt = axisLabelFormatter(at(2026, 8, 8, 9), at(2026, 8, 8, 17));
    expect(fmt(at(2026, 8, 8, 9))).toBe("09:00:00");
    expect(fmt(at(2026, 8, 8, 17))).toBe("17:00:00");
  });

  it("includes the date once the window crosses midnight", () => {
    const fmt = axisLabelFormatter(at(2026, 8, 7, 22), at(2026, 8, 8, 6));
    expect(fmt(at(2026, 8, 7, 22))).toBe("08-07 22:00");
    expect(fmt(at(2026, 8, 8, 6))).toBe("08-08 06:00");
  });

  // The reported defect: a 24h window labelled both ends "09:00:00.000".
  it("gives a 24h window distinct labels at each end", () => {
    const from = at(2026, 8, 7, 9);
    const to = at(2026, 8, 8, 9);
    const fmt = axisLabelFormatter(from, to);
    expect(fmt(from)).not.toBe(fmt(to));
  });

  it("drops to date-only for windows spanning many days", () => {
    const fmt = axisLabelFormatter(at(2026, 8, 1, 0), at(2026, 8, 8, 0));
    expect(fmt(at(2026, 8, 1, 0))).toBe("08-01");
    expect(fmt(at(2026, 8, 8, 0))).toBe("08-08");
  });
});

describe("stepOptionsForRange", () => {
  const hours = (n: number) => ({ fromMs: 0, toMs: n * 3600_000 });

  it("offers only steps that yield a sane bucket count", () => {
    const opts = stepOptionsForRange(hours(1));
    // 1h: 1s would be 3600 buckets, 1h would be 1 — neither is offered.
    expect(opts).not.toContain("1s");
    expect(opts).not.toContain("1h");
    expect(opts).toContain("1m");
    expect(opts).toContain("5m");
  });

  it("scales its offers with the window", () => {
    const opts = stepOptionsForRange(hours(24 * 7));
    expect(opts).not.toContain("1m");
    expect(opts).toContain("1h");
    expect(opts).toContain("6h");
  });

  it("always offers something", () => {
    expect(
      stepOptionsForRange({ fromMs: 0, toMs: 1000 }).length,
    ).toBeGreaterThan(0);
  });
});

describe("resolveStep", () => {
  const range = { fromMs: 0, toMs: 3600_000 };

  it("falls back to the automatic step when unset", () => {
    expect(resolveStep(range, "")).toBe(stepForRange(range));
  });

  it("honours an explicit step that fits the window", () => {
    expect(resolveStep(range, "5m")).toBe("5m");
  });

  // Keeping a fine step across a range change would ask for 10,080 buckets.
  it("falls back when the chosen step no longer fits the window", () => {
    const week = { fromMs: 0, toMs: 7 * 24 * 3600_000 };
    expect(resolveStep(week, "1m")).toBe(stepForRange(week));
  });

  it("falls back on a malformed step", () => {
    expect(resolveStep(range, "banana")).toBe(stepForRange(range));
  });
});
