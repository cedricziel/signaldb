import { describe, expect, it } from "vitest";
import { liveRefetchInterval, supportsLive } from "./live";
import { DEFAULT_RANGE, type TimeRange } from "./time";

const ABSOLUTE_RANGE: TimeRange = { type: "absolute", fromMs: 0, toMs: 1 };

describe("supportsLive", () => {
  it("is true for a tailable signal over a relative range", () => {
    expect(supportsLive("logs", DEFAULT_RANGE)).toBe(true);
    expect(supportsLive("traces", DEFAULT_RANGE)).toBe(true);
    expect(supportsLive("metrics", DEFAULT_RANGE)).toBe(true);
    expect(supportsLive("profiles", DEFAULT_RANGE)).toBe(true);
  });

  it("is false for catalog, errors, and query regardless of range", () => {
    expect(supportsLive("catalog", DEFAULT_RANGE)).toBe(false);
    expect(supportsLive("errors", DEFAULT_RANGE)).toBe(false);
    expect(supportsLive("query", DEFAULT_RANGE)).toBe(false);
  });

  it("is false for an absolute range even on a tailable signal", () => {
    expect(supportsLive("logs", ABSOLUTE_RANGE)).toBe(false);
  });
});

describe("liveRefetchInterval", () => {
  it("returns false when live is off", () => {
    expect(liveRefetchInterval(false)).toBe(false);
    expect(liveRefetchInterval(false, 2_000)).toBe(false);
  });

  it("defaults to a 15s interval when live is on", () => {
    expect(liveRefetchInterval(true)).toBe(15_000);
  });

  it("honors a custom interval when live is on", () => {
    expect(liveRefetchInterval(true, 2_000)).toBe(2_000);
  });
});
