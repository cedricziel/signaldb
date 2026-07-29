import { describe, expect, it } from "vitest";
import { buildSearch, DEFAULT_STATE, parseExploreState } from "./urlState";

describe("parseExploreState", () => {
  it("returns defaults for an empty search string", () => {
    expect(parseExploreState("")).toEqual(DEFAULT_STATE);
  });

  it("parses a fully-populated URL", () => {
    const state = parseExploreState(
      "?signal=traces&range=15m&f=level%7C%3D%7Cerror&q=timeout&limit=100&live=1&trace=abc123&promql=up",
    );
    expect(state.signal).toBe("traces");
    expect(state.range).toEqual({ type: "relative", seconds: 900 });
    expect(state.filters).toEqual([
      { label: "level", op: "=", value: "error" },
    ]);
    expect(state.search).toBe("timeout");
    expect(state.limit).toBe(100);
    expect(state.live).toBe(true);
    expect(state.trace).toBe("abc123");
    expect(state.promql).toBe("up");
  });

  it("ignores invalid signals, limits, and filters", () => {
    const state = parseExploreState("?signal=bogus&limit=-5&f=not-a-filter");
    expect(state.signal).toBe("logs");
    expect(state.limit).toBe(500);
    expect(state.filters).toEqual([]);
  });

  it("caps absurd limits", () => {
    expect(parseExploreState("?limit=999999").limit).toBe(5000);
  });
});

describe("buildSearch", () => {
  it("emits nothing for the default state", () => {
    expect(buildSearch(DEFAULT_STATE)).toBe("");
  });

  it("round-trips through parseExploreState", () => {
    const state = {
      ...DEFAULT_STATE,
      signal: "metrics" as const,
      range: { type: "absolute" as const, fromMs: 1000, toMs: 2000 },
      filters: [
        { label: "service_name", op: "=" as const, value: "checkout" },
        { label: "level", op: "!=" as const, value: "debug" },
      ],
      search: "a b",
      raw: '{x="y"}',
      limit: 1000,
      live: true,
      trace: "deadbeef",
      promql: "rate(x[5m])",
    };
    expect(parseExploreState(buildSearch(state))).toEqual(state);
  });
});
