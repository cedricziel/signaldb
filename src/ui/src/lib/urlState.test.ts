import { describe, expect, it } from "vitest";
import {
  buildSearch,
  DEFAULT_STATE,
  parseExploreState,
  signalFromParam,
} from "./urlState";

describe("signalFromParam", () => {
  it("passes through known signals", () => {
    expect(signalFromParam("traces")).toBe("traces");
    expect(signalFromParam("metrics")).toBe("metrics");
  });

  it("defaults unknown or missing segments to logs", () => {
    expect(signalFromParam("bogus")).toBe("logs");
    expect(signalFromParam(undefined)).toBe("logs");
  });
});

describe("parseExploreState", () => {
  it("returns defaults for an empty search string", () => {
    expect(parseExploreState("")).toEqual(DEFAULT_STATE);
  });

  it("parses a fully-populated URL", () => {
    const state = parseExploreState(
      "?range=15m&f=level%7C%3D%7Cerror&q=timeout&limit=100&live=1&trace=abc123&promql=up",
    );
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

  it("ignores invalid limits and filters", () => {
    const state = parseExploreState("?limit=-5&f=not-a-filter");
    expect(state.limit).toBe(500);
    expect(state.filters).toEqual([]);
  });

  it("caps absurd limits", () => {
    expect(parseExploreState("?limit=999999").limit).toBe(5000);
  });

  it("does not read the signal from the query string", () => {
    // The signal comes from the route path now; a leftover ?signal= from an
    // old bookmark is ignored rather than parsed.
    expect(parseExploreState("?signal=traces").signal).toBe("logs");
  });
});

describe("buildSearch", () => {
  it("emits nothing for the default state", () => {
    expect(buildSearch(DEFAULT_STATE)).toBe("");
  });

  it("never emits a signal param", () => {
    expect(buildSearch({ ...DEFAULT_STATE, signal: "metrics" })).not.toContain(
      "signal",
    );
  });

  it("round-trips through parseExploreState", () => {
    const state = {
      ...DEFAULT_STATE,
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
      group: "POST /checkout",
      groupBy: "resource.host.name",
      promql: "rate(x[5m])",
      profileType: "cpu:nanoseconds",
      profileService: "signaldb-router",
      tenant: "acme",
      dataset: "production",
    };
    expect(parseExploreState(buildSearch(state))).toEqual(state);
  });

  it("omits the groupBy param for the default dimension", () => {
    expect(buildSearch(DEFAULT_STATE)).not.toContain("groupBy");
    expect(parseExploreState("").groupBy).toBe("span.name");
  });

  it("round-trips the profiles signal's selectors", () => {
    const state = {
      ...DEFAULT_STATE,
      signal: "profiles" as const,
      profileType: "cpu:nanoseconds",
      profileService: "signaldb-querier",
    };
    const search = buildSearch(state);
    expect(search).toContain("ptype=cpu");
    expect(search).toContain("psvc=signaldb-querier");
    // signal isn't round-tripped through the query string — it comes from
    // the route path, which parseExploreState doesn't have here.
    expect(parseExploreState(search)).toEqual({ ...state, signal: "logs" });
  });

  it("omits tenant params for the ambient default context", () => {
    expect(buildSearch(DEFAULT_STATE)).not.toContain("tenant");
    const state = parseExploreState("?tenant=acme&dataset=prod");
    expect(state.tenant).toBe("acme");
    expect(state.dataset).toBe("prod");
  });
});

describe("chart scale", () => {
  it("defaults to linear when absent", () => {
    expect(parseExploreState("").scale).toBe("linear");
  });

  it("round-trips a selected scale", () => {
    expect(parseExploreState("?scale=log").scale).toBe("log");
    expect(buildSearch({ ...DEFAULT_STATE, scale: "log" })).toContain(
      "scale=log",
    );
  });

  it("omits the default from the serialized search", () => {
    expect(buildSearch({ ...DEFAULT_STATE, scale: "linear" })).not.toContain(
      "scale",
    );
  });

  it("falls back to linear for an unknown scale", () => {
    expect(parseExploreState("?scale=logarithmic").scale).toBe("linear");
  });
});
