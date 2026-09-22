import { describe, expect, it } from "vitest";
import { emptyQuery, nextRef, parseBuilderState } from "./metricQuery";

describe("nextRef", () => {
  it("assigns letters in order and skips used ones", () => {
    expect(nextRef([])).toBe("a");
    expect(nextRef([emptyQuery("a")])).toBe("b");
    expect(nextRef([emptyQuery("a"), emptyQuery("c")])).toBe("b");
  });
});

describe("parseBuilderState", () => {
  it("returns null for empty input", () => {
    expect(parseBuilderState("")).toBeNull();
  });

  it("returns null for malformed JSON", () => {
    expect(parseBuilderState("{not json")).toBeNull();
  });

  it("parses the current {queries, formula} encoding", () => {
    const raw = JSON.stringify({
      queries: [{ ref: "a", metric: "up", filters: [] }],
      formula: "a * 100",
    });
    expect(parseBuilderState(raw)).toEqual({
      queries: [{ ref: "a", metric: "up", filters: [] }],
      formula: "a * 100",
    });
  });

  it("accepts the legacy single-MetricQuery encoding, wrapped as one query with no formula", () => {
    const raw = JSON.stringify({ ref: "a", metric: "up", filters: [] });
    expect(parseBuilderState(raw)).toEqual({
      queries: [{ ref: "a", metric: "up", filters: [] }],
      formula: "",
    });
  });

  it("rejects a queries entry that isn't a well-formed MetricQuery", () => {
    const raw = JSON.stringify({
      queries: [{ ref: "a", metric: "up", filters: [{}] }],
      formula: "",
    });
    expect(parseBuilderState(raw)).toBeNull();
  });

  it("rejects a range that isn't shaped like a RangeFnSpec", () => {
    const raw = JSON.stringify({
      queries: [
        { ref: "a", metric: "up", filters: [], range: { fn: "bogus" } },
      ],
      formula: "",
    });
    expect(parseBuilderState(raw)).toBeNull();
  });

  it("accepts a well-formed range", () => {
    const raw = JSON.stringify({
      queries: [
        { ref: "a", metric: "up", filters: [], range: { fn: "increase" } },
      ],
      formula: "",
    });
    expect(parseBuilderState(raw)).toEqual({
      queries: [
        { ref: "a", metric: "up", filters: [], range: { fn: "increase" } },
      ],
      formula: "",
    });
  });

  it("accepts irate and the *_over_time range functions", () => {
    for (const fn of [
      "irate",
      "avg_over_time",
      "min_over_time",
      "max_over_time",
      "sum_over_time",
      "count_over_time",
    ]) {
      const raw = JSON.stringify({
        queries: [{ ref: "a", metric: "up", filters: [], range: { fn } }],
        formula: "",
      });
      expect(parseBuilderState(raw)).not.toBeNull();
    }
  });

  it("accepts a range with a well-formed across and window", () => {
    const raw = JSON.stringify({
      queries: [
        {
          ref: "a",
          metric: "up",
          filters: [],
          range: { fn: "rate", across: "avg", window: "5m" },
        },
      ],
      formula: "",
    });
    expect(parseBuilderState(raw)).toEqual({
      queries: [
        {
          ref: "a",
          metric: "up",
          filters: [],
          range: { fn: "rate", across: "avg", window: "5m" },
        },
      ],
      formula: "",
    });
  });

  it("rejects a range whose across isn't a valid SpaceAgg", () => {
    const raw = JSON.stringify({
      queries: [
        {
          ref: "a",
          metric: "up",
          filters: [],
          range: { fn: "rate", across: "bogus" },
        },
      ],
      formula: "",
    });
    expect(parseBuilderState(raw)).toBeNull();
  });

  it("rejects a range whose window isn't a string", () => {
    const raw = JSON.stringify({
      queries: [
        {
          ref: "a",
          metric: "up",
          filters: [],
          range: { fn: "rate", window: 300 },
        },
      ],
      formula: "",
    });
    expect(parseBuilderState(raw)).toBeNull();
  });

  it("never throws on adversarial input", () => {
    const inputs = [
      {
        queries: [{ ref: "a", metric: "up", filters: [], agg: {} }],
        formula: "",
      },
      { queries: "not an array", formula: "" },
      { queries: [], formula: 5 },
    ];
    for (const input of inputs) {
      expect(() => parseBuilderState(JSON.stringify(input))).not.toThrow();
    }
  });
});
