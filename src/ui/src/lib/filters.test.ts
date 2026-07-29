import { describe, expect, it } from "vitest";
import {
  compileHistogramQL,
  compileLogQL,
  compileSelector,
  filterFromParam,
  filterToParam,
  MATCH_ALL_SELECTOR,
  upsertFilter,
  type LabelFilter,
} from "./filters";

const f = (
  label: string,
  op: LabelFilter["op"],
  value: string,
): LabelFilter => ({
  label,
  op,
  value,
});

describe("compileSelector", () => {
  it("compiles a match-all selector for no filters", () => {
    expect(compileSelector([])).toBe(MATCH_ALL_SELECTOR);
  });

  it("compiles multiple matchers", () => {
    expect(
      compileSelector([
        f("service_name", "=", "checkout"),
        f("level", "!=", "debug"),
      ]),
    ).toBe('{service_name="checkout", level!="debug"}');
  });

  it("drops filters with invalid label names instead of emitting broken LogQL", () => {
    expect(compileSelector([f("bad-label!", "=", "x")])).toBe(
      MATCH_ALL_SELECTOR,
    );
  });

  it("escapes quotes and backslashes in values", () => {
    expect(compileSelector([f("path", "=", 'a"b\\c')])).toBe(
      '{path="a\\"b\\\\c"}',
    );
  });
});

describe("compileLogQL", () => {
  it("appends a line filter for search text", () => {
    expect(
      compileLogQL({ filters: [f("level", "=", "error")], search: "timeout" }),
    ).toBe('{level="error"} |= "timeout"');
  });

  it("prefers the raw override verbatim", () => {
    expect(
      compileLogQL({
        filters: [f("level", "=", "error")],
        search: "x",
        raw: '{service_name="api"} | json',
      }),
    ).toBe('{service_name="api"} | json');
  });

  it("ignores a whitespace-only raw override", () => {
    expect(compileLogQL({ filters: [], search: "", raw: "  " })).toBe(
      MATCH_ALL_SELECTOR,
    );
  });
});

describe("compileHistogramQL", () => {
  it("wraps the log query in count_over_time grouped by level", () => {
    expect(compileHistogramQL({ filters: [], search: "" }, "2m")).toBe(
      `sum by (level) (count_over_time(${MATCH_ALL_SELECTOR} [2m]))`,
    );
  });

  it("returns null for raw queries (may already be a metric query)", () => {
    expect(
      compileHistogramQL(
        { filters: [], search: "", raw: "count_over_time(...)" },
        "2m",
      ),
    ).toBeNull();
  });
});

describe("filter URL params", () => {
  it("round-trips every operator", () => {
    for (const op of ["=", "!=", "=~", "!~"] as const) {
      const filter = f("service_name", op, "check|out");
      expect(filterFromParam(filterToParam(filter))).toEqual(filter);
    }
  });

  it("rejects malformed params", () => {
    expect(filterFromParam("no-separators")).toBeNull();
    expect(filterFromParam("label|~|value")).toBeNull();
    expect(filterFromParam("bad name|=|x")).toBeNull();
  });

  it("preserves empty values", () => {
    expect(filterFromParam("l|=|")).toEqual(f("l", "=", ""));
  });
});

describe("upsertFilter", () => {
  it("replaces an existing filter with the same label and op", () => {
    const out = upsertFilter(
      [f("level", "=", "info")],
      f("level", "=", "error"),
    );
    expect(out).toEqual([f("level", "=", "error")]);
  });

  it("appends when label or op differ", () => {
    const out = upsertFilter(
      [f("level", "=", "info")],
      f("level", "!=", "debug"),
    );
    expect(out).toHaveLength(2);
  });
});
