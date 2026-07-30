import { describe, expect, it } from "vitest";
import type { LabelFilter } from "../../lib/filters";
import {
  buildFormula,
  buildPromQL,
  buildSelector,
  emptyQuery,
  nextRef,
  type MetricQuery,
} from "./buildPromQL";

const f = (
  label: string,
  op: LabelFilter["op"],
  value: string,
): LabelFilter => ({ label, op, value });

const q = (patch: Partial<MetricQuery>): MetricQuery => ({
  ...emptyQuery("a"),
  metric: "http_server_duration",
  ...patch,
});

describe("buildSelector", () => {
  it("returns a bare metric with no filters", () => {
    expect(buildSelector("http_server_duration", [])).toBe(
      "http_server_duration",
    );
  });

  it("compiles matchers for every operator", () => {
    expect(
      buildSelector("m", [
        f("service", "=", "checkout"),
        f("env", "!=", "dev"),
        f("host", "=~", "web-.+"),
        f("route", "!~", "/health"),
      ]),
    ).toBe(
      'm{service="checkout", env!="dev", host=~"web-.+", route!~"/health"}',
    );
  });

  it("drops filters with invalid label names", () => {
    expect(buildSelector("m", [f("bad.label", "=", "x")])).toBe("m");
  });

  it("escapes quotes and backslashes in values", () => {
    expect(buildSelector("m", [f("path", "=", 'a"b\\c')])).toBe(
      'm{path="a\\"b\\\\c"}',
    );
  });
});

describe("buildPromQL", () => {
  it("returns empty string when no metric is selected", () => {
    expect(buildPromQL(emptyQuery("a"))).toBe("");
  });

  it("emits a raw selector for the simplest query", () => {
    expect(buildPromQL(q({ filters: [f("service", "=", "checkout")] }))).toBe(
      'http_server_duration{service="checkout"}',
    );
  });

  it("wraps a range function around the selector", () => {
    expect(buildPromQL(q({ range: { fn: "rate", window: "5m" } }))).toBe(
      "rate(http_server_duration[5m])",
    );
  });

  it("supports *_over_time rollups", () => {
    expect(
      buildPromQL(
        q({
          filters: [f("service", "=", "checkout")],
          range: { fn: "avg_over_time", window: "1m" },
        }),
      ),
    ).toBe('avg_over_time(http_server_duration{service="checkout"}[1m])');
  });

  it("applies space aggregation with a group-by clause", () => {
    expect(buildPromQL(q({ agg: { op: "avg", by: ["service"] } }))).toBe(
      "avg by (service)(http_server_duration)",
    );
  });

  it("aggregates to a single series when group-by is empty", () => {
    expect(buildPromQL(q({ agg: { op: "sum", by: [] } }))).toBe(
      "sum(http_server_duration)",
    );
  });

  it("composes filter + range function + grouped aggregation (inner→outer)", () => {
    expect(
      buildPromQL(
        q({
          filters: [f("env", "=", "prod")],
          range: { fn: "rate", window: "1m" },
          agg: { op: "sum", by: ["service", "http_status"] },
        }),
      ),
    ).toBe(
      'sum by (service, http_status)(rate(http_server_duration{env="prod"}[1m]))',
    );
  });

  it("ignores invalid group-by labels", () => {
    expect(
      buildPromQL(q({ agg: { op: "max", by: ["service", "bad.tag"] } })),
    ).toBe("max by (service)(http_server_duration)");
  });
});

describe("nextRef", () => {
  it("assigns letters in order and skips used ones", () => {
    expect(nextRef([])).toBe("a");
    expect(nextRef([emptyQuery("a")])).toBe("b");
    expect(nextRef([emptyQuery("a"), emptyQuery("c")])).toBe("b");
  });
});

describe("buildFormula", () => {
  const qa = q({ ref: "a", metric: "http_server_duration" });
  const qb = q({ ref: "b", metric: "http_server_errors" });

  it("charts only the first query when there is no formula", () => {
    expect(buildFormula([qa, qb], "")).toBe("http_server_duration");
  });

  it("returns empty when there are no queries", () => {
    expect(buildFormula([], "")).toBe("");
  });

  it("substitutes each ref with its parenthesized compilation", () => {
    expect(buildFormula([qa, qb], "(a / b) * 100")).toBe(
      "((http_server_duration) / (http_server_errors)) * 100",
    );
  });

  it("leaves PromQL function names untouched (multi-char, not refs)", () => {
    const ra = q({
      ref: "a",
      metric: "http_server_duration",
      range: { fn: "rate", window: "1m" },
    });
    expect(buildFormula([ra], "a")).toBe("(rate(http_server_duration[1m]))");
  });

  it("is not runnable while a referenced query is still empty", () => {
    expect(buildFormula([qa, emptyQuery("b")], "a / b")).toBe("");
  });
});
