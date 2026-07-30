import { describe, expect, it } from "vitest";
import type { LabelFilter } from "../../lib/filters";
import {
  buildPromQL,
  buildSelector,
  emptyQuery,
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
