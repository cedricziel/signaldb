import { describe, expect, it } from "vitest";
import type { TraceSummary } from "../api/tempo";
import {
  DEFAULT_GROUP_BY,
  formatRate,
  groupDimensions,
  groupKey,
  groupTraces,
  groupValue,
  NOT_SET,
  parseGroupBy,
} from "./traceGroups";

function trace(
  traceId: string,
  service: string,
  name: string,
  startNs: string,
  durationMs: number,
  rootAttributes: TraceSummary["rootAttributes"] = {},
  rootError = false,
): TraceSummary {
  return {
    traceId,
    rootServiceName: service,
    rootTraceName: name,
    startNs,
    durationMs,
    rootAttributes,
    rootError,
  };
}

describe("parseGroupBy", () => {
  it("defaults to span.name", () => {
    expect(parseGroupBy("")).toEqual(["span.name"]);
    expect(parseGroupBy(DEFAULT_GROUP_BY)).toEqual(["span.name"]);
  });

  it("splits comma-separated dimensions and drops duplicates", () => {
    expect(parseGroupBy("span.name,service.name")).toEqual([
      "span.name",
      "service.name",
    ]);
    expect(parseGroupBy("span.name,span.name,")).toEqual(["span.name"]);
  });
});

describe("groupValue", () => {
  it("maps the built-in dimensions to root name and service", () => {
    const t = trace("a", "gateway", "POST /checkout", "1", 1);
    expect(groupValue(t, "span.name")).toBe("POST /checkout");
    expect(groupValue(t, "service.name")).toBe("gateway");
  });

  it("reads any root span attribute, stringified", () => {
    const t = trace("a", "gateway", "POST /checkout", "1", 1, {
      "resource.deployment.environment": "prod",
      "http.status_code": 500,
    });
    expect(groupValue(t, "resource.deployment.environment")).toBe("prod");
    expect(groupValue(t, "http.status_code")).toBe("500");
  });

  it("buckets missing attributes under a not-set marker", () => {
    const t = trace("a", "gateway", "POST /checkout", "1", 1);
    expect(groupValue(t, "resource.host.name")).toBe(NOT_SET);
  });
});

describe("groupTraces", () => {
  it("groups by root span name by default, largest group first", () => {
    const groups = groupTraces(
      [
        trace("a", "gateway", "POST /checkout", "300", 400),
        trace("b", "auth", "GET /login", "100", 50),
        trace("c", "gateway", "POST /checkout", "200", 100),
      ],
      ["span.name"],
    );
    expect(groups.map((g) => [g.values, g.traces.length])).toEqual([
      [["POST /checkout"], 2],
      [["GET /login"], 1],
    ]);
  });

  it("distinguishes same-named roots when also grouped by service", () => {
    const traces = [
      trace("a", "gateway", "GET /health", "1", 1),
      trace("b", "auth", "GET /health", "2", 1),
    ];
    expect(groupTraces(traces, ["span.name"])).toHaveLength(1);
    const two = groupTraces(traces, ["span.name", "service.name"]);
    expect(two.map((g) => g.values)).toEqual(
      [[["GET /health", "auth"]], [["GET /health", "gateway"]]].flat(),
    );
  });

  it("counts errored traces per group", () => {
    const groups = groupTraces(
      [
        trace("a", "gw", "op", "1", 1, {}, true),
        trace("b", "gw", "op", "2", 1),
        trace("c", "gw", "op", "3", 1),
      ],
      ["span.name"],
    );
    expect(groups[0]?.errorCount).toBe(1);
  });

  it("groups by an arbitrary attribute and lists each group's services", () => {
    const groups = groupTraces(
      [
        trace("a", "gateway", "POST /checkout", "1", 10, { env: "prod" }),
        trace("b", "auth", "GET /login", "2", 10, { env: "prod" }),
        trace("c", "gateway", "POST /checkout", "3", 10, { env: "staging" }),
      ],
      ["env"],
    );
    expect(groups.map((g) => [g.values, g.traces.length])).toEqual([
      [["prod"], 2],
      [["staging"], 1],
    ]);
    expect(groups[0]?.services).toEqual(["auth", "gateway"]);
    expect(groups[1]?.services).toEqual(["gateway"]);
  });

  it("sorts traces within a group newest first and tracks the latest start", () => {
    const groups = groupTraces(
      [
        trace("old", "gateway", "POST /checkout", "9000000000", 100),
        trace("new", "gateway", "POST /checkout", "10000000000", 100),
      ],
      ["span.name"],
    );
    expect(groups[0]?.traces.map((t) => t.traceId)).toEqual(["new", "old"]);
    expect(groups[0]?.lastStartNs).toBe("10000000000");
  });

  it("computes duration percentiles", () => {
    const groups = groupTraces(
      [10, 20, 30, 40, 50, 60, 70, 80, 90, 100].map((d, i) =>
        trace(`t${i}`, "svc", "op", String(i), d),
      ),
      ["span.name"],
    );
    expect(groups[0]?.p50Ms).toBe(50);
    expect(groups[0]?.p95Ms).toBe(100);
  });

  it("uses the single duration for all percentiles of a one-trace group", () => {
    const groups = groupTraces(
      [trace("a", "svc", "op", "1", 42)],
      ["span.name"],
    );
    expect(groups[0]?.p50Ms).toBe(42);
    expect(groups[0]?.p95Ms).toBe(42);
  });

  it("returns no groups for no traces", () => {
    expect(groupTraces([], ["span.name"])).toEqual([]);
  });
});

describe("groupKey", () => {
  it("matches the key of the group a trace lands in", () => {
    const t = trace("a", "gateway", "GET /health", "1", 1);
    const groups = groupTraces([t], ["span.name", "service.name"]);
    expect(groupKey(t, ["span.name", "service.name"])).toBe(groups[0]?.key);
  });
});

describe("groupDimensions", () => {
  it("offers the built-ins plus observed root attributes, sorted", () => {
    const dims = groupDimensions([
      trace("a", "gateway", "POST /checkout", "1", 1, {
        "resource.host.name": "web-1",
        "http.method": "POST",
      }),
      trace("b", "auth", "GET /login", "2", 1, {
        "resource.host.name": "web-2",
      }),
    ]);
    expect(dims).toEqual([
      "span.name",
      "service.name",
      "http.method",
      "resource.host.name",
    ]);
  });

  it("offers only the built-ins when traces carry no attributes", () => {
    expect(groupDimensions([])).toEqual(["span.name", "service.name"]);
  });
});

describe("formatRate", () => {
  it("picks a unit that keeps the number readable", () => {
    expect(formatRate(7200, 3600)).toBe("2/s");
    expect(formatRate(90, 3600)).toBe("1.5/min");
    expect(formatRate(2, 3600)).toBe("2/h");
    expect(formatRate(0, 3600)).toBe("0/h");
  });
});
