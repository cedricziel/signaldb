import { describe, expect, it } from "vitest";
import type { TraceSummary } from "../api/tempo";
import {
  DEFAULT_GROUP_BY,
  formatRate,
  groupKey,
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

describe("groupKey", () => {
  it("joins per-dimension values with the unit separator", () => {
    const t = trace("a", "gateway", "GET /health", "1", 1);
    expect(groupKey(t, ["span.name", "service.name"])).toBe(
      "GET /healthgateway",
    );
  });

  it("distinguishes same-named roots once a second dimension is added", () => {
    const a = trace("a", "gateway", "GET /health", "1", 1);
    const b = trace("b", "auth", "GET /health", "2", 1);
    expect(groupKey(a, ["span.name"])).toBe(groupKey(b, ["span.name"]));
    expect(groupKey(a, ["span.name", "service.name"])).not.toBe(
      groupKey(b, ["span.name", "service.name"]),
    );
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
