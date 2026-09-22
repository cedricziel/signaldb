import { describe, expect, it } from "vitest";
import {
  filterFromParam,
  filterToParam,
  isValidLogLabelName,
  logFilterFromParam,
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

describe("isValidLogLabelName", () => {
  it("accepts dotted segments", () => {
    expect(isValidLogLabelName("k8s.pod.name")).toBe(true);
    expect(isValidLogLabelName("service_name")).toBe(true);
  });

  it("rejects double, leading, and trailing dots", () => {
    expect(isValidLogLabelName("a..b")).toBe(false);
    expect(isValidLogLabelName(".a")).toBe(false);
    expect(isValidLogLabelName("a.")).toBe(false);
  });
});

describe("filter URL params", () => {
  it("round-trips every operator", () => {
    for (const op of ["=", "!=", "=~", "!~"] as const) {
      const filter = f("host", op, "check|out");
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

  it("round-trips a dotted label", () => {
    const filter = f("k8s.pod.name", "=", "x");
    expect(filterFromParam(filterToParam(filter))).toEqual(filter);
  });
});

describe("logFilterFromParam", () => {
  it("canonicalizes the old Loki-spelled labels from a bookmarked logs URL", () => {
    expect(logFilterFromParam("level|=|error")).toEqual(
      f("severity_text", "=", "error"),
    );
    expect(logFilterFromParam("service_name|!=|checkout")).toEqual(
      f("service.name", "!=", "checkout"),
    );
  });

  it("leaves an already-canonical IR field name as-is", () => {
    expect(logFilterFromParam("severity_text|=|error")).toEqual(
      f("severity_text", "=", "error"),
    );
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
