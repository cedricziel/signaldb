import { describe, expect, it } from "vitest";
import type { LogRow } from "../../api/ir/logs";
import { logScopes } from "./logScopes";

const row = (over: Partial<LogRow> = {}): LogRow => ({
  tsNs: "1000000000",
  tsMs: 1000,
  body: "hello",
  serviceName: "checkout",
  severityText: "info",
  traceId: null,
  spanId: null,
  scopeName: "",
  logAttributes: {},
  scopeAttributes: {},
  resourceAttributes: {},
  ...over,
});

describe("logScopes", () => {
  it("puts trace/span id and log attributes under This line", () => {
    const groups = logScopes(
      row({
        traceId: "t-1",
        spanId: "s-1",
        logAttributes: { "code.function.name": "handle" },
      }),
    );
    expect(groups.find((g) => g.title === "This line")?.entries).toEqual([
      ["code.function.name", "handle"],
      ["span_id", "s-1"],
      ["trace_id", "t-1"],
    ]);
  });

  it("keeps resource attributes separate even when the key also appears on the line", () => {
    const groups = logScopes(
      row({
        serviceName: "",
        logAttributes: { "service.name": "line-value" },
        resourceAttributes: { "service.name": "resource-value" },
      }),
    );
    expect(groups.find((g) => g.title === "This line")?.entries).toEqual([
      ["service.name", "line-value"],
    ]);
    expect(groups.find((g) => g.title === "Resource")?.entries).toEqual([
      ["service.name", "resource-value"],
    ]);
  });

  it("adds the promoted service.name field to Resource", () => {
    const groups = logScopes(row({ serviceName: "checkout" }));
    expect(groups.find((g) => g.title === "Resource")?.entries).toEqual([
      ["service.name", "checkout"],
    ]);
  });

  it("omits the Scope group when the row carries no scope attributes", () => {
    const groups = logScopes(row());
    expect(groups.map((g) => g.title)).toEqual(["This line", "Resource"]);
  });

  it("includes Scope when the row carries scope attributes", () => {
    const groups = logScopes(
      row({ scopeAttributes: { "otel.scope.name": "my-lib" } }),
    );
    expect(groups.map((g) => g.title)).toEqual([
      "This line",
      "Scope",
      "Resource",
    ]);
  });

  it("always renders This line, even with no attributes", () => {
    const groups = logScopes(row());
    expect(groups.find((g) => g.title === "This line")?.entries).toEqual([]);
  });
});
